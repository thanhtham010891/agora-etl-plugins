"""Consumer bootstrap, assignment refresh, and probe helpers for Kafka sources."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

import logstruct

from agora_plugins.kafka.sources._rebalance import (
    build_rebalance_listener,
    normalize_assignment_items,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from agora_plugins.kafka.sources._cursor_state import KafkaCursorState
    from agora_plugins.kafka.sources._operator_controls import KafkaOperatorController

logger = logstruct.getLogger("agora_plugins.kafka.sources.kafka")


@dataclass(frozen=True, slots=True)
class KafkaPartitionProbeSnapshot:
    positions: dict[tuple[str, int], int]
    committed_offsets: dict[tuple[str, int], int]
    end_offsets: dict[tuple[str, int], int]


class KafkaConsumerRuntime:
    """Owns consumer bootstrap and broker-side partition probes for a source."""

    def __init__(
        self,
        *,
        topics: list[str],
        topic_pattern: str | None,
        assignments: list[tuple[str, int]],
        group_id: str,
        rebalance_listener: object | None,
        operator_controls: KafkaOperatorController,
        cursor_state: KafkaCursorState,
        commit_if_needed: Callable[..., Awaitable[None]],
        on_rebalance: Callable[[], None],
        build_topic_partition: Callable[[str, int], object],
        on_change: Callable[[], None],
    ) -> None:
        self._topics = list(topics)
        self._topic_pattern = topic_pattern
        self._assignments = list(assignments)
        self._group_id = group_id
        self._rebalance_listener = rebalance_listener
        self._operator_controls = operator_controls
        self._cursor_state = cursor_state
        self._commit_if_needed = commit_if_needed
        self._on_rebalance = on_rebalance
        self._build_topic_partition = build_topic_partition
        self._on_change = on_change
        self._wrapped_rebalance_listener: object | None = None

    def subscribe_consumer(self, source: Any, consumer: Any) -> None:
        subscribe = getattr(consumer, "subscribe", None)
        if self._assignments:
            return
        if not callable(subscribe):
            return
        self._wrapped_rebalance_listener = build_rebalance_listener(
            source,
            self._rebalance_listener,
        )
        if self._topic_pattern is not None:
            subscribe(pattern=self._topic_pattern, listener=self._wrapped_rebalance_listener)
            return
        subscribe(topics=self._topics, listener=self._wrapped_rebalance_listener)

    def bind_consumer(self, consumer: Any) -> None:
        if not self._assignments:
            return
        assign = getattr(consumer, "assign", None)
        if not callable(assign):
            return
        topic_partitions = [
            self._build_topic_partition(topic, partition) for topic, partition in self._assignments
        ]
        assign(topic_partitions)
        self._operator_controls.active_assignment = set(self._assignments)

    async def refresh_assignment_state(
        self,
        consumer: Any | None,
        *,
        bootstrap: bool = False,
    ) -> None:
        if consumer is None:
            return
        assignment = getattr(consumer, "assignment", None)
        if not callable(assignment):
            return
        assigned = assignment()
        if not assigned and bootstrap:
            getmany = getattr(consumer, "getmany", None)
            if callable(getmany):
                with contextlib.suppress(Exception):
                    await getmany(timeout_ms=0, max_records=1)
                assigned = assignment()
        self._operator_controls.sync_active_assignment(assigned)

    def sync_assignment_from_consumer(self, consumer: Any | None) -> None:
        if consumer is None:
            return
        assignment = getattr(consumer, "assignment", None)
        if callable(assignment):
            self._operator_controls.sync_active_assignment(assignment())

    async def bootstrap_consumer_state(self, consumer: Any | None) -> None:
        await self.refresh_assignment_state(consumer, bootstrap=True)
        await self.apply_initial_positioning(consumer)
        self._operator_controls.apply_pause_state(
            consumer,
            self._build_topic_partition,
        )

    async def handle_partitions_assigned(
        self,
        consumer: Any | None,
        partitions: object,
    ) -> None:
        self._on_rebalance()
        self._operator_controls.sync_active_assignment(partitions)
        self._operator_controls.apply_pause_state(
            consumer,
            self._build_topic_partition,
            partitions,
        )

    async def handle_partitions_revoked(self, partitions: object) -> None:
        await self._commit_if_needed(force=True)
        revoked = {
            (str(topic), int(partition))
            for topic, partition in normalize_assignment_items(partitions)
        }
        if not revoked:
            return
        self._cursor_state.drop_revoked(revoked)
        self._operator_controls.drop_revoked(revoked)
        self._on_change()

    async def apply_initial_positioning(self, consumer: Any | None) -> None:
        if consumer is None or self._cursor_state.positioning_applied:
            return

        target_offsets = self._cursor_state.positioning_targets()
        if not target_offsets:
            self._cursor_state.mark_positioning_applied()
            return

        assignment = getattr(consumer, "assignment", None)
        assigned = assignment() if callable(assignment) else None
        if callable(assignment) and not self._assignment_contains_all(assigned, target_offsets):
            getmany = getattr(consumer, "getmany", None)
            if callable(getmany):
                with contextlib.suppress(Exception):
                    await getmany(timeout_ms=0, max_records=1)
            assigned = assignment() if callable(assignment) else None
            self._operator_controls.sync_active_assignment(assigned)
        seek = getattr(consumer, "seek", None)
        if not callable(seek):
            logger.warning("kafka_resume_seek_unsupported", group_id=self._group_id)
            self._cursor_state.mark_positioning_applied()
            return

        resume_count = 0
        is_exact_positioning = self._cursor_state.has_exact_start_offsets()
        for (topic, partition), offset in target_offsets.items():
            if (
                callable(assignment)
                and assigned
                and not self._assignment_contains(assigned, topic, partition)
            ):
                logger.warning(
                    "kafka_resume_partition_unassigned",
                    topic=topic,
                    partition=partition,
                    group_id=self._group_id,
                )
                continue

            seek(
                self._build_topic_partition(topic, partition),
                offset if is_exact_positioning else offset + 1,
            )
            resume_count += 1
        self._cursor_state.mark_positioning_applied()
        self._on_change()
        logger.info(
            "kafka_resume_applied",
            partitions=resume_count,
            group_id=self._group_id,
            exact_offsets=is_exact_positioning,
        )

    async def probe_partitions(self, consumer: Any | None) -> KafkaPartitionProbeSnapshot:
        return KafkaPartitionProbeSnapshot(
            positions=await self._partition_positions(consumer),
            committed_offsets=await self._partition_committed_offsets(consumer),
            end_offsets=await self._partition_end_offsets(consumer),
        )

    async def _partition_positions(self, consumer: Any | None) -> dict[tuple[str, int], int]:
        if consumer is None:
            return {}
        position = getattr(consumer, "position", None)
        if not callable(position):
            return {}
        assignments = sorted(self._operator_controls.active_assignment)
        values = await asyncio.gather(
            *[
                self._resolve_partition_value(
                    position,
                    self._build_topic_partition(topic, partition),
                )
                for topic, partition in assignments
            ]
        )
        return {
            assignment: int(value)
            for assignment, value in zip(assignments, values, strict=False)
            if value is not None
        }

    async def _partition_end_offsets(self, consumer: Any | None) -> dict[tuple[str, int], int]:
        if consumer is None:
            return {}
        end_offsets = getattr(consumer, "end_offsets", None)
        if not callable(end_offsets) or not self._operator_controls.active_assignment:
            return {}
        topic_partitions = [
            self._build_topic_partition(topic, partition)
            for topic, partition in sorted(self._operator_controls.active_assignment)
        ]
        values = end_offsets(topic_partitions)
        if isawaitable(values):
            values = await values
        normalized: dict[tuple[str, int], int] = {}
        for key, value in cast("dict[Any, Any]", values or {}).items():
            topic = getattr(key, "topic", None)
            partition = getattr(key, "partition", None)
            if topic is None and isinstance(key, tuple) and len(key) >= 2:
                topic = key[0]
                partition = key[1]
            if topic is None or partition is None or value is None:
                continue
            normalized[(str(topic), int(partition))] = int(value)
        return normalized

    async def _partition_committed_offsets(
        self,
        consumer: Any | None,
    ) -> dict[tuple[str, int], int]:
        if consumer is None:
            return {}
        committed = getattr(consumer, "committed", None)
        if not callable(committed):
            return {}
        assignments = sorted(self._operator_controls.active_assignment)
        values = await asyncio.gather(
            *[
                self._resolve_partition_value(
                    committed,
                    self._build_topic_partition(topic, partition),
                )
                for topic, partition in assignments
            ]
        )
        return {
            assignment: int(value)
            for assignment, value in zip(assignments, values, strict=False)
            if value is not None
        }

    async def _resolve_partition_value(
        self,
        resolver: Any,
        partition: object,
    ) -> int | None:
        value = resolver(partition)
        if isawaitable(value):
            value = await value
        if value is None:
            return None
        return int(value)

    @staticmethod
    def _assignment_contains(
        assignments: object,
        topic: str,
        partition: int,
    ) -> bool:
        if not assignments:
            return False
        for item in cast("Iterable[Any]", assignments):
            item_topic = getattr(item, "topic", None)
            item_partition = getattr(item, "partition", None)
            if item_topic is None and isinstance(item, tuple) and len(item) >= 2:
                item_topic = item[0]
                item_partition = item[1]
            if item_partition is None:
                continue
            if item_topic == topic and int(item_partition) == partition:
                return True
        return False

    @classmethod
    def _assignment_contains_all(
        cls,
        assignments: object,
        offsets: dict[tuple[str, int], int],
    ) -> bool:
        if not offsets:
            return True
        return all(
            cls._assignment_contains(assignments, topic, partition) for topic, partition in offsets
        )
