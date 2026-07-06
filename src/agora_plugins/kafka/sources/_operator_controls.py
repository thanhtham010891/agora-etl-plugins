"""Operator-control state and seek/pause helpers for Kafka sources."""

from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

from agora_plugins.kafka.sources._rebalance import (
    normalize_assignment_items,
    normalize_topic_partitions,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class KafkaOperatorController:
    """Owns assignment-aware operator controls for a Kafka consumer source."""

    def __init__(
        self,
        *,
        assignments: Iterable[tuple[str, int]],
        on_change: Callable[[], None],
    ) -> None:
        self._on_change = on_change
        self._active_assignment = set(normalize_topic_partitions(assignments))
        self._paused_partitions: set[tuple[str, int]] = set()
        self._pause_all_requested = False

    @property
    def active_assignment(self) -> set[tuple[str, int]]:
        return self._active_assignment

    @active_assignment.setter
    def active_assignment(self, assignments: Iterable[tuple[str, int]]) -> None:
        self._active_assignment = set(normalize_topic_partitions(assignments))
        self._on_change()

    @property
    def paused_partitions(self) -> set[tuple[str, int]]:
        return self._paused_partitions

    @paused_partitions.setter
    def paused_partitions(self, partitions: Iterable[tuple[str, int]]) -> None:
        self._paused_partitions = set(normalize_topic_partitions(partitions))
        self._on_change()

    @property
    def pause_all_requested(self) -> bool:
        return self._pause_all_requested

    @pause_all_requested.setter
    def pause_all_requested(self, value: bool) -> None:
        self._pause_all_requested = bool(value)
        self._on_change()

    def sync_active_assignment(self, assignments: object | None = None) -> None:
        if assignments is None:
            assignments = self._active_assignment
        normalized: set[tuple[str, int]] = set()
        for item in cast("Iterable[Any]", assignments or ()):
            topic = getattr(item, "topic", None)
            partition = getattr(item, "partition", None)
            if topic is None and isinstance(item, tuple) and len(item) >= 2:
                topic = item[0]
                partition = item[1]
            if topic is None or partition is None:
                continue
            normalized.add((str(topic), int(partition)))
        if normalized != self._active_assignment:
            self._active_assignment = normalized
            self._on_change()

    def drop_revoked(self, revoked: set[tuple[str, int]]) -> None:
        if not revoked:
            return
        for key in revoked:
            self._paused_partitions.discard(key)
        self._active_assignment.difference_update(revoked)
        self._on_change()

    def paused_partition_count(self) -> int:
        if self._pause_all_requested:
            return len(self._active_assignment)
        return len(self._paused_partitions)

    def resolve_seek_targets(
        self,
        partitions: Iterable[tuple[str, int]] | None,
    ) -> set[tuple[str, int]]:
        if partitions is None:
            if not self._active_assignment:
                raise RuntimeError(
                    "KafkaSource has no active assignment yet, so operator seek controls "
                    "cannot resolve target partitions."
                )
            return set(self._active_assignment)

        targets = set(normalize_topic_partitions(partitions))
        self.validate_assigned_partitions(targets)
        return targets

    def validate_assigned_partitions(self, targets: set[tuple[str, int]]) -> None:
        unassigned = sorted(targets.difference(self._active_assignment))
        if unassigned:
            raise ValueError(
                "KafkaSource operator controls can only target assigned partitions. "
                f"Unassigned targets: {unassigned!r}"
            )

    def pause(
        self,
        consumer: Any | None,
        build_topic_partition: Callable[[str, int], object],
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        if partitions is None:
            self._pause_all_requested = True
            targets = set(self._active_assignment)
        else:
            targets = set(normalize_topic_partitions(partitions))
            self.validate_assigned_partitions(targets)
            self._paused_partitions.update(targets)
        if partitions is None and targets:
            self._paused_partitions.update(targets)
        self.apply_pause_state(
            consumer,
            build_topic_partition,
            targets if targets else None,
        )
        self._on_change()

    def resume(
        self,
        consumer: Any | None,
        build_topic_partition: Callable[[str, int], object],
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        if partitions is None:
            self._pause_all_requested = False
            targets = set(self._paused_partitions or self._active_assignment)
            self._paused_partitions.clear()
        else:
            targets = set(normalize_topic_partitions(partitions))
            self.validate_assigned_partitions(targets)
            if self._pause_all_requested:
                self._pause_all_requested = False
                self._paused_partitions = set(self._active_assignment).difference(targets)
            self._paused_partitions.difference_update(targets)
        self.apply_resume_state(
            consumer,
            build_topic_partition,
            targets if targets else None,
        )
        self._on_change()

    def apply_pause_state(
        self,
        consumer: Any | None,
        build_topic_partition: Callable[[str, int], object],
        partitions: object | None = None,
    ) -> None:
        if consumer is None:
            return
        pause = getattr(consumer, "pause", None)
        if not callable(pause):
            return
        targets = (
            set(self._active_assignment)
            if self._pause_all_requested
            else set(self._paused_partitions)
        )
        if partitions is not None:
            requested = {
                (str(topic), int(partition))
                for topic, partition in normalize_assignment_items(partitions)
            }
            if requested:
                targets = targets.intersection(requested)
        if not targets:
            return
        pause(*[build_topic_partition(topic, partition) for topic, partition in sorted(targets)])

    def apply_resume_state(
        self,
        consumer: Any | None,
        build_topic_partition: Callable[[str, int], object],
        partitions: object | None = None,
    ) -> None:
        if consumer is None:
            return
        resume = getattr(consumer, "resume", None)
        if not callable(resume):
            return
        targets = (
            {
                (str(topic), int(partition))
                for topic, partition in normalize_assignment_items(partitions)
            }
            if partitions is not None
            else set(self._active_assignment)
        )
        if not targets:
            return
        resume(*[build_topic_partition(topic, partition) for topic, partition in sorted(targets)])

    async def seek_with_consumer_method(
        self,
        *,
        consumer: Any,
        build_topic_partition: Callable[[str, int], object],
        method_name: str,
        partitions: Iterable[tuple[str, int]] | None,
    ) -> None:
        targets = self.resolve_seek_targets(partitions)
        method = getattr(consumer, method_name, None)
        if not callable(method):
            raise TypeError(f"Kafka consumer does not support {method_name}().")
        result = method(
            *[build_topic_partition(topic, partition) for topic, partition in sorted(targets)]
        )
        if isawaitable(result):
            await result
