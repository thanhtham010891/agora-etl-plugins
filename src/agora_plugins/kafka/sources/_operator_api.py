"""Public-facing operator controls for Kafka sources."""

from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Any

from agora_plugins.kafka.sources._offsets import normalize_offset_map

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from agora_plugins.kafka.sources._consumer_runtime import KafkaConsumerRuntime
    from agora_plugins.kafka.sources._cursor_state import KafkaCursorState
    from agora_plugins.kafka.sources._operator_controls import KafkaOperatorController
    from agora_plugins.kafka.tracing import KafkaOpenTelemetryTracing


class KafkaOperatorAPI:
    """Owns operator-driven consumer controls exposed by ``KafkaSource``."""

    def __init__(
        self,
        *,
        group_id: str,
        tracing: KafkaOpenTelemetryTracing,
        cursor_state: KafkaCursorState,
        operator_controls: KafkaOperatorController,
        consumer_runtime: KafkaConsumerRuntime,
        build_topic_partition: Callable[[str, int], object],
        current_consumer: Callable[[], Any | None],
        require_open_consumer: Callable[[], Any],
        on_commit_recorded: Callable[[], None],
    ) -> None:
        self._group_id = group_id
        self._tracing = tracing
        self._cursor_state = cursor_state
        self._operator_controls = operator_controls
        self._consumer_runtime = consumer_runtime
        self._build_topic_partition = build_topic_partition
        self._current_consumer = current_consumer
        self._require_open_consumer = require_open_consumer
        self._on_commit_recorded = on_commit_recorded

    async def commit_now(self) -> None:
        consumer = self._require_open_consumer()
        offsets = self._cursor_state.build_commit_offsets(self._build_topic_partition)
        with self._tracing.start_span(
            "kafka.commit",
            kind="client",
            attributes={
                "messaging.system": "kafka",
                "messaging.kafka.consumer.group": self._group_id,
                "messaging.kafka.commit.partition_count": len(offsets),
            },
        ):
            if offsets:
                await consumer.commit(offsets=offsets)
            else:
                await consumer.commit()
        self._cursor_state.clear_pending_commit_count()
        self._on_commit_recorded()

    async def seek_to_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        normalized = normalize_offset_map(offsets)
        if not normalized:
            return

        consumer = self._require_open_consumer()
        await self._consumer_runtime.refresh_assignment_state(
            self._current_consumer(),
            bootstrap=True,
        )
        self._operator_controls.validate_assigned_partitions(set(normalized))

        seek = getattr(consumer, "seek", None)
        if not callable(seek):
            raise TypeError("Kafka consumer does not support seek().")

        for (topic, partition), offset in sorted(normalized.items()):
            result = seek(self._build_topic_partition(topic, partition), offset)
            if isawaitable(result):
                await result

        self._cursor_state.apply_exact_positioning(normalized)

    async def seek_with_consumer_method(
        self,
        method_name: str,
        partitions: Iterable[tuple[str, int]] | None,
    ) -> None:
        consumer = self._require_open_consumer()
        await self._consumer_runtime.refresh_assignment_state(
            self._current_consumer(),
            bootstrap=True,
        )
        await self._operator_controls.seek_with_consumer_method(
            consumer=consumer,
            build_topic_partition=self._build_topic_partition,
            method_name=method_name,
            partitions=partitions,
        )
        self._cursor_state.clear_positioning_targets()

    def pause(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        consumer = self._current_consumer()
        self._consumer_runtime.sync_assignment_from_consumer(consumer)
        self._operator_controls.pause(
            consumer,
            self._build_topic_partition,
            partitions,
        )

    def resume(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        consumer = self._current_consumer()
        self._consumer_runtime.sync_assignment_from_consumer(consumer)
        self._operator_controls.resume(
            consumer,
            self._build_topic_partition,
            partitions,
        )

    def apply_pause_state(self, partitions: object | None = None) -> None:
        self._operator_controls.apply_pause_state(
            self._current_consumer(),
            self._build_topic_partition,
            partitions,
        )
