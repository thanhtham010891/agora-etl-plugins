"""Facade and compatibility methods for KafkaSource."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora_plugins.kafka.sources._source_config import security_kwargs

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable

    from agora.core.dlq import DLQSink
    from agora.core.source import SourceRuntimeMetrics

    from agora_plugins.kafka.sources._models import (
        KafkaDeliveryContext,
        KafkaPoisonRecordPolicy,
        KafkaSourceHealthSnapshot,
        KafkaSourceOperationalMetrics,
    )


class KafkaSourceFacade:
    """Public API wrappers plus legacy private compatibility accessors."""

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return self._public_api.runtime_metrics()

    def operational_metrics(self) -> KafkaSourceOperationalMetrics:
        return self._public_api.operational_metrics()

    async def commit_now(self) -> None:
        """Flush tracked offsets immediately for operator-driven handoff/control."""

        await self._public_api.commit_now()

    async def seek_to_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        """Reposition assigned partitions to exact Kafka offsets."""

        await self._public_api.seek_to_offsets(offsets)

    async def seek_to_beginning(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        """Rewind assigned partitions to the earliest available offsets."""

        await self._public_api.seek_to_beginning(partitions)

    async def seek_to_end(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        """Advance assigned partitions to the latest available offsets."""

        await self._public_api.seek_to_end(partitions)

    async def stream(self) -> AsyncGenerator[Any, None]:
        stream = self._public_api.stream()
        try:
            async for record in stream:
                yield record
        finally:
            await stream.aclose()

    async def prepare_resume(self, checkpoint: Any) -> None:
        await self._public_api.prepare_resume(checkpoint)

    async def _bootstrap_consumer_state(self) -> None:
        await self._consumer_runtime.bootstrap_consumer_state(
            self._consumer,
        )

    def current_checkpoint(self) -> dict[str, Any] | None:
        return self._public_api.current_checkpoint()

    def _build_topic_partition(self, topic: str, partition: int) -> object:
        if self._topic_partition_cls is not None:
            return self._topic_partition_cls(topic, partition)
        return (topic, partition)

    async def health_snapshot(
        self,
        *,
        force_refresh: bool = False,
    ) -> KafkaSourceHealthSnapshot:
        return await self._public_api.health_snapshot(force_refresh=force_refresh)

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._public_api.delivery_success_callback()

    def delivery_transaction_offsets_callback(
        self,
    ) -> Callable[[], Awaitable[tuple[dict[Any, int], str]]] | None:
        return self._public_api.delivery_transaction_offsets_callback()

    def delivery_transaction_committed_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._public_api.delivery_transaction_committed_callback()

    def delivery_context(self) -> KafkaDeliveryContext | None:
        return self._public_api.delivery_context()

    @property
    def _active_assignment(self) -> set[tuple[str, int]]:
        return self._operator_controls.active_assignment

    @_active_assignment.setter
    def _active_assignment(self, assignments: Iterable[tuple[str, int]]) -> None:
        self._operator_controls.active_assignment = assignments

    @property
    def _paused_partitions(self) -> set[tuple[str, int]]:
        return self._operator_controls.paused_partitions

    @_paused_partitions.setter
    def _paused_partitions(self, partitions: Iterable[tuple[str, int]]) -> None:
        self._operator_controls.paused_partitions = partitions

    @property
    def _pause_all_requested(self) -> bool:
        return self._operator_controls.pause_all_requested

    @_pause_all_requested.setter
    def _pause_all_requested(self, value: bool) -> None:
        self._operator_controls.pause_all_requested = value

    @property
    def _poison_record_policy(self) -> KafkaPoisonRecordPolicy:
        return self._poison_controller._poison_record_policy

    @property
    def _poison_record_pipeline_id(self) -> str:
        return self._poison_controller._poison_record_pipeline_id

    @property
    def _poison_record_max_attempts(self) -> int | None:
        return self._poison_controller._poison_record_max_attempts

    @property
    def _poison_record_sink(self) -> DLQSink | None:
        return self._poison_controller._poison_record_sink

    @_poison_record_sink.setter
    def _poison_record_sink(self, sink: DLQSink | None) -> None:
        self._poison_controller._poison_record_sink = sink

    @property
    def _idle_poll_count(self) -> int:
        return self._runtime_state.idle_poll_count

    @_idle_poll_count.setter
    def _idle_poll_count(self, value: int) -> None:
        self._runtime_state.idle_poll_count = value

    @property
    def _rebalance_count(self) -> int:
        return self._runtime_state.rebalance_count

    @_rebalance_count.setter
    def _rebalance_count(self, value: int) -> None:
        self._runtime_state.rebalance_count = value

    def _require_open_consumer(self) -> Any:
        if self._consumer is None:
            raise RuntimeError(
                "KafkaSource operator controls require an open consumer. "
                "Call open() or use the source inside a running pipeline first."
            )
        return self._consumer

    def _set_consumer(self, consumer: Any | None) -> None:
        self._consumer = consumer

    def _set_topic_partition_cls(self, cls: Any | None) -> None:
        self._topic_partition_cls = cls

    def _handle_consumer_closed(self) -> None:
        self._operator_controls.active_assignment = set()
        self._delivery_controller.clear_active_delivery()
        self._invalidate_health_snapshot_cache()

    def _security_kwargs(self) -> dict[str, Any]:
        return security_kwargs(
            security_protocol=self._security_protocol,
            security=self._security,
        )

    async def _handle_partitions_assigned(self, partitions: object) -> None:
        await self._consumer_runtime.handle_partitions_assigned(
            self._consumer,
            partitions,
        )

    async def _handle_partitions_revoked(self, partitions: object) -> None:
        await self._consumer_runtime.handle_partitions_revoked(partitions)

    def pause(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self._public_api.pause(partitions)

    def resume(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self._public_api.resume(partitions)

    def _apply_pause_state(self, partitions: object | None = None) -> None:
        self._public_api.apply_pause_state(partitions)

    def _subscription_mode(self) -> str:
        if self._assignments:
            return "manual_assign"
        if self._topic_pattern is not None:
            return "topic_pattern"
        return "topics"

    def _invalidate_health_snapshot_cache(self) -> None:
        health_monitor = getattr(self, "_health_monitor", None)
        if health_monitor is not None:
            health_monitor.invalidate()
