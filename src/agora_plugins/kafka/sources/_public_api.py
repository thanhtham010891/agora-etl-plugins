"""Public-facing runtime and operator facade for Kafka sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable

    from agora.core.source import SourceRuntimeMetrics

    from agora_plugins.kafka.sources._health import KafkaHealthMonitor
    from agora_plugins.kafka.sources._models import (
        KafkaDeliveryContext,
        KafkaSourceHealthSnapshot,
        KafkaSourceOperationalMetrics,
    )
    from agora_plugins.kafka.sources._operator_api import KafkaOperatorAPI
    from agora_plugins.kafka.sources._runtime_state import KafkaRuntimeState
    from agora_plugins.kafka.sources._source_surface import KafkaSourceSurface
    from agora_plugins.kafka.sources._stream_runtime import KafkaStreamRuntime

T = TypeVar("T")


class KafkaSourcePublicAPI(Generic[T]):
    """Owns the public runtime/operator surface exposed by ``KafkaSource``."""

    def __init__(
        self,
        *,
        source_surface: KafkaSourceSurface,
        operator_api: KafkaOperatorAPI,
        health_monitor: KafkaHealthMonitor,
        stream_runtime: KafkaStreamRuntime[T],
        current_consumer: Callable[[], Any | None],
        runtime_state: KafkaRuntimeState,
        max_idle_polls: int | None,
    ) -> None:
        self._source_surface = source_surface
        self._operator_api = operator_api
        self._health_monitor = health_monitor
        self._stream_runtime = stream_runtime
        self._current_consumer = current_consumer
        self._runtime_state = runtime_state
        self._max_idle_polls = max_idle_polls

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return self._source_surface.runtime_metrics()

    def operational_metrics(self) -> KafkaSourceOperationalMetrics:
        return self._source_surface.operational_metrics()

    async def commit_now(self) -> None:
        await self._operator_api.commit_now()

    async def seek_to_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        await self._operator_api.seek_to_offsets(offsets)

    async def seek_to_beginning(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        await self._operator_api.seek_with_consumer_method("seek_to_beginning", partitions)

    async def seek_to_end(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        await self._operator_api.seek_with_consumer_method("seek_to_end", partitions)

    async def stream(self) -> AsyncGenerator[T, None]:
        consumer = self._current_consumer()
        if consumer is None:
            raise RuntimeError(
                "KafkaSource must be used as an async context manager "
                "or call open() before stream()."
            )
        stream = self._stream_runtime.stream(consumer=consumer)
        try:
            async for record in stream:
                yield record
        finally:
            await stream.aclose()

    async def prepare_resume(self, checkpoint: Any) -> None:
        await self._source_surface.prepare_resume(checkpoint)

    def current_checkpoint(self) -> dict[str, Any] | None:
        return self._source_surface.current_checkpoint()

    async def health_snapshot(
        self,
        *,
        force_refresh: bool = False,
    ) -> KafkaSourceHealthSnapshot:
        return await self._health_monitor.snapshot(
            force_refresh=force_refresh,
            consumer=self._current_consumer(),
            max_idle_polls=self._max_idle_polls,
            idle_poll_count=self._runtime_state.idle_poll_count,
            rebalance_count=self._runtime_state.rebalance_count,
            last_poll_monotonic=self._runtime_state.last_poll_monotonic,
            last_message_monotonic=self._runtime_state.last_message_monotonic,
            last_commit_monotonic=self._runtime_state.last_commit_monotonic,
            last_rebalance_monotonic=self._runtime_state.last_rebalance_monotonic,
        )

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._source_surface.delivery_success_callback()

    def delivery_transaction_offsets_callback(
        self,
    ) -> Callable[[], Awaitable[tuple[dict[Any, int], str]]] | None:
        return self._source_surface.delivery_transaction_offsets_callback()

    def delivery_transaction_committed_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._source_surface.delivery_transaction_committed_callback()

    def delivery_context(self) -> KafkaDeliveryContext | None:
        return self._source_surface.delivery_context()

    def pause(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self._operator_api.pause(partitions)

    def resume(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self._operator_api.resume(partitions)

    def apply_pause_state(self, partitions: object | None = None) -> None:
        self._operator_api.apply_pause_state(partitions)
