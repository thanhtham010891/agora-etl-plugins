"""Explicit runtime helpers for manually driving KafkaSource flows."""

from __future__ import annotations

import contextlib
from collections.abc import MutableMapping
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from agora.core.sink import BaseSink
    from agora.core.source import SourceRuntimeMetrics

    from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot
    from agora_plugins.kafka.sources.kafka import (
        KafkaSource,
        KafkaSourceHealthSnapshot,
        KafkaSourceOperationalMetrics,
    )

T = TypeVar("T")
U = TypeVar("U")
HealthT = TypeVar("HealthT")
SnapshotT = TypeVar("SnapshotT")
ThresholdsT = TypeVar("ThresholdsT")
ReportT = TypeVar("ReportT")
SinkMetricsT = TypeVar("SinkMetricsT")


class KafkaSourceRuntime(Generic[T]):
    """Manual orchestration helper for KafkaSource operator-driven flows.

    This keeps operator controls explicit while removing repetitive
    delivery-success orchestration from integration-style wedge code.
    """

    def __init__(self, source: KafkaSource[T]) -> None:
        self.source = source

    async def open(self) -> None:
        await self.source.open()

    async def close(self) -> None:
        await self.source.close()

    async def commit_now(self) -> None:
        await self.source.commit_now()

    async def seek_to_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        await self.source.seek_to_offsets(offsets)

    async def seek_to_beginning(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        await self.source.seek_to_beginning(partitions)

    async def seek_to_end(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        await self.source.seek_to_end(partitions)

    def pause(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self.source.pause(partitions)

    def resume(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self.source.resume(partitions)

    def current_checkpoint(self) -> dict[str, Any] | None:
        return self.source.current_checkpoint()

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return self.source.runtime_metrics()

    def operational_metrics(self) -> KafkaSourceOperationalMetrics:
        return self.source.operational_metrics()

    async def health_snapshot(self) -> KafkaSourceHealthSnapshot:
        return await self.source.health_snapshot()

    async def metrics_snapshot(self) -> KafkaSourceMetricsSnapshot:
        from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot

        return KafkaSourceMetricsSnapshot(
            health=await self.health_snapshot(),
            operational=self.operational_metrics(),
            runtime=self.runtime_metrics(),
        )

    async def render_prometheus_metrics(self, namespace: str = "agora_kafka") -> str:
        from agora_plugins.kafka.metrics import KafkaSourcePrometheusExporter

        exporter = KafkaSourcePrometheusExporter(namespace=namespace)
        return await exporter.render_runtime(self)

    def delivery_context(self) -> dict[str, Any] | None:
        context = self.source.delivery_context()
        if context is None:
            return None
        return context.to_dict()

    def delivery_key(self) -> str | None:
        context = self.source.delivery_context()
        if context is None:
            return None
        return context.delivery_id

    async def deliver(
        self,
        record: T,
        sink: BaseSink[U],
        *,
        transform: Callable[[T], U | Awaitable[U]] | None = None,
        flush: bool = True,
        delivery_metadata_field: str | None = None,
        delivery_key_field: str | None = None,
        transactional_offsets: bool = False,
    ) -> U:
        outbound = self._decorate_outbound_record(
            await self._transform_record(record, transform),
            delivery_metadata_field=delivery_metadata_field,
            delivery_key_field=delivery_key_field,
        )
        ack_hook = self.source.delivery_success_callback()
        if ack_hook is None:
            raise RuntimeError(
                "KafkaSourceRuntime.deliver() requires an active delivery success callback. "
                "Consume the record from KafkaSource.stream() immediately before calling "
                "deliver(), or use drain_to()."
            )

        if transactional_offsets:
            return await self._deliver_with_transactional_offsets(
                outbound,
                sink,
                flush=flush,
            )

        await sink.write(outbound)
        if flush:
            await sink.flush()
        else:
            wait_for_pending_acks = getattr(sink, "wait_for_pending_acks", None)
            if callable(wait_for_pending_acks):
                result = wait_for_pending_acks()
                if isawaitable(result):
                    await result
        await ack_hook()
        return outbound

    async def _deliver_with_transactional_offsets(
        self,
        outbound: U,
        sink: BaseSink[U],
        *,
        flush: bool,
    ) -> U:
        begin_transaction = getattr(sink, "begin_transaction", None)
        send_offsets_to_transaction = getattr(sink, "send_offsets_to_transaction", None)
        commit_transaction = getattr(sink, "commit_transaction", None)
        abort_transaction = getattr(sink, "abort_transaction", None)
        if not all(
            callable(method)
            for method in (
                begin_transaction,
                send_offsets_to_transaction,
                commit_transaction,
                abort_transaction,
            )
        ):
            raise TypeError(
                "transactional_offsets=True requires a sink with Kafka transaction methods."
            )
        begin_transaction = cast("Any", begin_transaction)
        send_offsets_to_transaction = cast("Any", send_offsets_to_transaction)
        commit_transaction = cast("Any", commit_transaction)
        abort_transaction = cast("Any", abort_transaction)

        offsets_callback = getattr(self.source, "delivery_transaction_offsets_callback", None)
        committed_callback = getattr(
            self.source,
            "delivery_transaction_committed_callback",
            None,
        )
        if not callable(offsets_callback) or not callable(committed_callback):
            raise TypeError(
                "transactional_offsets=True requires a KafkaSource with transaction callbacks."
            )
        offsets_callback = cast("Any", offsets_callback)
        committed_callback = cast("Any", committed_callback)
        offsets_hook = offsets_callback()
        committed_hook = committed_callback()
        if offsets_hook is None or committed_hook is None:
            raise RuntimeError(
                "Kafka transactional delivery requires an active source delivery context."
            )

        await _maybe_await(begin_transaction())
        try:
            await sink.write(outbound)
            if flush:
                await sink.flush()
            offsets, group_id = await offsets_hook()
            await send_offsets_to_transaction(offsets, group_id)
            await _maybe_await(commit_transaction())
        except Exception:
            await _maybe_await(abort_transaction())
            raise
        await committed_hook()
        return outbound

    async def drain_to(
        self,
        sink: BaseSink[U],
        *,
        transform: Callable[[T], U | Awaitable[U]] | None = None,
        max_records: int | None = None,
        flush_each_record: bool = True,
        delivery_metadata_field: str | None = None,
        delivery_key_field: str | None = None,
        transactional_offsets: bool = False,
    ) -> list[T]:
        stream = self.source.stream()
        records: list[T] = []
        try:
            while max_records is None or len(records) < max_records:
                try:
                    record = await anext(stream)
                except StopAsyncIteration:
                    break
                records.append(record)
                await self.deliver(
                    record,
                    sink,
                    transform=transform,
                    flush=flush_each_record,
                    delivery_metadata_field=delivery_metadata_field,
                    delivery_key_field=delivery_key_field,
                    transactional_offsets=transactional_offsets,
                )
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
        return records

    async def _transform_record(
        self,
        record: T,
        transform: Callable[[T], U | Awaitable[U]] | None,
    ) -> U:
        if transform is None:
            return record  # type: ignore[return-value]
        outbound = transform(record)
        if isawaitable(outbound):
            return await outbound
        return outbound

    def _decorate_outbound_record(
        self,
        outbound: U,
        *,
        delivery_metadata_field: str | None,
        delivery_key_field: str | None,
    ) -> U:
        if delivery_metadata_field is None and delivery_key_field is None:
            return outbound
        if not isinstance(outbound, MutableMapping):
            raise TypeError(
                "Kafka delivery metadata injection requires transformed records to be mutable mappings."
            )

        record = dict(outbound)
        if delivery_metadata_field is not None:
            context = self.delivery_context()
            if context is not None:
                record[delivery_metadata_field] = context
        if delivery_key_field is not None:
            delivery_key = self.delivery_key()
            if delivery_key is not None:
                record[delivery_key_field] = delivery_key
        return record  # type: ignore[return-value]


class KafkaTransformSinkRuntime(Generic[T, U]):
    """Reusable session for Kafka -> transform -> sink flows.

    This keeps operator controls available while removing repetitive
    transform/write/flush/ack orchestration from backend-specific wedges.
    """

    def __init__(
        self,
        source: KafkaSource[T],
        sink: BaseSink[U],
        *,
        transform: Callable[[T], U | Awaitable[U]] | None = None,
        flush_each_record: bool = True,
        delivery_metadata_field: str | None = None,
        delivery_key_field: str | None = None,
        transactional_offsets: bool = False,
    ) -> None:
        self.source_runtime = KafkaSourceRuntime(source)
        self.sink = sink
        self.transform = transform
        self.flush_each_record = flush_each_record
        self.delivery_metadata_field = delivery_metadata_field
        self.delivery_key_field = delivery_key_field
        self.transactional_offsets = transactional_offsets

    @property
    def source(self) -> KafkaSource[T]:
        return self.source_runtime.source

    async def open(self) -> None:
        await self.sink.open()
        try:
            await self.source_runtime.open()
        except Exception:
            with contextlib.suppress(Exception):
                await self.sink.close()
            raise

    async def close(self) -> None:
        source_error: Exception | None = None
        try:
            await self.source_runtime.close()
        except Exception as exc:
            source_error = exc
        try:
            await self.sink.close()
        except Exception:
            if source_error is not None:
                raise source_error from None
            raise
        if source_error is not None:
            raise source_error from None

    async def commit_now(self) -> None:
        await self.source_runtime.commit_now()

    async def seek_to_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        await self.source_runtime.seek_to_offsets(offsets)

    async def seek_to_beginning(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        await self.source_runtime.seek_to_beginning(partitions)

    async def seek_to_end(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        await self.source_runtime.seek_to_end(partitions)

    def pause(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self.source_runtime.pause(partitions)

    def resume(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self.source_runtime.resume(partitions)

    def current_checkpoint(self) -> dict[str, Any] | None:
        return self.source_runtime.current_checkpoint()

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return self.source_runtime.runtime_metrics()

    def operational_metrics(self) -> KafkaSourceOperationalMetrics:
        return self.source_runtime.operational_metrics()

    async def health_snapshot(self) -> KafkaSourceHealthSnapshot:
        return await self.source_runtime.health_snapshot()

    async def metrics_snapshot(self) -> KafkaSourceMetricsSnapshot:
        return await self.source_runtime.metrics_snapshot()

    async def render_prometheus_metrics(self, namespace: str = "agora_kafka") -> str:
        return await self.source_runtime.render_prometheus_metrics(namespace=namespace)

    async def deliver(
        self,
        record: T,
        *,
        flush: bool | None = None,
        delivery_metadata_field: str | None = None,
        delivery_key_field: str | None = None,
    ) -> U:
        return await self.source_runtime.deliver(
            record,
            self.sink,
            transform=self.transform,
            flush=self.flush_each_record if flush is None else flush,
            delivery_metadata_field=(
                self.delivery_metadata_field
                if delivery_metadata_field is None
                else delivery_metadata_field
            ),
            delivery_key_field=(
                self.delivery_key_field if delivery_key_field is None else delivery_key_field
            ),
            transactional_offsets=self.transactional_offsets,
        )

    async def drain(self, *, max_records: int | None = None) -> list[T]:
        return await self.source_runtime.drain_to(
            self.sink,
            transform=self.transform,
            max_records=max_records,
            flush_each_record=self.flush_each_record,
            delivery_metadata_field=self.delivery_metadata_field,
            delivery_key_field=self.delivery_key_field,
            transactional_offsets=self.transactional_offsets,
        )


class KafkaBackendRuntimeObservabilityMixin(
    Generic[HealthT, SnapshotT, ThresholdsT, ReportT, SinkMetricsT]
):
    """Shared orchestration for backend-specific Kafka wedge runtime surfaces.

    Backend runtimes only need to implement sink-specific metrics extraction,
    health snapshot construction, observability snapshot construction, and
    acceptance evaluation. The common async orchestration stays here.
    """

    source_runtime: KafkaSourceRuntime[Any]

    async def source_metrics_snapshot(self) -> KafkaSourceMetricsSnapshot:
        return await self.source_runtime.metrics_snapshot()

    def sink_metrics(self) -> SinkMetricsT:
        raise NotImplementedError

    def _build_runtime_health_snapshot(
        self,
        *,
        source: KafkaSourceMetricsSnapshot,
        sink: SinkMetricsT,
    ) -> HealthT:
        raise NotImplementedError

    def _build_runtime_observability_snapshot(
        self,
        *,
        health: HealthT,
        source: KafkaSourceMetricsSnapshot,
        sink: SinkMetricsT,
    ) -> SnapshotT:
        raise NotImplementedError

    def _evaluate_runtime_acceptance(
        self,
        *,
        snapshot: SnapshotT,
        thresholds: ThresholdsT | None,
    ) -> ReportT:
        raise NotImplementedError

    async def health_snapshot(self) -> HealthT:
        source_snapshot = await self.source_metrics_snapshot()
        sink_snapshot = self.sink_metrics()
        return self._build_runtime_health_snapshot(
            source=source_snapshot,
            sink=sink_snapshot,
        )

    async def observability_snapshot(self) -> SnapshotT:
        source_snapshot = await self.source_metrics_snapshot()
        sink_snapshot = self.sink_metrics()
        health_snapshot = self._build_runtime_health_snapshot(
            source=source_snapshot,
            sink=sink_snapshot,
        )
        return self._build_runtime_observability_snapshot(
            health=health_snapshot,
            source=source_snapshot,
            sink=sink_snapshot,
        )

    async def acceptance_report(
        self,
        thresholds: ThresholdsT | None = None,
    ) -> ReportT:
        return self._evaluate_runtime_acceptance(
            snapshot=await self.observability_snapshot(),
            thresholds=thresholds,
        )

    async def ensure_ready(
        self,
        thresholds: ThresholdsT | None = None,
    ) -> tuple[HealthT, SnapshotT, ReportT]:
        snapshot = await self.observability_snapshot()
        report = self._evaluate_runtime_acceptance(
            snapshot=snapshot,
            thresholds=thresholds,
        )
        if not bool(getattr(report, "passed", False)):
            raise KafkaRuntimeReadinessError(_format_runtime_readiness_error(report))
        snapshot_any = cast("Any", snapshot)
        return cast("HealthT", snapshot_any.health), snapshot, report


class KafkaRuntimeReadinessError(RuntimeError):
    """Raised when a Kafka backend runtime fails its readiness acceptance gate."""


def _format_runtime_readiness_error(report: object) -> str:
    findings = getattr(report, "findings", ())
    details = []
    for finding in findings:
        metric = getattr(finding, "metric", "unknown")
        message = getattr(finding, "message", "readiness gate failed")
        details.append(f"{metric}: {message}")
    if not details:
        return "Kafka backend runtime failed readiness acceptance."
    return "Kafka backend runtime failed readiness acceptance: " + "; ".join(details)


async def _maybe_await(value: object) -> None:
    if isawaitable(value):
        await value


__all__ = [
    "KafkaBackendRuntimeObservabilityMixin",
    "KafkaRuntimeReadinessError",
    "KafkaSourceRuntime",
    "KafkaTransformSinkRuntime",
]
