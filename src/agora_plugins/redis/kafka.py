"""Kafka -> Redis runtime helpers with consistent enterprise observability."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.health import ComponentHealthSnapshot
from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)

from agora_plugins.kafka.runtime import (
    KafkaBackendRuntimeObservabilityMixin,
    KafkaTransformSinkRuntime,
)
from agora_plugins.redis.sinks.redis import RedisSink, RedisSinkMetricsSnapshot

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from agora_plugins.kafka import KafkaSecurityConfig
    from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot
    from agora_plugins.kafka.sources.kafka import KafkaSource

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class KafkaRedisDeliveryConfig:
    """Default delivery-field contract for Kafka -> Redis wedges."""

    key_field: str = "kafka_delivery_key"
    metadata_field: str | None = "kafka_metadata"


@dataclass(frozen=True, slots=True)
class KafkaRedisStorageConfig:
    """Storage contract for Redis wedge records."""

    redis_key_field: str = "redis_key"
    value_field: str = "value"
    preserve_delivery_fields_in_value: bool = False


@dataclass(frozen=True, slots=True)
class KafkaRedisRuntimeHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for Kafka -> Redis wedges."""

    source_ready: bool
    source_stalled: bool
    sink_connection_ready: bool
    sink_mode: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "source_ready": self.source_ready,
            "source_stalled": self.source_stalled,
            "sink_connection_ready": self.sink_connection_ready,
            "sink_mode": self.sink_mode,
        }


@dataclass(frozen=True, slots=True)
class KafkaRedisRuntimeMetricsSnapshot:
    """Combined Kafka source and Redis sink observability snapshot."""

    health: KafkaRedisRuntimeHealthSnapshot
    source: KafkaSourceMetricsSnapshot
    sink: RedisSinkMetricsSnapshot
    delivery_key_field: str
    delivery_metadata_field: str | None
    redis_key_field: str
    value_field: str
    preserve_delivery_fields_in_value: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "health": self.health.to_dict(),
            "source": self.source.to_dict(),
            "sink": self.sink.to_dict(),
            "delivery_key_field": self.delivery_key_field,
            "delivery_metadata_field": self.delivery_metadata_field,
            "redis_key_field": self.redis_key_field,
            "value_field": self.value_field,
            "preserve_delivery_fields_in_value": self.preserve_delivery_fields_in_value,
        }


@dataclass(frozen=True, slots=True)
class KafkaRedisEnterpriseAcceptanceThresholds:
    """Production gate thresholds for Kafka -> Redis runtime health."""

    require_runtime_ready: bool = True
    require_source_ready: bool = True
    require_source_not_stalled: bool = True
    require_sink_connection_ready: bool = True
    max_pending_commit_count: int | None = 0
    max_idle_poll_count: int | None = 0
    max_total_lag: int | None = 0
    max_max_lag: int | None = 0
    max_total_commit_lag: int | None = 0
    max_max_commit_lag: int | None = 0
    max_last_poll_age_ms: float | None = 5_000.0
    max_last_message_age_ms: float | None = 5_000.0
    max_last_commit_age_ms: float | None = 10_000.0
    max_record_error_count: int = 0
    max_record_drop_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_runtime_ready": self.require_runtime_ready,
            "require_source_ready": self.require_source_ready,
            "require_source_not_stalled": self.require_source_not_stalled,
            "require_sink_connection_ready": self.require_sink_connection_ready,
            "max_pending_commit_count": self.max_pending_commit_count,
            "max_idle_poll_count": self.max_idle_poll_count,
            "max_total_lag": self.max_total_lag,
            "max_max_lag": self.max_max_lag,
            "max_total_commit_lag": self.max_total_commit_lag,
            "max_max_commit_lag": self.max_max_commit_lag,
            "max_last_poll_age_ms": self.max_last_poll_age_ms,
            "max_last_message_age_ms": self.max_last_message_age_ms,
            "max_last_commit_age_ms": self.max_last_commit_age_ms,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
        }


@dataclass(frozen=True, slots=True)
class KafkaRedisEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single threshold violation from enterprise acceptance evaluation."""


@dataclass(frozen=True, slots=True)
class KafkaRedisEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise acceptance verdict for a runtime snapshot."""

    findings: tuple[KafkaRedisEnterpriseAcceptanceFinding, ...] = ()


class KafkaRedisEnterpriseAcceptanceGate:
    """Evaluate Kafka -> Redis runtime snapshots against ops-grade thresholds."""

    def __init__(
        self,
        thresholds: KafkaRedisEnterpriseAcceptanceThresholds | None = None,
    ) -> None:
        self._thresholds = (
            KafkaRedisEnterpriseAcceptanceThresholds() if thresholds is None else thresholds
        )

    async def evaluate_runtime(
        self,
        runtime: KafkaRedisRuntime[Any],
    ) -> KafkaRedisEnterpriseAcceptanceReport:
        return self.evaluate(await runtime.observability_snapshot())

    def evaluate(
        self,
        snapshot: KafkaRedisRuntimeMetricsSnapshot,
    ) -> KafkaRedisEnterpriseAcceptanceReport:
        thresholds = self._thresholds
        findings: list[KafkaRedisEnterpriseAcceptanceFinding] = []
        runtime_health = snapshot.health
        source = snapshot.source
        health = source.health
        runtime_metrics = source.runtime

        if thresholds.require_runtime_ready and not runtime_health.ready:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="runtime.ready",
                    message="Kafka -> Redis runtime is not ready.",
                    value=runtime_health.ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_ready and not runtime_health.source_ready:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="source.ready",
                    message="Kafka source is not ready.",
                    value=runtime_health.source_ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_not_stalled and runtime_health.source_stalled:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="source.stalled",
                    message="Kafka source is stalled.",
                    value=runtime_health.source_stalled,
                    threshold=False,
                )
            )
        if thresholds.require_sink_connection_ready and not runtime_health.sink_connection_ready:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="sink.connection_ready",
                    message="Redis sink connection is not ready.",
                    value=runtime_health.sink_connection_ready,
                    threshold=True,
                )
            )

        self._check_max(
            findings,
            "source.pending_commit_count",
            health.pending_commit_count,
            thresholds.max_pending_commit_count,
        )
        self._check_max(
            findings,
            "source.idle_poll_count",
            health.idle_poll_count,
            thresholds.max_idle_poll_count,
        )
        self._check_max(findings, "source.total_lag", health.total_lag, thresholds.max_total_lag)
        self._check_max(findings, "source.max_lag", health.max_lag, thresholds.max_max_lag)
        self._check_max(
            findings,
            "source.total_commit_lag",
            health.total_commit_lag,
            thresholds.max_total_commit_lag,
        )
        self._check_max(
            findings, "source.max_commit_lag", health.max_commit_lag, thresholds.max_max_commit_lag
        )
        self._check_max(
            findings,
            "source.last_poll_age_ms",
            health.last_poll_age_ms,
            thresholds.max_last_poll_age_ms,
        )
        self._check_max(
            findings,
            "source.last_message_age_ms",
            health.last_message_age_ms,
            thresholds.max_last_message_age_ms,
        )
        self._check_max(
            findings,
            "source.last_commit_age_ms",
            health.last_commit_age_ms,
            thresholds.max_last_commit_age_ms,
        )
        self._check_max(
            findings,
            "source.record_error_count",
            runtime_metrics.record_error_count,
            thresholds.max_record_error_count,
        )
        self._check_max(
            findings,
            "source.record_drop_count",
            runtime_metrics.record_drop_count,
            thresholds.max_record_drop_count,
        )

        return KafkaRedisEnterpriseAcceptanceReport(
            passed=not findings,
            thresholds=thresholds,
            findings=tuple(findings),
        )

    @staticmethod
    def _check_max(
        findings: list[KafkaRedisEnterpriseAcceptanceFinding],
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric=metric,
                    message=f"{metric} exceeded enterprise threshold.",
                    value=value,
                    threshold=threshold,
                )
            )


class KafkaRedisPrometheusExporter:
    """Prometheus renderer for Kafka -> Redis helper runtimes."""

    def __init__(self, namespace: str = "agora_kafka_redis") -> None:
        self._ns = namespace

    async def render_runtime(self, runtime: KafkaRedisRuntime[Any]) -> str:
        return self.render(await runtime.observability_snapshot())

    def render(self, snapshot: KafkaRedisRuntimeMetricsSnapshot) -> str:
        from agora_plugins.kafka.metrics import KafkaSourcePrometheusExporter

        lines: list[str] = []
        source_rendered = KafkaSourcePrometheusExporter(namespace=f"{self._ns}_source").render(
            snapshot.source
        )
        lines.extend(
            line
            for line in source_rendered.splitlines()
            if line and not line.startswith("# scrape_time")
        )

        labels = self._base_labels(snapshot)
        append_metric_header(
            lines,
            help_text="Kafka -> Redis runtime readiness state",
            metric_type="gauge",
            name=f"{self._ns}_runtime_state",
        )
        for state_name, value in (
            ("ready", int(snapshot.health.ready)),
            ("source_ready", int(snapshot.health.source_ready)),
            ("source_stalled", int(snapshot.health.source_stalled)),
            ("sink_connection_ready", int(snapshot.health.sink_connection_ready)),
        ):
            lines.append(f'{self._ns}_runtime_state{{{labels},state="{state_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka -> Redis runtime configuration and readiness",
            metric_type="gauge",
            name=f"{self._ns}_runtime_config",
        )
        for config_name, value in (
            ("delivery_metadata_enabled", int(snapshot.delivery_metadata_field is not None)),
            (
                "preserve_delivery_fields_in_value",
                int(snapshot.preserve_delivery_fields_in_value),
            ),
            ("sink_connection_ready", int(snapshot.sink.connection_ready)),
            ("sink_uses_xadd", int(snapshot.sink.mode == "xadd")),
        ):
            lines.append(f'{self._ns}_runtime_config{{{labels},config="{config_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka -> Redis sink gauge values",
            metric_type="gauge",
            name=f"{self._ns}_sink_gauge",
        )
        if snapshot.sink.ttl_seconds is not None:
            lines.append(
                f'{self._ns}_sink_gauge{{{labels},gauge="ttl_seconds"}} {snapshot.sink.ttl_seconds}'
            )
        if snapshot.sink.maxlen is not None:
            lines.append(f'{self._ns}_sink_gauge{{{labels},gauge="maxlen"}} {snapshot.sink.maxlen}')

        append_metric_header(
            lines,
            help_text="Kafka -> Redis sink monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_sink_events_total",
        )
        for event_name, value in (
            ("write_call", snapshot.sink.write_call_count),
            ("write_batch_call", snapshot.sink.write_batch_call_count),
            ("direct_write", snapshot.sink.direct_write_count),
            ("mset_batch", snapshot.sink.mset_batch_count),
            ("pipeline_execute", snapshot.sink.pipeline_execute_count),
            ("written_record", snapshot.sink.written_record_count),
        ):
            lines.append(f'{self._ns}_sink_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka -> Redis sink last-write age in milliseconds",
            metric_type="gauge",
            name=f"{self._ns}_sink_age_ms",
        )
        last_write_age_ms = _age_ms(snapshot.sink.last_write_at)
        if last_write_age_ms is not None:
            lines.append(
                f'{self._ns}_sink_age_ms{{{labels},activity="write"}} {last_write_age_ms:.6f}'
            )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def _base_labels(self, snapshot: KafkaRedisRuntimeMetricsSnapshot) -> str:
        labels = [
            f'consumer_group="{escape_label_value(snapshot.source.health.consumer_group)}"',
            f'bootstrap_servers="{escape_label_value(snapshot.source.health.bootstrap_servers)}"',
            f'sink_target="{escape_label_value(snapshot.sink.target)}"',
            f'sink_mode="{escape_label_value(snapshot.health.sink_mode)}"',
            f'redis_key_field="{escape_label_value(snapshot.redis_key_field)}"',
            f'value_field="{escape_label_value(snapshot.value_field)}"',
            f'delivery_key_field="{escape_label_value(snapshot.delivery_key_field)}"',
        ]
        if snapshot.delivery_metadata_field is not None:
            labels.append(
                f'delivery_metadata_field="{escape_label_value(snapshot.delivery_metadata_field)}"'
            )
        return ",".join(labels)


def _age_ms(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return max((datetime.now(UTC) - timestamp).total_seconds() * 1000.0, 0.0)


class KafkaRedisEnvelopeDeserializer(Generic[T]):
    """Wrap a payload deserializer and attach Kafka metadata for wedge transforms."""

    def __init__(
        self,
        inner: Callable[..., T | Awaitable[T]],
        *,
        metadata_aware: bool = False,
    ) -> None:
        self._inner = inner
        self._metadata_aware = metadata_aware

    async def open(self) -> None:
        open_hook = getattr(self._inner, "open", None)
        if callable(open_hook):
            result = open_hook()
            if isawaitable(result):
                await result

    async def close(self) -> None:
        close_hook = getattr(self._inner, "close", None)
        if callable(close_hook):
            result = close_hook()
            if isawaitable(result):
                await result

    async def __call__(
        self,
        value: bytes,
        metadata: dict[str, object],
    ) -> dict[str, object]:
        payload = self._inner(value, metadata) if self._metadata_aware else self._inner(value)
        if isawaitable(payload):
            payload = await payload
        return {
            "payload": payload,
            "metadata": metadata,
        }


def wrap_kafka_redis_deserializer(
    inner: Callable[..., T | Awaitable[T]],
    *,
    metadata_aware: bool = False,
) -> KafkaRedisEnvelopeDeserializer[T]:
    """Build the canonical Kafka payload+metadata envelope deserializer."""

    return KafkaRedisEnvelopeDeserializer(inner, metadata_aware=metadata_aware)


def _default_key_fn(
    record: dict[str, Any],
    *,
    storage: KafkaRedisStorageConfig,
) -> str:
    if storage.redis_key_field not in record:
        raise KeyError(
            "Kafka -> Redis helper requires transformed records to include "
            f"{storage.redis_key_field!r}."
        )
    return str(record[storage.redis_key_field])


def _payload_with_delivery_fields(
    record: dict[str, Any],
    *,
    payload: Any,
    delivery: KafkaRedisDeliveryConfig,
    storage: KafkaRedisStorageConfig,
) -> dict[str, Any]:
    enriched = dict(payload) if isinstance(payload, Mapping) else {storage.value_field: payload}
    delivery_key = record.get(delivery.key_field)
    if delivery_key is not None and delivery.key_field not in enriched:
        enriched[delivery.key_field] = delivery_key
    metadata_field = delivery.metadata_field
    if metadata_field is not None:
        delivery_metadata = record.get(metadata_field)
        if delivery_metadata is not None and metadata_field not in enriched:
            enriched[metadata_field] = delivery_metadata
    return enriched


def _default_serializer(
    record: dict[str, Any],
    *,
    mode: str,
    delivery: KafkaRedisDeliveryConfig,
    storage: KafkaRedisStorageConfig,
) -> Any:
    if storage.value_field in record:
        payload = record[storage.value_field]
    else:
        payload = {key: value for key, value in record.items() if key != storage.redis_key_field}
    if storage.preserve_delivery_fields_in_value:
        payload = _payload_with_delivery_fields(
            record,
            payload=payload,
            delivery=delivery,
            storage=storage,
        )
    if mode == "xadd":
        if not isinstance(payload, Mapping):
            raise TypeError(
                "Kafka -> Redis helper requires 'value' to be a mapping when mode='xadd'."
            )
        return {
            str(key): (
                value
                if isinstance(value, bytes | bytearray | memoryview | str | int | float)
                else json.dumps(value, sort_keys=True, default=str)
            )
            for key, value in payload.items()
        }
    if isinstance(payload, (bytes, bytearray, memoryview, str)):
        return payload
    return json.dumps(payload, sort_keys=True, default=str)


def build_kafka_redis_sink(
    *,
    url: str,
    key_fn: Callable[[dict[str, Any]], str] | None = None,
    serializer: Callable[[dict[str, Any]], Any] | None = None,
    mode: str = "set",
    ttl_seconds: int | None = None,
    maxlen: int | None = None,
    delivery: KafkaRedisDeliveryConfig | None = None,
    storage: KafkaRedisStorageConfig | None = None,
) -> RedisSink[dict[str, Any]]:
    """Build a Redis sink with canonical Kafka wedge defaults."""

    resolved_delivery = delivery or KafkaRedisDeliveryConfig()
    resolved_storage = storage or KafkaRedisStorageConfig()
    return RedisSink[dict[str, Any]](
        url=url,
        key_fn=(
            (lambda record: _default_key_fn(record, storage=resolved_storage))
            if key_fn is None
            else key_fn
        ),
        serializer=(
            (
                lambda record: _default_serializer(
                    record,
                    mode=mode,
                    delivery=resolved_delivery,
                    storage=resolved_storage,
                )
            )
            if serializer is None
            else serializer
        ),
        mode=mode,
        ttl_seconds=ttl_seconds,
        maxlen=maxlen,
    )


def build_kafka_redis_source(
    *,
    topics: list[str] | None = None,
    topic_pattern: str | None = None,
    assignments: Iterable[tuple[str, int]] | None = None,
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "agora-consumer",
    deserializer: Callable[..., Any],
    deserializer_metadata_aware: bool = False,
    include_metadata: bool = True,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    commit_every: int = 100,
    poll_timeout_ms: int = 1000,
    max_idle_polls: int | None = None,
    max_poll_records: int = 500,
    fetch_min_bytes: int = 1,
    fetch_max_wait_ms: int = 500,
    max_partition_fetch_bytes: int = 1_048_576,
    security_protocol: str = "PLAINTEXT",
    security: KafkaSecurityConfig | None = None,
    extra_config: dict[str, Any] | None = None,
    start_offsets: dict[tuple[str, int], int] | None = None,
    rebalance_listener: object | None = None,
    health_snapshot_cache_ms: int = 250,
) -> KafkaSource[dict[str, object]]:
    """Build the canonical Kafka source for Kafka -> Redis wedges."""

    from agora_plugins.kafka import KafkaSource

    source_deserializer = (
        wrap_kafka_redis_deserializer(
            deserializer,
            metadata_aware=deserializer_metadata_aware,
        )
        if include_metadata
        else deserializer
    )
    source = KafkaSource[dict[str, object]](
        topics=topics,
        topic_pattern=topic_pattern,
        assignments=assignments,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        deserializer=source_deserializer,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        commit_every=commit_every,
        poll_timeout_ms=poll_timeout_ms,
        max_idle_polls=max_idle_polls,
        max_poll_records=max_poll_records,
        fetch_min_bytes=fetch_min_bytes,
        fetch_max_wait_ms=fetch_max_wait_ms,
        max_partition_fetch_bytes=max_partition_fetch_bytes,
        security_protocol=security_protocol,
        security=security,
        extra_config=extra_config,
        start_offsets=start_offsets,
        rebalance_listener=rebalance_listener,
        health_snapshot_cache_ms=health_snapshot_cache_ms,
    )
    return source


def build_kafka_redis_runtime(
    *,
    source: KafkaSource[T],
    url: str,
    transform: Callable[[T], dict[str, Any] | Awaitable[dict[str, Any]]] | None = None,
    key_fn: Callable[[dict[str, Any]], str] | None = None,
    serializer: Callable[[dict[str, Any]], Any] | None = None,
    mode: str = "set",
    ttl_seconds: int | None = None,
    maxlen: int | None = None,
    flush_each_record: bool = True,
    delivery: KafkaRedisDeliveryConfig | None = None,
    storage: KafkaRedisStorageConfig | None = None,
) -> KafkaRedisRuntime[T]:
    """Build the canonical Kafka -> Redis runtime with delivery-safe defaults."""

    resolved_delivery = delivery or KafkaRedisDeliveryConfig()
    resolved_storage = storage or KafkaRedisStorageConfig()
    sink = build_kafka_redis_sink(
        url=url,
        key_fn=key_fn,
        serializer=serializer,
        mode=mode,
        ttl_seconds=ttl_seconds,
        maxlen=maxlen,
        delivery=resolved_delivery,
        storage=resolved_storage,
    )
    return KafkaRedisRuntime(
        source,
        sink,
        transform=transform,
        flush_each_record=flush_each_record,
        delivery=resolved_delivery,
        storage=resolved_storage,
    )


class KafkaRedisRuntime(  # type: ignore[misc]
    KafkaBackendRuntimeObservabilityMixin[
        KafkaRedisRuntimeHealthSnapshot,
        KafkaRedisRuntimeMetricsSnapshot,
        KafkaRedisEnterpriseAcceptanceThresholds,
        KafkaRedisEnterpriseAcceptanceReport,
        RedisSinkMetricsSnapshot,
    ],
    KafkaTransformSinkRuntime[T, dict[str, Any]],
):
    """Kafka -> Redis runtime that defaults to delivery-key injection."""

    def __init__(
        self,
        source: KafkaSource[T],
        sink: RedisSink[dict[str, Any]],
        *,
        transform: Callable[[T], dict[str, Any] | Awaitable[dict[str, Any]]] | None = None,
        flush_each_record: bool = True,
        delivery: KafkaRedisDeliveryConfig | None = None,
        storage: KafkaRedisStorageConfig | None = None,
    ) -> None:
        resolved_delivery = delivery or KafkaRedisDeliveryConfig()
        resolved_storage = storage or KafkaRedisStorageConfig()
        super().__init__(
            source,
            sink,
            transform=transform,
            flush_each_record=flush_each_record,
            delivery_metadata_field=resolved_delivery.metadata_field,
            delivery_key_field=resolved_delivery.key_field,
        )
        self.delivery = resolved_delivery
        self.storage = resolved_storage

    def sink_metrics(self) -> RedisSinkMetricsSnapshot:
        return cast("RedisSink[dict[str, Any]]", self.sink).metrics_snapshot()

    async def render_prometheus_metrics(self, namespace: str = "agora_kafka_redis") -> str:
        return await KafkaRedisPrometheusExporter(namespace=namespace).render_runtime(self)

    def _build_runtime_health_snapshot(
        self,
        *,
        source: KafkaSourceMetricsSnapshot,
        sink: RedisSinkMetricsSnapshot,
    ) -> KafkaRedisRuntimeHealthSnapshot:
        return KafkaRedisRuntimeHealthSnapshot(
            ready=source.health.ready and not source.health.stalled and sink.connection_ready,
            source_ready=source.health.ready,
            source_stalled=source.health.stalled,
            sink_connection_ready=sink.connection_ready,
            sink_mode=sink.mode,
        )

    def _build_runtime_observability_snapshot(
        self,
        *,
        health: KafkaRedisRuntimeHealthSnapshot,
        source: KafkaSourceMetricsSnapshot,
        sink: RedisSinkMetricsSnapshot,
    ) -> KafkaRedisRuntimeMetricsSnapshot:
        return KafkaRedisRuntimeMetricsSnapshot(
            health=health,
            source=source,
            sink=sink,
            delivery_key_field=self.delivery.key_field,
            delivery_metadata_field=self.delivery.metadata_field,
            redis_key_field=self.storage.redis_key_field,
            value_field=self.storage.value_field,
            preserve_delivery_fields_in_value=self.storage.preserve_delivery_fields_in_value,
        )

    def _evaluate_runtime_acceptance(
        self,
        *,
        snapshot: KafkaRedisRuntimeMetricsSnapshot,
        thresholds: KafkaRedisEnterpriseAcceptanceThresholds | None,
    ) -> KafkaRedisEnterpriseAcceptanceReport:
        return KafkaRedisEnterpriseAcceptanceGate(thresholds).evaluate(snapshot)


__all__ = [
    "KafkaRedisDeliveryConfig",
    "KafkaRedisEnterpriseAcceptanceFinding",
    "KafkaRedisEnterpriseAcceptanceGate",
    "KafkaRedisEnterpriseAcceptanceReport",
    "KafkaRedisEnterpriseAcceptanceThresholds",
    "KafkaRedisEnvelopeDeserializer",
    "KafkaRedisPrometheusExporter",
    "KafkaRedisRuntime",
    "KafkaRedisRuntimeHealthSnapshot",
    "KafkaRedisRuntimeMetricsSnapshot",
    "KafkaRedisStorageConfig",
    "build_kafka_redis_runtime",
    "build_kafka_redis_sink",
    "build_kafka_redis_source",
    "wrap_kafka_redis_deserializer",
]
