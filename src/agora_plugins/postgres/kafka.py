"""Kafka -> PostgreSQL runtime helpers with safer delivery defaults."""

from __future__ import annotations

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
from agora_plugins.postgres.dlq import PostgresDLQSink
from agora_plugins.postgres.sinks.postgres import (
    PostgresSink,
    PostgresSinkMetricsSnapshot,
    QuotedIdentifier,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from agora_plugins.kafka import KafkaPoisonRecordPolicy, KafkaSecurityConfig
    from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot
    from agora_plugins.kafka.sources.kafka import KafkaSource
    from agora_plugins.postgres.connection import PostgresConnectionConfig

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class KafkaPostgresDeliveryConfig:
    """Default delivery-field contract for Kafka -> PostgreSQL wedges."""

    key_field: str = "kafka_delivery_key"
    metadata_field: str | None = "kafka_metadata"


@dataclass(frozen=True, slots=True)
class KafkaPostgresPoisonDLQConfig:
    """Default PostgreSQL-backed poison-record routing for Kafka sources."""

    dsn: str | None
    table: str = "agora_dlq"
    policy: KafkaPoisonRecordPolicy | str = "dlq_and_continue"
    pipeline_id: str | None = None
    max_attempts: int | None = None
    connection: PostgresConnectionConfig | None = None


@dataclass(frozen=True, slots=True)
class KafkaPostgresRuntimeHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for Kafka -> PostgreSQL wedges."""

    source_ready: bool
    source_stalled: bool
    sink_connection_ready: bool
    sink_write_safety_policy: str
    poison_dlq_enabled: bool
    poison_dlq_ready: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "source_ready": self.source_ready,
            "source_stalled": self.source_stalled,
            "sink_connection_ready": self.sink_connection_ready,
            "sink_write_safety_policy": self.sink_write_safety_policy,
            "poison_dlq_enabled": self.poison_dlq_enabled,
            "poison_dlq_ready": self.poison_dlq_ready,
        }


@dataclass(frozen=True, slots=True)
class KafkaPostgresRuntimeMetricsSnapshot:
    """Combined Kafka source and PostgreSQL sink observability snapshot."""

    health: KafkaPostgresRuntimeHealthSnapshot
    source: KafkaSourceMetricsSnapshot
    sink: PostgresSinkMetricsSnapshot
    delivery_key_field: str
    delivery_metadata_field: str | None
    poison_dlq_enabled: bool = False
    poison_dlq_table: str | None = None
    poison_dlq_policy: str | None = None
    poison_dlq_pipeline_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "health": self.health.to_dict(),
            "source": self.source.to_dict(),
            "sink": self.sink.to_dict(),
            "delivery_key_field": self.delivery_key_field,
            "delivery_metadata_field": self.delivery_metadata_field,
            "poison_dlq_enabled": self.poison_dlq_enabled,
            "poison_dlq_table": self.poison_dlq_table,
            "poison_dlq_policy": self.poison_dlq_policy,
            "poison_dlq_pipeline_id": self.poison_dlq_pipeline_id,
        }


@dataclass(frozen=True, slots=True)
class KafkaPostgresEnterpriseAcceptanceThresholds:
    """Production gate thresholds for Kafka -> PostgreSQL runtime health."""

    require_runtime_ready: bool = True
    require_source_ready: bool = True
    require_source_not_stalled: bool = True
    require_sink_connection_ready: bool = True
    require_poison_dlq_ready: bool = False
    max_pending_commit_count: int | None = 0
    max_idle_poll_count: int | None = 0
    max_total_lag: int | None = 0
    max_max_lag: int | None = 0
    max_total_commit_lag: int | None = 0
    max_max_commit_lag: int | None = 0
    max_last_poll_age_ms: float | None = 5_000.0
    max_last_message_age_ms: float | None = 5_000.0
    max_last_commit_age_ms: float | None = 10_000.0
    max_buffered_row_count: int | None = 0
    max_sink_retry_count: int | None = 0
    max_poison_dlq_write_count: int | None = 0
    max_record_error_count: int | None = 0
    max_record_drop_count: int | None = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_runtime_ready": self.require_runtime_ready,
            "require_source_ready": self.require_source_ready,
            "require_source_not_stalled": self.require_source_not_stalled,
            "require_sink_connection_ready": self.require_sink_connection_ready,
            "require_poison_dlq_ready": self.require_poison_dlq_ready,
            "max_pending_commit_count": self.max_pending_commit_count,
            "max_idle_poll_count": self.max_idle_poll_count,
            "max_total_lag": self.max_total_lag,
            "max_max_lag": self.max_max_lag,
            "max_total_commit_lag": self.max_total_commit_lag,
            "max_max_commit_lag": self.max_max_commit_lag,
            "max_last_poll_age_ms": self.max_last_poll_age_ms,
            "max_last_message_age_ms": self.max_last_message_age_ms,
            "max_last_commit_age_ms": self.max_last_commit_age_ms,
            "max_buffered_row_count": self.max_buffered_row_count,
            "max_sink_retry_count": self.max_sink_retry_count,
            "max_poison_dlq_write_count": self.max_poison_dlq_write_count,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
        }


@dataclass(frozen=True, slots=True)
class KafkaPostgresEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single threshold violation from enterprise acceptance evaluation."""


@dataclass(frozen=True, slots=True)
class KafkaPostgresEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise acceptance verdict for a runtime snapshot."""

    findings: tuple[KafkaPostgresEnterpriseAcceptanceFinding, ...] = ()


class KafkaPostgresEnterpriseAcceptanceGate:
    """Evaluate Kafka -> PostgreSQL runtime snapshots against ops-grade thresholds."""

    def __init__(
        self,
        thresholds: KafkaPostgresEnterpriseAcceptanceThresholds | None = None,
    ) -> None:
        self._thresholds = (
            KafkaPostgresEnterpriseAcceptanceThresholds() if thresholds is None else thresholds
        )

    async def evaluate_runtime(
        self,
        runtime: KafkaPostgresRuntime[Any],
    ) -> KafkaPostgresEnterpriseAcceptanceReport:
        return self.evaluate(await runtime.observability_snapshot())

    def evaluate(
        self,
        snapshot: KafkaPostgresRuntimeMetricsSnapshot,
    ) -> KafkaPostgresEnterpriseAcceptanceReport:
        thresholds = self._thresholds
        findings: list[KafkaPostgresEnterpriseAcceptanceFinding] = []
        runtime_health = snapshot.health
        source = snapshot.source
        health = source.health
        operational = source.operational
        runtime_metrics = source.runtime
        sink = snapshot.sink

        if thresholds.require_runtime_ready and not runtime_health.ready:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="runtime.ready",
                    message="Kafka -> PostgreSQL runtime is not ready.",
                    value=runtime_health.ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_ready and not runtime_health.source_ready:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="source.ready",
                    message="Kafka source is not ready.",
                    value=runtime_health.source_ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_not_stalled and runtime_health.source_stalled:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="source.stalled",
                    message="Kafka source is stalled.",
                    value=runtime_health.source_stalled,
                    threshold=False,
                )
            )
        if thresholds.require_sink_connection_ready and not runtime_health.sink_connection_ready:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="sink.connection_ready",
                    message="PostgreSQL sink connection is not ready.",
                    value=runtime_health.sink_connection_ready,
                    threshold=True,
                )
            )
        if (
            thresholds.require_poison_dlq_ready
            and runtime_health.poison_dlq_enabled
            and runtime_health.poison_dlq_ready is not True
        ):
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="poison_dlq.ready",
                    message="PostgreSQL poison DLQ is not ready.",
                    value=runtime_health.poison_dlq_ready,
                    threshold=True,
                )
            )

        self._check_max(
            findings,
            metric="source.pending_commit_count",
            value=health.pending_commit_count,
            threshold=thresholds.max_pending_commit_count,
        )
        self._check_max(
            findings,
            metric="source.idle_poll_count",
            value=health.idle_poll_count,
            threshold=thresholds.max_idle_poll_count,
        )
        self._check_max(
            findings,
            metric="source.total_lag",
            value=health.total_lag,
            threshold=thresholds.max_total_lag,
        )
        self._check_max(
            findings,
            metric="source.max_lag",
            value=health.max_lag,
            threshold=thresholds.max_max_lag,
        )
        self._check_max(
            findings,
            metric="source.total_commit_lag",
            value=health.total_commit_lag,
            threshold=thresholds.max_total_commit_lag,
        )
        self._check_max(
            findings,
            metric="source.max_commit_lag",
            value=health.max_commit_lag,
            threshold=thresholds.max_max_commit_lag,
        )
        self._check_max(
            findings,
            metric="source.last_poll_age_ms",
            value=health.last_poll_age_ms,
            threshold=thresholds.max_last_poll_age_ms,
        )
        self._check_max(
            findings,
            metric="source.last_message_age_ms",
            value=health.last_message_age_ms,
            threshold=thresholds.max_last_message_age_ms,
        )
        self._check_max(
            findings,
            metric="source.last_commit_age_ms",
            value=health.last_commit_age_ms,
            threshold=thresholds.max_last_commit_age_ms,
        )
        self._check_max(
            findings,
            metric="sink.buffered_row_count",
            value=sink.buffered_row_count,
            threshold=thresholds.max_buffered_row_count,
        )
        self._check_max(
            findings,
            metric="sink.retry_count",
            value=sink.retry_count,
            threshold=thresholds.max_sink_retry_count,
        )
        self._check_max(
            findings,
            metric="source.poison_record_dlq_write_count",
            value=operational.poison_record_dlq_write_count,
            threshold=thresholds.max_poison_dlq_write_count,
        )
        self._check_max(
            findings,
            metric="source.record_error_count",
            value=runtime_metrics.record_error_count,
            threshold=thresholds.max_record_error_count,
        )
        self._check_max(
            findings,
            metric="source.record_drop_count",
            value=runtime_metrics.record_drop_count,
            threshold=thresholds.max_record_drop_count,
        )

        return KafkaPostgresEnterpriseAcceptanceReport(
            passed=not findings,
            thresholds=thresholds,
            findings=tuple(findings),
        )

    @staticmethod
    def _check_max(
        findings: list[KafkaPostgresEnterpriseAcceptanceFinding],
        *,
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric=metric,
                    message=f"{metric} exceeded enterprise threshold.",
                    value=value,
                    threshold=threshold,
                )
            )


class KafkaPostgresPrometheusExporter:
    """Prometheus renderer for Kafka -> PostgreSQL helper runtimes."""

    def __init__(self, namespace: str = "agora_kafka_postgres") -> None:
        self._ns = namespace

    async def render_runtime(self, runtime: KafkaPostgresRuntime[Any]) -> str:
        return self.render(await runtime.observability_snapshot())

    def render(self, snapshot: KafkaPostgresRuntimeMetricsSnapshot) -> str:
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
            help_text="Kafka -> PostgreSQL runtime readiness state",
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
        if snapshot.health.poison_dlq_ready is not None:
            lines.append(
                f'{self._ns}_runtime_state{{{labels},state="poison_dlq_ready"}} '
                f"{int(snapshot.health.poison_dlq_ready)}"
            )

        append_metric_header(
            lines,
            help_text="Kafka -> PostgreSQL runtime configuration and readiness",
            metric_type="gauge",
            name=f"{self._ns}_runtime_config",
        )
        for config_name, value in (
            ("delivery_metadata_enabled", int(snapshot.delivery_metadata_field is not None)),
            ("poison_dlq_enabled", int(snapshot.poison_dlq_enabled)),
            ("sink_connection_ready", int(snapshot.sink.connection_ready)),
            ("sink_upsert_enabled", int(snapshot.sink.upsert)),
        ):
            lines.append(f'{self._ns}_runtime_config{{{labels},config="{config_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka -> PostgreSQL sink gauge values",
            metric_type="gauge",
            name=f"{self._ns}_sink_gauge",
        )
        for gauge_name, gauge_value in (
            ("buffered_row_count", snapshot.sink.buffered_row_count),
            ("batch_size", snapshot.sink.batch_size),
            ("pool_size", snapshot.sink.pool_size),
            ("max_parameters_per_statement", snapshot.sink.max_parameters_per_statement),
            ("pooled_connection_count", snapshot.sink.pooled_connection_count),
            ("pooled_available_count", snapshot.sink.pooled_available_count),
        ):
            lines.append(f'{self._ns}_sink_gauge{{{labels},gauge="{gauge_name}"}} {gauge_value}')
        if snapshot.sink.max_rows_per_statement is not None:
            lines.append(
                f'{self._ns}_sink_gauge{{{labels},gauge="max_rows_per_statement"}} '
                f"{snapshot.sink.max_rows_per_statement}"
            )

        append_metric_header(
            lines,
            help_text="Kafka -> PostgreSQL sink monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_sink_events_total",
        )
        for event_name, value in (
            ("write_call", snapshot.sink.write_call_count),
            ("write_batch_call", snapshot.sink.write_batch_call_count),
            ("enqueue", snapshot.sink.enqueue_count),
            ("flush", snapshot.sink.flush_count),
            ("flushed_row", snapshot.sink.flushed_row_count),
            ("retry", snapshot.sink.retry_count),
        ):
            lines.append(f'{self._ns}_sink_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka -> PostgreSQL sink last-flush age in milliseconds",
            metric_type="gauge",
            name=f"{self._ns}_sink_age_ms",
        )
        last_flush_age_ms = _age_ms(snapshot.sink.last_flush_at)
        if last_flush_age_ms is not None:
            lines.append(
                f'{self._ns}_sink_age_ms{{{labels},activity="flush"}} {last_flush_age_ms:.6f}'
            )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def _base_labels(self, snapshot: KafkaPostgresRuntimeMetricsSnapshot) -> str:
        labels = [
            f'consumer_group="{escape_label_value(snapshot.source.health.consumer_group)}"',
            f'bootstrap_servers="{escape_label_value(snapshot.source.health.bootstrap_servers)}"',
            f'table="{escape_label_value(snapshot.sink.table)}"',
            f'insert_mode="{escape_label_value(snapshot.sink.insert_mode)}"',
            (
                f'sink_write_safety_policy="'
                f'{escape_label_value(snapshot.health.sink_write_safety_policy)}"'
            ),
            f'delivery_key_field="{escape_label_value(snapshot.delivery_key_field)}"',
        ]
        if snapshot.delivery_metadata_field is not None:
            labels.append(
                f'delivery_metadata_field="{escape_label_value(snapshot.delivery_metadata_field)}"'
            )
        if snapshot.poison_dlq_table is not None:
            labels.append(f'poison_dlq_table="{escape_label_value(snapshot.poison_dlq_table)}"')
        if snapshot.poison_dlq_policy is not None:
            labels.append(f'poison_dlq_policy="{escape_label_value(snapshot.poison_dlq_policy)}"')
        if snapshot.poison_dlq_pipeline_id is not None:
            labels.append(
                f'poison_dlq_pipeline_id="{escape_label_value(snapshot.poison_dlq_pipeline_id)}"'
            )
        return ",".join(labels)


def _age_ms(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return max((datetime.now(UTC) - timestamp).total_seconds() * 1000.0, 0.0)


class KafkaPostgresEnvelopeDeserializer(Generic[T]):
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


def wrap_kafka_postgres_deserializer(
    inner: Callable[..., T | Awaitable[T]],
    *,
    metadata_aware: bool = False,
) -> KafkaPostgresEnvelopeDeserializer[T]:
    """Build the canonical Kafka payload+metadata envelope deserializer."""

    return KafkaPostgresEnvelopeDeserializer(inner, metadata_aware=metadata_aware)


def with_kafka_delivery_fields(
    row_mapper: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    delivery: KafkaPostgresDeliveryConfig | None = None,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Wrap a row mapper so Kafka delivery fields survive custom mapping."""

    resolved_delivery = delivery or KafkaPostgresDeliveryConfig()

    def _wrapped(record: dict[str, Any]) -> dict[str, Any]:
        row = dict(row_mapper(record))
        delivery_key = record.get(resolved_delivery.key_field)
        if delivery_key is not None and resolved_delivery.key_field not in row:
            row[resolved_delivery.key_field] = delivery_key

        metadata_field = resolved_delivery.metadata_field
        if metadata_field is not None:
            delivery_metadata = record.get(metadata_field)
            if delivery_metadata is not None and metadata_field not in row:
                row[metadata_field] = delivery_metadata
        return row

    return _wrapped


def build_kafka_postgres_sink(
    *,
    dsn: str | None = None,
    table: str,
    row_mapper: Callable[[dict[str, Any]], dict[str, Any]],
    conflict_key: str | list[str] | None = None,
    batch_size: int = 100,
    upsert: bool = True,
    insert_mode: str = "sql",
    pool_size: int = 1,
    max_rows_per_statement: int | None = None,
    max_parameters_per_statement: int = 32_000,
    retry_policy: Any | None = None,
    delivery: KafkaPostgresDeliveryConfig | None = None,
    connection: PostgresConnectionConfig | None = None,
) -> PostgresSink[dict[str, Any]]:
    """Build a Postgres sink with Kafka-idempotent defaults."""

    resolved_delivery = delivery or KafkaPostgresDeliveryConfig()
    resolved_conflict_key = cast(
        "str | list[str | QuotedIdentifier]",
        conflict_key or resolved_delivery.key_field,
    )
    return PostgresSink[dict[str, Any]](
        dsn=dsn,
        table=table,
        row_mapper=with_kafka_delivery_fields(row_mapper, delivery=resolved_delivery),
        conflict_key=resolved_conflict_key,
        batch_size=batch_size,
        upsert=upsert,
        insert_mode=insert_mode,  # type: ignore[arg-type]
        pool_size=pool_size,
        max_rows_per_statement=max_rows_per_statement,
        max_parameters_per_statement=max_parameters_per_statement,
        retry_policy=retry_policy,
        connection=connection,
    )


def build_kafka_postgres_source(
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
    poison_dlq: KafkaPostgresPoisonDLQConfig | None = None,
    health_snapshot_cache_ms: int = 250,
) -> KafkaSource[dict[str, object]]:
    """Build the canonical Kafka source for Kafka -> PostgreSQL wedges."""

    from agora_plugins.kafka import KafkaSource

    source_deserializer = (
        wrap_kafka_postgres_deserializer(
            deserializer,
            metadata_aware=deserializer_metadata_aware,
        )
        if include_metadata
        else deserializer
    )
    poison_record_sink = None
    poison_record_policy: KafkaPoisonRecordPolicy | str | None = None
    poison_record_pipeline_id: str | None = None
    poison_record_max_attempts: int | None = None
    if poison_dlq is not None:
        poison_record_sink = PostgresDLQSink(
            dsn=poison_dlq.dsn,
            table=poison_dlq.table,
            connection=poison_dlq.connection,
        )
        poison_record_policy = poison_dlq.policy
        poison_record_pipeline_id = poison_dlq.pipeline_id
        poison_record_max_attempts = poison_dlq.max_attempts

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
        poison_record_policy=poison_record_policy,
        poison_record_sink=poison_record_sink,
        poison_record_pipeline_id=poison_record_pipeline_id,
        poison_record_max_attempts=poison_record_max_attempts,
        health_snapshot_cache_ms=health_snapshot_cache_ms,
    )
    source._agora_postgres_poison_dlq_config = poison_dlq  # type: ignore[attr-defined]
    return source


def build_kafka_postgres_runtime(
    *,
    source: KafkaSource[T],
    dsn: str | None = None,
    table: str,
    transform: Callable[[T], dict[str, Any] | Awaitable[dict[str, Any]]] | None = None,
    row_mapper: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    conflict_key: str | list[str] | None = None,
    batch_size: int = 100,
    upsert: bool = True,
    insert_mode: str = "sql",
    pool_size: int = 1,
    max_rows_per_statement: int | None = None,
    max_parameters_per_statement: int = 32_000,
    retry_policy: Any | None = None,
    flush_each_record: bool = True,
    delivery: KafkaPostgresDeliveryConfig | None = None,
    connection: PostgresConnectionConfig | None = None,
) -> KafkaPostgresRuntime[T]:
    """Build the canonical Kafka -> PostgreSQL runtime with delivery-safe defaults."""

    sink = build_kafka_postgres_sink(
        dsn=dsn,
        table=table,
        row_mapper=(lambda row: row) if row_mapper is None else row_mapper,
        conflict_key=conflict_key,
        batch_size=batch_size,
        upsert=upsert,
        insert_mode=insert_mode,
        pool_size=pool_size,
        max_rows_per_statement=max_rows_per_statement,
        max_parameters_per_statement=max_parameters_per_statement,
        retry_policy=retry_policy,
        delivery=delivery,
        connection=connection,
    )
    return KafkaPostgresRuntime(
        source,
        sink,
        transform=transform,
        flush_each_record=flush_each_record,
        delivery=delivery,
    )


class KafkaPostgresRuntime(  # type: ignore[misc]
    KafkaBackendRuntimeObservabilityMixin[
        KafkaPostgresRuntimeHealthSnapshot,
        KafkaPostgresRuntimeMetricsSnapshot,
        KafkaPostgresEnterpriseAcceptanceThresholds,
        KafkaPostgresEnterpriseAcceptanceReport,
        PostgresSinkMetricsSnapshot,
    ],
    KafkaTransformSinkRuntime[T, dict[str, Any]],
):
    """Kafka -> PostgreSQL runtime that defaults to delivery-key injection."""

    def __init__(
        self,
        source: KafkaSource[T],
        sink: PostgresSink[dict[str, Any]],
        *,
        transform: Callable[[T], dict[str, Any] | Awaitable[dict[str, Any]]] | None = None,
        flush_each_record: bool = True,
        delivery: KafkaPostgresDeliveryConfig | None = None,
    ) -> None:
        resolved_delivery = delivery or KafkaPostgresDeliveryConfig()
        super().__init__(
            source,
            sink,
            transform=transform,
            flush_each_record=flush_each_record,
            delivery_metadata_field=resolved_delivery.metadata_field,
            delivery_key_field=resolved_delivery.key_field,
        )
        self.delivery = resolved_delivery
        self.poison_dlq = getattr(source, "_agora_postgres_poison_dlq_config", None)
        self._poison_record_sink = getattr(source, "_poison_record_sink", None)

    def sink_metrics(self) -> PostgresSinkMetricsSnapshot:
        return cast("PostgresSink[dict[str, Any]]", self.sink).metrics_snapshot()

    async def render_prometheus_metrics(self, namespace: str = "agora_kafka_postgres") -> str:
        return await KafkaPostgresPrometheusExporter(namespace=namespace).render_runtime(self)

    def _build_runtime_health_snapshot(
        self,
        *,
        source: KafkaSourceMetricsSnapshot,
        sink: PostgresSinkMetricsSnapshot,
    ) -> KafkaPostgresRuntimeHealthSnapshot:
        poison_dlq_enabled = self.poison_dlq is not None
        poison_dlq_ready = self._poison_dlq_ready()
        return KafkaPostgresRuntimeHealthSnapshot(
            ready=(
                source.health.ready
                and not source.health.stalled
                and sink.connection_ready
                and (poison_dlq_ready is not False)
            ),
            source_ready=source.health.ready,
            source_stalled=source.health.stalled,
            sink_connection_ready=sink.connection_ready,
            sink_write_safety_policy=sink.write_safety_policy,
            poison_dlq_enabled=poison_dlq_enabled,
            poison_dlq_ready=poison_dlq_ready,
        )

    def _build_runtime_observability_snapshot(
        self,
        *,
        health: KafkaPostgresRuntimeHealthSnapshot,
        source: KafkaSourceMetricsSnapshot,
        sink: PostgresSinkMetricsSnapshot,
    ) -> KafkaPostgresRuntimeMetricsSnapshot:
        poison_dlq = self.poison_dlq
        return KafkaPostgresRuntimeMetricsSnapshot(
            health=health,
            source=source,
            sink=sink,
            delivery_key_field=self.delivery.key_field,
            delivery_metadata_field=self.delivery.metadata_field,
            poison_dlq_enabled=poison_dlq is not None,
            poison_dlq_table=(None if poison_dlq is None else poison_dlq.table),
            poison_dlq_policy=(None if poison_dlq is None else str(poison_dlq.policy)),
            poison_dlq_pipeline_id=(None if poison_dlq is None else poison_dlq.pipeline_id),
        )

    def _evaluate_runtime_acceptance(
        self,
        *,
        snapshot: KafkaPostgresRuntimeMetricsSnapshot,
        thresholds: KafkaPostgresEnterpriseAcceptanceThresholds | None,
    ) -> KafkaPostgresEnterpriseAcceptanceReport:
        return KafkaPostgresEnterpriseAcceptanceGate(thresholds).evaluate(snapshot)

    def _poison_dlq_ready(self) -> bool | None:
        if self.poison_dlq is None:
            return None
        metrics_snapshot = getattr(self._poison_record_sink, "metrics_snapshot", None)
        if not callable(metrics_snapshot):
            return None
        snapshot = metrics_snapshot()
        connection_ready = getattr(snapshot, "connection_ready", None)
        table_ready = getattr(snapshot, "table_ready", None)
        if connection_ready is None:
            return None
        if table_ready is None:
            return bool(connection_ready)
        return bool(connection_ready) and bool(table_ready)


__all__ = [
    "KafkaPostgresDeliveryConfig",
    "KafkaPostgresEnterpriseAcceptanceFinding",
    "KafkaPostgresEnterpriseAcceptanceGate",
    "KafkaPostgresEnterpriseAcceptanceReport",
    "KafkaPostgresEnterpriseAcceptanceThresholds",
    "KafkaPostgresEnvelopeDeserializer",
    "KafkaPostgresPoisonDLQConfig",
    "KafkaPostgresPrometheusExporter",
    "KafkaPostgresRuntime",
    "KafkaPostgresRuntimeHealthSnapshot",
    "KafkaPostgresRuntimeMetricsSnapshot",
    "build_kafka_postgres_runtime",
    "build_kafka_postgres_sink",
    "build_kafka_postgres_source",
    "with_kafka_delivery_fields",
    "wrap_kafka_postgres_deserializer",
]
