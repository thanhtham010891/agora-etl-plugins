"""Kafka -> PostgreSQL runtime helpers with safer delivery defaults."""

from __future__ import annotations

from dataclasses import dataclass
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora_plugins.kafka.runtime import (
    KafkaBackendRuntimeObservabilityMixin,
    KafkaTransformSinkRuntime,
)
from agora_plugins.postgres._kafka_runtime_observability import (
    KafkaPostgresEnterpriseAcceptanceFinding,
    KafkaPostgresEnterpriseAcceptanceGate,
    KafkaPostgresEnterpriseAcceptanceReport,
    KafkaPostgresEnterpriseAcceptanceThresholds,
    KafkaPostgresPrometheusExporter,
    KafkaPostgresRuntimeHealthSnapshot,
    KafkaPostgresRuntimeMetricsSnapshot,
    KafkaPostgresRuntimeOperatorSurface,
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
    conflict_keys = (
        (resolved_conflict_key,)
        if isinstance(resolved_conflict_key, (str, QuotedIdentifier))
        else tuple(resolved_conflict_key)
    )
    replay_safe_delivery_contract = upsert and resolved_delivery.key_field in conflict_keys
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
        replay_safe_key_contract=replay_safe_delivery_contract,
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
        self._operator_surface = KafkaPostgresRuntimeOperatorSurface(self)

    def sink_metrics(self) -> PostgresSinkMetricsSnapshot:
        return self._operator_surface.sink_metrics()

    async def render_prometheus_metrics(self, namespace: str = "agora_kafka_postgres") -> str:
        return await self._operator_surface.render_prometheus_metrics(namespace=namespace)

    def _build_runtime_health_snapshot(
        self,
        *,
        source: KafkaSourceMetricsSnapshot,
        sink: PostgresSinkMetricsSnapshot,
    ) -> KafkaPostgresRuntimeHealthSnapshot:
        return self._operator_surface.build_runtime_health_snapshot(
            source=source,
            sink=sink,
        )

    def _build_runtime_observability_snapshot(
        self,
        *,
        health: KafkaPostgresRuntimeHealthSnapshot,
        source: KafkaSourceMetricsSnapshot,
        sink: PostgresSinkMetricsSnapshot,
    ) -> KafkaPostgresRuntimeMetricsSnapshot:
        return self._operator_surface.build_runtime_observability_snapshot(
            health=health,
            source=source,
            sink=sink,
        )

    def _evaluate_runtime_acceptance(
        self,
        *,
        snapshot: KafkaPostgresRuntimeMetricsSnapshot,
        thresholds: KafkaPostgresEnterpriseAcceptanceThresholds | None,
    ) -> KafkaPostgresEnterpriseAcceptanceReport:
        return self._operator_surface.evaluate_runtime_acceptance(
            snapshot=snapshot,
            thresholds=thresholds,
        )


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
