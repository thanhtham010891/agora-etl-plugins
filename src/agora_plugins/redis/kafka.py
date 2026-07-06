"""Kafka -> Redis runtime helpers with consistent enterprise observability."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, cast

from agora_plugins.kafka.runtime import (
    KafkaBackendRuntimeObservabilityMixin,
    KafkaTransformSinkRuntime,
)
from agora_plugins.redis._kafka_acceptance import (
    KafkaRedisEnterpriseAcceptanceFinding,
    KafkaRedisEnterpriseAcceptanceGate,
    KafkaRedisEnterpriseAcceptanceReport,
    KafkaRedisEnterpriseAcceptanceThresholds,
)
from agora_plugins.redis._kafka_builders import (
    build_kafka_redis_sink,
    build_kafka_redis_source,
)
from agora_plugins.redis._kafka_envelope import (
    KafkaRedisEnvelopeDeserializer,
    wrap_kafka_redis_deserializer,
)
from agora_plugins.redis._kafka_models import (
    KafkaRedisDeliveryConfig,
    KafkaRedisRuntimeHealthSnapshot,
    KafkaRedisRuntimeMetricsSnapshot,
    KafkaRedisStorageConfig,
)
from agora_plugins.redis._kafka_prometheus import (
    KafkaRedisPrometheusExporter,
)
from agora_plugins.redis.sinks.redis import RedisSink, RedisSinkMetricsSnapshot

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot
    from agora_plugins.kafka.sources.kafka import KafkaSource

T = TypeVar("T")


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
