"""Kafka -> Redis runtime contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.health import ComponentHealthSnapshot

if TYPE_CHECKING:
    from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot
    from agora_plugins.redis.sinks.redis import RedisSinkMetricsSnapshot


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

    def to_dict(self) -> dict[str, object]:
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

    def to_dict(self) -> dict[str, object]:
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
