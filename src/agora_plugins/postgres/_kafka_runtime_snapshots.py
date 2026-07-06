"""Snapshot models for Kafka -> PostgreSQL runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.health import ComponentHealthSnapshot

if TYPE_CHECKING:
    from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot
    from agora_plugins.postgres.sinks._metrics import PostgresSinkMetricsSnapshot


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


__all__ = [
    "KafkaPostgresRuntimeHealthSnapshot",
    "KafkaPostgresRuntimeMetricsSnapshot",
]
