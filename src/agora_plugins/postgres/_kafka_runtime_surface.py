"""Operator surface for Kafka -> PostgreSQL runtimes."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from agora_plugins.postgres._kafka_runtime_acceptance import (
    KafkaPostgresEnterpriseAcceptanceGate,
    KafkaPostgresEnterpriseAcceptanceReport,
    KafkaPostgresEnterpriseAcceptanceThresholds,
)
from agora_plugins.postgres._kafka_runtime_prometheus import KafkaPostgresPrometheusExporter
from agora_plugins.postgres._kafka_runtime_snapshots import (
    KafkaPostgresRuntimeHealthSnapshot,
    KafkaPostgresRuntimeMetricsSnapshot,
)

if TYPE_CHECKING:
    from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot
    from agora_plugins.postgres.sinks._metrics import PostgresSinkMetricsSnapshot


class KafkaPostgresRuntimeOperatorSurface:
    """Public-facing runtime supportability surface for Kafka -> PostgreSQL wedges."""

    def __init__(self, runtime: object) -> None:
        self._runtime = runtime

    def sink_metrics(self) -> PostgresSinkMetricsSnapshot:
        return cast("PostgresSinkMetricsSnapshot", self._runtime.sink.metrics_snapshot())

    async def render_prometheus_metrics(self, namespace: str = "agora_kafka_postgres") -> str:
        return await KafkaPostgresPrometheusExporter(namespace=namespace).render_runtime(
            self._runtime
        )

    def build_runtime_health_snapshot(
        self,
        *,
        source: KafkaSourceMetricsSnapshot,
        sink: PostgresSinkMetricsSnapshot,
    ) -> KafkaPostgresRuntimeHealthSnapshot:
        poison_dlq_enabled = self._runtime.poison_dlq is not None
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

    def build_runtime_observability_snapshot(
        self,
        *,
        health: KafkaPostgresRuntimeHealthSnapshot,
        source: KafkaSourceMetricsSnapshot,
        sink: PostgresSinkMetricsSnapshot,
    ) -> KafkaPostgresRuntimeMetricsSnapshot:
        poison_dlq = self._runtime.poison_dlq
        return KafkaPostgresRuntimeMetricsSnapshot(
            health=health,
            source=source,
            sink=sink,
            delivery_key_field=self._runtime.delivery.key_field,
            delivery_metadata_field=self._runtime.delivery.metadata_field,
            poison_dlq_enabled=poison_dlq is not None,
            poison_dlq_table=(None if poison_dlq is None else poison_dlq.table),
            poison_dlq_policy=(None if poison_dlq is None else str(poison_dlq.policy)),
            poison_dlq_pipeline_id=(None if poison_dlq is None else poison_dlq.pipeline_id),
        )

    def evaluate_runtime_acceptance(
        self,
        *,
        snapshot: KafkaPostgresRuntimeMetricsSnapshot,
        thresholds: KafkaPostgresEnterpriseAcceptanceThresholds | None,
    ) -> KafkaPostgresEnterpriseAcceptanceReport:
        return KafkaPostgresEnterpriseAcceptanceGate(thresholds).evaluate(snapshot)

    def _poison_dlq_ready(self) -> bool | None:
        poison_dlq = self._runtime.poison_dlq
        if poison_dlq is None:
            return None
        metrics_snapshot = getattr(
            getattr(self._runtime.source, "_poison_record_sink", None), "metrics_snapshot", None
        )
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


__all__ = ["KafkaPostgresRuntimeOperatorSurface"]
