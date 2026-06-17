"""Kafka source health metrics snapshots and Prometheus rendering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    extend_lines,
    render_scrape_time_line,
)

if TYPE_CHECKING:
    from agora.core.source import SourceRuntimeMetrics

    from agora_plugins.kafka.runtime import KafkaSourceRuntime, KafkaTransformSinkRuntime
    from agora_plugins.kafka.sources.kafka import (
        KafkaPartitionHealth,
        KafkaSourceHealthSnapshot,
        KafkaSourceOperationalMetrics,
    )


@dataclass(frozen=True, slots=True)
class KafkaSourceMetricsSnapshot:
    """Combined source-side Kafka observability snapshot."""

    health: KafkaSourceHealthSnapshot
    operational: KafkaSourceOperationalMetrics
    runtime: SourceRuntimeMetrics

    def to_dict(self) -> dict[str, Any]:
        return {
            "health": self.health.to_dict(),
            "operational": self.operational.to_dict(),
            "runtime": self.runtime.to_dict(),
        }


class KafkaSourcePrometheusExporter:
    """Zero-dependency Prometheus text renderer for Kafka source snapshots."""

    def __init__(self, namespace: str = "agora_kafka") -> None:
        self._ns = namespace

    async def render_runtime(
        self,
        runtime: KafkaSourceRuntime[Any] | KafkaTransformSinkRuntime[Any, Any],
    ) -> str:
        return self.render(await runtime.metrics_snapshot())

    def render(self, snapshot: KafkaSourceMetricsSnapshot) -> str:
        labels = self._base_labels(snapshot.health)
        lines: list[str] = []
        ns = self._ns

        append_metric_header(
            lines,
            help_text="Kafka source readiness and lifecycle state",
            metric_type="gauge",
            name=f"{ns}_source_state",
        )
        for state_name, value in (
            ("ready", int(snapshot.health.ready)),
            ("stalled", int(snapshot.health.stalled)),
        ):
            lines.append(f'{ns}_source_state{{{labels},state="{state_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka source gauge values",
            metric_type="gauge",
            name=f"{ns}_source_gauge",
        )
        gauge_metrics: tuple[tuple[str, int | None], ...] = (
            ("assignment_count", snapshot.health.assignment_count),
            ("paused_partition_count", snapshot.health.paused_partition_count),
            ("pending_commit_count", snapshot.health.pending_commit_count),
            ("idle_poll_count", snapshot.health.idle_poll_count),
            ("total_lag", snapshot.health.total_lag),
            ("lagging_partition_count", snapshot.health.lagging_partition_count),
            ("max_lag", snapshot.health.max_lag),
            ("total_commit_lag", snapshot.health.total_commit_lag),
            ("max_commit_lag", snapshot.health.max_commit_lag),
            (
                "manual_assign_partition_count",
                snapshot.operational.manual_assign_partition_count,
            ),
        )
        for gauge_name, gauge_value in gauge_metrics:
            if gauge_value is None:
                continue
            lines.append(f'{ns}_source_gauge{{{labels},gauge="{gauge_name}"}} {gauge_value}')

        append_metric_header(
            lines,
            help_text="Kafka source monotonic event counters",
            metric_type="counter",
            name=f"{ns}_source_events_total",
        )
        for event_name, value in (
            ("rebalance", snapshot.health.rebalance_count),
            (
                "batch_deserialize_error",
                snapshot.operational.batch_deserialize_error_count,
            ),
            ("poison_dlq_write", snapshot.operational.poison_record_dlq_write_count),
            (
                "poison_dlq_write_failure",
                snapshot.operational.poison_record_dlq_write_failure_count,
            ),
            ("poison_log_only", snapshot.operational.poison_record_log_only_count),
            ("poison_fail_closed", snapshot.operational.poison_record_fail_closed_count),
            (
                "poison_classification_deserialization",
                snapshot.operational.poison_record_deserialization_count,
            ),
            (
                "poison_classification_schema_evolution",
                snapshot.operational.poison_record_schema_evolution_count,
            ),
            (
                "poison_classification_schema_validation",
                snapshot.operational.poison_record_schema_validation_count,
            ),
            (
                "poison_classification_schema_registry_binding_mismatch",
                snapshot.operational.poison_record_schema_registry_binding_mismatch_count,
            ),
            (
                "poison_classification_unknown",
                snapshot.operational.poison_record_unknown_count,
            ),
            ("record_error", snapshot.runtime.record_error_count),
            ("record_drop", snapshot.runtime.record_drop_count),
        ):
            lines.append(f'{ns}_source_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka source last activity age in milliseconds",
            metric_type="gauge",
            name=f"{ns}_source_age_ms",
        )
        age_metrics: tuple[tuple[str, float | None], ...] = (
            ("poll", snapshot.health.last_poll_age_ms),
            ("message", snapshot.health.last_message_age_ms),
            ("commit", snapshot.health.last_commit_age_ms),
            ("rebalance", snapshot.health.last_rebalance_age_ms),
        )
        for age_name, age_value in age_metrics:
            if age_value is None:
                continue
            lines.append(f'{ns}_source_age_ms{{{labels},activity="{age_name}"}} {age_value:.6f}')

        append_metric_header(
            lines,
            help_text="Kafka source per-partition lag and offsets",
            metric_type="gauge",
            name=f"{ns}_partition_gauge",
        )
        for partition in snapshot.health.partitions:
            extend_lines(lines, self._render_partition_metrics(snapshot.health, partition))

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def _base_labels(self, health: KafkaSourceHealthSnapshot) -> str:
        return ",".join(
            [
                f'consumer_group="{escape_label_value(health.consumer_group)}"',
                f'bootstrap_servers="{escape_label_value(health.bootstrap_servers)}"',
                f'subscription_mode="{escape_label_value(health.subscription_mode)}"',
            ]
        )

    def _render_partition_metrics(
        self,
        health: KafkaSourceHealthSnapshot,
        partition: KafkaPartitionHealth,
    ) -> list[str]:
        labels = ",".join(
            [
                self._base_labels(health),
                f'topic="{escape_label_value(partition.topic)}"',
                f'partition="{partition.partition}"',
            ]
        )
        metrics: list[str] = [
            f'{self._ns}_partition_gauge{{{labels},gauge="paused"}} {int(partition.paused)}'
        ]
        if partition.current_offset is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="current_offset"}} '
                f"{partition.current_offset}"
            )
        if partition.committed_offset is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="committed_offset"}} '
                f"{partition.committed_offset}"
            )
        if partition.processed_offset is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="processed_offset"}} '
                f"{partition.processed_offset}"
            )
        if partition.committable_offset is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="committable_offset"}} '
                f"{partition.committable_offset}"
            )
        if partition.end_offset is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="end_offset"}} {partition.end_offset}'
            )
        if partition.lag is not None:
            metrics.append(f'{self._ns}_partition_gauge{{{labels},gauge="lag"}} {partition.lag}')
        if partition.commit_lag is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="commit_lag"}} {partition.commit_lag}'
            )
        if partition.delivery_gap is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="delivery_gap"}} '
                f"{partition.delivery_gap}"
            )
        if partition.commit_gap is not None:
            metrics.append(
                f'{self._ns}_partition_gauge{{{labels},gauge="commit_gap"}} {partition.commit_gap}'
            )
        return metrics


__all__ = [
    "KafkaSourceMetricsSnapshot",
    "KafkaSourcePrometheusExporter",
]
