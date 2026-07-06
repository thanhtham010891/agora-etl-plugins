"""Prometheus rendering for Kafka -> PostgreSQL runtimes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)

if TYPE_CHECKING:
    from agora_plugins.postgres._kafka_runtime_snapshots import KafkaPostgresRuntimeMetricsSnapshot


class KafkaPostgresPrometheusExporter:
    """Prometheus renderer for Kafka -> PostgreSQL helper runtimes."""

    def __init__(self, namespace: str = "agora_kafka_postgres") -> None:
        self._ns = namespace

    async def render_runtime(self, runtime: object) -> str:
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

        labels = _base_labels(snapshot)
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


def _base_labels(snapshot: KafkaPostgresRuntimeMetricsSnapshot) -> str:
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


__all__ = ["KafkaPostgresPrometheusExporter"]
