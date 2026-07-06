"""Prometheus exporter for PostgreSQL plugin observability surfaces."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)

if TYPE_CHECKING:
    from agora_plugins.postgres._observability_snapshots import (
        PostgresDLQSinkMetricsSnapshot,
        PostgresDLQSourceMetricsSnapshot,
        PostgresSourceMetricsSnapshot,
    )
    from agora_plugins.postgres.sinks._metrics import PostgresSinkMetricsSnapshot


class PostgresPrometheusExporter:
    """Render Prometheus metrics for PostgreSQL plugin component snapshots."""

    def __init__(self, namespace: str = "agora_postgres") -> None:
        self._ns = namespace

    def render_source(self, snapshot: PostgresSourceMetricsSnapshot) -> str:
        labels = self._source_labels(snapshot)
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Postgres source recovery contract and boolean config",
            metric_type="gauge",
            name=f"{self._ns}_source_config",
        )
        for config_name, value in (
            ("ready", int(snapshot.ready)),
            ("connection_ready", int(snapshot.connection_ready)),
            ("routing_ready", int(snapshot.routing_ready)),
            ("staleness_guard_ready", int(snapshot.staleness_guard_ready)),
            ("supports_checkpoint", int(snapshot.supports_checkpoint)),
            ("row_mapper_accepts_context", int(snapshot.row_mapper_accepts_context)),
            ("last_checkpoint_cursor_present", int(snapshot.last_checkpoint_cursor_present)),
            ("server_side_cursor_withhold", int(snapshot.server_side_cursor_withhold)),
            ("replica_role_primary", int(snapshot.connected_server_role == "primary")),
            ("replica_role_standby", int(snapshot.connected_server_role == "standby")),
            (
                "requires_pipeline_rerun",
                int(snapshot.recovery_contract.requires_pipeline_rerun),
            ),
            ("transparent_failover", int(snapshot.recovery_contract.transparent_failover)),
        ):
            lines.append(f'{self._ns}_source_config{{{labels},config="{config_name}"}} {value}')
        if snapshot.transaction_read_only is not None:
            lines.append(
                f'{self._ns}_source_config{{{labels},config="transaction_read_only"}} '
                f"{int(snapshot.transaction_read_only)}"
            )

        append_metric_header(
            lines,
            help_text="Postgres source monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_source_events_total",
        )
        for event_name, value in (
            ("stream_run", snapshot.stream_run_count),
            ("query_execution", snapshot.query_execution_count),
            ("resume_prepare", snapshot.resume_prepare_count),
            ("resume_checkpoint_apply", snapshot.resume_checkpoint_apply_count),
        ):
            lines.append(f'{self._ns}_source_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Postgres source gauges",
            metric_type="gauge",
            name=f"{self._ns}_source_gauge",
        )
        lines.append(
            f'{self._ns}_source_gauge{{{labels},gauge="active_stream_count"}} '
            f"{snapshot.active_stream_count}"
        )
        lines.append(
            f'{self._ns}_source_gauge{{{labels},gauge="batch_size"}} {snapshot.batch_size}'
        )
        for gauge_name, value in (
            ("rows_seen_current_run", snapshot.rows_seen),
            ("retry_count_current_run", snapshot.retry_count),
            ("staleness_guard_block_count_current_run", snapshot.staleness_guard_block_count),
            (
                "staleness_guard_primary_fallback_count_current_run",
                snapshot.staleness_guard_primary_fallback_count,
            ),
            ("record_error_count_current_run", snapshot.record_error_count),
            ("record_drop_count_current_run", snapshot.record_drop_count),
        ):
            lines.append(f'{self._ns}_source_gauge{{{labels},gauge="{gauge_name}"}} {value}')
        if snapshot.statement_timeout_ms is not None:
            lines.append(
                f'{self._ns}_source_gauge{{{labels},gauge="statement_timeout_ms"}} '
                f"{snapshot.statement_timeout_ms}"
            )
        if snapshot.max_replica_replay_lag_s is not None:
            lines.append(
                f'{self._ns}_source_gauge{{{labels},gauge="max_replica_replay_lag_s"}} '
                f"{snapshot.max_replica_replay_lag_s}"
            )
        if snapshot.last_replica_replay_lag_s is not None:
            lines.append(
                f'{self._ns}_source_gauge{{{labels},gauge="last_replica_replay_lag_s"}} '
                f"{snapshot.last_replica_replay_lag_s}"
            )
        if snapshot.last_stream_succeeded is not None:
            lines.append(
                f'{self._ns}_source_gauge{{{labels},gauge="last_stream_succeeded"}} '
                f"{int(snapshot.last_stream_succeeded)}"
            )

        append_metric_header(
            lines,
            help_text="Postgres source last-activity age in milliseconds",
            metric_type="gauge",
            name=f"{self._ns}_source_age_ms",
        )
        for activity_name, timestamp in (
            ("stream_started", snapshot.last_stream_started_at),
            ("stream_completed", snapshot.last_stream_completed_at),
            ("row", snapshot.last_row_at),
            ("health_check", snapshot.last_health_checked_at),
        ):
            age = _age_ms(timestamp)
            if age is not None:
                lines.append(
                    f'{self._ns}_source_age_ms{{{labels},activity="{activity_name}"}} {age:.6f}'
                )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_sink(self, snapshot: PostgresSinkMetricsSnapshot) -> str:
        labels = self._sink_labels(snapshot)
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Postgres sink config and readiness",
            metric_type="gauge",
            name=f"{self._ns}_sink_config",
        )
        for config_name, value in (
            ("connection_ready", int(snapshot.connection_ready)),
            ("upsert", int(snapshot.upsert)),
        ):
            lines.append(f'{self._ns}_sink_config{{{labels},config="{config_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Postgres sink gauges",
            metric_type="gauge",
            name=f"{self._ns}_sink_gauge",
        )
        for gauge_name, gauge_value in (
            ("buffered_row_count", snapshot.buffered_row_count),
            ("batch_size", snapshot.batch_size),
            ("pool_size", snapshot.pool_size),
            ("pooled_connection_count", snapshot.pooled_connection_count),
            ("pooled_available_count", snapshot.pooled_available_count),
            ("max_parameters_per_statement", snapshot.max_parameters_per_statement),
        ):
            lines.append(f'{self._ns}_sink_gauge{{{labels},gauge="{gauge_name}"}} {gauge_value}')
        if snapshot.max_rows_per_statement is not None:
            lines.append(
                f'{self._ns}_sink_gauge{{{labels},gauge="max_rows_per_statement"}} '
                f"{snapshot.max_rows_per_statement}"
            )

        append_metric_header(
            lines,
            help_text="Postgres sink monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_sink_events_total",
        )
        for event_name, value in (
            ("write_call", snapshot.write_call_count),
            ("write_batch_call", snapshot.write_batch_call_count),
            ("enqueue", snapshot.enqueue_count),
            ("flush", snapshot.flush_count),
            ("flushed_row", snapshot.flushed_row_count),
            ("retry", snapshot.retry_count),
            ("schema_refresh", snapshot.schema_refresh_count),
            ("schema_drift_detected", snapshot.schema_drift_detected_count),
            ("schema_drift_aligned", snapshot.schema_drift_aligned_count),
            ("poison_record", snapshot.poison_record_count),
            ("poison_record_schema_drift", snapshot.poison_record_schema_drift_count),
            (
                "poison_record_constraint_violation",
                snapshot.poison_record_constraint_violation_count,
            ),
            ("poison_record_type_mismatch", snapshot.poison_record_type_mismatch_count),
            ("poison_record_unknown", snapshot.poison_record_unknown_count),
        ):
            lines.append(f'{self._ns}_sink_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Postgres sink last-flush age in milliseconds",
            metric_type="gauge",
            name=f"{self._ns}_sink_age_ms",
        )
        last_flush_age = _age_ms(snapshot.last_flush_at)
        if last_flush_age is not None:
            lines.append(
                f'{self._ns}_sink_age_ms{{{labels},activity="flush"}} {last_flush_age:.6f}'
            )

        if snapshot.latency_histograms:
            append_metric_header(
                lines,
                help_text="Postgres sink operation latency in seconds",
                metric_type="histogram",
                name=f"{self._ns}_sink_latency_seconds",
            )
            for histogram in snapshot.latency_histograms:
                histogram_labels = (
                    f'{labels},operation="{escape_label_value(histogram.operation)}",'
                    f'outcome="{escape_label_value(histogram.outcome)}"'
                )
                for upper_bound_s, count in histogram.buckets:
                    lines.append(
                        f"{self._ns}_sink_latency_seconds_bucket"
                        f'{{{histogram_labels},le="{upper_bound_s:g}"}} {count}'
                    )
                lines.append(
                    f"{self._ns}_sink_latency_seconds_bucket"
                    f'{{{histogram_labels},le="+Inf"}} {histogram.count}'
                )
                lines.append(
                    f"{self._ns}_sink_latency_seconds_count{{{histogram_labels}}} {histogram.count}"
                )
                lines.append(
                    f"{self._ns}_sink_latency_seconds_sum"
                    f"{{{histogram_labels}}} {histogram.sum_s:.9f}"
                )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_dlq_sink(self, snapshot: PostgresDLQSinkMetricsSnapshot) -> str:
        labels = f'table="{escape_label_value(snapshot.table)}"'
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Postgres DLQ sink config and readiness",
            metric_type="gauge",
            name=f"{self._ns}_dlq_sink_config",
        )
        for config_name, value in (
            ("connection_ready", int(snapshot.connection_ready)),
            ("table_ready", int(snapshot.table_ready)),
        ):
            lines.append(f'{self._ns}_dlq_sink_config{{{labels},config="{config_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Postgres DLQ sink monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_dlq_sink_events_total",
        )
        for event_name, value in (
            ("write_call", snapshot.write_call_count),
            ("write_batch_call", snapshot.write_batch_call_count),
            ("inserted_record", snapshot.inserted_record_count),
            ("upserted_record", snapshot.upserted_record_count),
            ("updated_record", snapshot.updated_record_count),
            ("replay", snapshot.replay_count),
            ("replayed_record", snapshot.replayed_record_count),
            ("acknowledge", snapshot.acknowledge_count),
            ("acknowledged_record", snapshot.acknowledged_record_count),
        ):
            lines.append(
                f'{self._ns}_dlq_sink_events_total{{{labels},event="{event_name}"}} {value}'
            )

        append_metric_header(
            lines,
            help_text="Postgres DLQ sink last-activity age in milliseconds",
            metric_type="gauge",
            name=f"{self._ns}_dlq_sink_age_ms",
        )
        for activity_name, timestamp in (
            ("write", snapshot.last_write_at),
            ("replay", snapshot.last_replay_at),
            ("acknowledge", snapshot.last_acknowledge_at),
        ):
            age = _age_ms(timestamp)
            if age is not None:
                lines.append(
                    f'{self._ns}_dlq_sink_age_ms{{{labels},activity="{activity_name}"}} {age:.6f}'
                )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_dlq_source(self, snapshot: PostgresDLQSourceMetricsSnapshot) -> str:
        labels = [
            f'table="{escape_label_value(snapshot.table)}"',
            f'pipeline_id="{escape_label_value(snapshot.pipeline_id or "")}"',
            f'stage="{escape_label_value(snapshot.stage or "")}"',
        ]
        rendered_labels = ",".join(labels)
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Postgres DLQ source config and readiness",
            metric_type="gauge",
            name=f"{self._ns}_dlq_source_config",
        )
        lines.append(
            f'{self._ns}_dlq_source_config{{{rendered_labels},config="connection_ready"}} '
            f"{int(snapshot.connection_ready)}"
        )
        if snapshot.limit is not None:
            lines.append(
                f'{self._ns}_dlq_source_config{{{rendered_labels},config="limit"}} {snapshot.limit}'
            )

        append_metric_header(
            lines,
            help_text="Postgres DLQ source monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_dlq_source_events_total",
        )
        for event_name, value in (
            ("scan", snapshot.scan_count),
            ("emitted_record", snapshot.emitted_record_count),
        ):
            lines.append(
                f'{self._ns}_dlq_source_events_total{{{rendered_labels},event="{event_name}"}} {value}'
            )

        append_metric_header(
            lines,
            help_text="Postgres DLQ source last-activity age in milliseconds",
            metric_type="gauge",
            name=f"{self._ns}_dlq_source_age_ms",
        )
        for activity_name, timestamp in (
            ("scan", snapshot.last_scan_at),
            ("record", snapshot.last_record_at),
        ):
            age = _age_ms(timestamp)
            if age is not None:
                lines.append(
                    f'{self._ns}_dlq_source_age_ms{{{rendered_labels},activity="{activity_name}"}} {age:.6f}'
                )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def _source_labels(self, snapshot: PostgresSourceMetricsSnapshot) -> str:
        contract = snapshot.recovery_contract
        return ",".join(
            (
                f'recovery_mode="{escape_label_value(contract.mode.value)}"',
                f'on_record_error="{escape_label_value(contract.on_record_error)}"',
                f'fetch_strategy="{escape_label_value(snapshot.fetch_strategy)}"',
                f'read_routing="{escape_label_value(snapshot.read_routing)}"',
                f'target_session_attrs="{escape_label_value(snapshot.target_session_attrs)}"',
                f'on_replica_stale="{escape_label_value(snapshot.on_replica_stale)}"',
                (
                    'transaction_isolation_level="'
                    f'{escape_label_value(snapshot.transaction_isolation_level or "none")}"'
                ),
            )
        )

    def _sink_labels(self, snapshot: PostgresSinkMetricsSnapshot) -> str:
        return ",".join(
            (
                f'table="{escape_label_value(snapshot.table)}"',
                f'insert_mode="{escape_label_value(snapshot.insert_mode)}"',
                f'write_safety_policy="{escape_label_value(snapshot.write_safety_policy)}"',
            )
        )


def _age_ms(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return max((datetime.now(UTC) - timestamp).total_seconds() * 1000.0, 0.0)


__all__ = ["PostgresPrometheusExporter"]
