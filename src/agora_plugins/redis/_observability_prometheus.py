"""Prometheus rendering for Redis plugin components."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)

if TYPE_CHECKING:
    from agora_plugins.redis._observability_snapshots import (
        RedisDLQSinkMetricsSnapshot,
        RedisDLQSourceMetricsSnapshot,
        RedisStreamSourceMetricsSnapshot,
    )
    from agora_plugins.redis.sinks.redis import RedisSinkMetricsSnapshot


def age_ms(value: datetime | None) -> float | None:
    if value is None:
        return None
    return max((datetime.now(UTC) - value).total_seconds() * 1000.0, 0.0)


class RedisPrometheusExporter:
    """Prometheus renderer for Redis plugin components."""

    def __init__(self, namespace: str = "agora_redis") -> None:
        self._ns = namespace

    def render_source(self, snapshot: RedisStreamSourceMetricsSnapshot) -> str:
        labels = self._source_labels(snapshot)
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Redis stream source readiness state",
            metric_type="gauge",
            name=f"{self._ns}_source_state",
        )
        for state_name, value in (
            ("ready", int(snapshot.health.ready)),
            ("connection_ready", int(snapshot.health.connection_ready)),
            ("group_ready", int(snapshot.health.group_ready)),
            ("ack_enabled", int(snapshot.health.ack_enabled)),
            ("reclaim_enabled", int(snapshot.health.reclaim_enabled)),
            ("poison_loop_risk", int(snapshot.poison_loop_risk.detected)),
        ):
            lines.append(f'{self._ns}_source_state{{{labels},state="{state_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Redis stream source monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_source_events_total",
        )
        for event_name, value in (
            ("read_call", snapshot.read_call_count),
            ("reclaimed_message", snapshot.reclaimed_message_count),
            ("reclaim_fairness_yield", snapshot.reclaim_fairness_yield_count),
            ("reconnect", snapshot.reconnect_count),
            ("ack_flush", snapshot.ack_flush_count),
            ("acked_message", snapshot.acked_message_count),
            ("emitted_record", snapshot.emitted_record_count),
            ("record_error", snapshot.record_error_count),
            ("record_drop", snapshot.record_drop_count),
            ("poison_loop", snapshot.poison_loop_risk.loop_count),
        ):
            lines.append(f'{self._ns}_source_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Redis stream source gauges",
            metric_type="gauge",
            name=f"{self._ns}_source_gauge",
        )
        for gauge_name, value in (
            ("block_ms", snapshot.block_ms),
            ("batch_size", snapshot.batch_size),
            ("ack_batch_size", snapshot.ack_batch_size),
            ("reclaim_batch_size", snapshot.reclaim_batch_size),
            ("consecutive_reclaim_batch_count", snapshot.consecutive_reclaim_batch_count),
            ("pending_ack_count", snapshot.pending_ack_count),
            ("poison_loop_message_count", snapshot.poison_loop_risk.distinct_message_count),
        ):
            lines.append(f'{self._ns}_source_gauge{{{labels},gauge="{gauge_name}"}} {value}')
        if snapshot.reclaim_idle_ms is not None:
            lines.append(
                f'{self._ns}_source_gauge{{{labels},gauge="reclaim_idle_ms"}} '
                f"{snapshot.reclaim_idle_ms}"
            )
        if snapshot.max_consecutive_reclaim_batches is not None:
            lines.append(
                f'{self._ns}_source_gauge{{{labels},gauge="max_consecutive_reclaim_batches"}} '
                f"{snapshot.max_consecutive_reclaim_batches}"
            )

        age_lines: list[str] = []
        for activity_name, timestamp in (
            ("read", snapshot.last_read_at),
            ("reconnect", snapshot.last_reconnect_at),
            ("ack", snapshot.last_ack_at),
            ("reclaim", snapshot.last_reclaim_at),
            ("error", snapshot.last_error_at),
            ("poison_loop", snapshot.poison_loop_risk.last_detected_at),
        ):
            last_age_ms = age_ms(timestamp)
            if last_age_ms is not None:
                age_lines.append(
                    f'{self._ns}_source_age_ms{{{labels},activity="{activity_name}"}} {last_age_ms:.6f}'
                )
        if age_lines:
            append_metric_header(
                lines,
                help_text="Redis stream source last-activity age in milliseconds",
                metric_type="gauge",
                name=f"{self._ns}_source_age_ms",
            )
            lines.extend(age_lines)

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_sink(self, snapshot: RedisSinkMetricsSnapshot) -> str:
        labels = self._sink_labels(snapshot)
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Redis sink readiness state",
            metric_type="gauge",
            name=f"{self._ns}_sink_state",
        )
        lines.append(
            f'{self._ns}_sink_state{{{labels},state="connection_ready"}} '
            f"{int(snapshot.connection_ready)}"
        )

        append_metric_header(
            lines,
            help_text="Redis sink monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_sink_events_total",
        )
        for event_name, value in (
            ("write_call", snapshot.write_call_count),
            ("write_batch_call", snapshot.write_batch_call_count),
            ("direct_write", snapshot.direct_write_count),
            ("mset_batch", snapshot.mset_batch_count),
            ("pipeline_execute", snapshot.pipeline_execute_count),
            ("written_record", snapshot.written_record_count),
            ("accepted_record", snapshot.accepted_record_count),
            ("redis_mutation", snapshot.redis_mutation_count),
        ):
            lines.append(f'{self._ns}_sink_events_total{{{labels},event="{event_name}"}} {value}')

        gauge_lines: list[str] = []
        if snapshot.ttl_seconds is not None:
            gauge_lines.append(
                f'{self._ns}_sink_gauge{{{labels},gauge="ttl_seconds"}} {snapshot.ttl_seconds}'
            )
        if snapshot.maxlen is not None:
            gauge_lines.append(
                f'{self._ns}_sink_gauge{{{labels},gauge="maxlen"}} {snapshot.maxlen}'
            )
        if gauge_lines:
            append_metric_header(
                lines,
                help_text="Redis sink gauges",
                metric_type="gauge",
                name=f"{self._ns}_sink_gauge",
            )
            lines.extend(gauge_lines)

        last_write_age_ms = age_ms(snapshot.last_write_at)
        if last_write_age_ms is not None:
            append_metric_header(
                lines,
                help_text="Redis sink last-write age in milliseconds",
                metric_type="gauge",
                name=f"{self._ns}_sink_age_ms",
            )
            lines.append(
                f'{self._ns}_sink_age_ms{{{labels},activity="write"}} {last_write_age_ms:.6f}'
            )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_dlq_sink(self, snapshot: RedisDLQSinkMetricsSnapshot) -> str:
        labels = f'key_prefix="{escape_label_value(snapshot.key_prefix)}"'
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Redis DLQ sink readiness state",
            metric_type="gauge",
            name=f"{self._ns}_dlq_sink_state",
        )
        lines.append(
            f'{self._ns}_dlq_sink_state{{{labels},state="connection_ready"}} '
            f"{int(snapshot.connection_ready)}"
        )
        append_metric_header(
            lines,
            help_text="Redis DLQ sink monotonic counters",
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
        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_dlq_source(self, snapshot: RedisDLQSourceMetricsSnapshot) -> str:
        labels = [
            f'key_prefix="{escape_label_value(snapshot.key_prefix)}"',
            f'pipeline_id="{escape_label_value(snapshot.pipeline_id or "")}"',
            f'stage="{escape_label_value(snapshot.stage or "")}"',
        ]
        rendered_labels = ",".join(labels)
        lines: list[str] = []
        append_metric_header(
            lines,
            help_text="Redis DLQ source readiness state",
            metric_type="gauge",
            name=f"{self._ns}_dlq_source_state",
        )
        lines.append(
            f'{self._ns}_dlq_source_state{{{rendered_labels},state="connection_ready"}} '
            f"{int(snapshot.connection_ready)}"
        )
        append_metric_header(
            lines,
            help_text="Redis DLQ source monotonic counters",
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
        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def _source_labels(self, snapshot: RedisStreamSourceMetricsSnapshot) -> str:
        return ",".join(
            [
                f'stream="{escape_label_value(snapshot.stream)}"',
                f'group="{escape_label_value(snapshot.group)}"',
                f'consumer="{escape_label_value(snapshot.consumer)}"',
            ]
        )

    def _sink_labels(self, snapshot: RedisSinkMetricsSnapshot) -> str:
        return ",".join(
            [
                f'target="{escape_label_value(snapshot.target)}"',
                f'mode="{escape_label_value(snapshot.mode)}"',
            ]
        )
