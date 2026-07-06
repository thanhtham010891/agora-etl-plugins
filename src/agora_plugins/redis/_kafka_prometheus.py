"""Prometheus rendering for Kafka -> Redis runtimes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)

if TYPE_CHECKING:
    from agora_plugins.redis._kafka_models import KafkaRedisRuntimeMetricsSnapshot


class KafkaRedisPrometheusExporter:
    """Prometheus renderer for Kafka -> Redis helper runtimes."""

    def __init__(self, namespace: str = "agora_kafka_redis") -> None:
        self._ns = namespace

    async def render_runtime(self, runtime: object) -> str:
        snapshot = await runtime.observability_snapshot()  # type: ignore[attr-defined]
        return self.render(snapshot)

    def render(self, snapshot: KafkaRedisRuntimeMetricsSnapshot) -> str:
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
            help_text="Kafka -> Redis runtime readiness state",
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

        append_metric_header(
            lines,
            help_text="Kafka -> Redis runtime configuration and readiness",
            metric_type="gauge",
            name=f"{self._ns}_runtime_config",
        )
        for config_name, value in (
            ("delivery_metadata_enabled", int(snapshot.delivery_metadata_field is not None)),
            (
                "preserve_delivery_fields_in_value",
                int(snapshot.preserve_delivery_fields_in_value),
            ),
            ("sink_connection_ready", int(snapshot.sink.connection_ready)),
            ("sink_uses_xadd", int(snapshot.sink.mode == "xadd")),
        ):
            lines.append(f'{self._ns}_runtime_config{{{labels},config="{config_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka -> Redis sink gauge values",
            metric_type="gauge",
            name=f"{self._ns}_sink_gauge",
        )
        if snapshot.sink.ttl_seconds is not None:
            lines.append(
                f'{self._ns}_sink_gauge{{{labels},gauge="ttl_seconds"}} {snapshot.sink.ttl_seconds}'
            )
        if snapshot.sink.maxlen is not None:
            lines.append(f'{self._ns}_sink_gauge{{{labels},gauge="maxlen"}} {snapshot.sink.maxlen}')

        append_metric_header(
            lines,
            help_text="Kafka -> Redis sink monotonic counters",
            metric_type="counter",
            name=f"{self._ns}_sink_events_total",
        )
        for event_name, value in (
            ("write_call", snapshot.sink.write_call_count),
            ("write_batch_call", snapshot.sink.write_batch_call_count),
            ("direct_write", snapshot.sink.direct_write_count),
            ("mset_batch", snapshot.sink.mset_batch_count),
            ("pipeline_execute", snapshot.sink.pipeline_execute_count),
            ("written_record", snapshot.sink.written_record_count),
        ):
            lines.append(f'{self._ns}_sink_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka -> Redis sink last-write age in milliseconds",
            metric_type="gauge",
            name=f"{self._ns}_sink_age_ms",
        )
        last_write_age_ms = age_ms(snapshot.sink.last_write_at)
        if last_write_age_ms is not None:
            lines.append(
                f'{self._ns}_sink_age_ms{{{labels},activity="write"}} {last_write_age_ms:.6f}'
            )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def _base_labels(self, snapshot: KafkaRedisRuntimeMetricsSnapshot) -> str:
        labels = [
            f'consumer_group="{escape_label_value(snapshot.source.health.consumer_group)}"',
            f'bootstrap_servers="{escape_label_value(snapshot.source.health.bootstrap_servers)}"',
            f'sink_target="{escape_label_value(snapshot.sink.target)}"',
            f'sink_mode="{escape_label_value(snapshot.health.sink_mode)}"',
            f'redis_key_field="{escape_label_value(snapshot.redis_key_field)}"',
            f'value_field="{escape_label_value(snapshot.value_field)}"',
            f'delivery_key_field="{escape_label_value(snapshot.delivery_key_field)}"',
        ]
        if snapshot.delivery_metadata_field is not None:
            labels.append(
                f'delivery_metadata_field="{escape_label_value(snapshot.delivery_metadata_field)}"'
            )
        return ",".join(labels)


def age_ms(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return max((datetime.now(UTC) - timestamp).total_seconds() * 1000.0, 0.0)
