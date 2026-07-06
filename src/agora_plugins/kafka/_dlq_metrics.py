"""Metrics snapshots and Prometheus rendering for Kafka DLQ surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _age_ms(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return max((_now_utc() - timestamp).total_seconds() * 1000.0, 0.0)


@dataclass(frozen=True, slots=True)
class KafkaDLQSinkMetricsSnapshot:
    """Operational counters for Kafka DLQ sink activity."""

    topic: str
    bootstrap_servers: str
    write_count: int = 0
    write_batch_count: int = 0
    replay_count: int = 0
    acknowledge_count: int = 0
    upsert_count: int = 0
    delete_count: int = 0
    last_write_at: datetime | None = None
    last_replay_at: datetime | None = None
    last_acknowledge_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "bootstrap_servers": self.bootstrap_servers,
            "write_count": self.write_count,
            "write_batch_count": self.write_batch_count,
            "replay_count": self.replay_count,
            "acknowledge_count": self.acknowledge_count,
            "upsert_count": self.upsert_count,
            "delete_count": self.delete_count,
            "last_write_at": (
                None if self.last_write_at is None else self.last_write_at.isoformat()
            ),
            "last_replay_at": (
                None if self.last_replay_at is None else self.last_replay_at.isoformat()
            ),
            "last_acknowledge_at": (
                None if self.last_acknowledge_at is None else self.last_acknowledge_at.isoformat()
            ),
        }


@dataclass(frozen=True, slots=True)
class KafkaDLQSourceMetricsSnapshot:
    """Replay/backlog observability for Kafka DLQ source scans."""

    consumer_group: str
    bootstrap_servers: str
    subscription_mode: str
    scan_count: int = 0
    scanned_message_count: int = 0
    upsert_event_count: int = 0
    delete_event_count: int = 0
    start_offset_seek_count: int = 0
    highwater_stop_count: int = 0
    live_record_count: int = 0
    matched_record_count: int = 0
    replayable_record_count: int = 0
    retry_filtered_count: int = 0
    last_scan_completed_at: datetime | None = None
    last_record_seen_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "consumer_group": self.consumer_group,
            "bootstrap_servers": self.bootstrap_servers,
            "subscription_mode": self.subscription_mode,
            "scan_count": self.scan_count,
            "scanned_message_count": self.scanned_message_count,
            "upsert_event_count": self.upsert_event_count,
            "delete_event_count": self.delete_event_count,
            "start_offset_seek_count": self.start_offset_seek_count,
            "highwater_stop_count": self.highwater_stop_count,
            "live_record_count": self.live_record_count,
            "matched_record_count": self.matched_record_count,
            "replayable_record_count": self.replayable_record_count,
            "retry_filtered_count": self.retry_filtered_count,
            "last_scan_completed_at": (
                None
                if self.last_scan_completed_at is None
                else self.last_scan_completed_at.isoformat()
            ),
            "last_record_seen_at": (
                None if self.last_record_seen_at is None else self.last_record_seen_at.isoformat()
            ),
        }


class KafkaDLQPrometheusExporter:
    """Zero-dependency Prometheus renderer for Kafka DLQ sink/source metrics."""

    def __init__(self, namespace: str = "agora_kafka_dlq") -> None:
        self._ns = namespace

    def render_sink(self, snapshot: KafkaDLQSinkMetricsSnapshot) -> str:
        labels = ",".join(
            [
                f'topic="{escape_label_value(snapshot.topic)}"',
                f'bootstrap_servers="{escape_label_value(snapshot.bootstrap_servers)}"',
            ]
        )
        lines: list[str] = []
        ns = self._ns

        append_metric_header(
            lines,
            help_text="Kafka DLQ sink monotonic event counters",
            metric_type="counter",
            name=f"{ns}_sink_events_total",
        )
        for event_name, value in (
            ("write", snapshot.write_count),
            ("write_batch", snapshot.write_batch_count),
            ("replay", snapshot.replay_count),
            ("acknowledge", snapshot.acknowledge_count),
            ("upsert", snapshot.upsert_count),
            ("delete", snapshot.delete_count),
        ):
            lines.append(f'{ns}_sink_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka DLQ sink last-activity age in milliseconds",
            metric_type="gauge",
            name=f"{ns}_sink_age_ms",
        )
        for activity_name, age_value in (
            ("write", _age_ms(snapshot.last_write_at)),
            ("replay", _age_ms(snapshot.last_replay_at)),
            ("acknowledge", _age_ms(snapshot.last_acknowledge_at)),
        ):
            if age_value is None:
                continue
            lines.append(f'{ns}_sink_age_ms{{{labels},activity="{activity_name}"}} {age_value:.6f}')

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_source(self, snapshot: KafkaDLQSourceMetricsSnapshot) -> str:
        labels = ",".join(
            [
                f'consumer_group="{escape_label_value(snapshot.consumer_group)}"',
                f'bootstrap_servers="{escape_label_value(snapshot.bootstrap_servers)}"',
                f'subscription_mode="{escape_label_value(snapshot.subscription_mode)}"',
            ]
        )
        lines: list[str] = []
        ns = self._ns

        append_metric_header(
            lines,
            help_text="Kafka DLQ source backlog gauges from the latest scan",
            metric_type="gauge",
            name=f"{ns}_source_backlog",
        )
        for gauge_name, value in (
            ("live_record_count", snapshot.live_record_count),
            ("matched_record_count", snapshot.matched_record_count),
            ("replayable_record_count", snapshot.replayable_record_count),
        ):
            lines.append(f'{ns}_source_backlog{{{labels},gauge="{gauge_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka DLQ source monotonic scan and replay counters",
            metric_type="counter",
            name=f"{ns}_source_events_total",
        )
        for event_name, value in (
            ("scan", snapshot.scan_count),
            ("scanned_message", snapshot.scanned_message_count),
            ("upsert", snapshot.upsert_event_count),
            ("delete", snapshot.delete_event_count),
            ("start_offset_seek", snapshot.start_offset_seek_count),
            ("highwater_stop", snapshot.highwater_stop_count),
            ("retry_filtered", snapshot.retry_filtered_count),
        ):
            lines.append(f'{ns}_source_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka DLQ source last-activity age in milliseconds",
            metric_type="gauge",
            name=f"{ns}_source_age_ms",
        )
        for activity_name, age_value in (
            ("scan", _age_ms(snapshot.last_scan_completed_at)),
            ("record_seen", _age_ms(snapshot.last_record_seen_at)),
        ):
            if age_value is None:
                continue
            lines.append(
                f'{ns}_source_age_ms{{{labels},activity="{activity_name}"}} {age_value:.6f}'
            )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"
