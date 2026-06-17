"""Observability, acceptance gates, and Prometheus rendering for Redis plugins."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.health import ComponentHealthSnapshot
from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)

if TYPE_CHECKING:
    from agora_plugins.redis.sinks.redis import RedisSinkMetricsSnapshot


def _isoformat_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _age_ms(value: datetime | None) -> float | None:
    if value is None:
        return None
    return max((datetime.now(UTC) - value).total_seconds() * 1000.0, 0.0)


@dataclass(frozen=True, slots=True)
class RedisStreamSourceHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for RedisStreamSource."""

    connection_ready: bool
    group_ready: bool
    ack_enabled: bool
    reclaim_enabled: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "connection_ready": self.connection_ready,
            "group_ready": self.group_ready,
            "ack_enabled": self.ack_enabled,
            "reclaim_enabled": self.reclaim_enabled,
            "last_error": self.last_error,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class RedisSourcePoisonLoopRiskSnapshot:
    """Operator-facing snapshot for reclaimed poison messages looping unacked."""

    detected: bool
    loop_count: int = 0
    distinct_message_count: int = 0
    last_message_id: str | None = None
    last_detected_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "detected": self.detected,
            "loop_count": self.loop_count,
            "distinct_message_count": self.distinct_message_count,
            "last_message_id": self.last_message_id,
            "last_detected_at": _isoformat_or_none(self.last_detected_at),
        }


@dataclass(frozen=True, slots=True)
class RedisStreamSourceMetricsSnapshot:
    """Operational metrics for RedisStreamSource activity."""

    stream: str
    group: str
    consumer: str
    block_ms: int
    batch_size: int
    ack_batch_size: int
    ack_on_success: bool
    reclaim_idle_ms: int | None
    reclaim_batch_size: int
    max_consecutive_reclaim_batches: int | None
    health: RedisStreamSourceHealthSnapshot
    poison_loop_risk: RedisSourcePoisonLoopRiskSnapshot
    read_call_count: int = 0
    reconnect_count: int = 0
    reclaimed_message_count: int = 0
    consecutive_reclaim_batch_count: int = 0
    reclaim_fairness_yield_count: int = 0
    ack_flush_count: int = 0
    acked_message_count: int = 0
    emitted_record_count: int = 0
    pending_ack_count: int = 0
    record_error_count: int = 0
    record_drop_count: int = 0
    last_message_id: str | None = None
    last_read_at: datetime | None = None
    last_reconnect_at: datetime | None = None
    last_ack_at: datetime | None = None
    last_reclaim_at: datetime | None = None
    last_error_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "group": self.group,
            "consumer": self.consumer,
            "block_ms": self.block_ms,
            "batch_size": self.batch_size,
            "ack_batch_size": self.ack_batch_size,
            "ack_on_success": self.ack_on_success,
            "reclaim_idle_ms": self.reclaim_idle_ms,
            "reclaim_batch_size": self.reclaim_batch_size,
            "max_consecutive_reclaim_batches": self.max_consecutive_reclaim_batches,
            "health": self.health.to_dict(),
            "poison_loop_risk": self.poison_loop_risk.to_dict(),
            "read_call_count": self.read_call_count,
            "reconnect_count": self.reconnect_count,
            "reclaimed_message_count": self.reclaimed_message_count,
            "consecutive_reclaim_batch_count": self.consecutive_reclaim_batch_count,
            "reclaim_fairness_yield_count": self.reclaim_fairness_yield_count,
            "ack_flush_count": self.ack_flush_count,
            "acked_message_count": self.acked_message_count,
            "emitted_record_count": self.emitted_record_count,
            "pending_ack_count": self.pending_ack_count,
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
            "last_message_id": self.last_message_id,
            "last_read_at": _isoformat_or_none(self.last_read_at),
            "last_reconnect_at": _isoformat_or_none(self.last_reconnect_at),
            "last_ack_at": _isoformat_or_none(self.last_ack_at),
            "last_reclaim_at": _isoformat_or_none(self.last_reclaim_at),
            "last_error_at": _isoformat_or_none(self.last_error_at),
        }


@dataclass(frozen=True, slots=True)
class RedisDLQSinkMetricsSnapshot:
    """Operational metrics for Redis-backed DLQ sink activity."""

    key_prefix: str
    connection_ready: bool
    write_call_count: int = 0
    write_batch_call_count: int = 0
    inserted_record_count: int = 0
    upserted_record_count: int = 0
    updated_record_count: int = 0
    replay_count: int = 0
    replayed_record_count: int = 0
    acknowledge_count: int = 0
    acknowledged_record_count: int = 0
    last_write_at: datetime | None = None
    last_replay_at: datetime | None = None
    last_acknowledge_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key_prefix": self.key_prefix,
            "connection_ready": self.connection_ready,
            "write_call_count": self.write_call_count,
            "write_batch_call_count": self.write_batch_call_count,
            "inserted_record_count": self.inserted_record_count,
            "upserted_record_count": self.upserted_record_count,
            "updated_record_count": self.updated_record_count,
            "replay_count": self.replay_count,
            "replayed_record_count": self.replayed_record_count,
            "acknowledge_count": self.acknowledge_count,
            "acknowledged_record_count": self.acknowledged_record_count,
            "last_write_at": _isoformat_or_none(self.last_write_at),
            "last_replay_at": _isoformat_or_none(self.last_replay_at),
            "last_acknowledge_at": _isoformat_or_none(self.last_acknowledge_at),
        }


@dataclass(frozen=True, slots=True)
class RedisDLQSourceMetricsSnapshot:
    """Operational metrics for Redis-backed DLQ source scans."""

    key_prefix: str
    pipeline_id: str | None
    stage: str | None
    limit: int | None
    connection_ready: bool
    scan_count: int = 0
    emitted_record_count: int = 0
    last_scan_at: datetime | None = None
    last_record_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key_prefix": self.key_prefix,
            "pipeline_id": self.pipeline_id,
            "stage": self.stage,
            "limit": self.limit,
            "connection_ready": self.connection_ready,
            "scan_count": self.scan_count,
            "emitted_record_count": self.emitted_record_count,
            "last_scan_at": _isoformat_or_none(self.last_scan_at),
            "last_record_at": _isoformat_or_none(self.last_record_at),
        }


@dataclass(frozen=True, slots=True)
class RedisEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single enterprise threshold failure for a Redis component."""


@dataclass(frozen=True, slots=True)
class RedisEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise verdict for a Redis component snapshot."""

    findings: tuple[RedisEnterpriseAcceptanceFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class RedisSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisStreamSource behavior."""

    require_ready: bool = True
    require_connection_ready: bool = True
    require_group_ready: bool = True
    max_pending_ack_count: int | None = 0
    max_record_error_count: int = 0
    max_record_drop_count: int = 0
    max_poison_loop_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_ready": self.require_ready,
            "require_connection_ready": self.require_connection_ready,
            "require_group_ready": self.require_group_ready,
            "max_pending_ack_count": self.max_pending_ack_count,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
            "max_poison_loop_count": self.max_poison_loop_count,
        }


@dataclass(frozen=True, slots=True)
class RedisSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisSink behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"require_connection_ready": self.require_connection_ready}


@dataclass(frozen=True, slots=True)
class RedisDLQSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisDLQSink behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"require_connection_ready": self.require_connection_ready}


@dataclass(frozen=True, slots=True)
class RedisDLQSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisDLQSource behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"require_connection_ready": self.require_connection_ready}


class RedisEnterpriseAcceptanceGate:
    """Evaluate Redis component snapshots against ops-grade thresholds."""

    def evaluate_source(
        self,
        snapshot: RedisStreamSourceMetricsSnapshot,
        thresholds: RedisSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisSourceEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        health = snapshot.health
        if resolved.require_ready and not health.ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_source",
                    metric="ready",
                    message="Redis stream source is not ready.",
                    value=health.ready,
                    threshold=True,
                )
            )
        if resolved.require_connection_ready and not health.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_source",
                    metric="connection_ready",
                    message="Redis stream source connection is not ready.",
                    value=health.connection_ready,
                    threshold=True,
                )
            )
        if resolved.require_group_ready and not health.group_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_source",
                    metric="group_ready",
                    message="Redis stream source consumer group is not ready.",
                    value=health.group_ready,
                    threshold=True,
                )
            )
        self._check_max(
            findings,
            component="redis_source",
            metric="pending_ack_count",
            value=snapshot.pending_ack_count,
            threshold=resolved.max_pending_ack_count,
        )
        self._check_max(
            findings,
            component="redis_source",
            metric="record_error_count",
            value=snapshot.record_error_count,
            threshold=resolved.max_record_error_count,
        )
        self._check_max(
            findings,
            component="redis_source",
            metric="record_drop_count",
            value=snapshot.record_drop_count,
            threshold=resolved.max_record_drop_count,
        )
        self._check_max(
            findings,
            component="redis_source",
            metric="poison_loop_count",
            value=snapshot.poison_loop_risk.loop_count,
            threshold=resolved.max_poison_loop_count,
            message=(
                "reclaimed poison record(s) are looping without acknowledgment; "
                "drain or replay the pending poison before promoting this runtime."
            ),
        )
        return RedisEnterpriseAcceptanceReport(
            component="redis_source",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_sink(
        self,
        snapshot: RedisSinkMetricsSnapshot,
        thresholds: RedisSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisSinkEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        if resolved.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_sink",
                    metric="connection_ready",
                    message="Redis sink connection is not ready.",
                    value=snapshot.connection_ready,
                    threshold=True,
                )
            )
        return RedisEnterpriseAcceptanceReport(
            component="redis_sink",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_sink(
        self,
        snapshot: RedisDLQSinkMetricsSnapshot,
        thresholds: RedisDLQSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisDLQSinkEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        if resolved.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_dlq_sink",
                    metric="connection_ready",
                    message="Redis DLQ sink connection is not ready.",
                    value=snapshot.connection_ready,
                    threshold=True,
                )
            )
        return RedisEnterpriseAcceptanceReport(
            component="redis_dlq_sink",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_source(
        self,
        snapshot: RedisDLQSourceMetricsSnapshot,
        thresholds: RedisDLQSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisDLQSourceEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        if resolved.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_dlq_source",
                    metric="connection_ready",
                    message="Redis DLQ source connection is not ready.",
                    value=snapshot.connection_ready,
                    threshold=True,
                )
            )
        return RedisEnterpriseAcceptanceReport(
            component="redis_dlq_source",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    @staticmethod
    def _check_max(
        findings: list[RedisEnterpriseAcceptanceFinding],
        *,
        component: str,
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
        message: str | None = None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component=component,
                    metric=metric,
                    message=message or f"{metric} exceeded enterprise threshold.",
                    value=value,
                    threshold=threshold,
                )
            )


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
            age_ms = _age_ms(timestamp)
            if age_ms is not None:
                age_lines.append(
                    f'{self._ns}_source_age_ms{{{labels},activity="{activity_name}"}} {age_ms:.6f}'
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

        last_write_age_ms = _age_ms(snapshot.last_write_at)
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


__all__ = [
    "RedisDLQSinkEnterpriseAcceptanceThresholds",
    "RedisDLQSinkMetricsSnapshot",
    "RedisDLQSourceEnterpriseAcceptanceThresholds",
    "RedisDLQSourceMetricsSnapshot",
    "RedisEnterpriseAcceptanceFinding",
    "RedisEnterpriseAcceptanceGate",
    "RedisEnterpriseAcceptanceReport",
    "RedisPrometheusExporter",
    "RedisSinkEnterpriseAcceptanceThresholds",
    "RedisSourceEnterpriseAcceptanceThresholds",
    "RedisSourcePoisonLoopRiskSnapshot",
    "RedisStreamSourceHealthSnapshot",
    "RedisStreamSourceMetricsSnapshot",
]
