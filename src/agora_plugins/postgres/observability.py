"""Observability, acceptance gates, and Prometheus rendering for PostgreSQL plugins."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.health import ComponentHealthSnapshot
from agora.core.recovery import SourceRecoveryContractSnapshot, SourceRecoveryMode
from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)

PostgresSourceRecoveryMode = SourceRecoveryMode


@dataclass(frozen=True, slots=True)
class PostgresSourceRecoveryContractSnapshot(SourceRecoveryContractSnapshot):
    """Machine-readable recovery contract for PostgresSource."""


@dataclass(frozen=True, slots=True)
class PostgresSourceHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for PostgresSource."""

    connection_ready: bool
    routing_ready: bool
    staleness_guard_ready: bool
    read_routing: str
    target_session_attrs: str
    on_replica_stale: str
    connected_server_role: str | None
    max_replica_replay_lag_s: float | None
    last_replica_replay_lag_s: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "connection_ready": self.connection_ready,
            "routing_ready": self.routing_ready,
            "staleness_guard_ready": self.staleness_guard_ready,
            "read_routing": self.read_routing,
            "target_session_attrs": self.target_session_attrs,
            "on_replica_stale": self.on_replica_stale,
            "connected_server_role": self.connected_server_role,
            "max_replica_replay_lag_s": self.max_replica_replay_lag_s,
            "last_replica_replay_lag_s": self.last_replica_replay_lag_s,
            "last_error": self.last_error,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class PostgresSourceMetricsSnapshot:
    """Operational metrics and recovery contract for PostgresSource."""

    batch_size: int
    supports_checkpoint: bool
    row_mapper_accepts_context: bool
    fetch_strategy: str
    read_routing: str
    target_session_attrs: str
    statement_timeout_ms: int | None
    transaction_read_only: bool | None
    transaction_isolation_level: str | None
    server_side_cursor_withhold: bool
    max_replica_replay_lag_s: float | None
    on_replica_stale: str
    ready: bool
    connection_ready: bool
    routing_ready: bool
    staleness_guard_ready: bool
    active_stream_count: int
    rows_seen: int
    stream_run_count: int
    query_execution_count: int
    retry_count: int
    staleness_guard_block_count: int
    staleness_guard_primary_fallback_count: int
    resume_prepare_count: int
    resume_checkpoint_apply_count: int
    record_error_count: int
    record_drop_count: int
    connected_server_role: str | None
    last_replica_replay_lag_s: float | None
    last_checkpoint_cursor_present: bool
    last_stream_succeeded: bool | None
    last_stream_started_at: datetime | None
    last_stream_completed_at: datetime | None
    last_row_at: datetime | None
    last_health_error: str | None
    last_health_checked_at: datetime | None
    recovery_contract: PostgresSourceRecoveryContractSnapshot

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_size": self.batch_size,
            "supports_checkpoint": self.supports_checkpoint,
            "row_mapper_accepts_context": self.row_mapper_accepts_context,
            "fetch_strategy": self.fetch_strategy,
            "read_routing": self.read_routing,
            "target_session_attrs": self.target_session_attrs,
            "statement_timeout_ms": self.statement_timeout_ms,
            "transaction_read_only": self.transaction_read_only,
            "transaction_isolation_level": self.transaction_isolation_level,
            "server_side_cursor_withhold": self.server_side_cursor_withhold,
            "max_replica_replay_lag_s": self.max_replica_replay_lag_s,
            "on_replica_stale": self.on_replica_stale,
            "ready": self.ready,
            "connection_ready": self.connection_ready,
            "routing_ready": self.routing_ready,
            "staleness_guard_ready": self.staleness_guard_ready,
            "active_stream_count": self.active_stream_count,
            "rows_seen": self.rows_seen,
            "stream_run_count": self.stream_run_count,
            "query_execution_count": self.query_execution_count,
            "retry_count": self.retry_count,
            "staleness_guard_block_count": self.staleness_guard_block_count,
            "staleness_guard_primary_fallback_count": self.staleness_guard_primary_fallback_count,
            "resume_prepare_count": self.resume_prepare_count,
            "resume_checkpoint_apply_count": self.resume_checkpoint_apply_count,
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
            "connected_server_role": self.connected_server_role,
            "last_replica_replay_lag_s": self.last_replica_replay_lag_s,
            "last_checkpoint_cursor_present": self.last_checkpoint_cursor_present,
            "last_stream_succeeded": self.last_stream_succeeded,
            "last_stream_started_at": _isoformat_or_none(self.last_stream_started_at),
            "last_stream_completed_at": _isoformat_or_none(self.last_stream_completed_at),
            "last_row_at": _isoformat_or_none(self.last_row_at),
            "last_health_error": self.last_health_error,
            "last_health_checked_at": _isoformat_or_none(self.last_health_checked_at),
            "recovery_contract": self.recovery_contract.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class PostgresDLQSinkMetricsSnapshot:
    """Operational metrics for PostgreSQL-backed DLQ sink activity."""

    table: str
    connection_ready: bool
    table_ready: bool
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
            "table": self.table,
            "connection_ready": self.connection_ready,
            "table_ready": self.table_ready,
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
class PostgresDLQSourceMetricsSnapshot:
    """Operational metrics for PostgreSQL-backed DLQ source scans."""

    table: str
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
            "table": self.table,
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
class PostgresEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single enterprise threshold failure for a PostgreSQL component."""


@dataclass(frozen=True, slots=True)
class PostgresEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise verdict for a PostgreSQL component snapshot."""

    findings: tuple[PostgresEnterpriseAcceptanceFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class PostgresSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresSource behavior."""

    require_checkpoint_support: bool = False
    require_declared_recovery_contract: bool = True
    require_pipeline_rerun_contract: bool = True
    require_nontransparent_failover: bool = True
    require_ready: bool = False
    require_connection_ready: bool = False
    require_routing_ready: bool = False
    require_staleness_guard_ready: bool = False
    max_retry_count: int | None = None
    max_record_error_count: int = 0
    max_record_drop_count: int = 0
    max_active_stream_count: int | None = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_checkpoint_support": self.require_checkpoint_support,
            "require_declared_recovery_contract": self.require_declared_recovery_contract,
            "require_pipeline_rerun_contract": self.require_pipeline_rerun_contract,
            "require_nontransparent_failover": self.require_nontransparent_failover,
            "require_ready": self.require_ready,
            "require_connection_ready": self.require_connection_ready,
            "require_routing_ready": self.require_routing_ready,
            "require_staleness_guard_ready": self.require_staleness_guard_ready,
            "max_retry_count": self.max_retry_count,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
            "max_active_stream_count": self.max_active_stream_count,
        }


@dataclass(frozen=True, slots=True)
class PostgresSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresSink behavior."""

    require_connection_ready: bool = True
    max_buffered_row_count: int = 0
    max_retry_count: int = 0
    max_poison_record_count: int = 0
    max_poison_record_unknown_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
            "max_buffered_row_count": self.max_buffered_row_count,
            "max_retry_count": self.max_retry_count,
            "max_poison_record_count": self.max_poison_record_count,
            "max_poison_record_unknown_count": self.max_poison_record_unknown_count,
        }


@dataclass(frozen=True, slots=True)
class PostgresDLQSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresDLQSink behavior."""

    require_connection_ready: bool = True
    require_table_ready: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
            "require_table_ready": self.require_table_ready,
        }


@dataclass(frozen=True, slots=True)
class PostgresDLQSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresDLQSource behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
        }


class PostgresEnterpriseAcceptanceGate:
    """Evaluate PostgreSQL plugin component snapshots against enterprise thresholds."""

    def evaluate_source(
        self,
        snapshot: PostgresSourceMetricsSnapshot,
        thresholds: PostgresSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresSourceEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        contract = snapshot.recovery_contract
        if thresholds.require_declared_recovery_contract and not isinstance(
            contract.mode, PostgresSourceRecoveryMode
        ):
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.mode",
                    "Postgres source recovery contract is not declared.",
                    str(contract.mode),
                    tuple(mode.value for mode in PostgresSourceRecoveryMode),
                )
            )
        if thresholds.require_checkpoint_support and not contract.supports_checkpoint:
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.supports_checkpoint",
                    "Postgres source does not support checkpoint-based resume.",
                    contract.supports_checkpoint,
                    True,
                )
            )
        if thresholds.require_pipeline_rerun_contract and not contract.requires_pipeline_rerun:
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.requires_pipeline_rerun",
                    "Postgres source failover contract must declare pipeline rerun semantics.",
                    contract.requires_pipeline_rerun,
                    True,
                )
            )
        if thresholds.require_nontransparent_failover and contract.transparent_failover:
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.transparent_failover",
                    "Postgres source should not advertise transparent failover semantics.",
                    contract.transparent_failover,
                    False,
                )
            )
        if thresholds.require_ready and not snapshot.ready:
            findings.append(
                self._finding(
                    "source",
                    "ready",
                    "Postgres source readiness probe is not passing.",
                    snapshot.ready,
                    True,
                )
            )
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "source",
                    "connection_ready",
                    "Postgres source connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        if thresholds.require_routing_ready and not snapshot.routing_ready:
            findings.append(
                self._finding(
                    "source",
                    "routing_ready",
                    "Postgres source routing target is not satisfied.",
                    snapshot.routing_ready,
                    True,
                )
            )
        if thresholds.require_staleness_guard_ready and not snapshot.staleness_guard_ready:
            findings.append(
                self._finding(
                    "source",
                    "staleness_guard_ready",
                    "Postgres source replica staleness guard is blocking readiness.",
                    snapshot.staleness_guard_ready,
                    True,
                )
            )
        self._check_max(
            findings,
            component="source",
            metric="retry_count",
            value=snapshot.retry_count,
            threshold=thresholds.max_retry_count,
        )
        self._check_max(
            findings,
            component="source",
            metric="record_error_count",
            value=snapshot.record_error_count,
            threshold=thresholds.max_record_error_count,
        )
        self._check_max(
            findings,
            component="source",
            metric="record_drop_count",
            value=snapshot.record_drop_count,
            threshold=thresholds.max_record_drop_count,
        )
        self._check_max(
            findings,
            component="source",
            metric="active_stream_count",
            value=snapshot.active_stream_count,
            threshold=thresholds.max_active_stream_count,
        )
        return PostgresEnterpriseAcceptanceReport(
            component="source",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_sink(
        self,
        snapshot: Any,
        thresholds: PostgresSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresSinkEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "sink",
                    "connection_ready",
                    "Postgres sink connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        self._check_max(
            findings,
            component="sink",
            metric="buffered_row_count",
            value=snapshot.buffered_row_count,
            threshold=thresholds.max_buffered_row_count,
        )
        self._check_max(
            findings,
            component="sink",
            metric="retry_count",
            value=snapshot.retry_count,
            threshold=thresholds.max_retry_count,
        )
        self._check_max(
            findings,
            component="sink",
            metric="poison_record_count",
            value=snapshot.poison_record_count,
            threshold=thresholds.max_poison_record_count,
        )
        self._check_max(
            findings,
            component="sink",
            metric="poison_record_unknown_count",
            value=snapshot.poison_record_unknown_count,
            threshold=thresholds.max_poison_record_unknown_count,
        )
        return PostgresEnterpriseAcceptanceReport(
            component="sink",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_sink(
        self,
        snapshot: PostgresDLQSinkMetricsSnapshot,
        thresholds: PostgresDLQSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresDLQSinkEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "dlq_sink",
                    "connection_ready",
                    "Postgres DLQ sink connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        if thresholds.require_table_ready and not snapshot.table_ready:
            findings.append(
                self._finding(
                    "dlq_sink",
                    "table_ready",
                    "Postgres DLQ sink table is not ready.",
                    snapshot.table_ready,
                    True,
                )
            )
        return PostgresEnterpriseAcceptanceReport(
            component="dlq_sink",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_source(
        self,
        snapshot: PostgresDLQSourceMetricsSnapshot,
        thresholds: PostgresDLQSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresDLQSourceEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "dlq_source",
                    "connection_ready",
                    "Postgres DLQ source connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        return PostgresEnterpriseAcceptanceReport(
            component="dlq_source",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    @staticmethod
    def _finding(
        component: str,
        metric: str,
        message: str,
        value: Any,
        threshold: Any,
    ) -> PostgresEnterpriseAcceptanceFinding:
        return PostgresEnterpriseAcceptanceFinding(
            component=component,
            metric=metric,
            message=message,
            value=value,
            threshold=threshold,
        )

    def _check_max(
        self,
        findings: list[PostgresEnterpriseAcceptanceFinding],
        *,
        component: str,
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                self._finding(
                    component,
                    metric,
                    f"{component}.{metric} exceeded enterprise threshold.",
                    value,
                    threshold,
                )
            )


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

    def render_sink(self, snapshot: Any) -> str:
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

    def _sink_labels(self, snapshot: Any) -> str:
        return ",".join(
            (
                f'table="{escape_label_value(snapshot.table)}"',
                f'insert_mode="{escape_label_value(snapshot.insert_mode)}"',
                f'write_safety_policy="{escape_label_value(snapshot.write_safety_policy)}"',
            )
        )


def _isoformat_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _age_ms(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return max((datetime.now(UTC) - timestamp).total_seconds() * 1000.0, 0.0)


__all__ = [
    "PostgresDLQSinkEnterpriseAcceptanceThresholds",
    "PostgresDLQSinkMetricsSnapshot",
    "PostgresDLQSourceEnterpriseAcceptanceThresholds",
    "PostgresDLQSourceMetricsSnapshot",
    "PostgresEnterpriseAcceptanceFinding",
    "PostgresEnterpriseAcceptanceGate",
    "PostgresEnterpriseAcceptanceReport",
    "PostgresPrometheusExporter",
    "PostgresSinkEnterpriseAcceptanceThresholds",
    "PostgresSourceEnterpriseAcceptanceThresholds",
    "PostgresSourceHealthSnapshot",
    "PostgresSourceMetricsSnapshot",
    "PostgresSourceRecoveryContractSnapshot",
    "PostgresSourceRecoveryMode",
]
