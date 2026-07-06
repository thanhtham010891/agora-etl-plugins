"""Observability snapshots for BigQuery plugin surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.health import ComponentHealthSnapshot
from agora.core.recovery import SourceRecoveryContractSnapshot, SourceRecoveryMode

from agora_plugins.bigquery.acceptance_evaluators import (
    BigQuerySinkAcceptanceEvaluator,
    BigQuerySourceAcceptanceEvaluator,
    BigQueryStorageWriteSinkAcceptanceEvaluator,
)

BigQuerySourceRecoveryMode = SourceRecoveryMode


def _isoformat_or_none(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


@dataclass(frozen=True, slots=True)
class BigQuerySourceRecoveryContractSnapshot(SourceRecoveryContractSnapshot):
    """Machine-readable recovery contract for BigQuerySource."""


@dataclass(frozen=True, slots=True, kw_only=True)
class BigQuerySourceHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for BigQuerySource."""

    mode: str
    supports_checkpoint: bool
    query_executed: bool
    active_stream_count: int
    last_stream_succeeded: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "mode": self.mode,
            "supports_checkpoint": self.supports_checkpoint,
            "query_executed": self.query_executed,
            "active_stream_count": self.active_stream_count,
            "last_stream_succeeded": self.last_stream_succeeded,
        }


@dataclass(frozen=True, slots=True)
class BigQuerySourceMetricsSnapshot:
    """Operational metrics and recovery contract for BigQuerySource."""

    mode: str
    connection_ready: bool
    supports_checkpoint: bool
    stream_run_count: int
    active_stream_count: int
    rows_seen: int
    row_mapper_accepts_context: bool
    query_execution_count: int
    emitted_record_count: int
    record_error_count: int
    record_drop_count: int
    last_job_id: str | None
    last_checkpoint_cursor: Any | None
    last_stream_started_at: datetime | None
    last_stream_completed_at: datetime | None
    last_stream_succeeded: bool | None
    last_error: str | None
    recovery_contract: BigQuerySourceRecoveryContractSnapshot

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "connection_ready": self.connection_ready,
            "supports_checkpoint": self.supports_checkpoint,
            "stream_run_count": self.stream_run_count,
            "active_stream_count": self.active_stream_count,
            "rows_seen": self.rows_seen,
            "row_mapper_accepts_context": self.row_mapper_accepts_context,
            "query_execution_count": self.query_execution_count,
            "emitted_record_count": self.emitted_record_count,
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
            "last_job_id": self.last_job_id,
            "last_checkpoint_cursor": self.last_checkpoint_cursor,
            "last_stream_started_at": _isoformat_or_none(self.last_stream_started_at),
            "last_stream_completed_at": _isoformat_or_none(self.last_stream_completed_at),
            "last_stream_succeeded": self.last_stream_succeeded,
            "last_error": self.last_error,
            "recovery_contract": self.recovery_contract.to_dict(),
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class BigQuerySinkHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for BigQuerySink."""

    table: str
    buffered_row_count: int
    flush_count: int
    last_flush_succeeded: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "table": self.table,
            "buffered_row_count": self.buffered_row_count,
            "flush_count": self.flush_count,
            "last_flush_succeeded": self.last_flush_succeeded,
        }


@dataclass(frozen=True, slots=True)
class BigQuerySinkMetricsSnapshot:
    """Operational metrics for BigQuery sink activity."""

    table: str
    batch_size: int
    connection_ready: bool
    buffered_row_count: int
    flush_count: int
    flush_error_count: int
    submitted_row_count: int
    loaded_row_count: int
    write_disposition: str
    create_disposition: str
    last_job_id: str | None
    last_flush_at: datetime | None
    last_flush_succeeded: bool | None
    last_error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "batch_size": self.batch_size,
            "connection_ready": self.connection_ready,
            "buffered_row_count": self.buffered_row_count,
            "flush_count": self.flush_count,
            "flush_error_count": self.flush_error_count,
            "submitted_row_count": self.submitted_row_count,
            "loaded_row_count": self.loaded_row_count,
            "write_disposition": self.write_disposition,
            "create_disposition": self.create_disposition,
            "last_job_id": self.last_job_id,
            "last_flush_at": _isoformat_or_none(self.last_flush_at),
            "last_flush_succeeded": self.last_flush_succeeded,
            "last_error": self.last_error,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class BigQueryStorageWriteSinkHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for BigQueryStorageWriteSink."""

    table: str
    stream_name: str | None
    buffered_row_count: int
    flush_count: int
    last_append_succeeded: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "table": self.table,
            "stream_name": self.stream_name,
            "buffered_row_count": self.buffered_row_count,
            "flush_count": self.flush_count,
            "last_append_succeeded": self.last_append_succeeded,
        }


@dataclass(frozen=True, slots=True)
class BigQueryStorageWriteSinkMetricsSnapshot:
    """Operational metrics for BigQuery Storage Write sink activity."""

    table: str
    stream_name: str | None
    batch_size: int
    max_request_bytes: int
    connection_ready: bool
    buffered_row_count: int
    flush_count: int
    append_error_count: int
    submitted_row_count: int
    appended_row_count: int
    last_append_offset: int | None
    last_append_at: datetime | None
    last_append_succeeded: bool | None
    last_error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "stream_name": self.stream_name,
            "batch_size": self.batch_size,
            "max_request_bytes": self.max_request_bytes,
            "connection_ready": self.connection_ready,
            "buffered_row_count": self.buffered_row_count,
            "flush_count": self.flush_count,
            "append_error_count": self.append_error_count,
            "submitted_row_count": self.submitted_row_count,
            "appended_row_count": self.appended_row_count,
            "last_append_offset": self.last_append_offset,
            "last_append_at": _isoformat_or_none(self.last_append_at),
            "last_append_succeeded": self.last_append_succeeded,
            "last_error": self.last_error,
        }


@dataclass(frozen=True, slots=True)
class BigQueryEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single enterprise threshold failure for a BigQuery component."""


@dataclass(frozen=True, slots=True)
class BigQueryEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise verdict for a BigQuery component snapshot."""

    findings: tuple[BigQueryEnterpriseAcceptanceFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class BigQuerySourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for BigQuerySource behavior."""

    require_connection_ready: bool = True
    require_query_execution: bool = True
    require_last_job_id_after_query: bool = True
    require_last_stream_success: bool = True
    require_checkpoint_support: bool = False
    max_record_error_count: int = 0
    max_record_drop_count: int = 0
    max_active_stream_count: int | None = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
            "require_query_execution": self.require_query_execution,
            "require_last_job_id_after_query": self.require_last_job_id_after_query,
            "require_last_stream_success": self.require_last_stream_success,
            "require_checkpoint_support": self.require_checkpoint_support,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
            "max_active_stream_count": self.max_active_stream_count,
        }


@dataclass(frozen=True, slots=True)
class BigQuerySinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for BigQuerySink behavior."""

    require_connection_ready: bool = True
    require_flush_activity: bool = True
    require_last_job_id_after_flush: bool = True
    require_last_flush_success: bool = True
    require_loaded_row_count_match: bool = True
    max_buffered_row_count: int = 0
    max_flush_error_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
            "require_flush_activity": self.require_flush_activity,
            "require_last_job_id_after_flush": self.require_last_job_id_after_flush,
            "require_last_flush_success": self.require_last_flush_success,
            "require_loaded_row_count_match": self.require_loaded_row_count_match,
            "max_buffered_row_count": self.max_buffered_row_count,
            "max_flush_error_count": self.max_flush_error_count,
        }


@dataclass(frozen=True, slots=True)
class BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds:
    """Enterprise-style thresholds for BigQueryStorageWriteSink phase-1 behavior."""

    require_connection_ready: bool = True
    require_flush_activity: bool = True
    require_stream_name: bool = True
    require_last_append_success: bool = True
    require_appended_row_count_match: bool = True
    max_buffered_row_count: int = 0
    max_append_error_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
            "require_flush_activity": self.require_flush_activity,
            "require_stream_name": self.require_stream_name,
            "require_last_append_success": self.require_last_append_success,
            "require_appended_row_count_match": self.require_appended_row_count_match,
            "max_buffered_row_count": self.max_buffered_row_count,
            "max_append_error_count": self.max_append_error_count,
        }


class BigQueryEnterpriseAcceptanceGate:
    """Evaluate BigQuery plugin snapshots against enterprise thresholds."""

    def __init__(self) -> None:
        self._source = BigQuerySourceAcceptanceEvaluator(
            component="source",
            finding_factory=BigQueryEnterpriseAcceptanceFinding,
            report_factory=BigQueryEnterpriseAcceptanceReport,
        )
        self._sink = BigQuerySinkAcceptanceEvaluator(
            component="sink",
            finding_factory=BigQueryEnterpriseAcceptanceFinding,
            report_factory=BigQueryEnterpriseAcceptanceReport,
        )
        self._storage_write_sink = BigQueryStorageWriteSinkAcceptanceEvaluator(
            component="storage_write_sink",
            finding_factory=BigQueryEnterpriseAcceptanceFinding,
            report_factory=BigQueryEnterpriseAcceptanceReport,
        )

    def evaluate_source(
        self,
        snapshot: BigQuerySourceMetricsSnapshot,
        thresholds: BigQuerySourceEnterpriseAcceptanceThresholds | None = None,
    ) -> BigQueryEnterpriseAcceptanceReport:
        thresholds = thresholds or BigQuerySourceEnterpriseAcceptanceThresholds()
        return self._source.evaluate(snapshot, thresholds)

    def evaluate_sink(
        self,
        snapshot: BigQuerySinkMetricsSnapshot,
        thresholds: BigQuerySinkEnterpriseAcceptanceThresholds | None = None,
    ) -> BigQueryEnterpriseAcceptanceReport:
        thresholds = thresholds or BigQuerySinkEnterpriseAcceptanceThresholds()
        return self._sink.evaluate(snapshot, thresholds)

    def evaluate_storage_write_sink(
        self,
        snapshot: BigQueryStorageWriteSinkMetricsSnapshot,
        thresholds: BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> BigQueryEnterpriseAcceptanceReport:
        thresholds = thresholds or BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds()
        return self._storage_write_sink.evaluate(snapshot, thresholds)


__all__ = [
    "BigQueryEnterpriseAcceptanceFinding",
    "BigQueryEnterpriseAcceptanceGate",
    "BigQueryEnterpriseAcceptanceReport",
    "BigQuerySinkAcceptanceEvaluator",
    "BigQuerySinkEnterpriseAcceptanceThresholds",
    "BigQuerySinkHealthSnapshot",
    "BigQuerySinkMetricsSnapshot",
    "BigQuerySourceAcceptanceEvaluator",
    "BigQuerySourceEnterpriseAcceptanceThresholds",
    "BigQuerySourceHealthSnapshot",
    "BigQuerySourceMetricsSnapshot",
    "BigQuerySourceRecoveryContractSnapshot",
    "BigQuerySourceRecoveryMode",
    "BigQueryStorageWriteSinkAcceptanceEvaluator",
    "BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds",
    "BigQueryStorageWriteSinkHealthSnapshot",
    "BigQueryStorageWriteSinkMetricsSnapshot",
]
