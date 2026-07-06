"""Public snapshot models for PostgreSQL plugin observability surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.health import ComponentHealthSnapshot
from agora.core.recovery import SourceRecoveryContractSnapshot, SourceRecoveryMode

if TYPE_CHECKING:
    from datetime import datetime

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


def _isoformat_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


__all__ = [
    "PostgresDLQSinkMetricsSnapshot",
    "PostgresDLQSourceMetricsSnapshot",
    "PostgresSourceHealthSnapshot",
    "PostgresSourceMetricsSnapshot",
    "PostgresSourceRecoveryContractSnapshot",
    "PostgresSourceRecoveryMode",
]
