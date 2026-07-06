"""Operator-facing supportability surface for PostgreSQL sources."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from agora_plugins.postgres.observability import (
    PostgresPrometheusExporter,
    PostgresSourceHealthSnapshot,
    PostgresSourceMetricsSnapshot,
    PostgresSourceRecoveryContractSnapshot,
    PostgresSourceRecoveryMode,
)

if TYPE_CHECKING:
    from agora_plugins.postgres.sources.postgres import PostgresSource


class PostgresSourceOperatorSurface:
    """Public-facing supportability surface for PostgreSQL sources."""

    def __init__(self, source: PostgresSource[Any]) -> None:
        self._source = source

    def recovery_contract(self) -> PostgresSourceRecoveryContractSnapshot:
        source = self._source
        checkpoint_fields = (
            tuple(source._checkpoint_fields)
            if source._checkpoint_fields
            else ((source._checkpoint_field,) if source._checkpoint_field is not None else ())
        )
        checkpoint_params = (
            dict(source._checkpoint_params)
            if source._checkpoint_params
            else (
                {}
                if source._checkpoint_field is None or source._checkpoint_param is None
                else {source._checkpoint_field: source._checkpoint_param}
            )
        )
        return PostgresSourceRecoveryContractSnapshot(
            mode=(
                PostgresSourceRecoveryMode.CHECKPOINT_RERUN
                if source.supports_checkpoint
                else PostgresSourceRecoveryMode.FULL_RERUN
            ),
            supports_checkpoint=source.supports_checkpoint,
            checkpoint_fields=checkpoint_fields,
            checkpoint_params=checkpoint_params,
            on_record_error=str(source._on_record_error),
        )

    def metrics_snapshot(self) -> PostgresSourceMetricsSnapshot:
        source = self._source
        health = self.build_health_snapshot()
        return PostgresSourceMetricsSnapshot(
            batch_size=source._batch_size,
            supports_checkpoint=source.supports_checkpoint,
            row_mapper_accepts_context=source._row_mapper_accepts_context,
            fetch_strategy=source._fetch_strategy,
            read_routing=source._read_routing,
            target_session_attrs=health.target_session_attrs,
            statement_timeout_ms=source._statement_timeout_ms,
            transaction_read_only=source._transaction_read_only,
            transaction_isolation_level=source._transaction_isolation_level,
            server_side_cursor_withhold=source._server_side_cursor_withhold,
            max_replica_replay_lag_s=source._max_replica_replay_lag_s,
            on_replica_stale=source._on_replica_stale,
            ready=health.ready,
            connection_ready=health.connection_ready,
            routing_ready=health.routing_ready,
            staleness_guard_ready=health.staleness_guard_ready,
            active_stream_count=source._active_stream_count,
            rows_seen=source._rows_seen,
            stream_run_count=source._stream_run_count,
            query_execution_count=source._query_execution_count,
            retry_count=source._retry_count,
            staleness_guard_block_count=source._staleness_guard_block_count,
            staleness_guard_primary_fallback_count=source._staleness_guard_primary_fallback_count,
            resume_prepare_count=source._resume_prepare_count,
            resume_checkpoint_apply_count=source._resume_checkpoint_apply_count,
            record_error_count=source._record_error_count,
            record_drop_count=source._record_drop_count,
            connected_server_role=source._connected_server_role,
            last_replica_replay_lag_s=source._last_replica_replay_lag_s,
            last_checkpoint_cursor_present=source._last_checkpoint_cursor is not None,
            last_stream_succeeded=source._last_stream_succeeded,
            last_stream_started_at=source._last_stream_started_at,
            last_stream_completed_at=source._last_stream_completed_at,
            last_row_at=source._last_row_at,
            last_health_error=source._last_health_error,
            last_health_checked_at=source._last_health_checked_at,
            recovery_contract=self.recovery_contract(),
        )

    def render_prometheus_metrics(self, namespace: str = "agora_postgres") -> str:
        return PostgresPrometheusExporter(namespace=namespace).render_source(
            self.metrics_snapshot()
        )

    async def health_snapshot(
        self,
        *,
        force_refresh: bool = False,
    ) -> PostgresSourceHealthSnapshot:
        source = self._source
        if force_refresh or source._last_health_checked_at is None:
            await source._refresh_health_state()
        return self.build_health_snapshot()

    def build_health_snapshot(self) -> PostgresSourceHealthSnapshot:
        source = self._source
        connection_ready = source._connected_server_role is not None
        routing_ready = self._routing_ready(source._connected_server_role)
        staleness_guard_ready = self._staleness_guard_ready(
            source._connected_server_role,
            source._last_replica_replay_lag_s,
        )
        return PostgresSourceHealthSnapshot(
            ready=(
                connection_ready
                and routing_ready
                and staleness_guard_ready
                and source._last_health_error is None
            ),
            connection_ready=connection_ready,
            routing_ready=routing_ready,
            staleness_guard_ready=staleness_guard_ready,
            read_routing=source._read_routing,
            target_session_attrs=source._target_session_attrs(),
            on_replica_stale=source._on_replica_stale,
            connected_server_role=source._connected_server_role,
            max_replica_replay_lag_s=source._max_replica_replay_lag_s,
            last_replica_replay_lag_s=source._last_replica_replay_lag_s,
            last_error=source._last_health_error,
            checked_at=source._last_health_checked_at or datetime.now(UTC),
        )

    def _routing_ready(self, role: str | None) -> bool:
        if role is None:
            return False
        if self._source._read_routing == "primary":
            return role == "primary"
        if self._source._read_routing == "standby":
            return role == "standby"
        return True

    def _staleness_guard_ready(self, role: str | None, replay_lag_s: float | None) -> bool:
        if role is None:
            return False
        return not self._source._replica_is_stale(role=role, replay_lag_s=replay_lag_s)


__all__ = ["PostgresSourceOperatorSurface"]
