"""
agora_plugins.postgres.sources.postgres
=======================================
Async PostgreSQL source that streams rows from a SQL query.
"""

from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from agora.core.source import BaseSource, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.postgres.connection import (
    PostgresConnectionConfig,
    coerce_connection_config,
)
from agora_plugins.postgres.sources._operator_surface import PostgresSourceOperatorSurface
from agora_plugins.postgres.sources._resume_runtime import PostgresSourceResumeRuntime
from agora_plugins.postgres.sources._source_config import (
    READ_ROUTING_TARGET_SESSION_ATTRS as _READ_ROUTING_TARGET_SESSION_ATTRS,
)
from agora_plugins.postgres.sources._source_config import (
    callable_accepts_context as _callable_accepts_context,
)
from agora_plugins.postgres.sources._source_config import (
    default_source_retry_policy,
    validate_postgres_source_config,
)
from agora_plugins.postgres.sources._source_config import (
    generated_server_side_cursor_name as _generated_server_side_cursor_name,
)
from agora_plugins.postgres.sources._source_config import (
    sql_isolation_level as _sql_isolation_level,
)
from agora_plugins.postgres.sources._stream_runtime import PostgresSourceStreamRuntime

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from agora.core.retry import RetryPolicy

    from agora_plugins.postgres.observability import (
        PostgresSourceHealthSnapshot,
        PostgresSourceMetricsSnapshot,
        PostgresSourceRecoveryContractSnapshot,
    )

T = TypeVar("T")


class PostgresReplicaStalenessError(RuntimeError):
    """Raised when a standby replica exceeds the configured replay-lag budget."""

    def __init__(self, *, replay_lag_s: float, max_replica_replay_lag_s: float) -> None:
        super().__init__(
            "Postgres standby replay lag exceeded source staleness guard: "
            f"{replay_lag_s:.3f}s > {max_replica_replay_lag_s:.3f}s"
        )
        self.replay_lag_s = replay_lag_s
        self.max_replica_replay_lag_s = max_replica_replay_lag_s


class PostgresSource(BaseSource[T], Generic[T]):
    """Async PostgreSQL source that streams rows from a SQL query."""

    source_name = "postgres"

    def __init__(
        self,
        dsn: str | None,
        query: str,
        row_mapper: Callable[..., T | Awaitable[T] | None],
        params: dict[str, Any] | None = None,
        batch_size: int = 500,
        checkpoint_field: str | None = None,
        checkpoint_param: str | None = None,
        checkpoint_fields: list[str] | None = None,
        checkpoint_params: dict[str, str] | None = None,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        retry_policy: RetryPolicy[Any] | None = None,
        statement_timeout_ms: int | None = None,
        transaction_read_only: bool | None = None,
        transaction_isolation_level: Literal["read_committed", "repeatable_read", "serializable"]
        | None = None,
        read_routing: Literal["dsn", "primary", "standby", "prefer_standby", "any"] = "dsn",
        max_replica_replay_lag_s: float | None = None,
        on_replica_stale: Literal["fail_closed", "route_primary"] = "fail_closed",
        fetch_strategy: Literal["client", "server_side"] = "client",
        server_side_cursor_name: str | None = None,
        server_side_cursor_withhold: bool = False,
        connection: PostgresConnectionConfig | None = None,
    ) -> None:
        validate_postgres_source_config(
            checkpoint_field=checkpoint_field,
            checkpoint_param=checkpoint_param,
            checkpoint_fields=checkpoint_fields,
            checkpoint_params=checkpoint_params,
            statement_timeout_ms=statement_timeout_ms,
            read_routing=read_routing,
            max_replica_replay_lag_s=max_replica_replay_lag_s,
            on_replica_stale=on_replica_stale,
            fetch_strategy=fetch_strategy,
            batch_size=batch_size,
        )

        self._connection = coerce_connection_config(dsn=dsn, connection=connection)
        self._query = query
        self._row_mapper = row_mapper
        self._row_mapper_accepts_context = _callable_accepts_context(row_mapper)
        self._base_params = dict(params or {})
        self._params = dict(self._base_params)
        self._batch_size = batch_size
        self._checkpoint_field = checkpoint_field
        self._checkpoint_param = checkpoint_param
        self._checkpoint_fields = list(checkpoint_fields or [])
        self._checkpoint_params = dict(checkpoint_params or {})
        self._on_record_error = on_record_error
        self._retry_policy = retry_policy or default_source_retry_policy()
        self._statement_timeout_ms = statement_timeout_ms
        self._transaction_read_only = transaction_read_only
        self._transaction_isolation_level = transaction_isolation_level
        self._read_routing = read_routing
        self._max_replica_replay_lag_s = max_replica_replay_lag_s
        self._on_replica_stale = on_replica_stale
        self._fetch_strategy = fetch_strategy
        self._server_side_cursor_name = server_side_cursor_name
        self._server_side_cursor_withhold = server_side_cursor_withhold
        self.supports_checkpoint = bool(
            (checkpoint_field and checkpoint_param)
            or (self._checkpoint_fields and self._checkpoint_params)
        )
        self._rows_seen = 0
        self._last_checkpoint_cursor: Any | None = None
        self._current_row_checkpoint_cursor: Any | None = None
        self._record_error_count = 0
        self._record_drop_count = 0
        self._active_stream_count = 0
        self._stream_run_count = 0
        self._query_execution_count = 0
        self._retry_count = 0
        self._staleness_guard_block_count = 0
        self._staleness_guard_primary_fallback_count = 0
        self._resume_prepare_count = 0
        self._resume_checkpoint_apply_count = 0
        self._last_stream_started_at: datetime | None = None
        self._last_stream_completed_at: datetime | None = None
        self._last_row_at: datetime | None = None
        self._last_stream_succeeded: bool | None = None
        self._connected_server_role: str | None = None
        self._last_replica_replay_lag_s: float | None = None
        self._last_health_error: str | None = None
        self._last_health_checked_at: datetime | None = None
        self._resume_runtime = PostgresSourceResumeRuntime(self)
        self._stream_runtime: PostgresSourceStreamRuntime[T] = PostgresSourceStreamRuntime(self)
        self._operator_surface = PostgresSourceOperatorSurface(self)

    async def prepare_resume(self, checkpoint: Any) -> None:
        await self._resume_runtime.prepare_resume(checkpoint)

    def current_checkpoint(self) -> dict[str, Any] | None:
        return self._resume_runtime.current_checkpoint()

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def recovery_contract(self) -> PostgresSourceRecoveryContractSnapshot:
        return self._operator_surface.recovery_contract()

    def metrics_snapshot(self) -> PostgresSourceMetricsSnapshot:
        return self._operator_surface.metrics_snapshot()

    def render_prometheus_metrics(self, namespace: str = "agora_postgres") -> str:
        return self._operator_surface.render_prometheus_metrics(namespace=namespace)

    async def health_snapshot(self, *, force_refresh: bool = False) -> PostgresSourceHealthSnapshot:
        return await self._operator_surface.health_snapshot(force_refresh=force_refresh)

    async def stream(self) -> AsyncGenerator[T, None]:
        async for record in self._stream_runtime.stream():
            yield record

    async def _stream_attempt(
        self,
        psycopg: Any,
        dict_row: Any,
    ) -> AsyncGenerator[T, None]:
        async for record in self._stream_runtime.stream_attempt(psycopg, dict_row):
            yield record

    async def _connect_for_read_attempt(self, psycopg: Any, dict_row: Any) -> Any:
        conn, role, replay_lag_s = await self._resolve_read_connection(
            psycopg,
            dict_row,
            count_staleness_events=True,
        )
        self._set_server_observation(
            role=role,
            replay_lag_s=replay_lag_s,
            error=None,
        )
        with suppress(Exception):
            await conn.rollback()
        return conn

    async def _refresh_health_state(self) -> None:
        checked_at = datetime.now(UTC)
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            self._set_server_observation(
                role=None,
                replay_lag_s=None,
                error=(
                    "PostgresSource requires psycopg. Install via: "
                    "pip install 'agora-etl-plugins[postgres]'"
                ),
                checked_at=checked_at,
            )
            return

        conn: Any | None = None
        try:
            conn, role, replay_lag_s = await self._resolve_read_connection(
                psycopg,
                dict_row,
                count_staleness_events=False,
            )
        except Exception as exc:
            self._set_server_observation(
                role=(
                    self._connected_server_role
                    if isinstance(exc, PostgresReplicaStalenessError)
                    else None
                ),
                replay_lag_s=(
                    self._last_replica_replay_lag_s
                    if isinstance(exc, PostgresReplicaStalenessError)
                    else None
                ),
                error=str(exc),
                checked_at=checked_at,
            )
            return
        try:
            self._set_server_observation(
                role=role,
                replay_lag_s=replay_lag_s,
                error=None,
                checked_at=checked_at,
            )
        finally:
            await conn.close()

    async def _resolve_read_connection(
        self,
        psycopg: Any,
        dict_row: Any,
        *,
        count_staleness_events: bool,
    ) -> tuple[Any, str, float | None]:
        conn = await self._open_connection(
            psycopg,
            dict_row,
            target_session_attrs=self._connect_target_session_attrs(),
        )
        role, replay_lag_s = await self._inspect_server_role(conn)
        if not self._replica_is_stale(role=role, replay_lag_s=replay_lag_s):
            return conn, role, replay_lag_s

        if self._on_replica_stale == "route_primary":
            await conn.close()
            fallback_conn = await self._open_connection(
                psycopg,
                dict_row,
                target_session_attrs=self._read_target_session_attrs("primary"),
            )
            fallback_role, fallback_replay_lag_s = await self._inspect_server_role(fallback_conn)
            if fallback_role != "primary":
                if count_staleness_events:
                    self._staleness_guard_block_count += 1
                self._set_server_observation(
                    role=fallback_role,
                    replay_lag_s=fallback_replay_lag_s,
                    error=(
                        "PostgresSource route_primary fallback did not resolve to a primary "
                        f"server (connected role={fallback_role})."
                    ),
                )
                await fallback_conn.close()
                raise RuntimeError(
                    "PostgresSource route_primary fallback must connect to a primary server."
                )
            if count_staleness_events:
                self._staleness_guard_primary_fallback_count += 1
            return fallback_conn, fallback_role, fallback_replay_lag_s

        await self._raise_replica_staleness(
            role=role,
            replay_lag_s=replay_lag_s,
            close_conn=conn,
            count_staleness_events=count_staleness_events,
        )
        raise AssertionError("unreachable")

    async def _open_connection(
        self,
        psycopg: Any,
        dict_row: Any,
        *,
        target_session_attrs: str | None,
    ) -> Any:
        connection_config = (
            self._connection
            if target_session_attrs is None
            else self._connection.model_copy(update={"target_session_attrs": target_session_attrs})
        )
        return await psycopg.AsyncConnection.connect(
            **connection_config.connect_kwargs(row_factory=dict_row)
        )

    async def _inspect_server_role(self, conn: Any) -> tuple[str, float | None]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    pg_is_in_recovery() AS is_standby,
                    CASE
                        WHEN pg_is_in_recovery() THEN
                            COALESCE(
                                EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())::double precision,
                                1000000000.0
                            )
                        ELSE 0.0
                    END AS replay_lag_s
                """
            )
            row = await cur.fetchone()
        payload = dict(row) if row is not None else {}
        role = "standby" if bool(payload.get("is_standby")) else "primary"
        replay_lag_s = payload.get("replay_lag_s")
        if replay_lag_s is None:
            return role, None
        return role, float(replay_lag_s)

    def _replica_is_stale(self, *, role: str, replay_lag_s: float | None) -> bool:
        if role != "standby" or self._max_replica_replay_lag_s is None:
            return False
        if replay_lag_s is None:
            return True
        return replay_lag_s > self._max_replica_replay_lag_s

    async def _enforce_active_replica_freshness(self, conn: Any) -> None:
        role, replay_lag_s = await self._inspect_server_role(conn)
        if not self._replica_is_stale(role=role, replay_lag_s=replay_lag_s):
            self._set_server_observation(
                role=role,
                replay_lag_s=replay_lag_s,
                error=None,
            )
            return
        await self._raise_replica_staleness(
            role=role,
            replay_lag_s=replay_lag_s,
            close_conn=None,
            count_staleness_events=True,
        )

    async def _raise_replica_staleness(
        self,
        *,
        role: str,
        replay_lag_s: float | None,
        close_conn: Any | None,
        count_staleness_events: bool,
    ) -> None:
        if count_staleness_events:
            self._staleness_guard_block_count += 1
        replay_lag_value = float(replay_lag_s or 0.0)
        max_replay_lag_s = float(self._max_replica_replay_lag_s or 0.0)
        self._set_server_observation(
            role=role,
            replay_lag_s=replay_lag_s,
            error=(
                "Postgres standby replay lag exceeded source staleness guard: "
                f"{replay_lag_value:.3f}s > {max_replay_lag_s:.3f}s"
            ),
        )
        if close_conn is not None:
            await close_conn.close()
        raise PostgresReplicaStalenessError(
            replay_lag_s=replay_lag_value,
            max_replica_replay_lag_s=max_replay_lag_s,
        )

    def _read_target_session_attrs(
        self,
        read_routing: Literal["dsn", "primary", "standby", "prefer_standby", "any"],
    ) -> str | None:
        return _READ_ROUTING_TARGET_SESSION_ATTRS[read_routing]

    def _target_session_attrs(self) -> str:
        return self._read_target_session_attrs(self._read_routing) or "dsn"

    def _connect_target_session_attrs(self) -> str | None:
        return self._read_target_session_attrs(self._read_routing)

    async def _apply_read_safety_controls(self, cursor: Any) -> None:
        transaction_modes: list[str] = []
        if self._transaction_isolation_level is not None:
            transaction_modes.append(
                f"ISOLATION LEVEL {_sql_isolation_level(self._transaction_isolation_level).upper()}"
            )
        if self._transaction_read_only is not None:
            transaction_modes.append("READ ONLY" if self._transaction_read_only else "READ WRITE")
        if transaction_modes:
            await cursor.execute(f"SET TRANSACTION {', '.join(transaction_modes)}")
        if self._statement_timeout_ms is not None:
            await cursor.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(self._statement_timeout_ms),),
            )

    def _open_stream_cursor(self, conn: Any) -> Any:
        if self._fetch_strategy == "server_side":
            cursor_kwargs: dict[str, Any] = {
                "name": self._server_side_cursor_name or _generated_server_side_cursor_name()
            }
            if self._server_side_cursor_withhold:
                cursor_kwargs["withhold"] = True
            return conn.cursor(**cursor_kwargs)
        return conn.cursor()

    async def _map_row(self, row_dict: dict[str, Any]) -> T | None:
        return await self._stream_runtime._map_row(row_dict)

    def _row_context(self) -> dict[str, Any]:
        return {
            "row_number": self._rows_seen,
            "checkpoint_cursor": self._current_row_checkpoint_cursor,
            "query": self._query,
            "params": dict(self._params),
            "read_routing": self._read_routing,
            "connected_server_role": self._connected_server_role,
            "replica_replay_lag_s": self._last_replica_replay_lag_s,
        }

    def _extract_checkpoint_cursor(self, row_dict: dict[str, Any]) -> Any | None:
        return self._resume_runtime.extract_checkpoint_cursor(row_dict)

    def _reset_progress(self) -> None:
        self._resume_runtime.reset_progress()

    def _should_retry_read(self, exc: Exception, *, attempt: int) -> bool:
        if self._rows_seen > 0 or self._last_checkpoint_cursor is not None:
            return False
        return self._retry_policy.should_retry(exc, attempt=attempt)

    def _set_server_observation(
        self,
        *,
        role: str | None,
        replay_lag_s: float | None,
        error: str | None,
        checked_at: datetime | None = None,
    ) -> None:
        self._connected_server_role = role
        self._last_replica_replay_lag_s = replay_lag_s
        self._last_health_error = error
        self._last_health_checked_at = checked_at or datetime.now(UTC)


__all__ = ["PostgresReplicaStalenessError", "PostgresSource"]
