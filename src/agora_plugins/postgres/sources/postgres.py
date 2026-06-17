"""
agora_plugins.postgres.sources.postgres
=======================================
Async PostgreSQL source that streams rows from a SQL query.
"""

from __future__ import annotations

import uuid
from contextlib import suppress
from datetime import UTC, datetime
from inspect import Parameter, isawaitable, signature
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

import logstruct
from agora.core.retry import RetryPolicy
from agora.core.source import BaseSource, SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.postgres.connection import (
    PostgresConnectionConfig,
    coerce_connection_config,
)
from agora_plugins.postgres.observability import (
    PostgresPrometheusExporter,
    PostgresSourceHealthSnapshot,
    PostgresSourceMetricsSnapshot,
    PostgresSourceRecoveryContractSnapshot,
    PostgresSourceRecoveryMode,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

T = TypeVar("T")
_DEFAULT_SOURCE_RETRY_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    OSError,
    TimeoutError,
)
_READ_ROUTING_TARGET_SESSION_ATTRS: dict[str, str | None] = {
    "dsn": None,
    "primary": "primary",
    "standby": "standby",
    "prefer_standby": "prefer-standby",
    "any": "any",
}

logger = logstruct.getLogger(__name__)


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
        singular_config = checkpoint_field is not None or checkpoint_param is not None
        composite_config = checkpoint_fields is not None or checkpoint_params is not None

        if singular_config and composite_config:
            raise ValueError(
                "Use either checkpoint_field/checkpoint_param or "
                "checkpoint_fields/checkpoint_params, not both."
            )
        if singular_config and (checkpoint_field is None or checkpoint_param is None):
            raise ValueError("checkpoint_field and checkpoint_param must be provided together.")
        if composite_config and (not checkpoint_fields or not checkpoint_params):
            raise ValueError("checkpoint_fields and checkpoint_params must be provided together.")
        if checkpoint_fields is not None and checkpoint_params is not None:
            missing = [field for field in checkpoint_fields if field not in checkpoint_params]
            if missing:
                raise ValueError(
                    "checkpoint_params must provide a query parameter for every "
                    f"checkpoint field. Missing: {missing!r}"
                )
        if statement_timeout_ms is not None and statement_timeout_ms < 1:
            raise ValueError("statement_timeout_ms must be >= 1 when provided.")
        if read_routing not in _READ_ROUTING_TARGET_SESSION_ATTRS:
            raise ValueError(
                "read_routing must be one of: dsn, primary, standby, prefer_standby, any."
            )
        if max_replica_replay_lag_s is not None and max_replica_replay_lag_s < 0:
            raise ValueError("max_replica_replay_lag_s must be >= 0 when provided.")
        if on_replica_stale not in {"fail_closed", "route_primary"}:
            raise ValueError("on_replica_stale must be 'fail_closed' or 'route_primary'.")
        if fetch_strategy not in {"client", "server_side"}:
            raise ValueError("fetch_strategy must be 'client' or 'server_side'.")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1.")

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
        self._retry_policy = retry_policy or RetryPolicy[Any](
            max_attempts=3,
            initial_backoff_s=0.25,
            backoff_multiplier=2.0,
            max_backoff_s=2.0,
            retry_exceptions=_DEFAULT_SOURCE_RETRY_EXCEPTIONS,
            retry_if=_is_retriable_postgres_read_error,
        )
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

    async def prepare_resume(self, checkpoint: Any) -> None:
        self._resume_prepare_count += 1
        self._reset_progress()
        self._params = dict(self._base_params)
        if checkpoint is None or not self.supports_checkpoint:
            return

        value = checkpoint.value if isinstance(checkpoint.value, dict) else {}
        if "cursor" not in value:
            return
        cursor = value["cursor"]

        if self._checkpoint_param is not None:
            self._params[self._checkpoint_param] = cursor
            self._resume_checkpoint_apply_count += 1
            return

        if not isinstance(cursor, dict):
            raise TypeError(
                "Composite PostgresSource checkpoints require cursor values to be dicts."
            )
        for field in self._checkpoint_fields:
            param_name = self._checkpoint_params[field]
            if field not in cursor:
                raise ValueError(
                    f"Checkpoint cursor is missing composite field {field!r}: {cursor!r}"
                )
            self._params[param_name] = cursor[field]
        self._resume_checkpoint_apply_count += 1

    def current_checkpoint(self) -> dict[str, Any] | None:
        if self._rows_seen <= 0 and self._last_checkpoint_cursor is None:
            return None
        checkpoint: dict[str, Any] = {"row_number": self._rows_seen}
        if self._last_checkpoint_cursor is not None:
            checkpoint["cursor"] = self._last_checkpoint_cursor
        return checkpoint

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def recovery_contract(self) -> PostgresSourceRecoveryContractSnapshot:
        checkpoint_fields = (
            tuple(self._checkpoint_fields)
            if self._checkpoint_fields
            else ((self._checkpoint_field,) if self._checkpoint_field is not None else ())
        )
        checkpoint_params = (
            dict(self._checkpoint_params)
            if self._checkpoint_params
            else (
                {}
                if self._checkpoint_field is None or self._checkpoint_param is None
                else {self._checkpoint_field: self._checkpoint_param}
            )
        )
        return PostgresSourceRecoveryContractSnapshot(
            mode=(
                PostgresSourceRecoveryMode.CHECKPOINT_RERUN
                if self.supports_checkpoint
                else PostgresSourceRecoveryMode.FULL_RERUN
            ),
            supports_checkpoint=self.supports_checkpoint,
            checkpoint_fields=checkpoint_fields,
            checkpoint_params=checkpoint_params,
            on_record_error=str(self._on_record_error),
        )

    def metrics_snapshot(self) -> PostgresSourceMetricsSnapshot:
        health = self._build_health_snapshot()
        return PostgresSourceMetricsSnapshot(
            batch_size=self._batch_size,
            supports_checkpoint=self.supports_checkpoint,
            row_mapper_accepts_context=self._row_mapper_accepts_context,
            fetch_strategy=self._fetch_strategy,
            read_routing=self._read_routing,
            target_session_attrs=health.target_session_attrs,
            statement_timeout_ms=self._statement_timeout_ms,
            transaction_read_only=self._transaction_read_only,
            transaction_isolation_level=self._transaction_isolation_level,
            server_side_cursor_withhold=self._server_side_cursor_withhold,
            max_replica_replay_lag_s=self._max_replica_replay_lag_s,
            on_replica_stale=self._on_replica_stale,
            ready=health.ready,
            connection_ready=health.connection_ready,
            routing_ready=health.routing_ready,
            staleness_guard_ready=health.staleness_guard_ready,
            active_stream_count=self._active_stream_count,
            rows_seen=self._rows_seen,
            stream_run_count=self._stream_run_count,
            query_execution_count=self._query_execution_count,
            retry_count=self._retry_count,
            staleness_guard_block_count=self._staleness_guard_block_count,
            staleness_guard_primary_fallback_count=self._staleness_guard_primary_fallback_count,
            resume_prepare_count=self._resume_prepare_count,
            resume_checkpoint_apply_count=self._resume_checkpoint_apply_count,
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
            connected_server_role=self._connected_server_role,
            last_replica_replay_lag_s=self._last_replica_replay_lag_s,
            last_checkpoint_cursor_present=self._last_checkpoint_cursor is not None,
            last_stream_succeeded=self._last_stream_succeeded,
            last_stream_started_at=self._last_stream_started_at,
            last_stream_completed_at=self._last_stream_completed_at,
            last_row_at=self._last_row_at,
            last_health_error=self._last_health_error,
            last_health_checked_at=self._last_health_checked_at,
            recovery_contract=self.recovery_contract(),
        )

    def render_prometheus_metrics(self, namespace: str = "agora_postgres") -> str:
        return PostgresPrometheusExporter(namespace=namespace).render_source(
            self.metrics_snapshot()
        )

    async def health_snapshot(self, *, force_refresh: bool = False) -> PostgresSourceHealthSnapshot:
        if force_refresh or self._last_health_checked_at is None:
            await self._refresh_health_state()
        return self._build_health_snapshot()

    async def stream(self) -> AsyncGenerator[T, None]:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError(
                "PostgresSource requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
            ) from None

        logger.info("postgres_source_start", query=self._query[:80])
        self._reset_progress()
        fetched = 0
        self._stream_run_count += 1
        self._active_stream_count += 1
        self._last_stream_started_at = datetime.now(UTC)
        stream_failed = False
        attempt = 1
        try:
            while True:
                try:
                    async for record in self._stream_attempt(psycopg, dict_row):
                        fetched += 1
                        yield record
                    break
                except Exception as exc:
                    if not self._should_retry_read(exc, attempt=attempt):
                        raise
                    delay = self._retry_policy.backoff_for(attempt=attempt)
                    self._retry_count += 1
                    logger.warning(
                        "postgres_source_retrying_read",
                        attempt=attempt,
                        delay_s=delay,
                        error=str(exc),
                    )
                    if delay > 0:
                        import asyncio

                        await asyncio.sleep(delay)
                    attempt += 1
        except Exception:
            stream_failed = True
            raise
        finally:
            self._last_stream_completed_at = datetime.now(UTC)
            self._last_stream_succeeded = not stream_failed
            self._active_stream_count = max(self._active_stream_count - 1, 0)

        logger.info("postgres_source_done", records=fetched)

    async def _stream_attempt(
        self,
        psycopg: Any,
        dict_row: Any,
    ) -> AsyncGenerator[T, None]:
        self._query_execution_count += 1
        async with await self._connect_for_read_attempt(psycopg, dict_row) as conn:
            async with conn.cursor() as config_cur:
                await self._apply_read_safety_controls(config_cur)
            async with self._open_stream_cursor(conn) as cur:
                await cur.execute(self._query, self._params if self._params else None)
                while True:
                    rows = await cur.fetchmany(self._batch_size)
                    if not rows:
                        break
                    for row in rows:
                        row_dict: dict[str, Any] | None = None
                        try:
                            row_dict = dict(row)
                            self._rows_seen += 1
                            checkpoint_cursor = self._extract_checkpoint_cursor(row_dict)
                            self._current_row_checkpoint_cursor = checkpoint_cursor
                            self._last_row_at = datetime.now(UTC)
                            record = await self._map_row(row_dict)
                            self._last_checkpoint_cursor = checkpoint_cursor
                            if record is not None:
                                yield record
                            else:
                                self._record_drop_count += 1
                        except Exception as exc:
                            self._record_error_count += 1
                            logger.warning("postgres_source_row_error", error=str(exc))
                            if self._on_record_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                                self._record_drop_count += 1
                                continue
                            failed_record = row_dict if row_dict is not None else row
                            raise SourceRecordError(
                                exc,
                                record=failed_record,
                                checkpoint=self.current_checkpoint(),
                                source=self.source_name,
                            ) from exc
                        finally:
                            self._current_row_checkpoint_cursor = None

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
            if count_staleness_events:
                self._staleness_guard_primary_fallback_count += 1
            fallback_conn = await self._open_connection(
                psycopg,
                dict_row,
                target_session_attrs=self._read_target_session_attrs("primary"),
            )
            fallback_role, fallback_replay_lag_s = await self._inspect_server_role(fallback_conn)
            return fallback_conn, fallback_role, fallback_replay_lag_s

        if count_staleness_events:
            self._staleness_guard_block_count += 1
        self._set_server_observation(
            role=role,
            replay_lag_s=replay_lag_s,
            error=(
                "Postgres standby replay lag exceeded source staleness guard: "
                f"{float(replay_lag_s or 0.0):.3f}s > {float(self._max_replica_replay_lag_s or 0.0):.3f}s"
            ),
        )
        await conn.close()
        raise PostgresReplicaStalenessError(
            replay_lag_s=float(replay_lag_s or 0.0),
            max_replica_replay_lag_s=float(self._max_replica_replay_lag_s or 0.0),
        )

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
        if self._row_mapper_accepts_context:
            record = self._row_mapper(row_dict, self._row_context())
        else:
            record = self._row_mapper(row_dict)
        if isawaitable(record):
            return await cast("Awaitable[T | None]", record)
        return record

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
        if self._checkpoint_field is not None:
            if self._checkpoint_field not in row_dict:
                raise KeyError(f"Checkpoint field {self._checkpoint_field!r} missing from row")
            return row_dict[self._checkpoint_field]

        if self._checkpoint_fields:
            cursor: dict[str, Any] = {}
            for field in self._checkpoint_fields:
                if field not in row_dict:
                    raise KeyError(f"Checkpoint field {field!r} missing from row")
                cursor[field] = row_dict[field]
            return cursor

        return None

    def _reset_progress(self) -> None:
        self._rows_seen = 0
        self._last_checkpoint_cursor = None
        self._current_row_checkpoint_cursor = None
        self._record_error_count = 0
        self._record_drop_count = 0
        self._retry_count = 0
        self._staleness_guard_block_count = 0
        self._staleness_guard_primary_fallback_count = 0
        self._connected_server_role = None
        self._last_replica_replay_lag_s = None
        self._last_health_error = None
        self._last_health_checked_at = None

    def _should_retry_read(self, exc: Exception, *, attempt: int) -> bool:
        if self._rows_seen > 0 or self._last_checkpoint_cursor is not None:
            return False
        return self._retry_policy.should_retry(exc, attempt=attempt)

    def _build_health_snapshot(self) -> PostgresSourceHealthSnapshot:
        connection_ready = self._connected_server_role is not None
        routing_ready = self._routing_ready(self._connected_server_role)
        staleness_guard_ready = self._staleness_guard_ready(
            self._connected_server_role,
            self._last_replica_replay_lag_s,
        )
        return PostgresSourceHealthSnapshot(
            ready=(
                connection_ready
                and routing_ready
                and staleness_guard_ready
                and self._last_health_error is None
            ),
            connection_ready=connection_ready,
            routing_ready=routing_ready,
            staleness_guard_ready=staleness_guard_ready,
            read_routing=self._read_routing,
            target_session_attrs=self._target_session_attrs(),
            on_replica_stale=self._on_replica_stale,
            connected_server_role=self._connected_server_role,
            max_replica_replay_lag_s=self._max_replica_replay_lag_s,
            last_replica_replay_lag_s=self._last_replica_replay_lag_s,
            last_error=self._last_health_error,
            checked_at=self._last_health_checked_at or datetime.now(UTC),
        )

    def _routing_ready(self, role: str | None) -> bool:
        if role is None:
            return False
        if self._read_routing == "primary":
            return role == "primary"
        if self._read_routing == "standby":
            return role == "standby"
        return True

    def _staleness_guard_ready(self, role: str | None, replay_lag_s: float | None) -> bool:
        if role is None:
            return False
        return not self._replica_is_stale(role=role, replay_lag_s=replay_lag_s)

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


def _callable_accepts_context(func: object) -> bool:
    try:
        parameters = signature(cast("Callable[..., Any]", func)).parameters.values()
    except (TypeError, ValueError):
        return False

    positional = [
        parameter
        for parameter in parameters
        if parameter.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if any(parameter.kind is Parameter.VAR_POSITIONAL for parameter in parameters):
        return True
    return len(positional) >= 2


def _is_retriable_postgres_read_error(exc: Exception) -> bool:
    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(sqlstate, str) and (
        sqlstate.startswith("08") or sqlstate in {"57P01", "57P02", "57P03", "40001", "40P01"}
    ):
        return True
    error_name = type(exc).__name__.lower()
    message = str(exc).lower()
    return any(
        marker in error_name or marker in message
        for marker in (
            "connection",
            "connect",
            "timeout",
            "temporar",
            "operational",
            "admin shutdown",
            "server closed the connection",
        )
    )


def _sql_isolation_level(
    value: Literal["read_committed", "repeatable_read", "serializable"],
) -> str:
    return {
        "read_committed": "read committed",
        "repeatable_read": "repeatable read",
        "serializable": "serializable",
    }[value]


def _generated_server_side_cursor_name() -> str:
    return f"agora_pg_source_{uuid.uuid4().hex}"
