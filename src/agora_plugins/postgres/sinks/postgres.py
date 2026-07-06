"""
agora_plugins.postgres.sinks.postgres
=====================================
Generic async PostgreSQL sink with batch upsert plus schema adapter helpers.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

import logstruct
from agora.core.dlq import DLQRecord, DLQSink
from agora.core.failures import PoisonRecordClassification, PoisonRecordInfo
from agora.core.retry import RetryPolicy, retry_async
from agora.core.sink import BaseSink

from agora_plugins.postgres.connection import (
    PostgresConnectionConfig,
    coerce_connection_config,
    redact_postgres_dsn,
)
from agora_plugins.postgres.sinks._identifiers import (
    QuotedIdentifier,
    _identifier_parts,
    _postgres_type,
    _schema_advisory_lock_key,
    _table_catalog_condition,
    _table_lookup_condition,
)
from agora_plugins.postgres.sinks._identifiers import (
    _quote_identifier as _quote_identifier,
)
from agora_plugins.postgres.sinks._metrics import (
    PostgresLatencyHistogramSnapshot,
    PostgresSinkMetricsSnapshot,
)
from agora_plugins.postgres.sinks._pool import (
    acquire_write_conn,
    discard_pooled_connection,
    ensure_external_write_pool,
    pooled_connection_ready,
    release_write_conn,
)
from agora_plugins.postgres.sinks._write_plan import PostgresWritePlanner
from agora_plugins.postgres.sinks._write_strategies import (
    execute_copy_batch,
    execute_copy_merge_batch,
    execute_sql_batch,
    flush_via_copy,
    flush_via_copy_merge,
    flush_via_sql,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Sequence

    from agora.core.context import PipelineContext
    from agora.schema.types import Schema

T = TypeVar("T")

logger = logstruct.getLogger(__name__)
_POSTGRES_LATENCY_BUCKETS_S = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _resolve_write_safety_policy(
    value: PostgresWriteSafetyPolicy | str,
) -> PostgresWriteSafetyPolicy:
    if isinstance(value, PostgresWriteSafetyPolicy):
        return value
    try:
        return PostgresWriteSafetyPolicy(value)
    except ValueError as exc:
        allowed = ", ".join(policy.value for policy in PostgresWriteSafetyPolicy)
        raise ValueError(f"write_safety_policy must be one of: {allowed}. Got {value!r}.") from exc


class PostgresWriteSafetyPolicy(StrEnum):
    """Controls how PostgresSink reacts to live target-schema drift."""

    STRICT = "strict"
    ALIGN_TO_TARGET = "align_to_target"


PostgresPoisonRecordClassification = PoisonRecordClassification


@dataclass(frozen=True, slots=True, kw_only=True)
class PostgresPoisonRecordInfo(PoisonRecordInfo):
    """Structured poison metadata for DLQ and incident tooling."""

    classification: PostgresPoisonRecordClassification
    reason: str
    details: dict[str, Any]


class PostgresSinkWriteError(RuntimeError):
    """Structured sink write error carrying Postgres poison metadata."""

    def __init__(
        self,
        message: str,
        *,
        poison_info: PostgresPoisonRecordInfo,
    ) -> None:
        super().__init__(message)
        self.poison_info = poison_info
        self.dlq_details = {"postgres": poison_info.to_dict()}


@dataclass(frozen=True, slots=True)
class _TargetColumn:
    name: str
    nullable: bool
    has_default: bool
    writable: bool


@dataclass(frozen=True, slots=True)
class _PreparedWriteBatch:
    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    row_indexes: tuple[int, ...]


class PostgresSink(BaseSink[T], Generic[T]):
    """Generic async batch-upsert PostgreSQL sink."""

    sink_name = "postgres"

    def __init__(
        self,
        dsn: str | None,
        table: str | QuotedIdentifier,
        row_mapper: Callable[[T], dict[str, Any]],
        conflict_key: str | QuotedIdentifier | list[str | QuotedIdentifier],
        batch_size: int = 100,
        upsert: bool = True,
        insert_mode: Literal["sql", "copy", "copy_merge"] = "sql",
        pool_size: int = 1,
        max_rows_per_statement: int | None = None,
        max_parameters_per_statement: int = 32_000,
        retry_policy: RetryPolicy[Any] | None = None,
        write_safety_policy: PostgresWriteSafetyPolicy | str = PostgresWriteSafetyPolicy.STRICT,
        connection: PostgresConnectionConfig | None = None,
        poison_record_sink: DLQSink | None = None,
        poison_record_pipeline_id: str | None = None,
        poison_record_max_attempts: int | None = None,
        pool_acquire_timeout_s: float | None = 30.0,
        pool_health_check: bool = True,
        pool_max_lifetime_s: float = 3600.0,
        pool_max_idle_s: float = 600.0,
        allow_quoted_identifiers: bool = False,
    ) -> None:
        if insert_mode not in {"sql", "copy", "copy_merge"}:
            raise ValueError("insert_mode must be 'sql', 'copy', or 'copy_merge'")
        if upsert and insert_mode == "copy":
            raise ValueError("insert_mode='copy' is only supported when upsert=False")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")
        if max_rows_per_statement is not None and max_rows_per_statement < 1:
            raise ValueError("max_rows_per_statement must be >= 1 when provided")
        if max_parameters_per_statement < 1:
            raise ValueError("max_parameters_per_statement must be >= 1")
        self._connection = coerce_connection_config(dsn=dsn, connection=connection)
        self._table = table
        self._row_mapper = row_mapper
        self._conflict_keys = (
            [conflict_key] if isinstance(conflict_key, (str, QuotedIdentifier)) else conflict_key
        )
        self._allow_quoted_identifiers = allow_quoted_identifiers
        self._batch_size = batch_size
        self._upsert = upsert
        self._write_planner = PostgresWritePlanner(
            table=table,
            conflict_keys=tuple(self._conflict_keys),
            upsert=upsert,
            allow_quoted_identifiers=allow_quoted_identifiers,
        )
        self._insert_mode = insert_mode
        self._pool_size = pool_size
        self._max_rows_per_statement = max_rows_per_statement
        self._max_parameters_per_statement = max_parameters_per_statement
        self._retry_policy = retry_policy
        self._write_safety_policy = _resolve_write_safety_policy(write_safety_policy)
        self._poison_record_sink = poison_record_sink
        self._poison_record_pipeline_id = poison_record_pipeline_id or f"postgres:{table}"
        self._poison_record_max_attempts = poison_record_max_attempts
        if pool_acquire_timeout_s is not None and pool_acquire_timeout_s <= 0:
            raise ValueError("pool_acquire_timeout_s must be > 0 when provided")
        if pool_max_lifetime_s <= 0:
            raise ValueError("pool_max_lifetime_s must be > 0")
        if pool_max_idle_s <= 0:
            raise ValueError("pool_max_idle_s must be > 0")
        self._pool_acquire_timeout_s = pool_acquire_timeout_s
        self._pool_health_check = pool_health_check
        self._pool_max_lifetime_s = pool_max_lifetime_s
        self._pool_max_idle_s = pool_max_idle_s
        self._buffer: list[dict[str, Any]] = []
        self._conn: Any | None = None
        self._write_pool: asyncio.LifoQueue[Any] | None = None
        self._write_pool_open_connections = 0
        self._write_pool_lock = asyncio.Lock()
        self._external_write_pool: Any | None = None
        self._external_write_pool_conn_ids: set[int] = set()
        self._external_write_pool_unavailable = False
        self._psycopg: Any | None = None
        self._latency_bucket_counts: dict[tuple[str, str], list[int]] = {}
        self._latency_counts: dict[tuple[str, str], int] = {}
        self._latency_sums: dict[tuple[str, str], float] = {}
        self._write_call_count = 0
        self._write_batch_call_count = 0
        self._enqueue_count = 0
        self._flush_count = 0
        self._flushed_row_count = 0
        self._retry_count = 0
        self._schema_refresh_count = 0
        self._schema_drift_detected_count = 0
        self._schema_drift_aligned_count = 0
        self._poison_record_count = 0
        self._poison_record_classification_counts = dict.fromkeys(
            PostgresPoisonRecordClassification,
            0,
        )
        self._last_flush_at: datetime | None = None
        self._target_columns_cache: list[_TargetColumn] | None = None
        self._defer_upsert_constraint_preflight = False
        self._upsert_constraint_preflight_complete = False

    def invalidate_target_columns_cache(self) -> None:
        """Clear cached target-table metadata after an external schema change."""

        self._target_columns_cache = None

    def defer_upsert_constraint_preflight_until_schema_applied(self) -> None:
        """Let a schema adapter run DDL before validating upsert constraints."""

        self._defer_upsert_constraint_preflight = True
        self._upsert_constraint_preflight_complete = False

    async def _load_psycopg(self) -> Any:
        if self._psycopg is None:
            try:
                import psycopg
            except ImportError:
                raise ImportError(
                    "PostgresSink requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
                ) from None
            self._psycopg = cast("Any", psycopg)
        return self._psycopg

    async def _create_connection(self) -> Any:
        psycopg = await self._load_psycopg()
        started = time.perf_counter()
        try:
            conn = await psycopg.AsyncConnection.connect(
                **self._connection.connect_kwargs(autocommit=False)
            )
        except Exception:
            self._observe_latency("connect", "error", time.perf_counter() - started)
            raise
        self._observe_latency("connect", "success", time.perf_counter() - started)
        logger.info(
            "postgres_sink_connected",
            table=self._table,
            dsn=redact_postgres_dsn(self._connection.resolve_dsn()),
        )
        return conn

    async def _get_conn(self) -> Any:
        if self._conn is None:
            self._conn = await self._create_connection()
        return self._conn

    async def _acquire_write_conn(self) -> tuple[Any, bool]:
        return await acquire_write_conn(self)

    async def _pooled_connection_ready(self, conn: Any) -> bool:
        return await pooled_connection_ready(self, conn)

    async def _discard_pooled_connection(self, conn: Any) -> None:
        await discard_pooled_connection(self, conn)

    async def _release_write_conn(self, conn: Any, *, pooled: bool, discard: bool = False) -> None:
        await release_write_conn(self, conn, pooled=pooled, discard=discard)

    async def _ensure_external_write_pool(self) -> Any | None:
        return await ensure_external_write_pool(self)

    @asynccontextmanager
    async def _write_connection(self) -> AsyncIterator[Any]:
        started = time.perf_counter()
        try:
            conn, pooled = await self._acquire_write_conn()
        except Exception:
            self._observe_latency("pool_acquire", "error", time.perf_counter() - started)
            raise
        self._observe_latency("pool_acquire", "success", time.perf_counter() - started)
        discard = False
        try:
            yield conn
        except Exception:
            discard = True
            raise
        finally:
            await self._release_write_conn(conn, pooled=pooled, discard=discard)

    async def connection(self) -> Any:
        """Public API for obtaining the underlying connection (used by schema adapters)."""
        return await self._get_conn()

    async def open(self) -> None:
        poison_opened = False
        try:
            if self._poison_record_sink is not None:
                await self._poison_record_sink.open()
                poison_opened = True
            if self._upsert and not self._defer_upsert_constraint_preflight:
                await self.validate_upsert_constraint()
        except Exception:
            if poison_opened and self._poison_record_sink is not None:
                try:
                    await self._poison_record_sink.close()
                except Exception:
                    logger.exception("postgres_open_cleanup_error", table=self._table)
            raise

    async def validate_upsert_constraint(self) -> None:
        if not self._upsert or self._upsert_constraint_preflight_complete:
            return
        async with self._write_connection() as conn:
            has_constraint = await self._has_matching_upsert_constraint(conn)
            await conn.rollback()
            if not has_constraint:
                conflict_keys = [str(key) for key in self._conflict_keys]
                raise ValueError(
                    "PostgresSink upsert requires a PRIMARY KEY or UNIQUE constraint "
                    "whose key columns exactly match conflict_keys. "
                    f"table={self._table!r} conflict_keys={conflict_keys!r}. "
                    "Create the constraint or wrap the sink in PostgresSchemaAdapter."
                )
        self._upsert_constraint_preflight_complete = True

    async def _has_matching_upsert_constraint(self, conn: Any) -> bool:
        where_sql, where_params = _table_catalog_condition(
            self._table,
            namespace_alias="ns",
            table_alias="tbl",
            allow_quoted=self._allow_quoted_identifiers,
        )
        expected = sorted(
            _identifier_parts(
                key,
                allow_quoted=self._allow_quoted_identifiers,
            )[0]
            for key in self._conflict_keys
        )
        sql = f"""
            SELECT array_agg(att.attname ORDER BY att.attname)
            FROM pg_index idx
            JOIN pg_class tbl ON tbl.oid = idx.indrelid
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            JOIN unnest(idx.indkey) WITH ORDINALITY AS keycols(attnum, ordinality)
                ON keycols.ordinality <= idx.indnkeyatts
            JOIN pg_attribute att
                ON att.attrelid = tbl.oid
                AND att.attnum = keycols.attnum
            WHERE idx.indisunique
              AND idx.indpred IS NULL
              AND idx.indexprs IS NULL
              AND {where_sql}
            GROUP BY idx.indexrelid, idx.indnkeyatts
            HAVING count(*) = %s
        """
        async with conn.cursor() as cur:
            await cur.execute(sql, (*where_params, len(expected)))
            rows = await cur.fetchall()
        for row in rows:
            columns = row[0] if row else None
            if sorted(str(column) for column in columns or []) == expected:
                return True
        return False

    def _effective_retry_policy(self) -> RetryPolicy[Any]:
        if self._retry_policy is not None:
            return self._retry_policy
        if self._psycopg is None:
            return RetryPolicy[Any](max_attempts=1)
        return RetryPolicy[Any](
            max_attempts=3,
            initial_backoff_s=0.25,
            backoff_multiplier=2.0,
            max_backoff_s=2.0,
            retry_exceptions=(self._psycopg.OperationalError, self._psycopg.InterfaceError),
        )

    def _build_upsert_sql(self, columns: Sequence[str | QuotedIdentifier]) -> str:
        return self._write_planner.build_upsert_sql(columns)

    def _build_batch_upsert_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        *,
        row_count: int,
    ) -> str:
        return self._write_planner.build_batch_upsert_sql(
            columns,
            row_count=row_count,
        )

    def _build_copy_sql(self, columns: Sequence[str | QuotedIdentifier]) -> str:
        return self._write_planner.build_copy_sql(columns)

    def _build_copy_sql_for_table(
        self,
        table: str | QuotedIdentifier,
        columns: Sequence[str | QuotedIdentifier],
    ) -> str:
        return self._write_planner.build_copy_sql_for_table(table, columns)

    def _build_copy_merge_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        staging_table: str,
    ) -> str:
        return self._write_planner.build_copy_merge_sql(columns, staging_table)

    def _build_create_temp_table_sql(self, staging_table: str) -> str:
        return self._write_planner.build_create_temp_table_sql(staging_table)

    def _build_stage_table_name(self) -> str:
        return f"agora_stage_{uuid.uuid4().hex[:12]}"

    def _flatten_rows(self, rows: list[dict[str, Any]], columns: list[str]) -> list[Any]:
        params: list[Any] = []
        expected_columns = tuple(columns)
        for row in rows:
            row_columns = tuple(row.keys())
            if set(row_columns) != set(expected_columns):
                raise ValueError(
                    "PostgresSink rows in the same batch must have identical column sets. "
                    f"Expected {expected_columns!r}, got {row_columns!r}."
                )
            params.extend(row[column] for column in columns)
        return params

    def _statement_row_limit(self, column_count: int) -> int:
        if column_count <= 0:
            return 1
        by_params = max(1, self._max_parameters_per_statement // column_count)
        if self._max_rows_per_statement is None:
            return by_params
        return max(1, min(self._max_rows_per_statement, by_params))

    def _iter_sql_chunks(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> Iterator[list[dict[str, Any]]]:
        chunk_size = self._statement_row_limit(len(columns))
        for start in range(0, len(rows), chunk_size):
            yield rows[start : start + chunk_size]

    async def _prepared_write_batches(
        self,
        rows: list[dict[str, Any]],
    ) -> list[_PreparedWriteBatch]:
        if not rows:
            return []
        if self._write_safety_policy == PostgresWriteSafetyPolicy.STRICT:
            return [
                _PreparedWriteBatch(
                    columns=tuple(rows[0].keys()),
                    rows=tuple(rows),
                    row_indexes=tuple(range(len(rows))),
                )
            ]

        target_columns = await self._load_target_columns()
        return self._align_rows_to_target(rows, target_columns)

    async def _load_target_columns(self, *, force_refresh: bool = False) -> list[_TargetColumn]:
        if self._target_columns_cache is not None and not force_refresh:
            return list(self._target_columns_cache)

        conn = await self._get_conn()
        where_sql, where_params = _table_lookup_condition(
            self._table,
            allow_quoted=self._allow_quoted_identifiers,
        )
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT column_name, is_nullable, column_default, is_generated "
                "FROM information_schema.columns "
                f"WHERE {where_sql} "
                "ORDER BY ordinal_position",
                where_params,
            )
            rows = await cur.fetchall()
        self._schema_refresh_count += 1
        if not rows:
            raise self._make_write_error(
                "Postgres target table is missing.",
                classification=PostgresPoisonRecordClassification.SCHEMA_DRIFT,
                reason="undefined_table",
                details={"table": self._table},
            )
        target_columns = [
            _TargetColumn(
                name=str(column_name),
                nullable=str(is_nullable).upper() == "YES",
                has_default=(column_default is not None),
                writable=str(is_generated).upper() == "NEVER",
            )
            for column_name, is_nullable, column_default, is_generated in rows
        ]
        self._target_columns_cache = target_columns
        return list(target_columns)

    def _align_rows_to_target(
        self,
        rows: list[dict[str, Any]],
        target_columns: list[_TargetColumn],
    ) -> list[_PreparedWriteBatch]:
        writable_order = [column.name for column in target_columns if column.writable]
        grouped: dict[tuple[str, ...], list[tuple[int, dict[str, Any]]]] = {}
        order: list[tuple[str, ...]] = []

        for index, row in enumerate(rows):
            normalized_row = self._normalize_row_to_target(row, target_columns, row_index=index)
            ordered_columns = tuple(column for column in writable_order if column in normalized_row)
            if ordered_columns not in grouped:
                grouped[ordered_columns] = []
                order.append(ordered_columns)
            grouped[ordered_columns].append((index, normalized_row))

        return [
            _PreparedWriteBatch(
                columns=columns,
                rows=tuple(row for _index, row in grouped[columns]),
                row_indexes=tuple(index for index, _row in grouped[columns]),
            )
            for columns in order
        ]

    def _normalize_row_to_target(
        self,
        row: dict[str, Any],
        target_columns: list[_TargetColumn],
        *,
        row_index: int,
    ) -> dict[str, Any]:
        target_by_name = {column.name: column for column in target_columns}
        unknown_columns = [
            key for key in row if key not in target_by_name or not target_by_name[key].writable
        ]
        if unknown_columns:
            self._schema_drift_detected_count += 1
            self._schema_drift_aligned_count += len(unknown_columns)

        normalized = {
            key: value
            for key, value in row.items()
            if key in target_by_name and target_by_name[key].writable
        }

        missing_conflict_keys = [key for key in self._conflict_keys if key not in normalized]
        if missing_conflict_keys:
            raise self._make_write_error(
                "Postgres row is missing conflict keys after schema alignment.",
                classification=PostgresPoisonRecordClassification.SCHEMA_DRIFT,
                reason="missing_conflict_keys",
                details={
                    "table": self._table,
                    "row_index": row_index,
                    "missing_conflict_keys": missing_conflict_keys,
                    "input_columns": list(row.keys()),
                },
            )

        missing_required = [
            column.name
            for column in target_columns
            if (
                column.writable
                and column.name not in normalized
                and not column.nullable
                and not column.has_default
            )
        ]
        if missing_required:
            self._schema_drift_detected_count += 1
            raise self._make_write_error(
                "Postgres target schema requires non-null columns that are missing from the row.",
                classification=PostgresPoisonRecordClassification.SCHEMA_DRIFT,
                reason="missing_required_columns",
                details={
                    "table": self._table,
                    "row_index": row_index,
                    "missing_required_columns": missing_required,
                    "input_columns": list(row.keys()),
                },
            )

        if not normalized:
            raise self._make_write_error(
                "Postgres row has no writable target columns after schema alignment.",
                classification=PostgresPoisonRecordClassification.SCHEMA_DRIFT,
                reason="no_writable_columns",
                details={
                    "table": self._table,
                    "row_index": row_index,
                    "input_columns": list(row.keys()),
                },
            )
        return normalized

    def _make_write_error(
        self,
        message: str,
        *,
        classification: PostgresPoisonRecordClassification,
        reason: str,
        details: dict[str, Any],
    ) -> PostgresSinkWriteError:
        error = PostgresSinkWriteError(
            message,
            poison_info=PostgresPoisonRecordInfo(
                classification=classification,
                reason=reason,
                details=details,
            ),
        )
        self._observe_poison_record(error)
        return error

    def _wrap_write_error(
        self,
        exc: Exception,
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> PostgresSinkWriteError:
        if isinstance(exc, PostgresSinkWriteError):
            return exc

        classification = PostgresPoisonRecordClassification.UNKNOWN
        reason = type(exc).__name__
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate in {"42P01", "42703"}:
            classification = PostgresPoisonRecordClassification.SCHEMA_DRIFT
            reason = "undefined_table" if sqlstate == "42P01" else "undefined_column"
            self._schema_drift_detected_count += 1
        elif sqlstate in {"23502", "23505", "23503", "23514"}:
            classification = PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION
            reason = {
                "23502": "not_null_violation",
                "23505": "unique_violation",
                "23503": "foreign_key_violation",
                "23514": "check_violation",
            }[sqlstate]
        elif sqlstate in {"22P02", "42804"}:
            classification = PostgresPoisonRecordClassification.TYPE_MISMATCH
            reason = "invalid_text_representation" if sqlstate == "22P02" else "datatype_mismatch"

        error = PostgresSinkWriteError(
            f"Postgres sink write failed: {exc}",
            poison_info=PostgresPoisonRecordInfo(
                classification=classification,
                reason=reason,
                details={
                    "table": self._table,
                    "sqlstate": sqlstate,
                    "columns": list(columns),
                    "row_count": len(rows),
                },
            ),
        )
        self._observe_poison_record(error)
        return error

    async def write(self, record: T) -> None:
        row = self._row_mapper(record)
        if self._write_safety_policy == PostgresWriteSafetyPolicy.ALIGN_TO_TARGET:
            target_columns = await self._load_target_columns()
            row = self._normalize_row_to_target(row, target_columns, row_index=0)
        self._write_call_count += 1
        self._enqueue_count += 1
        self._buffer.append(row)
        if len(self._buffer) >= self._batch_size:
            try:
                await self.flush()
            except PostgresSinkWriteError as exc:
                await self._route_failed_buffer_to_dlq(exc)
                raise

    async def write_batch(self, records: list[T]) -> None:
        if not records:
            return
        self._write_batch_call_count += 1
        mapped_rows = [self._row_mapper(record) for record in records]
        if self._write_safety_policy == PostgresWriteSafetyPolicy.ALIGN_TO_TARGET:
            target_columns = await self._load_target_columns()
            mapped_rows = [
                self._normalize_row_to_target(row, target_columns, row_index=index)
                for index, row in enumerate(mapped_rows)
            ]
        for row in mapped_rows:
            self._enqueue_count += 1
            self._buffer.append(row)
            if len(self._buffer) >= self._batch_size:
                try:
                    await self.flush()
                except PostgresSinkWriteError as exc:
                    await self._route_failed_buffer_to_dlq(exc)
                    raise

    async def flush(self) -> None:
        if not self._buffer:
            return

        await self._load_psycopg()
        started = time.perf_counter()
        rows = list(self._buffer)
        count = len(rows)
        policy = self._effective_retry_policy()
        try:
            batches = await self._prepared_write_batches(rows)
        except Exception as exc:
            self._observe_latency("flush", "error", time.perf_counter() - started)
            columns = list(rows[0].keys())
            raise self._wrap_write_error(exc, rows=rows, columns=columns) from exc
        flushed_indexes: set[int] = set()
        try:
            if (
                self._write_safety_policy == PostgresWriteSafetyPolicy.ALIGN_TO_TARGET
                and len(batches) > 1
            ):
                await self._flush_aligned_batches_atomically(batches, rows, policy)
            else:
                for batch in batches:
                    columns = list(batch.columns)
                    batch_rows = list(batch.rows)
                    if self._insert_mode == "copy":
                        await self._flush_via_copy(batch_rows, columns, len(batch_rows), policy)
                    elif self._insert_mode == "copy_merge":
                        await self._flush_via_copy_merge(
                            batch_rows, columns, len(batch_rows), policy
                        )
                    else:
                        await self._flush_via_sql(batch_rows, columns, len(batch_rows), policy)
                    flushed_indexes.update(batch.row_indexes)
        except Exception:
            self._discard_buffer_indexes(flushed_indexes)
            self._observe_latency("flush", "error", time.perf_counter() - started)
            raise

        self._discard_buffer_indexes(set(range(len(rows))))
        self._flush_count += 1
        self._flushed_row_count += count
        self._last_flush_at = _now_utc()
        self._observe_latency("flush", "success", time.perf_counter() - started)
        logger.info("postgres_flush", table=self._table, count=count)

    def _discard_buffer_indexes(self, indexes: set[int]) -> None:
        for index in sorted(indexes, reverse=True):
            if index < len(self._buffer):
                del self._buffer[index]

    async def _flush_via_sql(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        policy: RetryPolicy[Any],
    ) -> None:
        await flush_via_sql(self, rows, columns, count, policy)

    async def _flush_via_copy(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        _policy: RetryPolicy[Any],
    ) -> None:
        await flush_via_copy(self, rows, columns, count)

    async def _flush_via_copy_merge(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        policy: RetryPolicy[Any],
    ) -> None:
        await flush_via_copy_merge(self, rows, columns, count, policy)

    async def _flush_aligned_batches_atomically(
        self,
        batches: list[_PreparedWriteBatch],
        rows: list[dict[str, Any]],
        policy: RetryPolicy[Any],
    ) -> None:
        count = len(rows)

        def _on_retry(attempt: int, exc: Exception, delay: float) -> None:
            logger.warning(
                "postgres_flush_retry",
                table=self._table,
                count=count,
                attempt=attempt,
                wait_s=delay,
                error=str(exc),
            )
            self._observe_retry()

        async def _execute_flush() -> None:
            async with self._write_connection() as conn:
                try:
                    for batch in batches:
                        batch_rows = list(batch.rows)
                        columns = list(batch.columns)
                        if self._insert_mode == "copy":
                            await execute_copy_batch(self, conn, batch_rows, columns)
                        elif self._insert_mode == "copy_merge":
                            await execute_copy_merge_batch(self, conn, batch_rows, columns)
                        else:
                            await execute_sql_batch(self, conn, batch_rows, columns)
                    await conn.commit()
                except Exception:
                    try:
                        await conn.rollback()
                    except Exception:
                        logger.exception(
                            "postgres_rollback_error",
                            table=self._table,
                            count=count,
                        )
                    raise

        try:
            if self._insert_mode == "copy":
                await _execute_flush()
            else:
                await retry_async(
                    _execute_flush,
                    policy=policy,
                    on_retry=_on_retry,
                )
        except Exception as exc:
            logger.exception("postgres_flush_error", table=self._table, count=count)
            raise self._wrap_write_error(
                exc,
                rows=rows,
                columns=list(rows[0].keys()),
            ) from exc

    def _observe_retry(self) -> None:
        self._retry_count += 1

    def _observe_latency(self, operation: str, outcome: str, duration_s: float) -> None:
        key = (operation, outcome)
        buckets = self._latency_bucket_counts.setdefault(
            key, [0 for _ in _POSTGRES_LATENCY_BUCKETS_S]
        )
        for index, upper_bound_s in enumerate(_POSTGRES_LATENCY_BUCKETS_S):
            if duration_s <= upper_bound_s:
                buckets[index] += 1
        self._latency_counts[key] = self._latency_counts.get(key, 0) + 1
        self._latency_sums[key] = self._latency_sums.get(key, 0.0) + duration_s

    def _latency_histogram_snapshots(self) -> tuple[PostgresLatencyHistogramSnapshot, ...]:
        snapshots: list[PostgresLatencyHistogramSnapshot] = []
        for operation, outcome in sorted(self._latency_counts):
            key = (operation, outcome)
            snapshots.append(
                PostgresLatencyHistogramSnapshot(
                    operation=operation,
                    outcome=outcome,
                    buckets=tuple(
                        zip(
                            _POSTGRES_LATENCY_BUCKETS_S,
                            self._latency_bucket_counts[key],
                            strict=True,
                        )
                    ),
                    count=self._latency_counts[key],
                    sum_s=self._latency_sums[key],
                )
            )
        return tuple(snapshots)

    def _observe_poison_record(self, error: PostgresSinkWriteError) -> None:
        self._poison_record_count += 1
        self._poison_record_classification_counts[error.poison_info.classification] += 1

    async def _route_failed_buffer_to_dlq(self, error: PostgresSinkWriteError) -> None:
        if self._poison_record_sink is None or not self._buffer:
            return

        failed_rows = list(self._buffer)
        run_id = f"postgres-flush-{_now_utc().isoformat()}"
        records = [
            DLQRecord(
                pipeline_id=self._poison_record_pipeline_id,
                run_id=run_id,
                stage="postgres_sink_flush",
                error_type=type(error).__name__,
                error_message=str(error),
                record=row,
                processed_record=row,
                details=error.dlq_details,
                sink=self.sink_name,
                max_attempts=self._poison_record_max_attempts,
            )
            for row in failed_rows
        ]
        await self._poison_record_sink.write_batch(records)
        del self._buffer[: len(failed_rows)]

    def metrics_snapshot(self) -> PostgresSinkMetricsSnapshot:
        return PostgresSinkMetricsSnapshot(
            table=str(self._table),
            conflict_keys=tuple(str(key) for key in self._conflict_keys),
            batch_size=self._batch_size,
            upsert=self._upsert,
            insert_mode=self._insert_mode,
            pool_size=self._pool_size,
            max_rows_per_statement=self._max_rows_per_statement,
            max_parameters_per_statement=self._max_parameters_per_statement,
            write_safety_policy=self._write_safety_policy.value,
            buffered_row_count=len(self._buffer),
            write_call_count=self._write_call_count,
            write_batch_call_count=self._write_batch_call_count,
            enqueue_count=self._enqueue_count,
            flush_count=self._flush_count,
            flushed_row_count=self._flushed_row_count,
            retry_count=self._retry_count,
            schema_refresh_count=self._schema_refresh_count,
            schema_drift_detected_count=self._schema_drift_detected_count,
            schema_drift_aligned_count=self._schema_drift_aligned_count,
            poison_record_count=self._poison_record_count,
            poison_record_schema_drift_count=self._poison_record_classification_counts[
                PostgresPoisonRecordClassification.SCHEMA_DRIFT
            ],
            poison_record_constraint_violation_count=self._poison_record_classification_counts[
                PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION
            ],
            poison_record_type_mismatch_count=self._poison_record_classification_counts[
                PostgresPoisonRecordClassification.TYPE_MISMATCH
            ],
            poison_record_unknown_count=self._poison_record_classification_counts[
                PostgresPoisonRecordClassification.UNKNOWN
            ],
            connection_ready=self._conn is not None or self._write_pool_open_connections > 0,
            pooled_connection_count=self._write_pool_open_connections,
            pooled_available_count=(0 if self._write_pool is None else self._write_pool.qsize()),
            last_flush_at=self._last_flush_at,
            latency_histograms=self._latency_histogram_snapshots(),
        )

    def render_prometheus_metrics(self, namespace: str = "agora_postgres") -> str:
        from agora_plugins.postgres.observability import PostgresPrometheusExporter

        return PostgresPrometheusExporter(namespace=namespace).render_sink(self.metrics_snapshot())

    async def close(self) -> None:
        flush_error: Exception | None = None
        try:
            await self.flush()
        except PostgresSinkWriteError as exc:
            if self._poison_record_sink is None:
                logger.exception("postgres_close_flush_error", table=self._table)
                flush_error = exc
            else:
                try:
                    await self._route_failed_buffer_to_dlq(exc)
                except Exception as dlq_exc:
                    logger.exception("postgres_close_flush_dlq_error", table=self._table)
                    flush_error = dlq_exc
        except Exception as exc:
            logger.exception("postgres_close_flush_error", table=self._table)
            flush_error = exc
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
        if self._write_pool is not None:
            while True:
                try:
                    conn = self._write_pool.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await conn.close()
            self._write_pool = None
            self._write_pool_open_connections = 0
        if self._external_write_pool is not None:
            await self._external_write_pool.close()
            self._external_write_pool = None
            self._external_write_pool_conn_ids.clear()
            self._write_pool_open_connections = 0
        if self._poison_record_sink is not None:
            await self._poison_record_sink.close()
        logger.info("postgres_sink_closed", table=self._table)
        if flush_error is not None:
            raise flush_error


class PostgresSchemaAdapter(BaseSink[T], Generic[T]):
    """Schema-aware wrapper for PostgresSink."""

    sink_name = "postgres_schema_adapter"

    def __init__(
        self,
        sink: BaseSink[T],
        auto_create: bool = True,
        auto_alter: bool = True,
        allow_quoted_identifiers: bool = False,
        schema_lock_timeout_ms: int | None = 5_000,
        schema_advisory_lock: bool = True,
    ) -> None:
        if schema_lock_timeout_ms is not None and schema_lock_timeout_ms <= 0:
            raise ValueError("schema_lock_timeout_ms must be > 0 when provided.")
        self._sink = sink
        self._auto_create = auto_create
        self._auto_alter = auto_alter
        self._allow_quoted_identifiers = allow_quoted_identifiers
        self._schema_lock_timeout_ms = schema_lock_timeout_ms
        self._schema_advisory_lock = schema_advisory_lock
        self._schema_apply_lock = asyncio.Lock()
        self._ctx: PipelineContext | None = None
        self._schema: Schema | None = None
        self._table_created = False
        self._existing_columns: set[str] = set()
        self._applied_schema_hash: str | None = None

    def _conflict_keys_for_create(self) -> list[str | QuotedIdentifier]:
        keys = getattr(self._sink, "_conflict_keys", None)
        if keys is None:
            return []
        if isinstance(keys, (str, QuotedIdentifier)):
            return [keys]
        if isinstance(keys, list | tuple):
            return list(keys)
        return []

    def bind_context(self, ctx: PipelineContext) -> None:
        self._ctx = ctx
        bind_sink = getattr(self._sink, "bind_context", None)
        if callable(bind_sink):
            bind_sink(ctx)

    async def open(self) -> None:
        defer_upsert_preflight = getattr(
            self._sink,
            "defer_upsert_constraint_preflight_until_schema_applied",
            None,
        )
        if callable(defer_upsert_preflight):
            defer_upsert_preflight()
        await self._sink.open()
        await self._ensure_schema_applied()
        validate_upsert_constraint = getattr(self._sink, "validate_upsert_constraint", None)
        if callable(validate_upsert_constraint):
            await validate_upsert_constraint()

    async def write(self, record: T) -> None:
        await self._ensure_schema_applied()
        await self._sink.write(record)

    async def write_batch(self, records: list[T]) -> None:
        await self._ensure_schema_applied()
        await self._sink.write_batch(records)

    async def flush(self) -> None:
        await self._sink.flush()

    async def close(self) -> None:
        await self._sink.close()

    async def _ensure_schema_applied(self) -> None:
        ctx = self._ctx
        if ctx is None:
            return

        schema = ctx.extras.get("schema")
        if schema is None:
            ctx.log.warning(
                "postgres_schema_adapter_no_schema",
                message="No schema found in ctx.extras — SchemaMiddleware not used?",
                sink=self.sink_name,
            )
            return

        if self._applied_schema_hash == schema.hash:
            return

        async with self._schema_apply_lock:
            if self._applied_schema_hash == schema.hash:
                return

            self._schema = schema

            create_applied = True
            if self._auto_create:
                create_applied = await self._create_table_if_not_exists(ctx)
            alter_applied = True
            if self._auto_alter:
                alter_applied = await self._alter_table_add_columns(ctx)

            if create_applied and alter_applied:
                self._invalidate_wrapped_sink_target_columns_cache()
                self._applied_schema_hash = schema.hash

    def _invalidate_wrapped_sink_target_columns_cache(self) -> None:
        invalidate = getattr(self._sink, "invalidate_target_columns_cache", None)
        if callable(invalidate):
            invalidate()

    async def _prepare_schema_ddl_transaction(self, conn: Any, table_name: str) -> None:
        if self._schema_lock_timeout_ms is None and not self._schema_advisory_lock:
            return
        async with conn.cursor() as cur:
            if self._schema_lock_timeout_ms is not None:
                await cur.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (f"{self._schema_lock_timeout_ms}ms",),
                )
            if self._schema_advisory_lock:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (
                        _schema_advisory_lock_key(
                            table_name,
                            allow_quoted=self._allow_quoted_identifiers,
                        ),
                    ),
                )

    async def _create_table_if_not_exists(self, ctx: PipelineContext) -> bool:
        if self._schema is None:
            return True

        conn = await self._get_connection()
        if conn is None:
            ctx.log.warning(
                "postgres_schema_adapter_no_connection",
                message="Cannot get connection from wrapped sink",
                sink=self.sink_name,
            )
            return False

        table_name = self._schema.table
        try:
            where_sql, where_params = _table_lookup_condition(
                table_name,
                allow_quoted=self._allow_quoted_identifiers,
            )
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE {where_sql})",
                    where_params,
                )
                result = await cur.fetchone()
                table_exists = result[0] if result else False

            if table_exists:
                ctx.log.info("postgres_table_exists", table=table_name, sink=self.sink_name)
                self._table_created = True
                await conn.commit()
                await self._load_existing_columns(ctx)
                return True
        except Exception:
            await conn.rollback()
            raise

        columns_sql: list[str] = []
        for col_name in sorted(self._schema.columns.keys()):
            col = self._schema.columns[col_name]
            pg_type = _postgres_type(col.data_type)
            nullable = "NULL" if col.nullable else "NOT NULL"
            columns_sql.append(
                f"{_quote_identifier(col_name, allow_quoted=self._allow_quoted_identifiers)} "
                f"{pg_type} {nullable}"
            )
        conflict_keys = self._conflict_keys_for_create()
        if conflict_keys:
            missing_conflict_keys = [
                str(key) for key in conflict_keys if str(key) not in self._schema.columns
            ]
            if missing_conflict_keys:
                raise ValueError(
                    "PostgresSchemaAdapter cannot auto-create a table for a sink whose "
                    f"conflict_keys are missing from the schema: {missing_conflict_keys!r}."
                )
            constraint_cols = ", ".join(
                _quote_identifier(key, allow_quoted=self._allow_quoted_identifiers)
                for key in conflict_keys
            )
            columns_sql.append(f"UNIQUE ({constraint_cols})")

        create_sql = (
            "CREATE TABLE IF NOT EXISTS "
            f"{_quote_identifier(table_name, allow_path=True, allow_quoted=self._allow_quoted_identifiers)} (\n"
            f"  {', '.join(columns_sql)}\n"
            f")"
        )

        try:
            await self._prepare_schema_ddl_transaction(conn, table_name)
            async with conn.cursor() as cur:
                await cur.execute(create_sql)
            await conn.commit()
            ctx.log.info(
                "postgres_table_created",
                table=table_name,
                columns=len(self._schema.columns),
                sink=self.sink_name,
            )
            self._table_created = True
            self._existing_columns = set(self._schema.columns.keys())
            return True
        except Exception as exc:
            await conn.rollback()
            ctx.log.exception(
                "postgres_create_table_failed",
                table=table_name,
                error=str(exc),
                sink=self.sink_name,
            )
            raise

    async def _alter_table_add_columns(self, ctx: PipelineContext) -> bool:
        if self._schema is None or not self._table_created:
            return True

        if not self._existing_columns:
            await self._load_existing_columns(ctx)

        new_columns = set(self._schema.columns.keys()) - self._existing_columns
        if not new_columns:
            return True

        conn = await self._get_connection()
        if conn is None:
            return False

        table_name = self._schema.table
        table_has_rows = await self._table_has_rows(conn, table_name)
        for col_name in sorted(new_columns):
            col = self._schema.columns[col_name]
            pg_type = _postgres_type(col.data_type)
            nullable = "NULL" if col.nullable or table_has_rows else "NOT NULL"
            alter_sql = (
                "ALTER TABLE "
                f"{_quote_identifier(table_name, allow_path=True, allow_quoted=self._allow_quoted_identifiers)} "
                "ADD COLUMN IF NOT EXISTS "
                f"{_quote_identifier(col_name, allow_quoted=self._allow_quoted_identifiers)} "
                f"{pg_type} {nullable}"
            )

            try:
                await self._prepare_schema_ddl_transaction(conn, table_name)
                async with conn.cursor() as cur:
                    await cur.execute(alter_sql)
                await conn.commit()
                ctx.log.info(
                    "postgres_column_added",
                    table=table_name,
                    column=col_name,
                    type=pg_type,
                    nullable=(nullable == "NULL"),
                    requested_nullable=col.nullable,
                    sink=self.sink_name,
                )
                if table_has_rows and not col.nullable:
                    ctx.log.warning(
                        "postgres_column_added_nullable_for_existing_rows",
                        table=table_name,
                        column=col_name,
                        message=(
                            "Added new non-null schema column as nullable because the table "
                            "already contains rows and no default/backfill value was provided."
                        ),
                        sink=self.sink_name,
                    )
                self._existing_columns.add(col_name)
            except Exception as exc:
                await conn.rollback()
                ctx.log.exception(
                    "postgres_add_column_failed",
                    table=table_name,
                    column=col_name,
                    error=str(exc),
                    sink=self.sink_name,
                )
                raise
        return True

    async def _table_has_rows(self, conn: Any, table_name: str) -> bool:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM "
                    f"{_quote_identifier(table_name, allow_path=True, allow_quoted=self._allow_quoted_identifiers)} "
                    "LIMIT 1)"
                )
                result = await cur.fetchone()
            await conn.commit()
            return bool(result[0]) if result else False
        except Exception:
            await conn.rollback()
            raise

    async def _load_existing_columns(self, ctx: PipelineContext) -> None:
        if self._schema is None:
            return

        conn = await self._get_connection()
        if conn is None:
            return

        table_name = self._schema.table
        where_sql, where_params = _table_lookup_condition(
            table_name,
            allow_quoted=self._allow_quoted_identifiers,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE {where_sql}",
                    where_params,
                )
                rows = await cur.fetchall()
                await conn.commit()
                self._existing_columns = {row[0] for row in rows}
                ctx.log.debug(
                    "postgres_loaded_columns",
                    table=table_name,
                    columns=len(self._existing_columns),
                    sink=self.sink_name,
                )
        except Exception as exc:
            await conn.rollback()
            ctx.log.warning(
                "postgres_load_columns_failed",
                table=table_name,
                error=str(exc),
                sink=self.sink_name,
            )

    async def _get_connection(self) -> Any:
        if hasattr(self._sink, "connection"):
            return await self._sink.connection()
        raise TypeError(
            f"PostgresSchemaAdapter requires a sink with a public `connection()` method. "
            f"Got {type(self._sink).__name__!r}. Use PostgresSink or a compatible sink."
        )


__all__ = [
    "PostgresPoisonRecordClassification",
    "PostgresPoisonRecordInfo",
    "PostgresSchemaAdapter",
    "PostgresSink",
    "PostgresSinkMetricsSnapshot",
    "PostgresSinkWriteError",
    "PostgresWriteSafetyPolicy",
    "QuotedIdentifier",
]
