"""
agora_plugins.postgres.sinks.postgres
=====================================
Generic async PostgreSQL sink with batch upsert plus schema adapter helpers.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar
from urllib.parse import urlparse

import logstruct
from agora.core.retry import RetryPolicy, retry_async
from agora.core.sink import BaseSink
from agora.schema.types import DataType

if TYPE_CHECKING:
    from collections.abc import Callable

    import psycopg
    from agora.core.context import PipelineContext
    from agora.schema.types import Schema

T = TypeVar("T")

logger = logstruct.getLogger(__name__)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _redact_dsn(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
        if parsed.password:
            return parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                + (f":{parsed.port}" if parsed.port else "")
            ).geturl()
    except Exception:
        pass
    return dsn


def _quote_identifier(identifier: str, *, allow_path: bool = False) -> str:
    parts = identifier.split(".") if allow_path else [identifier]
    if not parts or any(not part for part in parts):
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    if allow_path and len(parts) > 2:
        raise ValueError(
            f"Invalid SQL identifier: {identifier!r}. "
            "Only schema.table paths are supported (max 2 parts)."
        )

    quoted: list[str] = []
    for part in parts:
        if not _IDENTIFIER_RE.fullmatch(part):
            raise ValueError(
                f"Invalid SQL identifier: {identifier!r}. "
                "Only letters, numbers, and underscores are allowed, "
                "and identifiers must not start with a number."
            )
        quoted.append(f'"{part}"')
    return ".".join(quoted)


def _postgres_type(data_type: DataType) -> str:
    """Map Agora DataType to Postgres type."""
    mapping = {
        DataType.STRING: "TEXT",
        DataType.INTEGER: "BIGINT",
        DataType.FLOAT: "DOUBLE PRECISION",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.TIMESTAMP: "TIMESTAMPTZ",
        DataType.JSON: "JSONB",
        DataType.BYTES: "BYTEA",
        DataType.NULL: "TEXT",
    }
    return mapping.get(data_type, "TEXT")


class PostgresSink(BaseSink[T], Generic[T]):
    """Generic async batch-upsert PostgreSQL sink."""

    sink_name = "postgres"

    def __init__(
        self,
        dsn: str,
        table: str,
        row_mapper: Callable[[T], dict[str, Any]],
        conflict_key: str | list[str],
        batch_size: int = 100,
        upsert: bool = True,
        insert_mode: Literal["sql", "copy", "copy_merge"] = "sql",
        pool_size: int = 1,
        max_rows_per_statement: int | None = None,
        max_parameters_per_statement: int = 32_000,
        retry_policy: RetryPolicy[Any] | None = None,
    ) -> None:
        if insert_mode not in {"sql", "copy", "copy_merge"}:
            raise ValueError("insert_mode must be 'sql', 'copy', or 'copy_merge'")
        if upsert and insert_mode == "copy":
            raise ValueError("insert_mode='copy' is only supported when upsert=False")
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")
        if max_rows_per_statement is not None and max_rows_per_statement < 1:
            raise ValueError("max_rows_per_statement must be >= 1 when provided")
        if max_parameters_per_statement < 1:
            raise ValueError("max_parameters_per_statement must be >= 1")
        self._dsn = dsn
        self._table = table
        self._row_mapper = row_mapper
        self._conflict_keys = [conflict_key] if isinstance(conflict_key, str) else conflict_key
        self._batch_size = batch_size
        self._upsert = upsert
        self._insert_mode = insert_mode
        self._pool_size = pool_size
        self._max_rows_per_statement = max_rows_per_statement
        self._max_parameters_per_statement = max_parameters_per_statement
        self._retry_policy = retry_policy
        self._buffer: list[dict[str, Any]] = []
        self._conn = None
        self._write_pool: asyncio.LifoQueue[Any] | None = None
        self._write_pool_open_connections = 0
        self._write_pool_lock = asyncio.Lock()
        self._psycopg = None

    async def _load_psycopg(self):
        if self._psycopg is None:
            try:
                import psycopg
            except ImportError:
                raise ImportError(
                    "PostgresSink requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
                ) from None
            self._psycopg = psycopg
        return self._psycopg

    async def _create_connection(self):
        psycopg = await self._load_psycopg()
        conn = await psycopg.AsyncConnection.connect(self._dsn, autocommit=False)
        logger.info("postgres_sink_connected", table=self._table, dsn=_redact_dsn(self._dsn))
        return conn

    async def _get_conn(self) -> psycopg.AsyncConnection[Any]:
        if self._conn is None:
            self._conn = await self._create_connection()
        return self._conn

    async def _acquire_write_conn(self):
        if self._pool_size <= 1:
            return await self._get_conn(), False

        if self._write_pool is None:
            self._write_pool = asyncio.LifoQueue(maxsize=self._pool_size)

        try:
            return self._write_pool.get_nowait(), True
        except asyncio.QueueEmpty:
            pass

        should_create = False
        async with self._write_pool_lock:
            if self._write_pool_open_connections < self._pool_size:
                self._write_pool_open_connections += 1
                should_create = True

        if should_create:
            try:
                conn = await self._create_connection()
            except Exception:
                async with self._write_pool_lock:
                    self._write_pool_open_connections = max(
                        0, self._write_pool_open_connections - 1
                    )
                raise
            return conn, True

        return await self._write_pool.get(), True

    async def _release_write_conn(self, conn, *, pooled: bool, discard: bool = False) -> None:
        if not pooled:
            if discard and self._conn is conn:
                try:
                    await conn.close()
                except Exception:
                    pass
                finally:
                    self._conn = None
            return

        if discard:
            try:
                await conn.close()
            except Exception:
                pass
            finally:
                async with self._write_pool_lock:
                    self._write_pool_open_connections = max(
                        0, self._write_pool_open_connections - 1
                    )
            return

        assert self._write_pool is not None
        self._write_pool.put_nowait(conn)

    @asynccontextmanager
    async def _write_connection(self):
        conn, pooled = await self._acquire_write_conn()
        discard = False
        try:
            yield conn
        except Exception:
            discard = True
            raise
        finally:
            await self._release_write_conn(conn, pooled=pooled, discard=discard)

    async def connection(self):
        """Public API for obtaining the underlying connection (used by schema adapters)."""
        return await self._get_conn()

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

    def _build_upsert_sql(self, columns: list[str]) -> str:
        return self._build_batch_upsert_sql(columns, row_count=1)

    def _build_batch_upsert_sql(self, columns: list[str], *, row_count: int) -> str:
        quoted_table = _quote_identifier(self._table, allow_path=True)
        col_list = ", ".join(_quote_identifier(column) for column in columns)
        row_placeholder = f"({', '.join(['%s'] * len(columns))})"
        val_list = ", ".join([row_placeholder] * row_count)
        conflict_cols = ", ".join(_quote_identifier(key) for key in self._conflict_keys)

        update_set = ", ".join(
            f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
            for column in columns
            if column not in self._conflict_keys
        )

        if self._upsert and update_set:
            return (
                f"INSERT INTO {quoted_table} ({col_list}) "
                f"VALUES {val_list} "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
            )
        return (
            f"INSERT INTO {quoted_table} ({col_list}) "
            f"VALUES {val_list} "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )

    def _build_copy_sql(self, columns: list[str]) -> str:
        return self._build_copy_sql_for_table(self._table, columns)

    def _build_copy_sql_for_table(self, table: str, columns: list[str]) -> str:
        quoted_table = _quote_identifier(table, allow_path="." in table)
        col_list = ", ".join(_quote_identifier(column) for column in columns)
        return f"COPY {quoted_table} ({col_list}) FROM STDIN"

    def _build_copy_merge_sql(self, columns: list[str], staging_table: str) -> str:
        quoted_table = _quote_identifier(self._table, allow_path=True)
        quoted_staging = _quote_identifier(staging_table)
        col_list = ", ".join(_quote_identifier(column) for column in columns)
        select_list = ", ".join(
            f"{quoted_staging}.{_quote_identifier(column)}" for column in columns
        )
        conflict_cols = ", ".join(_quote_identifier(key) for key in self._conflict_keys)
        update_set = ", ".join(
            f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
            for column in columns
            if column not in self._conflict_keys
        )

        if self._upsert and update_set:
            return (
                f"INSERT INTO {quoted_table} ({col_list}) "
                f"SELECT {select_list} FROM {quoted_staging} "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
            )
        return (
            f"INSERT INTO {quoted_table} ({col_list}) "
            f"SELECT {select_list} FROM {quoted_staging} "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )

    def _build_create_temp_table_sql(self, staging_table: str) -> str:
        quoted_table = _quote_identifier(self._table, allow_path=True)
        quoted_staging = _quote_identifier(staging_table)
        return (
            f"CREATE TEMP TABLE {quoted_staging} "
            f"(LIKE {quoted_table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )

    def _build_stage_table_name(self) -> str:
        return f"agora_stage_{uuid.uuid4().hex[:12]}"

    def _flatten_rows(self, rows: list[dict[str, Any]], columns: list[str]) -> list[Any]:
        params: list[Any] = []
        expected_columns = tuple(columns)
        for row in rows:
            row_columns = tuple(row.keys())
            if row_columns != expected_columns:
                raise ValueError(
                    "PostgresSink rows in the same batch must have identical column order. "
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
    ):
        chunk_size = self._statement_row_limit(len(columns))
        for start in range(0, len(rows), chunk_size):
            yield rows[start : start + chunk_size]

    async def write(self, record: T) -> None:
        row = self._row_mapper(record)
        self._buffer.append(row)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def write_batch(self, records: list[T]) -> None:
        for record in records:
            row = self._row_mapper(record)
            self._buffer.append(row)
            if len(self._buffer) >= self._batch_size:
                await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return

        rows = list(self._buffer)
        count = len(rows)
        columns = list(rows[0].keys())
        policy = self._effective_retry_policy()
        if self._insert_mode == "copy":
            await self._flush_via_copy(rows, columns, count, policy)
        elif self._insert_mode == "copy_merge":
            await self._flush_via_copy_merge(rows, columns, count, policy)
        else:
            await self._flush_via_sql(rows, columns, count, policy)

        del self._buffer[: len(rows)]
        logger.info("postgres_flush", table=self._table, count=count)

    async def _flush_via_sql(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        policy: RetryPolicy[Any],
    ) -> None:
        async def _execute_flush() -> None:
            async with self._write_connection() as conn:
                try:
                    async with conn.cursor() as cur:
                        for chunk in self._iter_sql_chunks(rows, columns):
                            sql = self._build_batch_upsert_sql(columns, row_count=len(chunk))
                            params = self._flatten_rows(chunk, columns)
                            await cur.execute(sql, params)
                    await conn.commit()
                except Exception:
                    try:
                        await conn.rollback()
                    except Exception:
                        logger.exception("postgres_rollback_error", table=self._table, count=count)
                    raise

        try:
            await retry_async(
                _execute_flush,
                policy=policy,
                on_retry=lambda attempt, exc, delay: logger.warning(
                    "postgres_flush_retry",
                    table=self._table,
                    count=count,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                ),
            )
        except Exception:
            logger.exception("postgres_flush_error", table=self._table, count=count)
            raise

    async def _flush_via_copy(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        policy: RetryPolicy[Any],
    ) -> None:
        sql = self._build_copy_sql(columns)

        async def _execute_copy() -> None:
            async with self._write_connection() as conn:
                try:
                    async with conn.cursor() as cur, cur.copy(sql) as copy:
                        for row in rows:
                            await copy.write_row([row[column] for column in columns])
                    await conn.commit()
                except Exception:
                    try:
                        await conn.rollback()
                    except Exception:
                        logger.exception("postgres_rollback_error", table=self._table, count=count)
                    raise

        try:
            await retry_async(
                _execute_copy,
                policy=policy,
                on_retry=lambda attempt, exc, delay: logger.warning(
                    "postgres_copy_retry",
                    table=self._table,
                    count=count,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                ),
            )
        except Exception:
            logger.exception("postgres_copy_error", table=self._table, count=count)
            raise

    async def _flush_via_copy_merge(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        policy: RetryPolicy[Any],
    ) -> None:
        staging_table = self._build_stage_table_name()
        create_sql = self._build_create_temp_table_sql(staging_table)
        copy_sql = self._build_copy_sql_for_table(staging_table, columns)
        merge_sql = self._build_copy_merge_sql(columns, staging_table)

        async def _execute_copy_merge() -> None:
            async with self._write_connection() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(create_sql)
                    async with conn.cursor() as cur, cur.copy(copy_sql) as copy:
                        for row in rows:
                            await copy.write_row([row[column] for column in columns])
                    async with conn.cursor() as cur:
                        await cur.execute(merge_sql)
                    await conn.commit()
                except Exception:
                    try:
                        await conn.rollback()
                    except Exception:
                        logger.exception("postgres_rollback_error", table=self._table, count=count)
                    raise

        try:
            await retry_async(
                _execute_copy_merge,
                policy=policy,
                on_retry=lambda attempt, exc, delay: logger.warning(
                    "postgres_copy_merge_retry",
                    table=self._table,
                    count=count,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                ),
            )
        except Exception:
            logger.exception("postgres_copy_merge_error", table=self._table, count=count)
            raise

    async def close(self) -> None:
        try:
            await self.flush()
        except Exception:
            logger.exception("postgres_close_flush_error")
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
        logger.info("postgres_sink_closed", table=self._table)


class PostgresSchemaAdapter(BaseSink[T], Generic[T]):
    """Schema-aware wrapper for PostgresSink."""

    sink_name = "postgres_schema_adapter"

    def __init__(
        self,
        sink: BaseSink[T],
        auto_create: bool = True,
        auto_alter: bool = True,
    ) -> None:
        self._sink = sink
        self._auto_create = auto_create
        self._auto_alter = auto_alter
        self._ctx: PipelineContext | None = None
        self._schema: Schema | None = None
        self._table_created = False
        self._existing_columns: set[str] = set()
        self._applied_schema_hash: str | None = None

    def bind_context(self, ctx: PipelineContext) -> None:
        self._ctx = ctx
        bind_sink = getattr(self._sink, "bind_context", None)
        if callable(bind_sink):
            bind_sink(ctx)

    async def open(self) -> None:
        await self._sink.open()
        await self._ensure_schema_applied()

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

        self._schema = schema

        if self._auto_create:
            await self._create_table_if_not_exists(ctx)
        if self._auto_alter:
            await self._alter_table_add_columns(ctx)

        self._applied_schema_hash = schema.hash

    async def _create_table_if_not_exists(self, ctx: PipelineContext) -> None:
        if self._schema is None:
            return

        conn = await self._get_connection()
        if conn is None:
            ctx.log.warning(
                "postgres_schema_adapter_no_connection",
                message="Cannot get connection from wrapped sink",
                sink=self.sink_name,
            )
            return

        table_name = self._schema.table
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                (table_name.split(".")[-1],),
            )
            result = await cur.fetchone()
            table_exists = result[0] if result else False

        if table_exists:
            ctx.log.info("postgres_table_exists", table=table_name, sink=self.sink_name)
            self._table_created = True
            await self._load_existing_columns(ctx)
            return

        columns_sql: list[str] = []
        for col_name in sorted(self._schema.columns.keys()):
            col = self._schema.columns[col_name]
            pg_type = _postgres_type(col.data_type)
            nullable = "NULL" if col.nullable else "NOT NULL"
            columns_sql.append(f"{_quote_identifier(col_name)} {pg_type} {nullable}")

        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name, allow_path=True)} (\n"
            f"  {', '.join(columns_sql)}\n"
            f")"
        )

        try:
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
        except Exception as exc:
            await conn.rollback()
            ctx.log.exception(
                "postgres_create_table_failed",
                table=table_name,
                error=str(exc),
                sink=self.sink_name,
            )
            raise

    async def _alter_table_add_columns(self, ctx: PipelineContext) -> None:
        if self._schema is None or not self._table_created:
            return

        if not self._existing_columns:
            await self._load_existing_columns(ctx)

        new_columns = set(self._schema.columns.keys()) - self._existing_columns
        if not new_columns:
            return

        conn = await self._get_connection()
        if conn is None:
            return

        table_name = self._schema.table
        for col_name in sorted(new_columns):
            col = self._schema.columns[col_name]
            pg_type = _postgres_type(col.data_type)
            nullable = "NULL" if col.nullable else "NOT NULL"
            alter_sql = (
                f"ALTER TABLE {_quote_identifier(table_name, allow_path=True)} "
                f"ADD COLUMN {_quote_identifier(col_name)} {pg_type} {nullable}"
            )

            try:
                async with conn.cursor() as cur:
                    await cur.execute(alter_sql)
                await conn.commit()
                ctx.log.info(
                    "postgres_column_added",
                    table=table_name,
                    column=col_name,
                    type=pg_type,
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

    async def _load_existing_columns(self, ctx: PipelineContext) -> None:
        if self._schema is None:
            return

        conn = await self._get_connection()
        if conn is None:
            return

        table_name = self._schema.table
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (table_name.split(".")[-1],),
                )
                rows = await cur.fetchall()
                self._existing_columns = {row[0] for row in rows}
                ctx.log.debug(
                    "postgres_loaded_columns",
                    table=table_name,
                    columns=len(self._existing_columns),
                    sink=self.sink_name,
                )
        except Exception as exc:
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
    "PostgresSchemaAdapter",
    "PostgresSink",
]
