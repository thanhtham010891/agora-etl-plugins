"""PostgreSQL-backed DLQ sink/source implementations."""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from agora_plugins.dlq_policy import DLQPayloadPolicy

import logstruct
from agora.core.dlq import DLQRecord, DLQSink, DLQSource

from agora_plugins.postgres._dlq_payloads import (
    DLQ_COLUMNS as _DLQ_COLUMNS,
)
from agora_plugins.postgres._dlq_payloads import (
    DLQ_INSERT_COLUMNS as _DLQ_INSERT_COLUMNS,
)
from agora_plugins.postgres._dlq_payloads import (
    record_to_row as _record_to_row,
)
from agora_plugins.postgres._dlq_payloads import (
    row_to_record as _row_to_record,
)
from agora_plugins.postgres.connection import (
    PostgresConnectionConfig,
    coerce_connection_config,
    redact_postgres_dsn,
)
from agora_plugins.postgres.observability import (
    PostgresDLQSinkMetricsSnapshot,
    PostgresDLQSourceMetricsSnapshot,
    PostgresPrometheusExporter,
)
from agora_plugins.postgres.sinks.postgres import _quote_identifier

logger = logstruct.getLogger(__name__)


def _is_reconnectable_postgres_error(exc: Exception) -> bool:
    try:
        import psycopg
    except ImportError:
        return isinstance(exc, (ConnectionError, OSError, TimeoutError))
    return isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError))


class PostgresDLQSink(DLQSink):
    """Store DLQ records in a PostgreSQL table."""

    sink_name = "postgres_dlq"

    def __init__(
        self,
        dsn: str | None = None,
        table: str = "agora_dlq",
        *,
        connection: PostgresConnectionConfig | None = None,
        payload_policy: DLQPayloadPolicy | None = None,
    ) -> None:
        self._connection = coerce_connection_config(dsn=dsn, connection=connection)
        self._table = table
        self._payload_policy = payload_policy
        self._conn: Any | None = None
        self._table_ready = False
        self._write_call_count = 0
        self._write_batch_call_count = 0
        self._inserted_record_count = 0
        self._upserted_record_count = 0
        self._updated_record_count = 0
        self._replay_count = 0
        self._replayed_record_count = 0
        self._acknowledge_count = 0
        self._acknowledged_record_count = 0
        self._last_write_at: datetime | None = None
        self._last_replay_at: datetime | None = None
        self._last_acknowledge_at: datetime | None = None

    async def open(self) -> None:
        await self._ensure_table()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def write(self, record: DLQRecord) -> None:
        self._write_call_count += 1
        inserted_count, updated_count = await self._insert_rows([record])
        self._inserted_record_count += inserted_count
        self._updated_record_count += updated_count
        self._upserted_record_count += 1
        self._last_write_at = datetime.now(UTC)

    async def write_batch(self, records: list[DLQRecord]) -> None:
        if not records:
            return
        self._write_batch_call_count += 1
        inserted_count, updated_count = await self._insert_rows(records)
        self._inserted_record_count += inserted_count
        self._updated_record_count += updated_count
        self._upserted_record_count += len(records)
        self._last_write_at = datetime.now(UTC)

    async def replay(self, record: DLQRecord) -> DLQRecord:
        updated = await super().replay(record)

        async def _update_attempt() -> None:
            conn = await self._get_conn()
            table_sql = _quote_identifier(self._table, allow_path=True)
            if record._storage_id is not None:
                sql = f"UPDATE {table_sql} SET attempt = %s WHERE id = %s"
                params: tuple[Any, ...] = (updated.attempt, record._storage_id)
            else:
                sql = (
                    f"UPDATE {table_sql} SET attempt = %s "
                    f"WHERE id = (SELECT id FROM {table_sql} "
                    "WHERE pipeline_id = %s AND run_id = %s AND stage = %s AND created_at = %s "
                    "ORDER BY id ASC LIMIT 1)"
                )
                params = (
                    updated.attempt,
                    record.pipeline_id,
                    record.run_id,
                    record.stage,
                    record.created_at,
                )
            try:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                await conn.commit()
            except Exception:
                await self._rollback_quietly(conn)
                raise

        await self._run_with_reconnect(_update_attempt, context="replay")
        self._replay_count += 1
        self._replayed_record_count += 1
        self._last_replay_at = datetime.now(UTC)
        return updated

    async def acknowledge(self, record: DLQRecord) -> None:
        async def _delete_record() -> None:
            conn = await self._get_conn()
            table_sql = _quote_identifier(self._table, allow_path=True)
            if record._storage_id is not None:
                sql = f"DELETE FROM {table_sql} WHERE id = %s"
                params: tuple[Any, ...] = (record._storage_id,)
            else:
                sql = (
                    f"DELETE FROM {table_sql} "
                    f"WHERE id = (SELECT id FROM {table_sql} "
                    "WHERE pipeline_id = %s AND run_id = %s AND stage = %s AND created_at = %s "
                    "ORDER BY id ASC LIMIT 1)"
                )
                params = (
                    record.pipeline_id,
                    record.run_id,
                    record.stage,
                    record.created_at,
                )
            try:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                await conn.commit()
            except Exception:
                await self._rollback_quietly(conn)
                raise

        await self._run_with_reconnect(_delete_record, context="acknowledge")
        self._acknowledge_count += 1
        self._acknowledged_record_count += 1
        self._last_acknowledge_at = datetime.now(UTC)

    def metrics_snapshot(self) -> PostgresDLQSinkMetricsSnapshot:
        return PostgresDLQSinkMetricsSnapshot(
            table=self._table,
            connection_ready=self._conn is not None,
            table_ready=self._table_ready,
            write_call_count=self._write_call_count,
            write_batch_call_count=self._write_batch_call_count,
            inserted_record_count=self._inserted_record_count,
            upserted_record_count=self._upserted_record_count,
            updated_record_count=self._updated_record_count,
            replay_count=self._replay_count,
            replayed_record_count=self._replayed_record_count,
            acknowledge_count=self._acknowledge_count,
            acknowledged_record_count=self._acknowledged_record_count,
            last_write_at=self._last_write_at,
            last_replay_at=self._last_replay_at,
            last_acknowledge_at=self._last_acknowledge_at,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_postgres") -> str:
        return PostgresPrometheusExporter(namespace=namespace).render_dlq_sink(
            self.metrics_snapshot()
        )

    async def _insert_rows(self, records: list[DLQRecord]) -> tuple[int, int]:
        insert_update_counts: tuple[int, int] = (0, 0)

        async def _insert_once() -> None:
            nonlocal insert_update_counts
            conn = await self._get_conn()
            await self._ensure_table()
            table_sql = _quote_identifier(self._table, allow_path=True)
            columns_sql = ", ".join(_quote_identifier(column) for column in _DLQ_INSERT_COLUMNS)
            placeholders_sql = ", ".join(["%s"] * len(_DLQ_INSERT_COLUMNS))
            sql = (
                f"INSERT INTO {table_sql} ({columns_sql}) VALUES ({placeholders_sql}) "
                "ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL "
                "DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key "
                "RETURNING id, (xmax = 0) AS inserted"
            )
            rows = [
                _record_to_row(record, payload_policy=self._payload_policy) for record in records
            ]
            params_batch = [tuple(row[column] for column in _DLQ_INSERT_COLUMNS) for row in rows]
            inserted_count = 0
            updated_count = 0
            try:
                async with conn.cursor() as cur:
                    for record, params in zip(records, params_batch, strict=True):
                        await cur.execute(sql, params)
                        inserted = await cur.fetchone()
                        if isinstance(inserted, dict):
                            inserted_id = inserted.get("id")
                            was_inserted = bool(inserted.get("inserted", True))
                        else:
                            inserted_id = inserted[0] if inserted else None
                            was_inserted = (
                                bool(inserted[1]) if inserted and len(inserted) > 1 else True
                            )
                        if inserted_id is not None:
                            object.__setattr__(record, "_storage_id", inserted_id)
                        if was_inserted:
                            inserted_count += 1
                        else:
                            updated_count += 1
                await conn.commit()
                insert_update_counts = (inserted_count, updated_count)
            except Exception:
                await self._rollback_quietly(conn)
                raise

        await self._run_with_reconnect(_insert_once, context="insert")
        return insert_update_counts

    async def _get_conn(self) -> Any:
        if self._conn is None or getattr(self._conn, "closed", False):
            try:
                import psycopg
            except ImportError:
                raise ImportError(
                    "PostgresDLQSink requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
                ) from None
            self._conn = cast(
                "Any",
                await psycopg.AsyncConnection.connect(
                    **self._connection.connect_kwargs(autocommit=False)
                ),
            )
            logger.info(
                "postgres_dlq_sink_connected",
                table=self._table,
                dsn=redact_postgres_dsn(self._connection.resolve_dsn()),
            )
        return self._conn

    async def _run_with_reconnect(
        self,
        operation: Callable[[], Awaitable[None]],
        *,
        context: str,
    ) -> None:
        try:
            await operation()
        except Exception as exc:
            if not self._is_reconnectable_error(exc):
                raise
            logger.warning(
                "postgres_dlq_sink_reconnecting",
                table=self._table,
                context=context,
                error=str(exc),
            )
            await self._discard_conn()
            self._table_ready = False
            await operation()

    async def _discard_conn(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()

    @staticmethod
    async def _rollback_quietly(conn: Any) -> None:
        with contextlib.suppress(Exception):
            await conn.rollback()

    @staticmethod
    def _is_reconnectable_error(exc: Exception) -> bool:
        return _is_reconnectable_postgres_error(exc)

    async def _ensure_table(self) -> None:
        if self._table_ready:
            return
        conn = await self._get_conn()
        table_sql = _quote_identifier(self._table, allow_path=True)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_sql} (
            id BIGSERIAL PRIMARY KEY,
            pipeline_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL,
            record JSONB,
            original_record JSONB,
            processed_record JSONB,
            dedupe_key TEXT,
            source TEXT,
            checkpoint JSONB,
            details JSONB,
            middleware TEXT,
            sink TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER
        )
        """
        async with conn.cursor() as cur:
            await cur.execute(create_sql)
            await cur.execute(
                f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS original_record JSONB"
            )
            await cur.execute(
                f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS processed_record JSONB"
            )
            await cur.execute(f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS dedupe_key TEXT")
            await cur.execute(f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS details JSONB")
            await cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {self._dedupe_index_sql()} "
                f"ON {table_sql} (dedupe_key) WHERE dedupe_key IS NOT NULL"
            )
        await conn.commit()
        self._table_ready = True

    def _dedupe_index_sql(self) -> str:
        raw = self._table.split(".")[-1].strip('"')
        safe = "".join(char if char.isalnum() or char == "_" else "_" for char in raw)
        return _quote_identifier(f"{safe}_dedupe_key_idx")


class PostgresDLQSource(DLQSource):
    """Read DLQ records from a PostgreSQL table for replay."""

    source_name = "postgres_dlq_source"

    def __init__(
        self,
        dsn: str | None = None,
        table: str = "agora_dlq",
        *,
        pipeline_id: str | None = None,
        stage: str | None = None,
        limit: int | None = None,
        connection: PostgresConnectionConfig | None = None,
        payload_policy: DLQPayloadPolicy | None = None,
    ) -> None:
        self._connection = coerce_connection_config(dsn=dsn, connection=connection)
        self._table = table
        self._pipeline_id = pipeline_id
        self._stage = stage
        self._limit = limit
        self._payload_policy = payload_policy
        self._conn: Any | None = None
        self._scan_count = 0
        self._emitted_record_count = 0
        self._last_scan_at: datetime | None = None
        self._last_record_at: datetime | None = None

    async def open(self) -> None:
        await self._get_conn()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        self._scan_count += 1
        self._last_scan_at = datetime.now(UTC)
        emitted_before_scan = self._emitted_record_count
        for attempt in range(2):
            try:
                async for record in self._iter_records_once():
                    yield record
                return
            except Exception as exc:
                if (
                    attempt > 0
                    or self._emitted_record_count > emitted_before_scan
                    or not _is_reconnectable_postgres_error(exc)
                ):
                    raise
                logger.warning(
                    "postgres_dlq_source_reconnecting",
                    table=self._table,
                    error=str(exc),
                )
                await self._discard_conn()

    async def _iter_records_once(self) -> AsyncGenerator[DLQRecord, None]:
        conn = await self._get_conn()
        params: list[Any] = []
        conditions: list[str] = []
        if self._pipeline_id is not None:
            conditions.append("pipeline_id = %s")
            params.append(self._pipeline_id)
        if self._stage is not None:
            conditions.append("stage = %s")
            params.append(self._stage)

        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        table_sql = _quote_identifier(self._table, allow_path=True)
        columns_sql = ", ".join(_quote_identifier(column) for column in _DLQ_COLUMNS)
        sql = f"SELECT id, {columns_sql} FROM {table_sql} {where_sql} ORDER BY created_at ASC"
        if self._limit is not None:
            sql += " LIMIT %s"
            params.append(self._limit)

        async with conn.cursor() as cur:
            await cur.execute(sql, params or None)
            while True:
                rows = await cur.fetchmany(100)
                if not rows:
                    break
                for row in rows:
                    payload = dict(row)
                    record = _row_to_record(payload, payload_policy=self._payload_policy)
                    object.__setattr__(record, "_storage_id", payload.get("id"))
                    self._emitted_record_count += 1
                    self._last_record_at = datetime.now(UTC)
                    yield record

    def metrics_snapshot(self) -> PostgresDLQSourceMetricsSnapshot:
        return PostgresDLQSourceMetricsSnapshot(
            table=self._table,
            pipeline_id=self._pipeline_id,
            stage=self._stage,
            limit=self._limit,
            connection_ready=self._conn is not None,
            scan_count=self._scan_count,
            emitted_record_count=self._emitted_record_count,
            last_scan_at=self._last_scan_at,
            last_record_at=self._last_record_at,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_postgres") -> str:
        return PostgresPrometheusExporter(namespace=namespace).render_dlq_source(
            self.metrics_snapshot()
        )

    async def _get_conn(self) -> Any:
        if self._conn is None or getattr(self._conn, "closed", False):
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError:
                raise ImportError(
                    "PostgresDLQSource requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
                ) from None
            self._conn = cast(
                "Any",
                await psycopg.AsyncConnection.connect(
                    **self._connection.connect_kwargs(row_factory=dict_row)
                ),
            )
        return self._conn

    async def _discard_conn(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()


__all__ = ["PostgresDLQSink", "PostgresDLQSource"]
