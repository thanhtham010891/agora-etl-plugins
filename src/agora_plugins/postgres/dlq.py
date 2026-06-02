"""PostgreSQL-backed DLQ sink/source implementations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

import logstruct
from agora.core.dlq import DLQRecord, DLQSink, DLQSource

from agora_plugins.postgres.sinks.postgres import _quote_identifier

logger = logstruct.getLogger(__name__)


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


_DLQ_COLUMNS = (
    "pipeline_id",
    "run_id",
    "stage",
    "error_type",
    "error_message",
    "record",
    "original_record",
    "processed_record",
    "source",
    "checkpoint",
    "middleware",
    "sink",
    "created_at",
    "attempt",
    "max_attempts",
)


def _serialize_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value)


def _record_to_row(record: DLQRecord) -> dict[str, Any]:
    return {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": _serialize_json(record.record),
        "original_record": _serialize_json(record.original_record),
        "processed_record": _serialize_json(record.processed_record),
        "source": record.source,
        "checkpoint": _serialize_json(record.checkpoint),
        "middleware": record.middleware,
        "sink": record.sink,
        "created_at": record.created_at,
        "attempt": record.attempt,
        "max_attempts": record.max_attempts,
    }


def _decode_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value


def _row_to_record(row: dict[str, Any]) -> DLQRecord:
    return DLQRecord(
        pipeline_id=row["pipeline_id"],
        run_id=row["run_id"],
        stage=row["stage"],
        error_type=row["error_type"],
        error_message=row["error_message"],
        record=_decode_json(row["record"]),
        original_record=_decode_json(row.get("original_record")),
        processed_record=_decode_json(row.get("processed_record")),
        source=row.get("source"),
        checkpoint=_decode_json(row.get("checkpoint")),
        middleware=row.get("middleware"),
        sink=row.get("sink"),
        created_at=_coerce_datetime(row["created_at"]),
        attempt=int(row.get("attempt", 0)),
        max_attempts=(int(row["max_attempts"]) if row.get("max_attempts") is not None else None),
    )


class PostgresDLQSink(DLQSink):
    """Store DLQ records in a PostgreSQL table."""

    sink_name = "postgres_dlq"

    def __init__(
        self,
        dsn: str,
        table: str = "agora_dlq",
    ) -> None:
        self._dsn = dsn
        self._table = table
        self._conn: Any | None = None
        self._table_ready = False

    async def open(self) -> None:
        await self._ensure_table()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def write(self, record: DLQRecord) -> None:
        await self._insert_rows([record])

    async def write_batch(self, records: list[DLQRecord]) -> None:
        if not records:
            return
        await self._insert_rows(records)

    async def replay(self, record: DLQRecord) -> DLQRecord:
        updated = await super().replay(record)
        conn = await self._get_conn()
        table_sql = _quote_identifier(self._table, allow_path=True)
        if record._storage_id is not None:
            sql = f"UPDATE {table_sql} SET attempt = %s WHERE id = %s"
            params: tuple[Any, ...] = (updated.attempt, record._storage_id)
        else:
            sql = (
                f"UPDATE {table_sql} SET attempt = %s "
                "WHERE pipeline_id = %s AND run_id = %s AND stage = %s AND created_at = %s"
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
            await conn.rollback()
            raise
        return updated

    async def acknowledge(self, record: DLQRecord) -> None:
        conn = await self._get_conn()
        table_sql = _quote_identifier(self._table, allow_path=True)
        if record._storage_id is not None:
            sql = f"DELETE FROM {table_sql} WHERE id = %s"
            params: tuple[Any, ...] = (record._storage_id,)
        else:
            sql = (
                f"DELETE FROM {table_sql} "
                "WHERE pipeline_id = %s AND run_id = %s AND stage = %s AND created_at = %s"
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
            await conn.rollback()
            raise

    async def _insert_rows(self, records: list[DLQRecord]) -> None:
        conn = await self._get_conn()
        await self._ensure_table()
        table_sql = _quote_identifier(self._table, allow_path=True)
        columns_sql = ", ".join(_quote_identifier(column) for column in _DLQ_COLUMNS)
        placeholders_sql = ", ".join(["%s"] * len(_DLQ_COLUMNS))
        sql = f"INSERT INTO {table_sql} ({columns_sql}) VALUES ({placeholders_sql})"
        params_batch = [
            tuple(_record_to_row(record)[column] for column in _DLQ_COLUMNS) for record in records
        ]
        try:
            async with conn.cursor() as cur:
                executemany = getattr(cur, "executemany", None)
                if callable(executemany):
                    await executemany(sql, params_batch)
                else:
                    for params in params_batch:
                        await cur.execute(sql, params)
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def _get_conn(self) -> Any:
        if self._conn is None:
            try:
                import psycopg
            except ImportError:
                raise ImportError(
                    "PostgresDLQSink requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
                ) from None
            self._conn = cast(
                "Any", await psycopg.AsyncConnection.connect(self._dsn, autocommit=False)
            )
            logger.info(
                "postgres_dlq_sink_connected", table=self._table, dsn=_redact_dsn(self._dsn)
            )
        return self._conn

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
            source TEXT,
            checkpoint JSONB,
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
        await conn.commit()
        self._table_ready = True


class PostgresDLQSource(DLQSource):
    """Read DLQ records from a PostgreSQL table for replay."""

    source_name = "postgres_dlq_source"

    def __init__(
        self,
        dsn: str,
        table: str = "agora_dlq",
        *,
        pipeline_id: str | None = None,
        stage: str | None = None,
        limit: int | None = None,
    ) -> None:
        self._dsn = dsn
        self._table = table
        self._pipeline_id = pipeline_id
        self._stage = stage
        self._limit = limit
        self._conn: Any | None = None

    async def open(self) -> None:
        await self._get_conn()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
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
                    record = _row_to_record(payload)
                    object.__setattr__(record, "_storage_id", payload.get("id"))
                    yield record

    async def _get_conn(self) -> Any:
        if self._conn is None:
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError:
                raise ImportError(
                    "PostgresDLQSource requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
                ) from None
            self._conn = cast(
                "Any", await psycopg.AsyncConnection.connect(self._dsn, row_factory=dict_row)
            )
        return self._conn


__all__ = ["PostgresDLQSink", "PostgresDLQSource"]
