"""Stream/read-loop runtime for PostgreSQL sources."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct
from agora.core.source import SourceRecordError
from agora.core.types import SourceRecordFailurePolicy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class PostgresSourceStreamRuntime(Generic[T]):
    """Owns retry, row mapping, checkpoint progression, and read-loop state."""

    def __init__(self, source: Any) -> None:
        self._source = source

    async def stream(self) -> AsyncGenerator[T, None]:
        source = self._source
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError(
                "PostgresSource requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
            ) from None

        logger.info("postgres_source_start", query=source._query[:80])
        source._reset_progress()
        fetched = 0
        source._stream_run_count += 1
        source._active_stream_count += 1
        source._last_stream_started_at = datetime.now(UTC)
        stream_failed = False
        attempt = 1
        try:
            while True:
                try:
                    async for record in self.stream_attempt(psycopg, dict_row):
                        fetched += 1
                        yield record
                    break
                except Exception as exc:
                    if not source._should_retry_read(exc, attempt=attempt):
                        raise
                    delay = source._retry_policy.backoff_for(attempt=attempt)
                    source._retry_count += 1
                    logger.warning(
                        "postgres_source_retrying_read",
                        attempt=attempt,
                        delay_s=delay,
                        error=str(exc),
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    attempt += 1
        except Exception:
            stream_failed = True
            raise
        finally:
            source._last_stream_completed_at = datetime.now(UTC)
            source._last_stream_succeeded = not stream_failed
            source._active_stream_count = max(source._active_stream_count - 1, 0)

        logger.info("postgres_source_done", records=fetched)

    async def stream_attempt(
        self,
        psycopg: Any,
        dict_row: Any,
    ) -> AsyncGenerator[T, None]:
        source = self._source
        source._query_execution_count += 1
        async with await source._connect_for_read_attempt(psycopg, dict_row) as conn:
            async with conn.cursor() as config_cur:
                await source._apply_read_safety_controls(config_cur)
            async with source._open_stream_cursor(conn) as cur:
                await cur.execute(source._query, source._params if source._params else None)
                while True:
                    await source._enforce_active_replica_freshness(conn)
                    rows = await cur.fetchmany(source._batch_size)
                    if not rows:
                        break
                    async for record in self._emit_rows(rows):
                        yield record

    async def _emit_rows(self, rows: object) -> AsyncGenerator[T, None]:
        source = self._source
        for row in rows:
            row_dict: dict[str, Any] | None = None
            try:
                row_dict = dict(row)
                source._rows_seen += 1
                checkpoint_cursor = source._extract_checkpoint_cursor(row_dict)
                source._current_row_checkpoint_cursor = checkpoint_cursor
                source._last_row_at = datetime.now(UTC)
                record = await self._map_row(row_dict)
                source._last_checkpoint_cursor = checkpoint_cursor
                if record is not None:
                    yield record
                else:
                    source._record_drop_count += 1
            except Exception as exc:
                source._record_error_count += 1
                logger.warning("postgres_source_row_error", error=str(exc))
                if source._on_record_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                    source._record_drop_count += 1
                    continue
                failed_record = row_dict if row_dict is not None else row
                raise SourceRecordError(
                    exc,
                    record=failed_record,
                    checkpoint=source.current_checkpoint(),
                    source=source.source_name,
                ) from exc
            finally:
                source._current_row_checkpoint_cursor = None

    async def _map_row(self, row_dict: dict[str, Any]) -> T | None:
        source = self._source
        if source._row_mapper_accepts_context:
            record = source._row_mapper(row_dict, source._row_context())
        else:
            record = source._row_mapper(row_dict)
        if isawaitable(record):
            return await cast("Awaitable[T | None]", record)
        return record


__all__ = ["PostgresSourceStreamRuntime"]
