"""Flush-time write orchestration helpers for PostgreSQL sinks."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import logstruct
from agora.core.retry import retry_async

from agora_plugins.postgres.sinks._write_strategies import (
    execute_copy_batch,
    execute_copy_merge_batch,
    execute_sql_batch,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import datetime

    from agora.core.retry import RetryPolicy

    from agora_plugins.postgres.sinks._write_preparation import _PreparedWriteBatch
    from agora_plugins.postgres.sinks.postgres import (
        PostgresSinkWriteError,
        PostgresWriteSafetyPolicy,
    )

logger = logstruct.getLogger("agora_plugins.postgres.sinks.postgres")


class PostgresFlushRuntime:
    """Owns batch flush orchestration while preserving sink-level compat wrappers."""

    def __init__(
        self,
        *,
        sink: Any,
        table: str | object,
        insert_mode: str,
        write_safety_policy: PostgresWriteSafetyPolicy | str,
        current_buffer: Callable[[], list[dict[str, Any]]],
        load_psycopg: Callable[[], Awaitable[Any]],
        effective_retry_policy: Callable[[], RetryPolicy[Any]],
        prepared_write_batches: Callable[
            [list[dict[str, Any]]], Awaitable[list[_PreparedWriteBatch]]
        ],
        wrap_write_error: Callable[..., PostgresSinkWriteError],
        observe_latency: Callable[[str, str, float], None],
        discard_buffer_indexes: Callable[[set[int]], None],
        observe_retry: Callable[[], None],
        write_connection: Callable[[], Any],
        note_flush_success: Callable[[int, datetime], None],
        now_utc: Callable[[], datetime],
    ) -> None:
        self._sink = sink
        self._table = table
        self._insert_mode = insert_mode
        self._write_safety_policy = write_safety_policy
        self._current_buffer = current_buffer
        self._load_psycopg = load_psycopg
        self._effective_retry_policy = effective_retry_policy
        self._prepared_write_batches = prepared_write_batches
        self._wrap_write_error = wrap_write_error
        self._observe_latency = observe_latency
        self._discard_buffer_indexes = discard_buffer_indexes
        self._observe_retry = observe_retry
        self._write_connection = write_connection
        self._note_flush_success = note_flush_success
        self._now_utc = now_utc

    async def flush(self) -> None:
        if not self._current_buffer():
            return

        await self._load_psycopg()
        started = time.perf_counter()
        rows = list(self._current_buffer())
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
            if self._write_safety_policy == "align_to_target" and len(batches) > 1:
                await self.flush_aligned_batches_atomically(batches, rows, policy)
            else:
                for batch in batches:
                    columns = list(batch.columns)
                    batch_rows = list(batch.rows)
                    if self._insert_mode == "copy":
                        await self._sink._flush_via_copy(
                            batch_rows, columns, len(batch_rows), policy
                        )
                    elif self._insert_mode == "copy_merge":
                        await self._sink._flush_via_copy_merge(
                            batch_rows, columns, len(batch_rows), policy
                        )
                    else:
                        await self._sink._flush_via_sql(
                            batch_rows, columns, len(batch_rows), policy
                        )
                    flushed_indexes.update(batch.row_indexes)
        except Exception:
            self._discard_buffer_indexes(flushed_indexes)
            self._observe_latency("flush", "error", time.perf_counter() - started)
            raise

        self._discard_buffer_indexes(set(range(len(rows))))
        self._note_flush_success(count, self._now_utc())
        self._observe_latency("flush", "success", time.perf_counter() - started)
        logger.info("postgres_flush", table=self._table, count=count)

    async def flush_aligned_batches_atomically(
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
                            await execute_copy_batch(self._sink, conn, batch_rows, columns)
                        elif self._insert_mode == "copy_merge":
                            await execute_copy_merge_batch(self._sink, conn, batch_rows, columns)
                        else:
                            await execute_sql_batch(self._sink, conn, batch_rows, columns)
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
