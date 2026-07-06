"""Close-time flush and connection cleanup helpers for PostgreSQL sinks."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import logstruct

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.dlq import DLQSink

    from agora_plugins.postgres.sinks.postgres import PostgresSinkWriteError

logger = logstruct.getLogger("agora_plugins.postgres.sinks.postgres")


class PostgresCloseRuntime:
    """Owns final flush handling and connection/pool teardown for ``PostgresSink``."""

    def __init__(
        self,
        *,
        table: str | object,
        current_conn: Callable[[], Any | None],
        set_conn: Callable[[Any | None], None],
        current_write_pool: Callable[[], asyncio.LifoQueue[Any] | None],
        set_write_pool: Callable[[asyncio.LifoQueue[Any] | None], None],
        current_external_write_pool: Callable[[], Any | None],
        set_external_write_pool: Callable[[Any | None], None],
        external_write_pool_conn_ids: set[int],
        current_write_pool_open_connections: Callable[[], int],
        set_write_pool_open_connections: Callable[[int], None],
        current_poison_sink: Callable[[], DLQSink | None],
        route_failed_buffer_to_dlq: Callable[[PostgresSinkWriteError], Awaitable[None]],
    ) -> None:
        self._table = table
        self._current_conn = current_conn
        self._set_conn = set_conn
        self._current_write_pool = current_write_pool
        self._set_write_pool = set_write_pool
        self._current_external_write_pool = current_external_write_pool
        self._set_external_write_pool = set_external_write_pool
        self._external_write_pool_conn_ids = external_write_pool_conn_ids
        self._current_write_pool_open_connections = current_write_pool_open_connections
        self._set_write_pool_open_connections = set_write_pool_open_connections
        self._current_poison_sink = current_poison_sink
        self._route_failed_buffer_to_dlq = route_failed_buffer_to_dlq

    async def close(self, *, flush: Callable[[], Awaitable[None]]) -> None:
        flush_error: Exception | None = None
        try:
            await flush()
        except Exception as exc:
            flush_error = await self._handle_flush_error(exc)

        conn = self._current_conn()
        if conn is not None:
            await conn.close()
            self._set_conn(None)

        write_pool = self._current_write_pool()
        if write_pool is not None:
            while True:
                try:
                    conn = write_pool.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await conn.close()
            self._set_write_pool(None)
            self._set_write_pool_open_connections(0)

        external_write_pool = self._current_external_write_pool()
        if external_write_pool is not None:
            await external_write_pool.close()
            self._set_external_write_pool(None)
            self._external_write_pool_conn_ids.clear()
            self._set_write_pool_open_connections(0)

        poison_sink = self._current_poison_sink()
        if poison_sink is not None:
            await poison_sink.close()
        logger.info("postgres_sink_closed", table=self._table)
        if flush_error is not None:
            raise flush_error

    async def _handle_flush_error(self, exc: Exception) -> Exception | None:
        poison_sink = self._current_poison_sink()
        if hasattr(exc, "poison_info") and poison_sink is not None:
            try:
                await self._route_failed_buffer_to_dlq(exc)  # type: ignore[arg-type]
                return None
            except Exception as dlq_exc:
                logger.exception("postgres_close_flush_dlq_error", table=self._table)
                return dlq_exc

        logger.exception("postgres_close_flush_error", table=self._table)
        return exc
