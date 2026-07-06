"""Connection acquisition runtime for PostgreSQL sinks."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import logstruct

from agora_plugins.postgres.connection import redact_postgres_dsn
from agora_plugins.postgres.sinks._pool import (
    acquire_write_conn,
    discard_pooled_connection,
    ensure_external_write_pool,
    pooled_connection_ready,
    release_write_conn,
)

logger = logstruct.getLogger(__name__)


class PostgresSinkConnectionRuntime:
    """Owns psycopg loading, connection creation, and write acquisition."""

    def __init__(self, sink: object) -> None:
        self._sink = sink

    async def load_psycopg(self) -> object:
        sink = self._sink
        if sink._psycopg is None:
            try:
                import psycopg
            except ImportError:
                raise ImportError(
                    "PostgresSink requires psycopg. Install via: pip install 'agora-etl-plugins[postgres]'"
                ) from None
            sink._psycopg = psycopg
        return sink._psycopg

    async def create_connection(self) -> object:
        sink = self._sink
        psycopg = await self.load_psycopg()
        started = time.perf_counter()
        try:
            conn = await psycopg.AsyncConnection.connect(
                **sink._connection.connect_kwargs(autocommit=False)
            )
        except Exception:
            sink._observe_latency("connect", "error", time.perf_counter() - started)
            raise
        sink._observe_latency("connect", "success", time.perf_counter() - started)
        logger.info(
            "postgres_sink_connected",
            table=sink._table,
            dsn=redact_postgres_dsn(sink._connection.resolve_dsn()),
        )
        return conn

    async def get_conn(self) -> object:
        sink = self._sink
        if sink._conn is None:
            sink._conn = await self.create_connection()
        return sink._conn

    async def acquire_write_conn(self) -> tuple[object, bool]:
        return await acquire_write_conn(self._sink)

    async def pooled_connection_ready(self, conn: object) -> bool:
        return await pooled_connection_ready(self._sink, conn)

    async def discard_pooled_connection(self, conn: object) -> None:
        await discard_pooled_connection(self._sink, conn)

    async def release_write_conn(
        self, conn: object, *, pooled: bool, discard: bool = False
    ) -> None:
        await release_write_conn(self._sink, conn, pooled=pooled, discard=discard)

    async def ensure_external_write_pool(self) -> object | None:
        return await ensure_external_write_pool(self._sink)

    @asynccontextmanager
    async def write_connection(self):
        sink = self._sink
        started = time.perf_counter()
        try:
            conn, pooled = await self.acquire_write_conn()
        except Exception:
            sink._observe_latency("pool_acquire", "error", time.perf_counter() - started)
            raise
        sink._observe_latency("pool_acquire", "success", time.perf_counter() - started)
        discard = False
        try:
            yield conn
        except Exception:
            discard = True
            raise
        finally:
            await self.release_write_conn(conn, pooled=pooled, discard=discard)
