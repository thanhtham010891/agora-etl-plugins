"""PostgreSQL sink connection-pool lifecycle helpers."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol

import logstruct

if TYPE_CHECKING:
    from agora_plugins.postgres.connection import PostgresConnectionConfig
    from agora_plugins.postgres.sinks._identifiers import QuotedIdentifier

logger = logstruct.getLogger("agora_plugins.postgres.sinks.postgres")


class PostgresPoolOwner(Protocol):
    """Mutable sink state required by pool operations."""

    _table: str | QuotedIdentifier
    _connection: PostgresConnectionConfig
    _conn: object | None
    _pool_size: int
    _pool_acquire_timeout_s: float | None
    _pool_health_check: bool
    _pool_max_lifetime_s: float
    _pool_max_idle_s: float
    _write_pool: asyncio.LifoQueue[object] | None
    _write_pool_open_connections: int
    _write_pool_lock: asyncio.Lock
    _external_write_pool: object | None
    _external_write_pool_conn_ids: set[int]
    _external_write_pool_unavailable: bool

    async def _get_conn(self) -> object: ...

    async def _create_connection(self) -> object: ...

    async def _pooled_connection_ready(self, conn: object) -> bool: ...

    async def _discard_pooled_connection(self, conn: object) -> None: ...

    async def _acquire_write_conn(self) -> tuple[object, bool]: ...


async def acquire_write_conn(owner: PostgresPoolOwner) -> tuple[object, bool]:
    if owner._pool_size <= 1:
        return await owner._get_conn(), False

    external_pool = await ensure_external_write_pool(owner)
    if external_pool is not None:
        conn = await external_pool.getconn(timeout=owner._pool_acquire_timeout_s)
        owner._external_write_pool_conn_ids.add(id(conn))
        owner._write_pool_open_connections = max(
            owner._write_pool_open_connections,
            len(owner._external_write_pool_conn_ids),
        )
        return conn, True

    if owner._write_pool is None:
        owner._write_pool = asyncio.LifoQueue(maxsize=owner._pool_size)

    while True:
        try:
            conn = owner._write_pool.get_nowait()
        except asyncio.QueueEmpty:
            break
        if await owner._pooled_connection_ready(conn):
            return conn, True
        await owner._discard_pooled_connection(conn)

    should_create = False
    async with owner._write_pool_lock:
        if owner._write_pool_open_connections < owner._pool_size:
            owner._write_pool_open_connections += 1
            should_create = True

    if should_create:
        try:
            conn = await owner._create_connection()
        except Exception:
            async with owner._write_pool_lock:
                owner._write_pool_open_connections = max(
                    0,
                    owner._write_pool_open_connections - 1,
                )
            raise
        return conn, True

    try:
        if owner._pool_acquire_timeout_s is None:
            conn = await owner._write_pool.get()
        else:
            conn = await asyncio.wait_for(
                owner._write_pool.get(),
                timeout=owner._pool_acquire_timeout_s,
            )
    except TimeoutError as exc:
        raise TimeoutError(
            "Timed out waiting for a PostgreSQL sink pooled connection "
            f"after {owner._pool_acquire_timeout_s}s."
        ) from exc
    if await owner._pooled_connection_ready(conn):
        return conn, True
    await owner._discard_pooled_connection(conn)
    return await owner._acquire_write_conn()


async def pooled_connection_ready(owner: PostgresPoolOwner, conn: object) -> bool:
    if not owner._pool_health_check:
        return True
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
        return True
    except Exception:
        logger.warning("postgres_sink_pooled_connection_unhealthy", table=owner._table)
        return False


async def discard_pooled_connection(owner: PostgresPoolOwner, conn: object) -> None:
    try:
        await close_connection_best_effort(owner, conn, reason="discard")
    finally:
        async with owner._write_pool_lock:
            owner._write_pool_open_connections = max(
                0,
                owner._write_pool_open_connections - 1,
            )


async def release_write_conn(
    owner: PostgresPoolOwner,
    conn: object,
    *,
    pooled: bool,
    discard: bool = False,
) -> None:
    if not pooled:
        if discard and owner._conn is conn:
            try:
                await close_connection_best_effort(owner, conn, reason="single_connection_discard")
            finally:
                owner._conn = None
        return

    if id(conn) in owner._external_write_pool_conn_ids and owner._external_write_pool is not None:
        owner._external_write_pool_conn_ids.discard(id(conn))
        if discard:
            await close_connection_best_effort(owner, conn, reason="external_pool_discard")
        await owner._external_write_pool.putconn(conn)
        owner._write_pool_open_connections = len(owner._external_write_pool_conn_ids)
        return

    if discard:
        try:
            await close_connection_best_effort(owner, conn, reason="pool_discard")
        finally:
            async with owner._write_pool_lock:
                owner._write_pool_open_connections = max(
                    0,
                    owner._write_pool_open_connections - 1,
                )
        return

    assert owner._write_pool is not None
    owner._write_pool.put_nowait(conn)


async def close_connection_best_effort(
    owner: PostgresPoolOwner,
    conn: object,
    *,
    reason: str,
) -> None:
    try:
        await conn.close()
    except Exception as exc:
        logger.debug(
            "postgres_sink_connection_close_error",
            table=owner._table,
            reason=reason,
            error=str(exc),
        )


async def ensure_external_write_pool(owner: PostgresPoolOwner) -> object | None:
    if owner._external_write_pool_unavailable:
        return None
    if owner._external_write_pool is not None:
        return owner._external_write_pool
    try:
        from psycopg_pool import AsyncConnectionPool
    except ImportError:
        owner._external_write_pool_unavailable = True
        return None

    connect_kwargs = owner._connection.connect_kwargs(autocommit=False)
    conninfo = str(connect_kwargs.pop("conninfo"))
    pool = AsyncConnectionPool(
        conninfo=conninfo,
        kwargs=connect_kwargs,
        min_size=0,
        max_size=owner._pool_size,
        timeout=owner._pool_acquire_timeout_s or 30.0,
        max_lifetime=owner._pool_max_lifetime_s,
        max_idle=owner._pool_max_idle_s,
        open=False,
    )
    await pool.open()
    owner._external_write_pool = pool
    return pool
