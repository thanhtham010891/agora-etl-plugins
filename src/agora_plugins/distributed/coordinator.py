"""
agora_plugins.distributed.coordinator
======================================
``RedisWorkerCoordinator`` — Redis-backed distributed worker coordination.

Uses ``redis.asyncio`` for native async operations. Each worker:
- Registers itself in Redis with a TTL-based heartbeat
- Acquires per-pipeline leases via SET NX EX before each run
- Releases leases atomically via Lua script after each run
- Deletes all leases and deregisters on graceful shutdown (SIGTERM)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
from datetime import UTC, datetime
from urllib.parse import urlparse

import logstruct
from agora.runner.coordinator import WorkerCoordinator, WorkerInfo

logger = logstruct.getLogger(__name__)

try:
    import redis.asyncio as aioredis
except ImportError as _exc:
    raise ImportError(
        "RedisWorkerCoordinator requires 'redis'. "
        "Install with: pip install 'agora-etl-plugins[distributed]'"
    ) from _exc

_SCAN_COUNT = 100

# Lua script: delete lease only if this worker owns it (atomic check-and-delete)
_RELEASE_SCRIPT = """
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
local ok, data = pcall(cjson.decode, val)
if not ok then return 0 end
if data["worker_id"] == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
"""


def _redact_url(url: str) -> str:
    """Return URL with password replaced by ***."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            return parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                + (f":{parsed.port}" if parsed.port else "")
            ).geturl()
    except Exception:
        pass
    return url


class RedisWorkerCoordinator(WorkerCoordinator):
    """Distributed worker coordinator backed by Redis.

    Parameters
    ----------
    redis_url:
        Redis connection URL (default: ``redis://localhost:6379``).
    lease_ttl_seconds:
        How long a pipeline lease is valid. Must be greater than the
        longest expected pipeline run duration.
    heartbeat_interval:
        Seconds between worker heartbeat refreshes (default: 30s).
        Worker TTL is set to ``heartbeat_interval * 3``.
        Must be less than ``lease_ttl_seconds``.
    key_prefix:
        Namespace prefix for all Redis keys (default: ``agora:distributed:``).
    fallback_to_local:
        If ``True``, treat Redis unavailability as "lease acquired" so the
        worker continues running without coordination. Risks duplicate runs.
        Default: ``False`` (fail-safe).
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        lease_ttl_seconds: int = 300,
        heartbeat_interval: int = 30,
        key_prefix: str = "agora:distributed:",
        fallback_to_local: bool = False,
    ) -> None:
        if heartbeat_interval >= lease_ttl_seconds:
            raise ValueError(
                f"heartbeat_interval ({heartbeat_interval}s) must be less than "
                f"lease_ttl_seconds ({lease_ttl_seconds}s)"
            )
        self._redis_url = redis_url
        self._lease_ttl = lease_ttl_seconds
        self._heartbeat_interval = heartbeat_interval
        self._prefix = key_prefix
        self._fallback_to_local = fallback_to_local

        self._worker_id: str = ""
        self._pipeline_ids: list[str] = []
        self._redis: aioredis.Redis | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._release_script: aioredis.client.Script | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Keys                                                                 #
    # ------------------------------------------------------------------ #

    def _worker_key(self) -> str:
        return f"{self._prefix}workers:{self._worker_id}"

    def _lease_key(self, pipeline_id: str) -> str:
        return f"{self._prefix}lease:{pipeline_id}"

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
        self._worker_id = worker_id
        self._pipeline_ids = list(pipeline_ids)

        try:
            self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
            await self._redis.ping()
        except Exception as exc:
            if self._fallback_to_local:
                logger.warning(
                    "coordinator_redis_unavailable_fallback",
                    error=str(exc),
                    url=_redact_url(self._redis_url),
                )
                self._redis = None
                return
            raise RuntimeError(
                f"agora-etl-plugins distributed coordinator cannot connect to Redis at "
                f"{_redact_url(self._redis_url)!r}: {exc}"
            ) from exc

        self._release_script = self._redis.register_script(_RELEASE_SCRIPT)
        await self._register_worker("running")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"agora-heartbeat-{worker_id}"
        )
        logger.info("coordinator_started", worker_id=worker_id)

    async def stop(self) -> None:
        if self._redis is None:
            return

        await self._register_worker("draining")

        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(asyncio.shield(self._heartbeat_task), timeout=5)

        for pipeline_id in self._pipeline_ids:
            try:
                await self.release_lease(pipeline_id)
            except Exception as exc:
                logger.warning(
                    "coordinator_release_error",
                    pipeline_id=pipeline_id,
                    error=str(exc),
                )

        with contextlib.suppress(Exception):
            await self._redis.delete(self._worker_key())

        await self._redis.aclose()
        self._redis = None
        logger.info("coordinator_stopped", worker_id=self._worker_id)

    # ------------------------------------------------------------------ #
    # Lease                                                                #
    # ------------------------------------------------------------------ #

    async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
        if self._redis is None:
            return self._fallback_to_local

        value = json.dumps(
            {
                "worker_id": self._worker_id,
                "acquired_at": _utcnow(),
                "pipeline_id": pipeline_id,
                "run_number": run_number,
            }
        )
        try:
            result = await self._redis.set(
                self._lease_key(pipeline_id),
                value,
                nx=True,
                ex=self._lease_ttl,
            )
            acquired = result is not None
            if not acquired:
                logger.debug(
                    "coordinator_lease_skipped",
                    pipeline_id=pipeline_id,
                    run=run_number,
                )
            return acquired
        except Exception as exc:
            logger.warning(
                "coordinator_lease_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )
            return self._fallback_to_local

    async def release_lease(self, pipeline_id: str) -> None:
        if self._redis is None or self._release_script is None:
            return
        try:
            await self._release_script(
                keys=[self._lease_key(pipeline_id)],
                args=[self._worker_id],
            )
        except Exception as exc:
            logger.warning(
                "coordinator_release_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )

    # ------------------------------------------------------------------ #
    # Fleet discovery                                                      #
    # ------------------------------------------------------------------ #

    async def list_workers(self) -> list[WorkerInfo]:
        if self._redis is None:
            return []

        pattern = f"{self._prefix}workers:*"
        all_keys: list[str] = []
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(cursor, match=pattern, count=_SCAN_COUNT)
            all_keys.extend(keys)
            if cursor == 0:
                break

        if not all_keys:
            return []

        # Batch fetch all worker records in one round trip
        raw_values = await self._redis.mget(*all_keys)
        workers: list[WorkerInfo] = []
        for raw in raw_values:
            if raw is None:
                continue
            try:
                data = json.loads(raw)
                workers.append(
                    WorkerInfo(
                        worker_id=data.get("worker_id", ""),
                        hostname=data.get("hostname", ""),
                        pid=data.get("pid", 0),
                        status=data.get("status", "unknown"),
                        assigned_pipelines=data.get("assigned_pipelines", []),
                        last_heartbeat_at=data.get("last_heartbeat_at", ""),
                    )
                )
            except Exception:
                continue

        return workers

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    async def connect(self) -> None:
        """Open a read-only Redis connection for fleet inspection."""
        if self._redis is not None:
            return
        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()

    async def close(self) -> None:
        """Close the read-only connection opened by ``connect()``."""
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def _register_worker(self, status: str) -> None:
        # Guard against concurrent calls from stop() and _heartbeat_loop()
        async with self._lock:
            if self._redis is None:
                return
            ttl = self._heartbeat_interval * 3
            value = json.dumps(
                {
                    "worker_id": self._worker_id,
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                    "status": status,
                    "assigned_pipelines": self._pipeline_ids,
                    "last_heartbeat_at": _utcnow(),
                }
            )
            await self._redis.set(self._worker_key(), value, ex=ttl)

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                await self._register_worker("running")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("coordinator_heartbeat_error", error=str(exc))


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()
