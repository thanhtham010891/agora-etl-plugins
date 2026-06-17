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
import time
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import urlparse

import logstruct
from agora.runner import LeaseState, WorkerCoordinator, WorkerInfo

logger = logstruct.getLogger(__name__)

try:
    import redis.asyncio as aioredis
except ImportError as _exc:
    raise ImportError(
        "RedisWorkerCoordinator requires 'redis'. "
        "Install with: pip install 'agora-etl-plugins[distributed]'"
    ) from _exc

_SCAN_COUNT = 100
_DEFAULT_FENCING_KEY_TTL_SECONDS = 30 * 24 * 60 * 60

# Lua script: delete lease only if this worker owns it (atomic check-and-delete)
_RELEASE_SCRIPT = """
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
local ok, data = pcall(cjson.decode, val)
if not ok then return 0 end
if data["worker_id"] == ARGV[1] and tostring(data["fencing_token"]) == ARGV[2] then
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
"""

_RENEW_SCRIPT = """
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
local ok, data = pcall(cjson.decode, val)
if not ok then return 0 end
if data["worker_id"] == ARGV[1] and tostring(data["fencing_token"]) == ARGV[2] then
    redis.call("EXPIRE", KEYS[1], tonumber(ARGV[3]))
    return 1
end
return 0
"""

_ACQUIRE_SCRIPT = """
local lease = redis.call("GET", KEYS[1])
if lease ~= false then return {0, false} end
local fencing_token = redis.call("INCR", KEYS[2])
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[6]))
local value = cjson.encode({
    worker_id = ARGV[1],
    acquired_at = ARGV[2],
    pipeline_id = ARGV[3],
    run_number = tonumber(ARGV[4]),
    fencing_token = fencing_token
})
local ok = redis.call("SET", KEYS[1], value, "NX", "EX", tonumber(ARGV[5]))
if ok then return {1, fencing_token} end
return {0, false}
"""

_REDLOCK_ACQUIRE_SCRIPT = """
-- REDLOCK_ACQUIRE
local lease = redis.call("GET", KEYS[1])
if lease ~= false then return 0 end
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", tonumber(ARGV[2]))
if ok then return 1 end
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
        How long a pipeline lease is valid between renewals.
    heartbeat_interval:
        Seconds between worker heartbeat refreshes (default: 30s).
        Worker TTL is set to ``heartbeat_interval * 3``.
        Must be less than ``lease_ttl_seconds``. Active leases are renewed
        periodically while the worker is alive.
    key_prefix:
        Namespace prefix for all Redis keys (default: ``agora:distributed:``).
    fallback_to_local:
        If ``True``, treat Redis unavailability as "lease acquired" so the
        worker continues running without coordination. Risks duplicate runs.
        Default: ``False`` (fail-safe).
    redlock_redis_urls:
        Optional list of at least three independent Redis master URLs. When
        provided, per-pipeline leases are acquired and renewed by Redlock-style
        majority quorum across those nodes. Worker registry and fencing-token
        generation continue to use ``redis_url``.
    redlock_clock_drift_factor:
        Safety margin factor subtracted from Redlock lease validity.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        lease_ttl_seconds: int = 300,
        heartbeat_interval: int = 30,
        key_prefix: str = "agora:distributed:",
        fallback_to_local: bool = False,
        fencing_key_ttl_seconds: int = _DEFAULT_FENCING_KEY_TTL_SECONDS,
        lease_renewal_deadline_seconds: float | None = None,
        redlock_redis_urls: list[str] | None = None,
        redlock_clock_drift_factor: float = 0.01,
    ) -> None:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be > 0")
        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be > 0")
        if heartbeat_interval >= lease_ttl_seconds:
            raise ValueError(
                f"heartbeat_interval ({heartbeat_interval}s) must be less than "
                f"lease_ttl_seconds ({lease_ttl_seconds}s)"
            )
        if fencing_key_ttl_seconds <= 0:
            raise ValueError("fencing_key_ttl_seconds must be > 0")
        if lease_renewal_deadline_seconds is not None and lease_renewal_deadline_seconds <= 0:
            raise ValueError("lease_renewal_deadline_seconds must be > 0 when provided")
        if redlock_redis_urls is not None and len(redlock_redis_urls) < 3:
            raise ValueError("redlock_redis_urls requires at least 3 independent Redis URLs")
        if redlock_clock_drift_factor < 0:
            raise ValueError("redlock_clock_drift_factor must be >= 0")
        self._redis_url = redis_url
        self._redlock_redis_urls = list(redlock_redis_urls or [])
        self._redlock_clock_drift_factor = redlock_clock_drift_factor
        self._lease_ttl = lease_ttl_seconds
        self._heartbeat_interval = heartbeat_interval
        self._fencing_key_ttl_seconds = fencing_key_ttl_seconds
        self._lease_renewal_deadline_seconds = lease_renewal_deadline_seconds
        self._prefix = key_prefix
        self._fallback_to_local = fallback_to_local

        self._worker_id: str = ""
        self._pipeline_ids: list[str] = []
        self._redis: Any | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._lease_renew_task: asyncio.Task[None] | None = None
        self._release_script: Any | None = None
        self._renew_script: Any | None = None
        self._acquire_script: Any | None = None
        self._redlock_redis_nodes: list[Any] = []
        self._redlock_acquire_scripts: list[Any] = []
        self._redlock_release_scripts: list[Any] = []
        self._redlock_renew_scripts: list[Any] = []
        self._redlock_lease_nodes: dict[str, set[int]] = {}
        self._lease_tokens: dict[str, LeaseState] = {}
        self._lease_lost_callback: Any | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Keys                                                                 #
    # ------------------------------------------------------------------ #

    def _worker_key(self) -> str:
        return f"{self._prefix}workers:{self._worker_id}"

    def _lease_key(self, pipeline_id: str) -> str:
        return f"{self._prefix}lease:{pipeline_id}"

    def _fencing_key(self, pipeline_id: str) -> str:
        return f"{self._prefix}fence:{pipeline_id}"

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
        self._worker_id = worker_id
        self._pipeline_ids = list(pipeline_ids)

        try:
            self._redis = cast("Any", aioredis.from_url(self._redis_url, decode_responses=True))
            await self._redis.ping()
            await self._start_redlock_nodes()
        except Exception as exc:
            await self._close_redlock_nodes()
            if self._redis is not None:
                with contextlib.suppress(Exception):
                    await self._redis.aclose()
                self._redis = None
            if self._fallback_to_local:
                logger.warning(
                    "coordinator_redis_unavailable_fallback",
                    error=str(exc),
                    url=_redact_url(self._redis_url),
                )
                self._redis = None
                return
            redlock_urls = [_redact_url(url) for url in self._redlock_redis_urls]
            raise RuntimeError(
                "agora-etl-plugins distributed coordinator cannot connect to Redis "
                f"primary={_redact_url(self._redis_url)!r} "
                f"redlock_nodes={redlock_urls!r}: {exc}"
            ) from exc

        self._release_script = self._redis.register_script(_RELEASE_SCRIPT)
        self._renew_script = self._redis.register_script(_RENEW_SCRIPT)
        self._acquire_script = self._redis.register_script(_ACQUIRE_SCRIPT)
        await self._register_worker("running")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"agora-heartbeat-{worker_id}"
        )
        self._lease_renew_task = asyncio.create_task(
            self._lease_renew_loop(), name=f"agora-lease-renew-{worker_id}"
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
        if self._lease_renew_task is not None and not self._lease_renew_task.done():
            self._lease_renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(asyncio.shield(self._lease_renew_task), timeout=5)

        for pipeline_id in list(self._lease_tokens):
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
        await self._close_redlock_nodes()
        self._redis = None
        self._lease_tokens.clear()
        self._redlock_lease_nodes.clear()
        logger.info("coordinator_stopped", worker_id=self._worker_id)

    # ------------------------------------------------------------------ #
    # Lease                                                                #
    # ------------------------------------------------------------------ #

    async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
        if self._redis is None:
            return self._fallback_to_local
        if self._redlock_enabled():
            return await self._try_acquire_redlock_lease(pipeline_id, run_number)

        try:
            if self._acquire_script is None:
                return False
            acquired_at = _utcnow()
            result = await self._acquire_script(
                keys=[self._lease_key(pipeline_id), self._fencing_key(pipeline_id)],
                args=[
                    self._worker_id,
                    acquired_at,
                    pipeline_id,
                    str(run_number),
                    str(self._lease_ttl),
                    str(self._fencing_key_ttl_seconds),
                ],
            )
            acquired = isinstance(result, list) and int(result[0] or 0) == 1
            if not acquired:
                logger.debug(
                    "coordinator_lease_skipped",
                    pipeline_id=pipeline_id,
                    run=run_number,
                )
                return False
            fencing_token = int(result[1])
            self._lease_tokens[pipeline_id] = LeaseState(
                pipeline_id=pipeline_id,
                run_number=run_number,
                worker_id=self._worker_id,
                fencing_token=fencing_token,
                acquired_at=acquired_at,
            )
            return acquired
        except Exception as exc:
            logger.warning(
                "coordinator_lease_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )
            return False

    async def release_lease(self, pipeline_id: str) -> None:
        if self._redis is None or self._release_script is None:
            return
        lease = self._lease_tokens.pop(pipeline_id, None)
        self._redlock_lease_nodes.pop(pipeline_id, None)
        if lease is None:
            logger.warning(
                "coordinator_release_without_local_lease",
                pipeline_id=pipeline_id,
            )
            return
        if self._redlock_enabled():
            await self._release_redlock_indices(
                pipeline_id,
                lease.fencing_token,
                range(len(self._redlock_release_scripts)),
            )
            return
        try:
            await self._release_script(
                keys=[self._lease_key(pipeline_id)],
                args=[self._worker_id, str(lease.fencing_token)],
            )
        except Exception as exc:
            logger.warning(
                "coordinator_release_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )

    def current_lease(self, pipeline_id: str) -> LeaseState | None:
        """Return the local held lease and fencing token for *pipeline_id*."""

        return self._lease_tokens.get(pipeline_id)

    async def validate_lease(self, pipeline_id: str, fencing_token: int) -> bool:
        """Authoritatively validate that this worker still owns the lease token."""
        if self._redis is None:
            return False
        if self._redlock_enabled():
            return await self._validate_redlock_lease(pipeline_id, fencing_token)
        try:
            raw = await self._redis.get(self._lease_key(pipeline_id))
        except Exception as exc:
            logger.warning(
                "coordinator_lease_validate_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )
            return False
        if raw is None:
            self._lease_tokens.pop(pipeline_id, None)
            return False
        try:
            data = json.loads(raw)
        except Exception:
            return False
        valid = data.get("worker_id") == self._worker_id and str(data.get("fencing_token")) == str(
            fencing_token
        )
        if not valid:
            self._lease_tokens.pop(pipeline_id, None)
        return valid

    def set_lease_lost_callback(self, callback: Any | None) -> None:
        """Register a callback invoked when Redis proves this worker lost a held lease."""

        self._lease_lost_callback = callback

    async def renew_lease(self, pipeline_id: str) -> bool:
        """Renew a held lease if this worker still owns its fencing token."""

        if self._redis is None or self._renew_script is None:
            return False
        if self._redlock_enabled():
            return await self._renew_redlock_lease(pipeline_id)
        lease = self._lease_tokens.get(pipeline_id)
        if lease is None:
            return False
        try:
            renewed = await self._renew_script(
                keys=[self._lease_key(pipeline_id)],
                args=[self._worker_id, str(lease.fencing_token), str(self._lease_ttl)],
            )
        except Exception as exc:
            logger.warning(
                "coordinator_lease_renew_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )
            return False
        if int(renewed or 0) != 1:
            await self._mark_lease_lost(pipeline_id, lease, reason="renew_failed")
            return False
        self._lease_tokens[pipeline_id] = LeaseState(
            pipeline_id=lease.pipeline_id,
            run_number=lease.run_number,
            worker_id=lease.worker_id,
            fencing_token=lease.fencing_token,
            acquired_at=lease.acquired_at,
            renewed_at=_utcnow(),
        )
        return True

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

        all_keys = list(dict.fromkeys(all_keys))
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
        self._redis = cast("Any", aioredis.from_url(self._redis_url, decode_responses=True))
        await self._redis.ping()

    async def close(self) -> None:
        """Close the read-only connection opened by ``connect()``."""
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
        await self._close_redlock_nodes()

    def _redlock_enabled(self) -> bool:
        return bool(self._redlock_redis_urls)

    def _redlock_quorum(self) -> int:
        return (len(self._redlock_redis_nodes) // 2) + 1

    def _redlock_validity_s(self, elapsed_s: float) -> float:
        drift_s = (self._lease_ttl * self._redlock_clock_drift_factor) + 0.002
        return self._lease_ttl - elapsed_s - drift_s

    async def _start_redlock_nodes(self) -> None:
        if not self._redlock_redis_urls:
            return
        for url in self._redlock_redis_urls:
            node = cast("Any", aioredis.from_url(url, decode_responses=True))
            await node.ping()
            self._redlock_redis_nodes.append(node)
            self._redlock_acquire_scripts.append(node.register_script(_REDLOCK_ACQUIRE_SCRIPT))
            self._redlock_release_scripts.append(node.register_script(_RELEASE_SCRIPT))
            self._redlock_renew_scripts.append(node.register_script(_RENEW_SCRIPT))

    async def _close_redlock_nodes(self) -> None:
        for node in self._redlock_redis_nodes:
            with contextlib.suppress(Exception):
                await node.aclose()
        self._redlock_redis_nodes.clear()
        self._redlock_acquire_scripts.clear()
        self._redlock_release_scripts.clear()
        self._redlock_renew_scripts.clear()
        self._redlock_lease_nodes.clear()

    async def _try_acquire_redlock_lease(self, pipeline_id: str, run_number: int) -> bool:
        if self._redis is None:
            return False
        if not self._redlock_acquire_scripts:
            return False

        acquired_at = _utcnow()
        fencing_token = int(await self._redis.incr(self._fencing_key(pipeline_id)))
        await self._redis.expire(self._fencing_key(pipeline_id), self._fencing_key_ttl_seconds)
        payload = json.dumps(
            {
                "worker_id": self._worker_id,
                "acquired_at": acquired_at,
                "pipeline_id": pipeline_id,
                "run_number": run_number,
                "fencing_token": fencing_token,
            }
        )
        acquired_indices: set[int] = set()
        started = time.monotonic()
        for index, script in enumerate(self._redlock_acquire_scripts):
            try:
                result = await script(
                    keys=[self._lease_key(pipeline_id)],
                    args=[payload, str(self._lease_ttl)],
                )
            except Exception as exc:
                logger.warning(
                    "coordinator_redlock_node_acquire_error",
                    pipeline_id=pipeline_id,
                    node_index=index,
                    error=str(exc),
                )
                continue
            if int(result or 0) == 1:
                acquired_indices.add(index)

        elapsed = time.monotonic() - started
        if (
            len(acquired_indices) >= self._redlock_quorum()
            and self._redlock_validity_s(elapsed) > 0
        ):
            self._lease_tokens[pipeline_id] = LeaseState(
                pipeline_id=pipeline_id,
                run_number=run_number,
                worker_id=self._worker_id,
                fencing_token=fencing_token,
                acquired_at=acquired_at,
            )
            self._redlock_lease_nodes[pipeline_id] = acquired_indices
            return True

        await self._release_redlock_indices(pipeline_id, fencing_token, acquired_indices)
        logger.debug(
            "coordinator_redlock_lease_skipped",
            pipeline_id=pipeline_id,
            run=run_number,
            acquired_nodes=len(acquired_indices),
            quorum=self._redlock_quorum(),
            validity_s=round(self._redlock_validity_s(elapsed), 6),
        )
        return False

    async def _release_redlock_indices(
        self,
        pipeline_id: str,
        fencing_token: int,
        indices: range | set[int],
    ) -> None:
        for index in indices:
            try:
                await self._redlock_release_scripts[index](
                    keys=[self._lease_key(pipeline_id)],
                    args=[self._worker_id, str(fencing_token)],
                )
            except Exception as exc:
                logger.warning(
                    "coordinator_redlock_node_release_error",
                    pipeline_id=pipeline_id,
                    node_index=index,
                    error=str(exc),
                )

    async def _validate_redlock_lease(self, pipeline_id: str, fencing_token: int) -> bool:
        matching_nodes = 0
        for index, node in enumerate(self._redlock_redis_nodes):
            try:
                raw = await node.get(self._lease_key(pipeline_id))
            except Exception as exc:
                logger.warning(
                    "coordinator_redlock_node_validate_error",
                    pipeline_id=pipeline_id,
                    node_index=index,
                    error=str(exc),
                )
                continue
            if _lease_payload_matches(raw, self._worker_id, fencing_token):
                matching_nodes += 1
        valid = matching_nodes >= self._redlock_quorum()
        if not valid:
            self._lease_tokens.pop(pipeline_id, None)
            self._redlock_lease_nodes.pop(pipeline_id, None)
        return valid

    async def _renew_redlock_lease(self, pipeline_id: str) -> bool:
        lease = self._lease_tokens.get(pipeline_id)
        if lease is None:
            return False
        candidate_indices = self._redlock_lease_nodes.get(
            pipeline_id,
            set(range(len(self._redlock_renew_scripts))),
        )
        renewed_indices: set[int] = set()
        started = time.monotonic()
        for index in candidate_indices:
            try:
                renewed = await self._redlock_renew_scripts[index](
                    keys=[self._lease_key(pipeline_id)],
                    args=[self._worker_id, str(lease.fencing_token), str(self._lease_ttl)],
                )
            except Exception as exc:
                logger.warning(
                    "coordinator_redlock_node_renew_error",
                    pipeline_id=pipeline_id,
                    node_index=index,
                    error=str(exc),
                )
                continue
            if int(renewed or 0) == 1:
                renewed_indices.add(index)

        elapsed = time.monotonic() - started
        if len(renewed_indices) >= self._redlock_quorum() and self._redlock_validity_s(elapsed) > 0:
            self._redlock_lease_nodes[pipeline_id] = renewed_indices
            self._lease_tokens[pipeline_id] = LeaseState(
                pipeline_id=lease.pipeline_id,
                run_number=lease.run_number,
                worker_id=lease.worker_id,
                fencing_token=lease.fencing_token,
                acquired_at=lease.acquired_at,
                renewed_at=_utcnow(),
            )
            return True

        await self._release_redlock_indices(pipeline_id, lease.fencing_token, renewed_indices)
        await self._mark_lease_lost(pipeline_id, lease, reason="redlock_renew_quorum_failed")
        self._redlock_lease_nodes.pop(pipeline_id, None)
        return False

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

    async def _lease_renew_loop(self) -> None:
        interval = max(1.0, min(self._heartbeat_interval, self._lease_ttl / 3))
        while True:
            try:
                await asyncio.sleep(interval)
                await self._renew_leases_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("coordinator_lease_renew_loop_error", error=str(exc))

    async def _renew_leases_once(self) -> None:
        started = time.monotonic()
        try:
            async with asyncio.timeout(self._renewal_cycle_deadline_s()):
                for pipeline_id in list(self._lease_tokens):
                    await self.renew_lease(pipeline_id)
        except TimeoutError:
            logger.warning(
                "coordinator_lease_renew_cycle_deadline_exceeded",
                lease_count=len(self._lease_tokens),
                deadline_s=self._renewal_cycle_deadline_s(),
            )
            await self._mark_all_leases_lost(reason="renew_cycle_deadline_exceeded")
            return
        logger.debug(
            "coordinator_lease_renewed",
            lease_count=len(self._lease_tokens),
            duration_ms=round((time.monotonic() - started) * 1000, 3),
        )

    def _renewal_cycle_deadline_s(self) -> float:
        if self._lease_renewal_deadline_seconds is not None:
            return self._lease_renewal_deadline_seconds
        return max(0.1, self._lease_ttl * 0.5)

    async def _mark_all_leases_lost(self, *, reason: str) -> None:
        leases = list(self._lease_tokens.items())
        for pipeline_id, lease in leases:
            await self._mark_lease_lost(pipeline_id, lease, reason=reason)

    async def _mark_lease_lost(
        self,
        pipeline_id: str,
        lease: LeaseState,
        *,
        reason: str,
    ) -> None:
        self._lease_tokens.pop(pipeline_id, None)
        self._redlock_lease_nodes.pop(pipeline_id, None)
        logger.warning(
            "coordinator_lease_lost",
            pipeline_id=pipeline_id,
            fencing_token=lease.fencing_token,
            reason=reason,
        )
        if self._lease_lost_callback is not None:
            try:
                await self._lease_lost_callback(pipeline_id)
            except Exception as exc:
                logger.warning(
                    "coordinator_lease_lost_callback_error",
                    pipeline_id=pipeline_id,
                    error=str(exc),
                )


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _lease_payload_matches(raw: object, worker_id: str, fencing_token: int) -> bool:
    if raw is None:
        return False
    try:
        data = json.loads(str(raw))
    except Exception:
        return False
    return data.get("worker_id") == worker_id and str(data.get("fencing_token")) == str(
        fencing_token
    )
