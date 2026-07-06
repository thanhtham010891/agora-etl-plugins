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

from datetime import UTC, datetime
from typing import Any

from agora.runner import LeaseState, WorkerCoordinator, WorkerInfo

from agora_plugins.distributed.coordinator_lifecycle import RedisCoordinatorLifecycle
from agora_plugins.distributed.lease_controller import RedisLeaseController
from agora_plugins.distributed.lease_manager import RedisLeaseManager
from agora_plugins.distributed.worker_registry import RedisWorkerRegistry
from agora_plugins.distributed.worker_session import RedisWorkerSession

try:
    import redis.asyncio as aioredis
except ImportError as _exc:
    raise ImportError(
        "RedisWorkerCoordinator requires 'redis'. "
        "Install with: pip install 'agora-etl-plugins[distributed]'"
    ) from _exc

_SCAN_COUNT = 100
_DEFAULT_FENCING_KEY_TTL_SECONDS: int | None = None
_DEFAULT_WORKER_LIST_FETCH_BATCH_SIZE = 128


class RedisWorkerCoordinator(WorkerCoordinator):
    """Distributed worker coordinator backed by Redis."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        lease_ttl_seconds: int = 300,
        heartbeat_interval: int = 30,
        key_prefix: str = "agora:distributed:",
        fallback_to_local: bool = False,
        fencing_key_ttl_seconds: int | None = _DEFAULT_FENCING_KEY_TTL_SECONDS,
        lease_renewal_deadline_seconds: float | None = None,
        redlock_redis_urls: list[str] | None = None,
        redlock_clock_drift_factor: float = 0.01,
        worker_list_fetch_batch_size: int = _DEFAULT_WORKER_LIST_FETCH_BATCH_SIZE,
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
        if fencing_key_ttl_seconds is not None and fencing_key_ttl_seconds <= 0:
            raise ValueError("fencing_key_ttl_seconds must be > 0 when provided")
        if lease_renewal_deadline_seconds is not None and lease_renewal_deadline_seconds <= 0:
            raise ValueError("lease_renewal_deadline_seconds must be > 0 when provided")
        if redlock_redis_urls is not None and len(redlock_redis_urls) < 3:
            raise ValueError("redlock_redis_urls requires at least 3 independent Redis URLs")
        if redlock_clock_drift_factor < 0:
            raise ValueError("redlock_clock_drift_factor must be >= 0")
        if worker_list_fetch_batch_size <= 0:
            raise ValueError("worker_list_fetch_batch_size must be > 0")

        redlock_urls = list(redlock_redis_urls or [])
        self._lease_manager = RedisLeaseManager(
            key_prefix=key_prefix,
            lease_ttl=lease_ttl_seconds,
            fencing_key_ttl_seconds=fencing_key_ttl_seconds or 0,
            redlock_redis_urls=redlock_urls,
            redlock_clock_drift_factor=redlock_clock_drift_factor,
            now_fn=_utcnow,
        )
        self._worker_registry = RedisWorkerRegistry(
            key_prefix=key_prefix,
            heartbeat_interval=heartbeat_interval,
            fetch_batch_size=worker_list_fetch_batch_size,
            scan_count=_SCAN_COUNT,
            now_fn=_utcnow,
        )
        self._session = RedisWorkerSession(
            redis_url=redis_url,
            redlock_redis_urls=redlock_urls,
            heartbeat_interval=heartbeat_interval,
            fallback_to_local=fallback_to_local,
            worker_registry=self._worker_registry,
            lease_manager=self._lease_manager,
        )
        self._lease_controller = RedisLeaseController(
            lease_manager=self._lease_manager,
            heartbeat_interval=heartbeat_interval,
            lease_ttl=lease_ttl_seconds,
            lease_renewal_deadline_seconds=lease_renewal_deadline_seconds,
            fallback_to_local=fallback_to_local,
        )
        self._lifecycle = RedisCoordinatorLifecycle(
            session=self._session,
            lease_manager=self._lease_manager,
            lease_controller=self._lease_controller,
        )

    async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
        await self._lifecycle.start(
            aioredis=aioredis,
            worker_id=worker_id,
            pipeline_ids=pipeline_ids,
        )

    async def stop(self) -> None:
        await self._lifecycle.stop()

    async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
        return await self._lease_controller.try_acquire(
            redis=self._session.redis,
            worker_id=self._session.worker_id,
            pipeline_id=pipeline_id,
            run_number=run_number,
        )

    async def release_lease(self, pipeline_id: str) -> None:
        await self._lease_controller.release(
            redis=self._session.redis,
            worker_id=self._session.worker_id,
            pipeline_id=pipeline_id,
        )

    def current_lease(self, pipeline_id: str) -> LeaseState | None:
        return self._lease_manager.current_lease(pipeline_id)

    async def validate_lease(self, pipeline_id: str, fencing_token: int) -> bool:
        return await self._lease_controller.validate(
            redis=self._session.redis,
            worker_id=self._session.worker_id,
            pipeline_id=pipeline_id,
            fencing_token=fencing_token,
            fallback_validate=super().validate_lease,
        )

    def set_lease_lost_callback(self, callback: Any | None) -> None:
        self._lease_controller.set_lease_lost_callback(callback)

    async def renew_lease(self, pipeline_id: str) -> bool:
        return await self._lease_controller.renew(
            redis=self._session.redis,
            worker_id=self._session.worker_id,
            pipeline_id=pipeline_id,
        )

    async def list_workers(self) -> list[WorkerInfo]:
        return await self._session.list_workers()

    async def connect(self) -> None:
        await self._lifecycle.connect(aioredis=aioredis)

    async def close(self) -> None:
        await self._lifecycle.close()


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()
