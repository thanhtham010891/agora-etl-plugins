from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora_plugins.distributed.lease_operations import RedisLeaseOperations
from agora_plugins.distributed.lease_runtime import RedisLeaseRuntime
from agora_plugins.distributed.primary_lease_store import RedisPrimaryLeaseStore
from agora_plugins.distributed.redlock_quorum import RedisRedlockQuorum

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.runner import LeaseState


class RedisLeaseManager:
    """Facade over single-Redis and Redlock lease collaborators."""

    def __init__(
        self,
        *,
        key_prefix: str,
        lease_ttl: int,
        fencing_key_ttl_seconds: int,
        redlock_redis_urls: list[str],
        redlock_clock_drift_factor: float,
        now_fn: Callable[[], str],
    ) -> None:
        self._primary = RedisPrimaryLeaseStore(
            key_prefix=key_prefix,
            lease_ttl=lease_ttl,
            fencing_key_ttl_seconds=fencing_key_ttl_seconds,
            now_fn=now_fn,
        )
        self._redlock = RedisRedlockQuorum(
            redis_urls=redlock_redis_urls,
            key_prefix=key_prefix,
            lease_ttl=lease_ttl,
            fencing_key_ttl_seconds=fencing_key_ttl_seconds,
            clock_drift_factor=redlock_clock_drift_factor,
            now_fn=now_fn,
        )
        self._runtime = RedisLeaseRuntime(primary=self._primary, redlock=self._redlock)
        self._operations = RedisLeaseOperations(self)

    @property
    def runtime(self) -> RedisLeaseRuntime:
        return self._runtime

    @property
    def lease_tokens(self) -> dict[str, LeaseState]:
        return self._runtime.lease_tokens

    @property
    def local_fencing_tokens(self) -> dict[str, int]:
        return self._runtime.local_fencing_tokens

    @property
    def acquire_script(self) -> Any | None:
        return self._primary.acquire_script

    @acquire_script.setter
    def acquire_script(self, script: Any | None) -> None:
        self._primary.acquire_script = script

    @property
    def release_script(self) -> Any | None:
        return self._primary.release_script

    @release_script.setter
    def release_script(self, script: Any | None) -> None:
        self._primary.release_script = script

    @property
    def renew_script(self) -> Any | None:
        return self._primary.renew_script

    @renew_script.setter
    def renew_script(self, script: Any | None) -> None:
        self._primary.renew_script = script

    @property
    def redlock_lease_nodes(self) -> dict[str, set[int]]:
        return self._runtime.redlock_lease_nodes

    def lease_key(self, pipeline_id: str) -> str:
        return self._primary.lease_key(pipeline_id)

    def fencing_key(self, pipeline_id: str) -> str:
        return self._primary.fencing_key(pipeline_id)

    def current_lease(self, pipeline_id: str) -> LeaseState | None:
        return self._runtime.current_lease(pipeline_id)

    def clear_runtime_state(self) -> None:
        self._runtime.clear()

    def redlock_enabled(self) -> bool:
        return self._redlock.enabled()

    def redlock_quorum(self) -> int:
        return self._redlock.quorum()

    def redlock_validity_s(self, elapsed_s: float) -> float:
        return self._redlock.validity_s(elapsed_s)

    async def register_primary_scripts(self, redis: Any) -> None:
        await self._primary.register_scripts(redis)

    async def start_redlock_nodes(self, aioredis: Any) -> None:
        await self._redlock.start_nodes(aioredis)

    async def close_redlock_nodes(self) -> None:
        await self._redlock.close_nodes()

    async def try_acquire(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
        fallback_to_local: bool,
    ) -> bool:
        return await self._operations.try_acquire(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            run_number=run_number,
            fallback_to_local=fallback_to_local,
        )

    async def release(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
    ) -> None:
        await self._operations.release(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
        )

    async def validate(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
        fencing_token: int,
        fallback_to_local: bool,
        fallback_validate: Callable[[str, int], Awaitable[bool]],
    ) -> bool:
        return await self._operations.validate(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            fencing_token=fencing_token,
            fallback_to_local=fallback_to_local,
            fallback_validate=fallback_validate,
        )

    async def renew(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
        fallback_to_local: bool,
        mark_lease_lost: Callable[[str, LeaseState, str], Awaitable[None]],
    ) -> bool:
        return await self._operations.renew(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            fallback_to_local=fallback_to_local,
            mark_lease_lost=mark_lease_lost,
        )

    async def try_acquire_redlock(
        self,
        *,
        primary_redis: Any | None,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
    ) -> bool:
        return await self._redlock.try_acquire(
            primary_redis=primary_redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            run_number=run_number,
            lease_tokens=self._runtime.lease_tokens,
            lease_key=self.lease_key(pipeline_id),
            fencing_key=self.fencing_key(pipeline_id),
        )

    async def release_redlock_lease(
        self,
        *,
        pipeline_id: str,
        worker_id: str,
        acquired_at: str,
        fencing_token: int,
    ) -> None:
        await self._redlock.release_all_nodes(
            lease_key=self.lease_key(pipeline_id),
            pipeline_id=pipeline_id,
            worker_id=worker_id,
            acquired_at=acquired_at,
            fencing_token=fencing_token,
        )

    async def validate_redlock(
        self,
        *,
        worker_id: str,
        pipeline_id: str,
        fencing_token: int,
    ) -> bool:
        return await self._redlock.validate(
            lease_key=self.lease_key(pipeline_id),
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            fencing_token=fencing_token,
            lease_tokens=self._runtime.lease_tokens,
        )

    async def renew_redlock(
        self,
        *,
        worker_id: str,
        pipeline_id: str,
        mark_lease_lost: Callable[[str, LeaseState, str], Awaitable[None]],
    ) -> bool:
        return await self._redlock.renew(
            lease_key=self.lease_key(pipeline_id),
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            lease_tokens=self._runtime.lease_tokens,
            mark_lease_lost=mark_lease_lost,
        )

    def _acquire_local_fallback_lease(
        self,
        *,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
        fallback_to_local: bool,
    ) -> bool:
        return self._operations.acquire_local_fallback_lease(
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            run_number=run_number,
            fallback_to_local=fallback_to_local,
        )


__all__ = ["RedisLeaseManager"]
