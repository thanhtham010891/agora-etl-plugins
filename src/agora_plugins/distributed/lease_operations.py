from __future__ import annotations

from typing import TYPE_CHECKING, Any

import logstruct

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.runner import LeaseState

    from agora_plugins.distributed.lease_manager import RedisLeaseManager

logger = logstruct.getLogger(__name__)


class RedisLeaseOperations:
    """Public-facing lease operation surface over primary and Redlock collaborators."""

    def __init__(self, lease_manager: RedisLeaseManager) -> None:
        self._lease_manager = lease_manager

    async def try_acquire(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
        fallback_to_local: bool,
    ) -> bool:
        if redis is None:
            return self.acquire_local_fallback_lease(
                worker_id=worker_id,
                pipeline_id=pipeline_id,
                run_number=run_number,
                fallback_to_local=fallback_to_local,
            )
        if self._lease_manager.redlock_enabled():
            return await self._lease_manager.try_acquire_redlock(
                primary_redis=redis,
                worker_id=worker_id,
                pipeline_id=pipeline_id,
                run_number=run_number,
            )
        return await self._lease_manager._primary.try_acquire(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            run_number=run_number,
        )

    async def release(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
    ) -> None:
        if redis is None:
            self._lease_manager.runtime.discard_lease(pipeline_id)
            return
        if self._lease_manager._primary.release_script is None:
            return
        lease = self._lease_manager.runtime.discard_lease(pipeline_id)
        if lease is None:
            logger.warning(
                "coordinator_release_without_local_lease",
                pipeline_id=pipeline_id,
            )
            return
        if self._lease_manager.redlock_enabled():
            await self._lease_manager.release_redlock_lease(
                pipeline_id=pipeline_id,
                worker_id=worker_id,
                acquired_at=lease.acquired_at,
                fencing_token=lease.fencing_token,
            )
            return
        await self._lease_manager._primary.release(
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            lease=lease,
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
        if redis is None:
            if not fallback_to_local:
                return False
            return await fallback_validate(pipeline_id, fencing_token)
        if self._lease_manager.redlock_enabled():
            return await self._lease_manager.validate_redlock(
                worker_id=worker_id,
                pipeline_id=pipeline_id,
                fencing_token=fencing_token,
            )
        return await self._lease_manager._primary.validate(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            fencing_token=fencing_token,
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
        if redis is None:
            return self._lease_manager._primary.renew_local_fallback_lease(
                pipeline_id,
                fallback_to_local=fallback_to_local,
            )
        if self._lease_manager.redlock_enabled():
            return await self._lease_manager.renew_redlock(
                worker_id=worker_id,
                pipeline_id=pipeline_id,
                mark_lease_lost=mark_lease_lost,
            )
        return await self._lease_manager._primary.renew(
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            mark_lease_lost=mark_lease_lost,
        )

    def acquire_local_fallback_lease(
        self,
        *,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
        fallback_to_local: bool,
    ) -> bool:
        return self._lease_manager._primary.acquire_local_fallback_lease(
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            run_number=run_number,
            fallback_to_local=fallback_to_local,
        )


__all__ = ["RedisLeaseOperations"]
