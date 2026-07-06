from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

import logstruct

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.runner import LeaseState

    from agora_plugins.distributed.lease_manager import RedisLeaseManager

logger = logstruct.getLogger(__name__)


class RedisLeaseController:
    """Public lease orchestration collaborator for Redis worker coordination."""

    def __init__(
        self,
        *,
        lease_manager: RedisLeaseManager,
        heartbeat_interval: int,
        lease_ttl: int,
        lease_renewal_deadline_seconds: float | None,
        fallback_to_local: bool,
    ) -> None:
        self._lease_manager = lease_manager
        self._heartbeat_interval = heartbeat_interval
        self._lease_ttl = lease_ttl
        self._lease_renewal_deadline_seconds = lease_renewal_deadline_seconds
        self._fallback_to_local = fallback_to_local
        self._lease_renew_task: asyncio.Task[None] | None = None
        self._lease_lost_callback: Any | None = None

    def set_lease_lost_callback(self, callback: Any | None) -> None:
        self._lease_lost_callback = callback

    async def try_acquire(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
    ) -> bool:
        return await self._lease_manager.try_acquire(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            run_number=run_number,
            fallback_to_local=self._fallback_to_local,
        )

    async def release(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
    ) -> None:
        await self._lease_manager.release(
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
        fallback_validate: Callable[[str, int], Awaitable[bool]],
    ) -> bool:
        return await self._lease_manager.validate(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            fencing_token=fencing_token,
            fallback_to_local=self._fallback_to_local,
            fallback_validate=fallback_validate,
        )

    async def renew(
        self,
        *,
        redis: Any | None,
        worker_id: str,
        pipeline_id: str,
    ) -> bool:
        return await self._lease_manager.renew(
            redis=redis,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            fallback_to_local=self._fallback_to_local,
            mark_lease_lost=self.mark_lease_lost_from_manager,
        )

    def start_renewal_loop(
        self,
        *,
        redis_provider: Callable[[], Any | None],
        worker_id_provider: Callable[[], str],
    ) -> None:
        if self._lease_renew_task is not None and not self._lease_renew_task.done():
            return
        self._lease_renew_task = asyncio.create_task(
            self._lease_renew_loop(
                redis_provider=redis_provider,
                worker_id_provider=worker_id_provider,
            ),
            name=f"agora-lease-renew-{worker_id_provider()}",
        )

    async def stop_renewal_loop(self) -> None:
        if self._lease_renew_task is None or self._lease_renew_task.done():
            return
        self._lease_renew_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(asyncio.shield(self._lease_renew_task), timeout=5)

    async def renew_leases_once(
        self,
        *,
        redis: Any | None,
        worker_id: str,
    ) -> None:
        started = time.monotonic()
        try:
            async with asyncio.timeout(self.renewal_cycle_deadline_s()):
                for pipeline_id in self._lease_manager.runtime.active_pipeline_ids():
                    await self.renew(
                        redis=redis,
                        worker_id=worker_id,
                        pipeline_id=pipeline_id,
                    )
        except TimeoutError:
            logger.warning(
                "coordinator_lease_renew_cycle_deadline_exceeded",
                lease_count=self._lease_manager.runtime.lease_count(),
                deadline_s=self.renewal_cycle_deadline_s(),
            )
            await self.mark_all_leases_lost(reason="renew_cycle_deadline_exceeded")
            return
        logger.debug(
            "coordinator_lease_renewed",
            lease_count=self._lease_manager.runtime.lease_count(),
            duration_ms=round((time.monotonic() - started) * 1000, 3),
        )

    def renewal_cycle_deadline_s(self) -> float:
        if self._lease_renewal_deadline_seconds is not None:
            return self._lease_renewal_deadline_seconds
        return max(0.1, self._lease_ttl * 0.5)

    async def mark_all_leases_lost(self, *, reason: str) -> None:
        for pipeline_id, lease in self._lease_manager.runtime.active_leases():
            await self.mark_lease_lost(pipeline_id, lease, reason=reason)

    async def mark_lease_lost_from_manager(
        self,
        pipeline_id: str,
        lease: LeaseState,
        reason: str,
    ) -> None:
        await self.mark_lease_lost(pipeline_id, lease, reason=reason)

    async def mark_lease_lost(
        self,
        pipeline_id: str,
        lease: LeaseState,
        *,
        reason: str,
    ) -> None:
        self._lease_manager.runtime.discard_lease(pipeline_id)
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

    async def _lease_renew_loop(
        self,
        *,
        redis_provider: Callable[[], Any | None],
        worker_id_provider: Callable[[], str],
    ) -> None:
        interval = max(1.0, min(self._heartbeat_interval, self._lease_ttl / 3))
        while True:
            try:
                await asyncio.sleep(interval)
                await self.renew_leases_once(
                    redis=redis_provider(),
                    worker_id=worker_id_provider(),
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("coordinator_lease_renew_loop_error", error=str(exc))


__all__ = ["RedisLeaseController"]
