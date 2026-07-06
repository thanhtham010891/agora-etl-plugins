from __future__ import annotations

from typing import TYPE_CHECKING, Any

import logstruct

if TYPE_CHECKING:
    from agora_plugins.distributed.lease_controller import RedisLeaseController
    from agora_plugins.distributed.lease_manager import RedisLeaseManager
    from agora_plugins.distributed.worker_session import RedisWorkerSession

logger = logstruct.getLogger(__name__)


class RedisCoordinatorLifecycle:
    """Public-facing lifecycle collaborator for Redis worker coordination."""

    def __init__(
        self,
        *,
        session: RedisWorkerSession,
        lease_manager: RedisLeaseManager,
        lease_controller: RedisLeaseController,
    ) -> None:
        self._session = session
        self._lease_manager = lease_manager
        self._lease_controller = lease_controller

    async def start(
        self,
        *,
        aioredis: Any,
        worker_id: str,
        pipeline_ids: list[str],
    ) -> None:
        await self._session.open(
            aioredis=aioredis,
            worker_id=worker_id,
            pipeline_ids=pipeline_ids,
        )
        if self._session.redis is None:
            return
        await self._lease_manager.register_primary_scripts(self._session.redis)
        await self._session.activate()
        self._lease_controller.start_renewal_loop(
            redis_provider=lambda: self._session.redis,
            worker_id_provider=lambda: self._session.worker_id,
        )

    async def stop(self) -> None:
        if self._session.redis is None:
            self._lease_manager.clear_runtime_state()
            return

        await self._lease_controller.stop_renewal_loop()
        for pipeline_id in self._lease_manager.runtime.active_pipeline_ids():
            try:
                await self._lease_controller.release(
                    redis=self._session.redis,
                    worker_id=self._session.worker_id,
                    pipeline_id=pipeline_id,
                )
            except Exception as exc:
                logger.warning(
                    "coordinator_release_error",
                    pipeline_id=pipeline_id,
                    error=str(exc),
                )
        await self._session.stop()
        self._lease_manager.clear_runtime_state()

    async def connect(self, *, aioredis: Any) -> None:
        await self._session.connect(aioredis=aioredis)

    async def close(self) -> None:
        await self._session.close()


__all__ = ["RedisCoordinatorLifecycle"]
