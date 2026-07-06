from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, cast

import logstruct

from agora_plugins.distributed._shared import redact_url

if TYPE_CHECKING:
    from agora.runner import WorkerInfo

    from agora_plugins.distributed.lease_manager import RedisLeaseManager
    from agora_plugins.distributed.worker_registry import RedisWorkerRegistry

logger = logstruct.getLogger(__name__)


class RedisWorkerSession:
    """Public worker-session collaborator for Redis-backed coordination."""

    def __init__(
        self,
        *,
        redis_url: str,
        redlock_redis_urls: list[str],
        heartbeat_interval: int,
        fallback_to_local: bool,
        worker_registry: RedisWorkerRegistry,
        lease_manager: RedisLeaseManager,
    ) -> None:
        self._redis_url = redis_url
        self._redlock_redis_urls = list(redlock_redis_urls)
        self._heartbeat_interval = heartbeat_interval
        self._fallback_to_local = fallback_to_local
        self._worker_registry = worker_registry
        self._lease_manager = lease_manager
        self._worker_id: str = ""
        self._pipeline_ids: list[str] = []
        self._redis: Any | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    @property
    def redis(self) -> Any | None:
        return self._redis

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def open(self, *, aioredis: Any, worker_id: str, pipeline_ids: list[str]) -> None:
        self._worker_id = worker_id
        self._pipeline_ids = list(pipeline_ids)

        try:
            self._redis = cast("Any", aioredis.from_url(self._redis_url, decode_responses=True))
            await self._redis.ping()
            await self._lease_manager.start_redlock_nodes(aioredis)
        except Exception as exc:
            await self._lease_manager.close_redlock_nodes()
            if self._redis is not None:
                with contextlib.suppress(Exception):
                    await self._redis.aclose()
                self._redis = None
            if self._fallback_to_local:
                logger.warning(
                    "coordinator_redis_unavailable_fallback",
                    error=str(exc),
                    url=redact_url(self._redis_url),
                )
                self._redis = None
                return
            redlock_urls = [redact_url(url) for url in self._redlock_redis_urls]
            raise RuntimeError(
                "agora-etl-plugins distributed coordinator cannot connect to Redis "
                f"primary={redact_url(self._redis_url)!r} "
                f"redlock_nodes={redlock_urls!r}: {exc}"
            ) from exc

    async def activate(self) -> None:
        await self.register("running")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"agora-heartbeat-{self._worker_id}",
        )
        logger.info("coordinator_started", worker_id=self._worker_id)

    async def stop(self) -> None:
        if self._redis is None:
            return

        await self.register("draining")
        await self._stop_heartbeat()
        with contextlib.suppress(Exception):
            await self._redis.delete(self.worker_key(self._worker_id))
        await self._redis.aclose()
        await self._lease_manager.close_redlock_nodes()
        self._redis = None
        logger.info("coordinator_stopped", worker_id=self._worker_id)

    async def connect(self, *, aioredis: Any) -> None:
        if self._redis is not None:
            return
        self._redis = cast("Any", aioredis.from_url(self._redis_url, decode_responses=True))
        await self._redis.ping()

    async def close(self) -> None:
        await self._stop_heartbeat()
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
        await self._lease_manager.close_redlock_nodes()

    async def register(self, status: str) -> None:
        await self._worker_registry.register(
            redis=self._redis,
            lock=self._lock,
            worker_id=self._worker_id,
            pipeline_ids=self._pipeline_ids,
            status=status,
        )

    async def list_workers(self) -> list[WorkerInfo]:
        return await self._worker_registry.list_workers(self._redis)

    def worker_key(self, worker_id: str) -> str:
        return self._worker_registry.worker_key(worker_id)

    async def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is None or self._heartbeat_task.done():
            return
        self._heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(asyncio.shield(self._heartbeat_task), timeout=5)

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                await self.register("running")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("coordinator_heartbeat_error", error=str(exc))


__all__ = ["RedisWorkerSession"]
