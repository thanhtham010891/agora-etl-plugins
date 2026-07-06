from __future__ import annotations

import json
import os
import socket
from typing import TYPE_CHECKING, Any

from agora_plugins.distributed._shared import worker_info_from_raw

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable

    from agora.runner import WorkerInfo


class RedisWorkerRegistry:
    def __init__(
        self,
        *,
        key_prefix: str,
        heartbeat_interval: int,
        fetch_batch_size: int,
        scan_count: int,
        now_fn: Callable[[], str],
    ) -> None:
        self._key_prefix = key_prefix
        self._heartbeat_interval = heartbeat_interval
        self._fetch_batch_size = fetch_batch_size
        self._scan_count = scan_count
        self._now = now_fn

    def worker_key(self, worker_id: str) -> str:
        return f"{self._key_prefix}workers:{worker_id}"

    async def register(
        self,
        *,
        redis: Any | None,
        lock: asyncio.Lock,
        worker_id: str,
        pipeline_ids: list[str],
        status: str,
    ) -> None:
        async with lock:
            if redis is None:
                return
            ttl = self._heartbeat_interval * 3
            value = json.dumps(
                {
                    "worker_id": worker_id,
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                    "status": status,
                    "assigned_pipelines": pipeline_ids,
                    "last_heartbeat_at": self._now(),
                }
            )
            await redis.set(self.worker_key(worker_id), value, ex=ttl)

    async def list_workers(self, redis: Any | None) -> list[WorkerInfo]:
        if redis is None:
            return []

        pattern = f"{self._key_prefix}workers:*"
        seen_keys: set[str] = set()
        pending_keys: list[str] = []
        workers: list[WorkerInfo] = []
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor, match=pattern, count=self._scan_count)
            for key in keys:
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                pending_keys.append(key)
                if len(pending_keys) >= self._fetch_batch_size:
                    workers.extend(await self._fetch_worker_batch(redis, pending_keys))
                    pending_keys.clear()
            if cursor == 0:
                break

        if pending_keys:
            workers.extend(await self._fetch_worker_batch(redis, pending_keys))
        if not workers and not seen_keys:
            return []
        return workers

    async def _fetch_worker_batch(self, redis: Any, keys: list[str]) -> list[WorkerInfo]:
        if not keys:
            return []
        raw_values = await redis.mget(*keys)
        workers: list[WorkerInfo] = []
        for raw in raw_values:
            worker = worker_info_from_raw(raw)
            if worker is not None:
                workers.append(worker)
        return workers
