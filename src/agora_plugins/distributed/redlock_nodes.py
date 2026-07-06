from __future__ import annotations

import contextlib
from typing import Any

from agora_plugins.distributed.redlock_scripts import (
    REDLOCK_ACQUIRE_SCRIPT,
    REDLOCK_RELEASE_SCRIPT,
    REDLOCK_RENEW_SCRIPT,
)


class RedisRedlockNodeSet:
    """Owns Redlock Redis node lifecycle and registered scripts."""

    def __init__(self) -> None:
        self.redis_nodes: list[Any] = []
        self.acquire_scripts: list[Any] = []
        self.release_scripts: list[Any] = []
        self.renew_scripts: list[Any] = []

    async def start(self, aioredis: Any, redis_urls: list[str]) -> None:
        for url in redis_urls:
            node = aioredis.from_url(url, decode_responses=True)
            await node.ping()
            self.redis_nodes.append(node)
            self.acquire_scripts.append(node.register_script(REDLOCK_ACQUIRE_SCRIPT))
            self.release_scripts.append(node.register_script(REDLOCK_RELEASE_SCRIPT))
            self.renew_scripts.append(node.register_script(REDLOCK_RENEW_SCRIPT))

    async def close(self) -> None:
        for node in self.redis_nodes:
            with contextlib.suppress(Exception):
                await node.aclose()
        self.clear()

    def clear(self) -> None:
        self.redis_nodes.clear()
        self.acquire_scripts.clear()
        self.release_scripts.clear()
        self.renew_scripts.clear()


__all__ = ["RedisRedlockNodeSet"]
