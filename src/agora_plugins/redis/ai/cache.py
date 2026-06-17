"""Redis-backed AI cache for Agora."""

from __future__ import annotations

import asyncio

from agora.ai.cache import LLM_CACHE_DEFAULT_TTL_S, StateBackendLLMCache

from agora_plugins.redis.state import RedisBackend

_DEFAULT_REDIS_URL = "redis://localhost:6379"


class RedisLLMCache(StateBackendLLMCache):
    """Redis-backed distributed cache."""

    def __init__(
        self,
        url: str = _DEFAULT_REDIS_URL,
        *,
        key_prefix: str = "agora:llm:",
        default_ttl_s: int = LLM_CACHE_DEFAULT_TTL_S,
    ) -> None:
        super().__init__(
            RedisBackend(url=url, prefix=key_prefix),
            namespace="llm",
            default_ttl_s=default_ttl_s,
        )

    async def get(self, key: str) -> str | None:
        value = await asyncio.to_thread(self._store.get, key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"LLM cache entry for {key!r} must be a string, got {type(value)!r}")
        return value

    async def set(self, key: str, value: str, ttl: int | None = None) -> None:
        await asyncio.to_thread(self._store.set, key, value, ttl_s=ttl)

    async def close(self) -> None:
        await asyncio.to_thread(self._store.close)


__all__ = ["RedisLLMCache"]
