"""Redis-backed AI cache for Agora."""

from __future__ import annotations

from agora.ai.cache import _BackendLLMCache
from agora.core.constants import LLM_CACHE_DEFAULT_TTL_S, REDIS_DEFAULT_URL
from agora.state import TTLKeyValueStore

from agora_plugins.redis.state import RedisBackend


class RedisLLMCache(_BackendLLMCache):
    """Redis-backed distributed cache."""

    def __init__(
        self,
        url: str = REDIS_DEFAULT_URL,
        *,
        key_prefix: str = "agora:llm:",
    ) -> None:
        backend = RedisBackend(url=url, prefix=key_prefix)
        super().__init__(
            TTLKeyValueStore(
                backend=backend,
                namespace="llm",
                default_ttl_s=LLM_CACHE_DEFAULT_TTL_S,
            )
        )


__all__ = ["RedisLLMCache"]
