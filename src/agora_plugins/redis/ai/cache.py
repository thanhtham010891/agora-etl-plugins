"""Redis-backed AI cache for Agora."""

from __future__ import annotations

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


__all__ = ["RedisLLMCache"]
