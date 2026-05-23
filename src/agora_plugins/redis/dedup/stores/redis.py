"""
agora_plugins.redis.dedup.stores.redis
==============================
Redis-backed exact dedup store built on the shared ``RedisBackend``.
"""

from __future__ import annotations

from agora.middlewares.dedup.stores.backend import BackendDedupStore
from agora.state import MembershipKeyStore

from agora_plugins.redis.state.redis import RedisBackend


class RedisStore(BackendDedupStore):
    """Redis-backed exact dedup store."""

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        key_prefix: str = "agora:dedup:",
        ttl_seconds: int | None = None,
    ) -> None:
        backend = RedisBackend(url=url, prefix=key_prefix)
        super().__init__(
            MembershipKeyStore(backend=backend, namespace=""),
            default_ttl_seconds=ttl_seconds,
            offload_blocking_calls=True,
        )


__all__ = ["RedisStore"]
