"""
agora_plugins.redis.dedup.stores.redis
==============================
Redis-backed exact dedup store built on the shared ``RedisBackend``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.middlewares.dedup.stores.backend import BackendDedupStore
from agora.state import MembershipKeyStore

from agora_plugins.redis.state.redis import RedisBackend

if TYPE_CHECKING:
    from agora_plugins.redis.connection import RedisClusterAddressRemap


class RedisStore(BackendDedupStore):
    """Redis-backed exact dedup store."""

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        key_prefix: str = "agora:dedup:",
        ttl_seconds: int | None = None,
        redis_cluster: bool = False,
        redis_cluster_address_remap: RedisClusterAddressRemap | None = None,
        sentinel_service_name: str | None = None,
        sentinel_urls: list[str] | None = None,
    ) -> None:
        backend = RedisBackend(
            url=url,
            prefix=key_prefix,
            redis_cluster=redis_cluster,
            redis_cluster_address_remap=redis_cluster_address_remap,
            sentinel_service_name=sentinel_service_name,
            sentinel_urls=sentinel_urls,
        )
        super().__init__(
            MembershipKeyStore(backend=backend, namespace=""),
            default_ttl_seconds=ttl_seconds,
            offload_blocking_calls=True,
        )


__all__ = ["RedisStore"]
