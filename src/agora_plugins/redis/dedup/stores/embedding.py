"""Redis-backed semantic dedup store for Agora."""

from __future__ import annotations

import asyncio
import warnings
from typing import TYPE_CHECKING, Literal

from agora.ai.providers.base import require_embedding_provider
from agora.middlewares.dedup.stores.base import DedupStore

from agora_plugins.redis.connection import RedisClusterAddressRemap, build_async_redis_client
from agora_plugins.redis.dedup.stores._embedding_runtime import (
    _DEFAULT_MAX_ENTRIES,
    RedisEmbeddingRuntime,
)

if TYPE_CHECKING:
    from agora.ai.providers.base import EmbeddingProvider
    from redis.asyncio import Redis as AsyncRedis


class RedisEmbeddingStore(RedisEmbeddingRuntime, DedupStore[str]):
    """Semantic dedup store backed by Redis.

    Similarity search is O(N) over all stored embeddings — suitable for up to
    ~10k entries. For larger datasets, use a dedicated vector database instead.
    """

    def __init__(
        self,
        provider: EmbeddingProvider,
        *,
        similarity_threshold: float = 0.92,
        redis_url: str = "redis://localhost:6379",
        redis_key_prefix: str = "agora:dedup:emb:",
        max_entries: int = _DEFAULT_MAX_ENTRIES,
        lock_timeout_s: float = 10.0,
        lock_blocking_timeout_s: float = 5.0,
        redis_cluster: bool = False,
        redis_cluster_address_remap: RedisClusterAddressRemap | None = None,
        sentinel_service_name: str | None = None,
        sentinel_urls: list[str] | None = None,
        use_redisearch: bool = False,
        backend_mode: Literal["scan", "redisearch"] | None = None,
        redisearch_index_name: str | None = None,
    ) -> None:
        self._provider = require_embedding_provider(provider, consumer="RedisEmbeddingStore")
        self._threshold = similarity_threshold
        self._redis_url = redis_url
        self._redis_prefix = redis_key_prefix
        self._max_entries = max_entries
        self._lock_timeout_s = lock_timeout_s
        self._lock_blocking_timeout_s = lock_blocking_timeout_s
        self._redis_cluster = redis_cluster
        self._redis_cluster_address_remap = redis_cluster_address_remap
        self._sentinel_service_name = sentinel_service_name
        self._sentinel_urls = list(sentinel_urls or [])
        self._backend_mode = self._resolve_backend_mode(
            backend_mode=backend_mode,
            use_redisearch=use_redisearch,
        )
        self._use_redisearch = self._backend_mode == "redisearch"
        if self._backend_mode == "scan" and max_entries > _DEFAULT_MAX_ENTRIES:
            warnings.warn(
                "RedisEmbeddingStore backend_mode='scan' performs O(N) similarity scans. "
                "Use backend_mode='redisearch' for larger enterprise datasets.",
                RuntimeWarning,
                stacklevel=2,
            )
        self._redisearch_index_name = redisearch_index_name or f"{redis_key_prefix}idx"
        self._redisearch_dimensions: int | None = None
        self._redis: AsyncRedis | None = None
        self._mark_lock = asyncio.Lock()

    @staticmethod
    def _resolve_backend_mode(
        *,
        backend_mode: Literal["scan", "redisearch"] | None,
        use_redisearch: bool,
    ) -> Literal["scan", "redisearch"]:
        if backend_mode is None:
            return "redisearch" if use_redisearch else "scan"
        if backend_mode not in {"scan", "redisearch"}:
            raise ValueError("backend_mode must be 'scan' or 'redisearch'.")
        if use_redisearch and backend_mode != "redisearch":
            raise ValueError("use_redisearch=True conflicts with backend_mode='scan'.")
        return backend_mode

    async def open(self) -> None:
        await self._ensure_redis()

    async def exists(self, key: str) -> bool:
        embedding = (await self._provider.embed(key)).embedding
        return await self._redis_exists(query_embedding=embedding)

    async def add(self, key: str) -> None:
        embedding = (await self._provider.embed(key)).embedding
        await self._redis_add(key, embedding)

    async def mark_if_new(self, key: str, *, ttl_seconds: int | None = None) -> bool:
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero when provided.")
        embedding = (await self._provider.embed(key)).embedding
        redis = await self._ensure_redis()
        lock_key = f"{self._redis_prefix}__lock__"
        # Cross-process lock: the check-then-add must be atomic across workers,
        # not just within this event loop. _redis_add is itself a MULTI, but the
        # similarity scan in _redis_exists happens before it, so two workers can
        # otherwise both pass the check and insert near-duplicates.
        lock = redis.lock(
            lock_key,
            timeout=self._lock_timeout_s,
            blocking_timeout=self._lock_blocking_timeout_s,
            raise_on_release_error=False,
        )
        async with self._mark_lock, lock, self._renew_mark_lock(lock) as ensure_lock_healthy:
            if await self._redis_exists(
                query_embedding=embedding,
                on_scan_step=ensure_lock_healthy,
            ):
                ensure_lock_healthy()
                return False
            ensure_lock_healthy()
            await self._redis_add(key, embedding, ttl_seconds=ttl_seconds)
            ensure_lock_healthy()
            return True

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def _ensure_redis(self) -> AsyncRedis:
        if self._redis is None:
            try:
                __import__("redis.asyncio")
            except ImportError as exc:
                raise ImportError(
                    "RedisEmbeddingStore requires 'redis'. "
                    "Install with: pip install 'agora-etl-plugins[redis]'"
                ) from exc
            self._redis = await build_async_redis_client(
                url=self._redis_url,
                decode_responses=False,
                redis_cluster=self._redis_cluster,
                redis_cluster_address_remap=self._redis_cluster_address_remap,
                sentinel_service_name=self._sentinel_service_name,
                sentinel_urls=self._sentinel_urls,
            )
        return self._redis


__all__ = ["RedisEmbeddingStore"]
