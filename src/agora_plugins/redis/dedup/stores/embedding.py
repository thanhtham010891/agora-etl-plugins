"""Redis-backed semantic dedup store for Agora."""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

import logstruct
from agora.middlewares.dedup.stores.base import DedupStore
from agora.utils.math import cosine_similarity as _cosine_similarity

if TYPE_CHECKING:
    from agora.ai.providers.base import AIProvider
    from redis.asyncio import Redis as AsyncRedis

logger = logstruct.getLogger(__name__)

_FLOAT_FORMAT = "f"  # 32-bit float — consistent precision for all stored embeddings


_DEFAULT_MAX_ENTRIES = 10_000
_SCAN_COUNT = 256


class RedisEmbeddingStore(DedupStore[str]):
    """Semantic dedup store backed by Redis.

    Similarity search is O(N) over all stored embeddings — suitable for up to
    ~10k entries. For larger datasets, use a dedicated vector database instead.
    """

    def __init__(
        self,
        provider: AIProvider,
        *,
        similarity_threshold: float = 0.92,
        redis_url: str = "redis://localhost:6379",
        redis_key_prefix: str = "agora:dedup:emb:",
        max_entries: int = _DEFAULT_MAX_ENTRIES,
    ) -> None:
        self._provider = provider
        self._threshold = similarity_threshold
        self._redis_url = redis_url
        self._redis_prefix = redis_key_prefix
        self._max_entries = max_entries
        self._redis: AsyncRedis | None = None

    async def open(self) -> None:
        await self._ensure_redis()

    async def exists(self, key: str) -> bool:
        embedding = (await self._provider.embed(key)).embedding
        return await self._redis_exists(query_embedding=embedding)

    async def add(self, key: str) -> None:
        redis = await self._ensure_redis()
        index_key = f"{self._redis_prefix}__index__"
        current_size = await redis.scard(index_key)
        if current_size >= self._max_entries:
            raise RuntimeError(
                f"RedisEmbeddingStore has reached max_entries={self._max_entries}. "
                "Use a dedicated vector database for larger datasets."
            )
        embedding = (await self._provider.embed(key)).embedding
        await self._redis_add(key, embedding)

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def _ensure_redis(self) -> AsyncRedis:
        if self._redis is None:
            try:
                import redis.asyncio as aioredis_mod
            except ImportError as exc:
                raise ImportError(
                    "RedisEmbeddingStore requires 'redis'. "
                    "Install with: pip install 'agora-etl-plugins[redis]'"
                ) from exc
            self._redis = aioredis_mod.from_url(self._redis_url, decode_responses=False)
        return self._redis

    async def _redis_add(self, key: str, embedding: list[float]) -> None:
        redis = await self._ensure_redis()
        packed = struct.pack(f"{len(embedding)}{_FLOAT_FORMAT}", *embedding)
        field_key = f"{self._redis_prefix}{key}"
        index_key = f"{self._redis_prefix}__index__"
        # Atomic pipeline: store embedding and add to index together
        async with redis.pipeline(transaction=False) as pipe:
            pipe.set(field_key, packed)
            pipe.sadd(index_key, key)
            await pipe.execute()

    async def _redis_exists(self, query_embedding: list[float]) -> bool:
        redis = await self._ensure_redis()
        index_key = f"{self._redis_prefix}__index__"
        cursor = 0
        while True:
            cursor, batch_keys = await redis.sscan(index_key, cursor=cursor, count=_SCAN_COUNT)
            if batch_keys:
                stored_keys = [k.decode() if isinstance(k, bytes) else k for k in batch_keys]
                field_keys = [f"{self._redis_prefix}{k}" for k in stored_keys]

                async with redis.pipeline(transaction=False) as pipe:
                    for fk in field_keys:
                        pipe.get(fk)
                    packed_values = await pipe.execute()

                for stored_key, packed in zip(stored_keys, packed_values, strict=True):
                    if packed is None:
                        continue
                    dimensions = len(packed) // 4
                    stored_embedding = list(struct.unpack(f"{dimensions}{_FLOAT_FORMAT}", packed))
                    similarity = _cosine_similarity(query_embedding, stored_embedding)
                    if similarity >= self._threshold:
                        logger.debug(
                            "embedding_dedup_hit",
                            similarity=round(similarity, 4),
                            matched_key=stored_key,
                        )
                        return True
            if cursor == 0:
                break
        return False


__all__ = ["RedisEmbeddingStore"]
