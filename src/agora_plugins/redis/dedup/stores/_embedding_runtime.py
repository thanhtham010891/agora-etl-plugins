"""Redis embedding store scan, lock, and RediSearch runtime helpers."""

from __future__ import annotations

import asyncio
import struct
from contextlib import asynccontextmanager, suppress
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

import logstruct
from agora.utils.math import cosine_similarity as _cosine_similarity

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

logger = logstruct.getLogger(__name__)

_FLOAT_FORMAT = "f"  # 32-bit float; consistent precision for all stored embeddings
_DEFAULT_MAX_ENTRIES = 10_000
_SCAN_COUNT = 256
_INDEX_KEY_SUFFIX = "__index__"
_RESERVED_KEYS = frozenset({_INDEX_KEY_SUFFIX, "__lock__"})
_LOCK_RENEWAL_MIN_INTERVAL_S = 0.05
_LOCK_RENEWAL_MAX_INTERVAL_S = 1.0


class RedisEmbeddingRuntime:
    """Owns Redis mutation, scan, lock-renewal, and RediSearch runtime paths."""

    async def _redis_add(
        self,
        key: str,
        embedding: list[float],
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        if key in _RESERVED_KEYS:
            raise ValueError(
                f"RedisEmbeddingStore key '{key}' is reserved for internal bookkeeping."
            )
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero when provided.")
        redis = await self._ensure_redis()
        packed = struct.pack(f"{len(embedding)}{_FLOAT_FORMAT}", *embedding)
        field_key = f"{self._redis_prefix}{key}"
        if self._use_redisearch:
            await self._ensure_redisearch_index(len(embedding))
            hset = cast("Any", redis.hset)
            result = hset(field_key, mapping={"key": key, "vector": packed})
            if isawaitable(result):
                await result
            if ttl_seconds is not None:
                expire = cast("Any", redis.expire)
                expire_result = expire(field_key, ttl_seconds)
                if isawaitable(expire_result):
                    await expire_result
            return

        index_key = f"{self._redis_prefix}{_INDEX_KEY_SUFFIX}"
        await self._redis_prune_stale_index_entries(redis, index_key)
        scard = cast("Any", redis.scard)
        current_size = int(await scard(index_key))
        if current_size >= self._max_entries:
            raise RuntimeError(
                f"RedisEmbeddingStore has reached max_entries={self._max_entries}. "
                "Use a dedicated vector database for larger datasets."
            )
        # Atomic pipeline: store embedding and add to index together.
        async with redis.pipeline(transaction=not self._redis_cluster) as pipe:
            if ttl_seconds is None:
                pipe.set(field_key, packed)
            else:
                pipe.set(field_key, packed, ex=ttl_seconds)
            pipe.sadd(index_key, key)
            await pipe.execute()

    async def _redis_exists(
        self,
        query_embedding: list[float],
        *,
        on_scan_step: Callable[[], None] | None = None,
    ) -> bool:
        if self._use_redisearch:
            return await self._redisearch_exists(query_embedding)

        redis = await self._ensure_redis()
        index_key = f"{self._redis_prefix}{_INDEX_KEY_SUFFIX}"
        cursor = 0
        while True:
            if on_scan_step is not None:
                on_scan_step()
            cursor, batch_keys = await redis.sscan(index_key, cursor=cursor, count=_SCAN_COUNT)
            if batch_keys:
                stored_keys = [k.decode() if isinstance(k, bytes) else k for k in batch_keys]
                field_keys = [f"{self._redis_prefix}{k}" for k in stored_keys]

                async with redis.pipeline(transaction=False) as pipe:
                    for field_key in field_keys:
                        pipe.get(field_key)
                    packed_values = await pipe.execute()
                if on_scan_step is not None:
                    on_scan_step()

                for stored_key, packed in zip(stored_keys, packed_values, strict=True):
                    if packed is None:
                        await self._redis_remove_index_members(redis, index_key, [stored_key])
                        continue
                    if len(packed) % 4 != 0:
                        logger.warning(
                            "embedding_dedup_malformed_vector",
                            matched_key=stored_key,
                            byte_count=len(packed),
                        )
                        await self._redis_remove_index_members(redis, index_key, [stored_key])
                        continue
                    dimensions = len(packed) // 4
                    if dimensions != len(query_embedding):
                        logger.warning(
                            "embedding_dedup_dimension_mismatch",
                            matched_key=stored_key,
                            stored_dimensions=dimensions,
                            query_dimensions=len(query_embedding),
                        )
                        await self._redis_remove_index_members(redis, index_key, [stored_key])
                        continue
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

    @asynccontextmanager
    async def _renew_mark_lock(self, lock: Any) -> AsyncIterator[Callable[[], None]]:
        renewal_error: Exception | None = None
        interval_s = self._mark_lock_renewal_interval_s()

        def _ensure_lock_healthy() -> None:
            if renewal_error is not None:
                raise RuntimeError(
                    "RedisEmbeddingStore lost the distributed mark lock during mark_if_new()."
                ) from renewal_error

        async def _renew_loop() -> None:
            nonlocal renewal_error
            try:
                while True:
                    await asyncio.sleep(interval_s)
                    renewed = await lock.reacquire()
                    if renewed is False:
                        raise RuntimeError(
                            "RedisEmbeddingStore could not renew the distributed mark lock."
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                renewal_error = exc

        task = asyncio.create_task(
            _renew_loop(),
            name="agora-redis-embedding-lock-renewal",
        )
        try:
            yield _ensure_lock_healthy
            _ensure_lock_healthy()
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            _ensure_lock_healthy()

    def _mark_lock_renewal_interval_s(self) -> float:
        return max(
            min(self._lock_timeout_s / 3.0, _LOCK_RENEWAL_MAX_INTERVAL_S),
            _LOCK_RENEWAL_MIN_INTERVAL_S,
        )

    async def _redis_prune_stale_index_entries(self, redis: Any, index_key: str) -> None:
        cursor = 0
        while True:
            cursor, batch_keys = await redis.sscan(index_key, cursor=cursor, count=_SCAN_COUNT)
            if batch_keys:
                stored_keys = [k.decode() if isinstance(k, bytes) else k for k in batch_keys]
                field_keys = [f"{self._redis_prefix}{k}" for k in stored_keys]
                async with redis.pipeline(transaction=False) as pipe:
                    for field_key in field_keys:
                        pipe.get(field_key)
                    packed_values = await pipe.execute()
                stale_keys = [
                    stored_key
                    for stored_key, packed in zip(stored_keys, packed_values, strict=True)
                    if packed is None or len(packed) % 4 != 0
                ]
                await self._redis_remove_index_members(redis, index_key, stale_keys)
            if cursor == 0:
                break

    async def _redis_remove_index_members(
        self,
        redis: Any,
        index_key: str,
        members: list[str],
    ) -> None:
        if not members:
            return
        srem = cast("Any", redis.srem)
        result = srem(index_key, *members)
        if isawaitable(result):
            await result

    async def _ensure_redisearch_index(self, dimensions: int) -> None:
        if self._redisearch_dimensions == dimensions:
            return
        redis = await self._ensure_redis()
        execute_command = cast("Any", redis.execute_command)
        try:
            await execute_command(
                "FT.CREATE",
                self._redisearch_index_name,
                "ON",
                "HASH",
                "PREFIX",
                "1",
                self._redis_prefix,
                "SCHEMA",
                "key",
                "TEXT",
                "vector",
                "VECTOR",
                "HNSW",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                dimensions,
                "DISTANCE_METRIC",
                "COSINE",
            )
        except Exception as exc:
            if "Index already exists" in str(exc) or "already exists" in str(exc):
                await self._validate_existing_redisearch_index(dimensions)
            else:
                raise RuntimeError(
                    "RedisEmbeddingStore use_redisearch=True requires RediSearch "
                    "with vector index support."
                ) from exc
        self._redisearch_dimensions = dimensions

    async def _validate_existing_redisearch_index(self, dimensions: int) -> None:
        redis = await self._ensure_redis()
        execute_command = cast("Any", redis.execute_command)
        try:
            response = await execute_command("FT.INFO", self._redisearch_index_name)
        except Exception as exc:
            raise RuntimeError(
                "RedisEmbeddingStore could not validate existing RediSearch index "
                f"'{self._redisearch_index_name}'."
            ) from exc
        actual_dimensions = _redisearch_index_vector_dimensions(response)
        if actual_dimensions is not None and actual_dimensions != dimensions:
            raise ValueError(
                "RedisEmbeddingStore RediSearch index dimension mismatch: "
                f"index '{self._redisearch_index_name}' has DIM {actual_dimensions}, "
                f"but provider returned DIM {dimensions}."
            )

    async def _redisearch_exists(self, query_embedding: list[float]) -> bool:
        redis = await self._ensure_redis()
        packed = struct.pack(f"{len(query_embedding)}{_FLOAT_FORMAT}", *query_embedding)
        await self._ensure_redisearch_index(len(query_embedding))
        execute_command = cast("Any", redis.execute_command)
        response = await execute_command(
            "FT.SEARCH",
            self._redisearch_index_name,
            "*=>[KNN 1 @vector $query_vector AS vector_score]",
            "PARAMS",
            "2",
            "query_vector",
            packed,
            "RETURN",
            "1",
            "vector_score",
            "SORTBY",
            "vector_score",
            "DIALECT",
            "2",
        )
        distance = _redisearch_first_distance(response)
        return distance is not None and distance <= (1.0 - self._threshold)


def _redisearch_first_distance(response: Any) -> float | None:
    if not isinstance(response, list) or not response or int(response[0] or 0) < 1:
        return None
    fields = response[2] if len(response) > 2 else []
    if not isinstance(fields, list):
        return None
    for index, item in enumerate(fields):
        label = item.decode("utf-8") if isinstance(item, bytes) else item
        if label == "vector_score" and index + 1 < len(fields):
            raw_value = fields[index + 1]
            value = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _redisearch_index_vector_dimensions(response: Any) -> int | None:
    if not isinstance(response, list):
        return None
    for index, item in enumerate(response):
        label = item.decode("utf-8") if isinstance(item, bytes) else item
        if label != "attributes" or index + 1 >= len(response):
            continue
        attributes = response[index + 1]
        if not isinstance(attributes, list):
            return None
        for attribute in attributes:
            if not isinstance(attribute, list):
                continue
            name = _redisearch_attribute_value(attribute, "identifier")
            if name != "vector":
                continue
            dimensions = _redisearch_attribute_value(attribute, "dim")
            if dimensions is None:
                dimensions = _redisearch_attribute_value(attribute, "DIM")
            try:
                return int(dimensions) if dimensions is not None else None
            except (TypeError, ValueError):
                return None
    return None


def _redisearch_attribute_value(attribute: list[Any], key: str) -> Any | None:
    for index, item in enumerate(attribute):
        label = item.decode("utf-8") if isinstance(item, bytes) else item
        if label == key and index + 1 < len(attribute):
            value = attribute[index + 1]
            return value.decode("utf-8") if isinstance(value, bytes) else value
    return None
