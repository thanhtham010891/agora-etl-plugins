from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace

import pytest
from agora.core.dlq import DLQRecord

from agora_plugins.redis import RedisBackend, RedisDLQSink, RedisEmbeddingStore, RedisSink

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 20.0
_REDIS_CLUSTER_REMAP = {
    ("redis-cluster-1", 6379): ("127.0.0.1", 16385),
    ("redis-cluster-2", 6379): ("127.0.0.1", 16386),
    ("redis-cluster-3", 6379): ("127.0.0.1", 16387),
}


def _redis_cluster_address_remap(address: tuple[str, int]) -> tuple[str, int]:
    return _REDIS_CLUSTER_REMAP.get(address, address)


def _delete_matching_keys(url: str, pattern: str) -> None:
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(url, decode_responses=True)
    try:
        keys = list(client.scan_iter(match=pattern))
        if keys:
            client.delete(*keys)
    finally:
        client.close()


async def _wait_for_condition(
    predicate, *, timeout_s: float = 5.0, interval_s: float = 0.05
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval_s)
    raise AssertionError("Condition did not become true before timeout.")


@pytest.mark.asyncio
async def test_redis_sentinel_sink_and_dlq_write_across_failover(
    redis_sentinel_url: str,
    redis_sentinel_control,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    key_prefix = f"agora:it:sentinel-matrix:{unique_suffix}"
    before_key = f"{key_prefix}:before"
    after_key = f"{key_prefix}:after"
    dlq_prefix = f"{key_prefix}:dlq"
    client = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
    sink = RedisSink(
        url=redis_sentinel_url,
        key_fn=lambda record: str(record["key"]),
        serializer=lambda record: str(record["value"]),
    )
    dlq_sink = RedisDLQSink(url=redis_sentinel_url, key_prefix=dlq_prefix)

    try:
        await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready, timeout_s=60.0)
        await sink.open()
        await dlq_sink.open()
        await sink.write({"key": before_key, "value": "before"})
        await dlq_sink.write(
            DLQRecord(
                pipeline_id="redis-sentinel-matrix",
                run_id=unique_suffix,
                stage="before_failover",
                error_type="ValueError",
                error_message="before",
                record={"key": before_key},
                checkpoint={"offset": 1},
            )
        )

        await asyncio.to_thread(redis_sentinel_control.graceful_failover, timeout_s=60.0)
        await asyncio.to_thread(redis_sentinel_control.wait_for_proxy_writable)
        await sink.write({"key": after_key, "value": "after"})
        await dlq_sink.write(
            DLQRecord(
                pipeline_id="redis-sentinel-matrix",
                run_id=unique_suffix,
                stage="after_failover",
                error_type="ValueError",
                error_message="after",
                record={"key": after_key},
                checkpoint={"offset": 2},
            )
        )

        await _wait_for_condition(
            lambda: client.get(before_key) == "before" and client.get(after_key) == "after",
            timeout_s=_INTEGRATION_TIMEOUT_S,
        )
        assert client.llen(f"{dlq_prefix}:__index__") == 2
    finally:
        with contextlib.suppress(Exception):
            await sink.close()
        with contextlib.suppress(Exception):
            await dlq_sink.close()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_delete_matching_keys, redis_sentinel_url, f"{key_prefix}*")
        client.close()


@pytest.mark.asyncio
async def test_redis_cluster_sink_batches_without_cross_slot_mset(
    redis_cluster_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    client = redis.RedisCluster.from_url(
        redis_cluster_url,
        decode_responses=True,
        address_remap=_redis_cluster_address_remap,
    )
    keys = [
        f"agora:it:cluster:{unique_suffix}:alpha",
        f"agora:it:cluster:{unique_suffix}:bravo",
    ]
    sink = RedisSink(
        url=redis_cluster_url,
        key_fn=lambda record: str(record["key"]),
        serializer=lambda record: str(record["value"]),
        redis_cluster=True,
        redis_cluster_address_remap=_redis_cluster_address_remap,
    )
    try:
        await sink.open()
        await sink.write_batch(
            [
                {"key": keys[0], "value": "alpha"},
                {"key": keys[1], "value": "bravo"},
            ]
        )

        assert client.get(keys[0]) == "alpha"
        assert client.get(keys[1]) == "bravo"
        metrics = sink.metrics_snapshot()
        assert metrics.mset_batch_count == 0
        assert metrics.pipeline_execute_count == 1
    finally:
        with contextlib.suppress(Exception):
            await sink.close()
        with contextlib.suppress(Exception):
            client.delete(*keys)
        client.close()


def test_redis_cluster_backend_delete_prefix_deletes_cross_slot_keys(
    redis_cluster_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    prefix = f"agora:it:cluster-state:{unique_suffix}:"
    backend = RedisBackend(
        url=redis_cluster_url,
        prefix=prefix,
        redis_cluster=True,
        redis_cluster_address_remap=_redis_cluster_address_remap,
    )
    client = redis.RedisCluster.from_url(
        redis_cluster_url,
        decode_responses=True,
        address_remap=_redis_cluster_address_remap,
    )
    keys = [f"item:{name}" for name in ("alpha", "bravo", "charlie")]

    try:
        for index, key in enumerate(keys):
            backend.set(key, {"index": index})

        assert backend.delete_prefix("item:") == len(keys)
        assert [client.exists(f"{prefix}{key}") for key in keys] == [0, 0, 0]
    finally:
        with contextlib.suppress(Exception):
            backend.close()
        with contextlib.suppress(Exception):
            stale_keys = list(client.scan_iter(match=f"{prefix}*"))
            if stale_keys:
                client.delete(*stale_keys)
        client.close()


class _MatrixEmbeddingProvider:
    async def embed(self, text: str):
        if text.startswith("alpha"):
            return SimpleNamespace(embedding=[1.0, 0.0, 0.0])
        if text.startswith("bravo"):
            return SimpleNamespace(embedding=[0.0, 1.0, 0.0])
        return SimpleNamespace(embedding=[0.0, 0.0, 1.0])

    async def embed_batch(self, texts: list[str]):
        return [await self.embed(text) for text in texts]


@pytest.mark.asyncio
async def test_redisearch_embedding_store_uses_real_vector_index(
    redis_stack_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    prefix = f"agora:it:redisearch:{unique_suffix}:"
    index_name = f"agora_it_redisearch_{unique_suffix}"
    client = redis.Redis.from_url(redis_stack_url, decode_responses=False)
    store = RedisEmbeddingStore(
        provider=_MatrixEmbeddingProvider(),
        redis_url=redis_stack_url,
        redis_key_prefix=prefix,
        similarity_threshold=0.92,
        use_redisearch=True,
        redisearch_index_name=index_name,
    )

    try:
        await store.open()
        await store.add("alpha-seed")

        assert await store.exists("alpha-copy") is True
        assert await store.exists("bravo") is False
        index_list = client.execute_command("FT._LIST")
        decoded = [item.decode("utf-8") if isinstance(item, bytes) else item for item in index_list]
        assert index_name in decoded
    finally:
        with contextlib.suppress(Exception):
            await store.close()
        with contextlib.suppress(Exception):
            client.execute_command("FT.DROPINDEX", index_name, "DD")
        with contextlib.suppress(Exception):
            keys = list(client.scan_iter(match=f"{prefix}*"))
            if keys:
                client.delete(*keys)
        client.close()


@pytest.mark.asyncio
async def test_redis_cluster_embedding_store_handles_cross_slot_index_and_fields(
    redis_cluster_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    prefix = f"agora:it:cluster-emb:{unique_suffix}:"
    index_key = f"{prefix}__index__"
    client = redis.RedisCluster.from_url(
        redis_cluster_url,
        decode_responses=True,
        address_remap=_redis_cluster_address_remap,
    )
    store = RedisEmbeddingStore(
        provider=_MatrixEmbeddingProvider(),
        redis_url=redis_cluster_url,
        redis_key_prefix=prefix,
        similarity_threshold=0.92,
        redis_cluster=True,
        redis_cluster_address_remap=_redis_cluster_address_remap,
    )

    try:
        await store.open()
        await store.add("alpha-seed")
        await store.add("bravo-seed")

        assert await store.exists("alpha-copy") is True
        assert await store.exists("charlie") is False
        assert client.scard(index_key) == 2
        assert client.exists(f"{prefix}alpha-seed") == 1
        assert client.exists(f"{prefix}bravo-seed") == 1
    finally:
        with contextlib.suppress(Exception):
            await store.close()
        with contextlib.suppress(Exception):
            keys = list(client.scan_iter(match=f"{prefix}*"))
            if keys:
                client.delete(*keys)
        client.close()
