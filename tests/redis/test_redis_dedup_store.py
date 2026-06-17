from __future__ import annotations

import asyncio
import json
import struct
import sys
from types import SimpleNamespace

import pytest

from agora_plugins.redis import RedisEmbeddingStore, RedisStore


@pytest.mark.asyncio
async def test_redis_store_mark_if_new_uses_atomic_set_nx() -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    class _FakeRedis:
        def __init__(self) -> None:
            self._values: set[str] = set()

        def set(self, key: str, value: str, **kwargs: object) -> bool | None:
            calls.append((key, value, kwargs))
            if kwargs.get("nx"):
                if key in self._values:
                    return None
                self._values.add(key)
                return True
            self._values.add(key)
            return True

        def close(self) -> None:
            return None

    store = RedisStore(url="redis://example:6379", ttl_seconds=60)
    store._store.backend._redis = _FakeRedis()

    assert await store.mark_if_new("abc")
    assert not await store.mark_if_new("abc")
    assert len(calls) == 2
    assert [call[0] for call in calls] == ["agora:dedup:abc", "agora:dedup:abc"]
    assert [call[2]["nx"] for call in calls] == [True, True]
    assert all(isinstance(call[2]["px"], int) and 1 <= call[2]["px"] <= 60_000 for call in calls)
    for _, payload, _ in calls:
        assert json.loads(payload)["value"] == 1


@pytest.mark.asyncio
async def test_redis_embedding_store_scans_index_in_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    class _FakePipeline:
        def __init__(self, redis: _FakeAsyncRedis) -> None:
            self._redis = redis
            self._keys: list[str] = []

        def get(self, key: str) -> None:
            self._keys.append(key)

        async def execute(self) -> list[bytes | None]:
            return [self._redis.values.get(key) for key in self._keys]

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class _FakeAsyncRedis:
        def __init__(self) -> None:
            self.values: dict[str, bytes] = {}
            self.index = [b"a", b"b", b"c"]
            self.sscan_calls: list[tuple[str, int, int]] = []

        async def sscan(
            self,
            key: str,
            *,
            cursor: int,
            count: int,
        ) -> tuple[int, list[bytes]]:
            self.sscan_calls.append((key, cursor, count))
            if cursor == 0:
                return 1, self.index[:2]
            return 0, self.index[2:]

        def pipeline(self, *, transaction: bool):
            assert transaction is False
            return _FakePipeline(self)

        async def aclose(self) -> None:
            return None

    fake_redis = _FakeAsyncRedis()
    fake_redis.values = {
        "agora:dedup:emb:a": struct.pack("2f", 0.0, 1.0),
        "agora:dedup:emb:b": struct.pack("2f", 0.0, 1.0),
        "agora:dedup:emb:c": struct.pack("2f", 1.0, 0.0),
    }

    class _RedisFactory:
        @staticmethod
        def from_url(url: str, **kwargs: object):
            del url, kwargs
            return fake_redis

    fake_module = SimpleNamespace(from_url=_RedisFactory.from_url)
    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=fake_module))
    monkeypatch.setitem(sys.modules, "redis.asyncio", fake_module)

    store = RedisEmbeddingStore(provider=_FakeProvider(), redis_key_prefix="agora:dedup:emb:")

    assert await store.exists("needle") is True
    assert fake_redis.sscan_calls == [
        ("agora:dedup:emb:__index__", 0, 256),
        ("agora:dedup:emb:__index__", 1, 256),
    ]


@pytest.mark.asyncio
async def test_redis_embedding_store_mark_if_new_is_atomic() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            return SimpleNamespace(embedding=[1.0, float(len(text))])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    store = RedisEmbeddingStore(provider=_FakeProvider())
    seen = False
    lock_acquisitions = 0

    class _FakeLock:
        async def __aenter__(self) -> _FakeLock:
            nonlocal lock_acquisitions
            lock_acquisitions += 1
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _FakeRedis:
        def lock(self, name: str, *, timeout: float, blocking_timeout: float) -> _FakeLock:
            del name, timeout, blocking_timeout
            return _FakeLock()

    store._redis = _FakeRedis()  # type: ignore[assignment]

    async def _exists(*, query_embedding: list[float]) -> bool:
        del query_embedding
        await asyncio.sleep(0)
        return seen

    async def _add(
        key: str,
        embedding: list[float],
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        del key, embedding, ttl_seconds
        nonlocal seen
        await asyncio.sleep(0)
        seen = True

    store._redis_exists = _exists  # type: ignore[method-assign]
    store._redis_add = _add  # type: ignore[method-assign]

    first, second = await asyncio.gather(store.mark_if_new("abc"), store.mark_if_new("abc"))

    assert (first, second) == (True, False)
    assert lock_acquisitions == 2


@pytest.mark.asyncio
async def test_redis_embedding_store_uses_redisearch_vector_commands() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    class _FakeRedis:
        def __init__(self) -> None:
            self.commands: list[tuple[object, ...]] = []
            self.hsets: list[tuple[str, dict[str, object]]] = []

        async def execute_command(self, *args: object) -> object:
            self.commands.append(args)
            if args[0] == "FT.SEARCH":
                return [1, b"agora:dedup:emb:existing", [b"vector_score", b"0.04"]]
            return b"OK"

        async def hset(self, key: str, *, mapping: dict[str, object]) -> None:
            self.hsets.append((key, mapping))

    fake_redis = _FakeRedis()
    store = RedisEmbeddingStore(
        provider=_FakeProvider(),
        redis_key_prefix="agora:dedup:emb:",
        similarity_threshold=0.92,
        use_redisearch=True,
        redisearch_index_name="agora-dedup-idx",
    )
    store._redis = fake_redis  # type: ignore[assignment]

    assert await store.exists("needle") is True
    await store.add("needle")

    assert fake_redis.commands[0][:7] == (
        "FT.CREATE",
        "agora-dedup-idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "agora:dedup:emb:",
    )
    assert fake_redis.commands[1][:3] == (
        "FT.SEARCH",
        "agora-dedup-idx",
        "*=>[KNN 1 @vector $query_vector AS vector_score]",
    )
    assert fake_redis.hsets == [
        (
            "agora:dedup:emb:needle",
            {"key": "needle", "vector": struct.pack("2f", 1.0, 0.0)},
        )
    ]


def test_redis_embedding_store_backend_mode_selects_redisearch() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    store = RedisEmbeddingStore(
        provider=_FakeProvider(),
        backend_mode="redisearch",
    )

    assert store._backend_mode == "redisearch"
    assert store._use_redisearch is True


def test_redis_embedding_store_rejects_conflicting_backend_mode() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    with pytest.raises(ValueError, match="conflicts"):
        RedisEmbeddingStore(
            provider=_FakeProvider(),
            backend_mode="scan",
            use_redisearch=True,
        )


def test_redis_embedding_store_warns_for_large_scan_mode() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    with pytest.warns(RuntimeWarning, match="backend_mode='scan'"):
        RedisEmbeddingStore(
            provider=_FakeProvider(),
            backend_mode="scan",
            max_entries=10_001,
        )


def test_redis_embedding_store_rejects_declared_no_embedding_provider() -> None:
    class _NoEmbeddingProvider:
        supports_embeddings = False

        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    with pytest.raises(TypeError, match="supports_embeddings=False"):
        RedisEmbeddingStore(provider=_NoEmbeddingProvider())


@pytest.mark.asyncio
async def test_redis_embedding_store_applies_ttl_when_marking_new_key() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    class _FakeLock:
        async def __aenter__(self) -> _FakeLock:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _FakePipeline:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

        def set(self, *args: object, **kwargs: object) -> None:
            self.calls.append(("set", args, kwargs))

        def sadd(self, *args: object, **kwargs: object) -> None:
            self.calls.append(("sadd", args, kwargs))

        async def execute(self) -> list[object]:
            return []

        async def __aenter__(self) -> _FakePipeline:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    class _FakeRedis:
        def __init__(self) -> None:
            self.pipeline_obj = _FakePipeline()

        def lock(self, name: str, *, timeout: float, blocking_timeout: float) -> _FakeLock:
            del name, timeout, blocking_timeout
            return _FakeLock()

        async def sscan(
            self,
            key: str,
            *,
            cursor: int,
            count: int,
        ) -> tuple[int, list[bytes]]:
            del key, cursor, count
            return 0, []

        async def scard(self, key: str) -> int:
            del key
            return 0

        def pipeline(self, *, transaction: bool) -> _FakePipeline:
            assert transaction is True
            return self.pipeline_obj

    store = RedisEmbeddingStore(provider=_FakeProvider(), redis_key_prefix="agora:dedup:emb:")
    fake_redis = _FakeRedis()
    store._redis = fake_redis  # type: ignore[assignment]

    assert await store.mark_if_new("needle", ttl_seconds=60) is True

    assert fake_redis.pipeline_obj.calls[0][0] == "set"
    assert fake_redis.pipeline_obj.calls[0][2] == {"ex": 60}


@pytest.mark.asyncio
async def test_redis_embedding_store_prunes_expired_index_members_before_max_entries() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    class _FakePipeline:
        def __init__(self, redis: _FakeRedis, *, transaction: bool) -> None:
            self._redis = redis
            self._transaction = transaction
            self._keys: list[str] = []
            self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

        def get(self, key: str) -> None:
            self._keys.append(key)

        def set(self, *args: object, **kwargs: object) -> None:
            self.calls.append(("set", args, kwargs))

        def sadd(self, *args: object, **kwargs: object) -> None:
            self.calls.append(("sadd", args, kwargs))

        async def execute(self) -> list[bytes | None]:
            if self._transaction:
                for name, args, _kwargs in self.calls:
                    if name == "sadd":
                        self._redis.index.add(str(args[1]))
                return []
            return [self._redis.values.get(key) for key in self._keys]

        async def __aenter__(self) -> _FakePipeline:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    class _FakeRedis:
        def __init__(self) -> None:
            self.index = {"expired"}
            self.values: dict[str, bytes] = {}
            self.pipeline_calls: list[_FakePipeline] = []
            self.removed: list[str] = []

        async def sscan(
            self,
            key: str,
            *,
            cursor: int,
            count: int,
        ) -> tuple[int, list[bytes]]:
            del key, count
            return (0, [item.encode() for item in sorted(self.index)]) if cursor == 0 else (0, [])

        async def srem(self, key: str, *members: str) -> int:
            del key
            for member in members:
                self.index.discard(member)
                self.removed.append(member)
            return len(members)

        async def scard(self, key: str) -> int:
            del key
            return len(self.index)

        def pipeline(self, *, transaction: bool) -> _FakePipeline:
            pipe = _FakePipeline(self, transaction=transaction)
            self.pipeline_calls.append(pipe)
            return pipe

    store = RedisEmbeddingStore(
        provider=_FakeProvider(),
        redis_key_prefix="agora:dedup:emb:",
        max_entries=1,
    )
    fake_redis = _FakeRedis()
    store._redis = fake_redis  # type: ignore[assignment]

    await store._redis_add("fresh", [1.0, 0.0], ttl_seconds=60)

    assert fake_redis.removed == ["expired"]
    assert fake_redis.index == {"fresh"}


@pytest.mark.asyncio
async def test_redis_embedding_store_reaps_dimension_mismatches_during_scan() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    class _FakePipeline:
        def __init__(self, redis: _FakeRedis) -> None:
            self._redis = redis
            self._keys: list[str] = []

        def get(self, key: str) -> None:
            self._keys.append(key)

        async def execute(self) -> list[bytes | None]:
            return [self._redis.values.get(key) for key in self._keys]

        async def __aenter__(self) -> _FakePipeline:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    class _FakeRedis:
        def __init__(self) -> None:
            self.index = [b"bad", b"good"]
            self.values = {
                "agora:dedup:emb:bad": struct.pack("3f", 1.0, 0.0, 0.0),
                "agora:dedup:emb:good": struct.pack("2f", 1.0, 0.0),
            }
            self.removed: list[str] = []

        async def sscan(
            self,
            key: str,
            *,
            cursor: int,
            count: int,
        ) -> tuple[int, list[bytes]]:
            del key, count
            return (0, self.index) if cursor == 0 else (0, [])

        async def srem(self, key: str, *members: str) -> int:
            del key
            self.removed.extend(members)
            return len(members)

        def pipeline(self, *, transaction: bool) -> _FakePipeline:
            assert transaction is False
            return _FakePipeline(self)

    store = RedisEmbeddingStore(provider=_FakeProvider(), redis_key_prefix="agora:dedup:emb:")
    fake_redis = _FakeRedis()
    store._redis = fake_redis  # type: ignore[assignment]

    assert await store.exists("needle") is True
    assert fake_redis.removed == ["bad"]


@pytest.mark.asyncio
async def test_redis_embedding_store_applies_ttl_to_redisearch_hash() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    class _FakeRedis:
        def __init__(self) -> None:
            self.commands: list[tuple[object, ...]] = []
            self.expires: list[tuple[str, int]] = []

        async def execute_command(self, *args: object) -> object:
            self.commands.append(args)
            return b"OK"

        async def hset(self, key: str, *, mapping: dict[str, object]) -> None:
            del key, mapping

        async def expire(self, key: str, ttl_seconds: int) -> bool:
            self.expires.append((key, ttl_seconds))
            return True

    store = RedisEmbeddingStore(
        provider=_FakeProvider(),
        redis_key_prefix="agora:dedup:emb:",
        use_redisearch=True,
    )
    fake_redis = _FakeRedis()
    store._redis = fake_redis  # type: ignore[assignment]

    await store._redis_add("needle", [1.0, 0.0], ttl_seconds=60)

    assert fake_redis.expires == [("agora:dedup:emb:needle", 60)]


@pytest.mark.asyncio
async def test_redis_embedding_store_rejects_reserved_internal_keys() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    store = RedisEmbeddingStore(provider=_FakeProvider(), redis_key_prefix="agora:dedup:emb:")

    with pytest.raises(ValueError, match="reserved"):
        await store._redis_add("__index__", [1.0, 0.0])


@pytest.mark.asyncio
async def test_redisearch_existing_index_dimension_mismatch_is_rejected() -> None:
    class _FakeProvider:
        async def embed(self, text: str):
            del text
            return SimpleNamespace(embedding=[1.0, 0.0])

        async def embed_batch(self, texts: list[str]):
            return [await self.embed(text) for text in texts]

    class _FakeRedis:
        async def execute_command(self, *args: object) -> object:
            if args[0] == "FT.CREATE":
                raise RuntimeError("Index already exists")
            if args[0] == "FT.INFO":
                return [
                    b"index_name",
                    b"agora-dedup-idx",
                    b"attributes",
                    [
                        [
                            b"identifier",
                            b"vector",
                            b"attribute",
                            b"vector",
                            b"type",
                            b"VECTOR",
                            b"dim",
                            b"3",
                        ]
                    ],
                ]
            raise AssertionError(args)

    store = RedisEmbeddingStore(
        provider=_FakeProvider(),
        redis_key_prefix="agora:dedup:emb:",
        use_redisearch=True,
        redisearch_index_name="agora-dedup-idx",
    )
    store._redis = _FakeRedis()  # type: ignore[assignment]

    with pytest.raises(ValueError, match="dimension mismatch"):
        await store._ensure_redisearch_index(2)
