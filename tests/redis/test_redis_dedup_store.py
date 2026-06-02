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
        def from_url(url: str, *, decode_responses: bool):
            del url, decode_responses
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

    store = RedisEmbeddingStore(provider=_FakeProvider())
    seen = False

    async def _exists(*, query_embedding: list[float]) -> bool:
        del query_embedding
        await asyncio.sleep(0)
        return seen

    async def _add(key: str, embedding: list[float]) -> None:
        del key, embedding
        nonlocal seen
        await asyncio.sleep(0)
        seen = True

    store._redis_exists = _exists  # type: ignore[method-assign]
    store._redis_add = _add  # type: ignore[method-assign]

    first, second = await asyncio.gather(store.mark_if_new("abc"), store.mark_if_new("abc"))

    assert (first, second) == (True, False)
