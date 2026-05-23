from __future__ import annotations

import json

import pytest

from agora_plugins.redis import RedisStore


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
    assert all(isinstance(call[2]["ex"], int) and 1 <= call[2]["ex"] <= 60 for call in calls)
    for _, payload, _ in calls:
        assert json.loads(payload)["value"] == 1
