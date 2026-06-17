from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agora_plugins.redis.ai.cache import RedisLLMCache


@pytest.mark.asyncio
async def test_redis_llm_cache_offloads_sync_state_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    class _FakeStore:
        def get(self, key: str) -> str:
            calls.append(("get", (key,), {}))
            return "cached"

        def set(self, key: str, value: str, *, ttl_s: int) -> None:
            calls.append(("set", (key, value), {"ttl_s": ttl_s}))

        def close(self) -> None:
            calls.append(("close", (), {}))

    async def _fake_to_thread(
        func: Any,
        /,
        *args: object,
        **kwargs: object,
    ) -> Any:
        calls.append(("to_thread", (getattr(func, "__name__", repr(func)),), {}))
        return func(*args, **kwargs)

    cache = RedisLLMCache.__new__(RedisLLMCache)
    cache._store = _FakeStore()  # type: ignore[attr-defined]
    monkeypatch.setattr(asyncio, "to_thread", _fake_to_thread)

    assert await cache.get("k") == "cached"
    await cache.set("k", "v", ttl=30)
    await cache.close()

    assert calls == [
        ("to_thread", ("get",), {}),
        ("get", ("k",), {}),
        ("to_thread", ("set",), {}),
        ("set", ("k", "v"), {"ttl_s": 30}),
        ("to_thread", ("close",), {}),
        ("close", (), {}),
    ]


@pytest.mark.asyncio
async def test_redis_llm_cache_set_without_ttl_uses_store_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    class _FakeStore:
        def set(self, key: str, value: str, *, ttl_s: int | None) -> None:
            calls.append(("set", (key, value), {"ttl_s": ttl_s}))

    async def _fake_to_thread(
        func: Any,
        /,
        *args: object,
        **kwargs: object,
    ) -> Any:
        return func(*args, **kwargs)

    cache = RedisLLMCache.__new__(RedisLLMCache)
    cache._store = _FakeStore()  # type: ignore[attr-defined]
    monkeypatch.setattr(asyncio, "to_thread", _fake_to_thread)

    await cache.set("k", "v")

    assert calls == [("set", ("k", "v"), {"ttl_s": None})]
