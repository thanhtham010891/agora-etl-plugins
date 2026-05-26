from __future__ import annotations

import json
import sys
import time
from types import SimpleNamespace

from agora.state.backend import StoredValue

from agora_plugins.redis import RedisBackend


class _FakeRedis:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._set_calls: list[tuple[str, str, dict[str, object]]] = []
        self._delete_calls: list[str] = []

    def get(self, key: str) -> str | None:
        return self._values.get(key)

    def set(self, key: str, value: str, **kwargs: object) -> bool:
        self._set_calls.append((key, value, kwargs))
        if kwargs.get("nx") and key in self._values:
            return False
        self._values[key] = value
        return True

    def delete(self, *keys: str) -> None:
        for key in keys:
            self._delete_calls.append(key)
            self._values.pop(key, None)

    def close(self) -> None:
        return None

    def scan_iter(self, *, match: str):
        prefix = match[:-1]
        for key in sorted(self._values):
            if key.startswith(prefix):
                yield key


def _install_fake_redis_module(monkeypatch) -> _FakeRedis:
    fake = _FakeRedis()

    class _RedisFactory:
        @staticmethod
        def from_url(url: str, *, decode_responses: bool):
            del url, decode_responses
            return fake

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=_RedisFactory))
    return fake


def test_redis_backend_stores_and_expires_values(monkeypatch) -> None:
    _install_fake_redis_module(monkeypatch)
    backend = RedisBackend(prefix="agora:test:")

    backend.set("alpha", {"ok": True})
    backend.set("expired", "gone", expires_at=time.time() - 1)

    assert backend.get("alpha") == StoredValue(value={"ok": True}, expires_at=None)
    assert backend.get("expired") is None


def test_redis_backend_set_if_absent_persists_first_value(monkeypatch) -> None:
    fake = _install_fake_redis_module(monkeypatch)
    backend = RedisBackend(prefix="agora:test:")

    assert backend.set_if_absent("alpha", {"count": 1}) is True
    assert backend.set_if_absent("alpha", {"count": 2}) is False
    assert backend.get("alpha") == StoredValue(value={"count": 1}, expires_at=None)
    payload = json.loads(fake._values["agora:test:alpha"])
    assert payload["value"] == {"count": 1}


def test_redis_backend_uses_millisecond_precision_for_ttl(monkeypatch) -> None:
    fake = _install_fake_redis_module(monkeypatch)
    backend = RedisBackend(prefix="agora:test:")

    backend.set("alpha", {"ok": True}, expires_at=time.time() + 0.25)

    assert fake._set_calls[-1][0] == "agora:test:alpha"
    assert "px" in fake._set_calls[-1][2]
    assert isinstance(fake._set_calls[-1][2]["px"], int)
    assert 1 <= fake._set_calls[-1][2]["px"] <= 250


def test_redis_backend_does_not_persist_already_expired_values(monkeypatch) -> None:
    fake = _install_fake_redis_module(monkeypatch)
    backend = RedisBackend(prefix="agora:test:")

    backend.set("expired", "gone", expires_at=time.time() - 1)

    assert "agora:test:expired" not in fake._values
    assert fake._delete_calls == ["agora:test:expired"]


def test_redis_backend_set_if_absent_with_expired_ttl_does_not_block_future_writes(
    monkeypatch,
) -> None:
    fake = _install_fake_redis_module(monkeypatch)
    backend = RedisBackend(prefix="agora:test:")

    assert backend.set_if_absent("alpha", {"count": 1}, expires_at=time.time() - 1) is True
    assert "agora:test:alpha" not in fake._values
    assert backend.set_if_absent("alpha", {"count": 2}) is True
    assert backend.get("alpha") == StoredValue(value={"count": 2}, expires_at=None)
