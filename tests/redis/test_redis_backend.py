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

    def get(self, key: str) -> str | None:
        return self._values.get(key)

    def set(self, key: str, value: str, **kwargs: object) -> bool:
        if kwargs.get("nx") and key in self._values:
            return False
        self._values[key] = value
        return True

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        del ttl_seconds
        self._values[key] = value

    def delete(self, *keys: str) -> None:
        for key in keys:
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
