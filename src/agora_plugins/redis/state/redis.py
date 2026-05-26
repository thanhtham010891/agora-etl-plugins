"""Redis state backend."""

from __future__ import annotations

import json
import math
import time
from typing import cast

from agora.state.backend import StateBackend, StateValue, StoredValue


class RedisBackend(StateBackend):
    """Redis-backed state backend.

    Uses the synchronous ``redis.Redis`` client because ``StateBackend``
    defines a synchronous interface. Callers that need async execution
    should wrap this backend with ``offload_blocking_calls=True`` (e.g.
    ``BackendDedupStore``) or call methods via ``asyncio.to_thread``.
    """

    backend_name = "redis"

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        prefix: str = "agora:state:",
    ) -> None:
        try:
            import redis
        except ImportError:
            raise ImportError(
                "RedisBackend requires 'redis'. "
                "Install with: pip install 'agora-etl-plugins[redis]'"
            ) from None

        self._redis = redis.Redis.from_url(url, decode_responses=True)
        self._prefix = prefix

    def get(self, key: str) -> StoredValue | None:
        payload = self._redis.get(self._key(key))
        if payload is None:
            return None
        data = cast("dict[str, StateValue | float | None]", json.loads(payload))
        expires_at = cast("float | None", data.get("expires_at"))
        if expires_at is not None and time.time() >= expires_at:
            self.delete(key)
            return None
        return StoredValue(value=cast("StateValue", data.get("value")), expires_at=expires_at)

    def set(self, key: str, value: StateValue, *, expires_at: float | None = None) -> None:
        payload = json.dumps({"value": value, "expires_at": expires_at}, ensure_ascii=False)
        redis_key = self._key(key)
        ttl_ms = self._ttl_ms(expires_at)
        if ttl_ms is None:
            self._redis.set(redis_key, payload)
            return
        if ttl_ms <= 0:
            self._redis.delete(redis_key)
            return
        self._redis.set(redis_key, payload, px=ttl_ms)

    def set_if_absent(
        self,
        key: str,
        value: StateValue,
        *,
        expires_at: float | None = None,
    ) -> bool:
        payload = json.dumps({"value": value, "expires_at": expires_at}, ensure_ascii=False)
        redis_key = self._key(key)
        ttl_ms = self._ttl_ms(expires_at)
        if ttl_ms is None:
            return bool(self._redis.set(redis_key, payload, nx=True))
        if ttl_ms <= 0:
            self._redis.delete(redis_key)
            return True
        return bool(self._redis.set(redis_key, payload, nx=True, px=ttl_ms))

    def delete(self, key: str) -> None:
        self._redis.delete(self._key(key))

    def count_prefix(self, prefix: str) -> int:
        return sum(1 for _ in self._scan_prefixed_keys(prefix))

    def delete_prefix(self, prefix: str) -> int:
        keys = list(self._scan_prefixed_keys(prefix))
        if not keys:
            return 0
        self._redis.delete(*keys)
        return len(keys)

    def close(self) -> None:
        self._redis.close()

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def _scan_prefixed_keys(self, prefix: str):
        match = self._key(f"{prefix}*")
        yield from self._redis.scan_iter(match=match)

    @staticmethod
    def _ttl_ms(expires_at: float | None) -> int | None:
        if expires_at is None:
            return None
        remaining_s = expires_at - time.time()
        return math.ceil(remaining_s * 1000)


__all__ = ["RedisBackend"]
