"""Redis state backend."""

from __future__ import annotations

import json
import math
import time
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterator

from agora.state import StateBackend, StateValue, StoredValue

from agora_plugins.redis.connection import RedisClusterAddressRemap, build_sync_redis_client

_COMPARE_AND_SET_SCRIPT = """
local current = redis.call("GET", KEYS[1])
if not current then
  return 0
end

local ok, decoded = pcall(cjson.decode, current)
if ok then
  local expires_at = decoded["expires_at"]
  if expires_at ~= nil and expires_at ~= cjson.null and tonumber(expires_at) <= tonumber(ARGV[4]) then
    redis.call("DEL", KEYS[1])
    return 0
  end
end

if current ~= ARGV[1] then
  return 0
end

local ttl_ms = tonumber(ARGV[3])
if ttl_ms < 0 then
  redis.call("SET", KEYS[1], ARGV[2])
else
  redis.call("SET", KEYS[1], ARGV[2], "PX", ttl_ms)
end
return 1
"""


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
        *,
        redis_cluster: bool = False,
        redis_cluster_address_remap: RedisClusterAddressRemap | None = None,
        sentinel_service_name: str | None = None,
        sentinel_urls: list[str] | None = None,
    ) -> None:
        try:
            __import__("redis")
        except ImportError:
            raise ImportError(
                "RedisBackend requires 'redis'. "
                "Install with: pip install 'agora-etl-plugins[redis]'"
            ) from None

        self._redis = build_sync_redis_client(
            url=url,
            decode_responses=True,
            redis_cluster=redis_cluster,
            redis_cluster_address_remap=redis_cluster_address_remap,
            sentinel_service_name=sentinel_service_name,
            sentinel_urls=sentinel_urls,
        )
        self._prefix = prefix
        self._redis_cluster = redis_cluster

    def get(self, key: str) -> StoredValue | None:
        payload = cast("str | None", self._redis.get(self._key(key)))
        if payload is None:
            return None
        data = cast("dict[str, StateValue | float | None]", json.loads(payload))
        expires_at = cast("float | None", data.get("expires_at"))
        if expires_at is not None and time.time() >= expires_at:
            self.delete(key)
            return None
        return StoredValue(value=data.get("value"), expires_at=expires_at)

    def set(self, key: str, value: StateValue, *, expires_at: float | None = None) -> None:
        payload = _state_payload(value, expires_at=expires_at)
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
        payload = _state_payload(value, expires_at=expires_at)
        redis_key = self._key(key)
        ttl_ms = self._ttl_ms(expires_at)
        if ttl_ms is None:
            return bool(self._redis.set(redis_key, payload, nx=True))
        if ttl_ms <= 0:
            return False
        return bool(self._redis.set(redis_key, payload, nx=True, px=ttl_ms))

    def compare_and_set(
        self,
        key: str,
        expected: StoredValue | None,
        value: StateValue,
        *,
        expires_at: float | None = None,
    ) -> bool:
        """Atomically replace *key* only when its stored value still matches *expected*.

        Pass ``expected=None`` to create the key only if it is absent, matching
        ``set_if_absent`` semantics. For existing keys, callers should pass the
        exact ``StoredValue`` returned by ``get()`` so the value and expiry token
        are both protected against concurrent updates.
        """
        if expected is None:
            return self.set_if_absent(key, value, expires_at=expires_at)
        ttl_ms = self._ttl_ms(expires_at)
        if ttl_ms is not None and ttl_ms <= 0:
            return False
        redis_key = self._key(key)
        expected_payload = _state_payload(expected.value, expires_at=expected.expires_at)
        next_payload = _state_payload(value, expires_at=expires_at)
        ttl_arg = -1 if ttl_ms is None else ttl_ms
        return bool(
            self._redis.eval(
                _COMPARE_AND_SET_SCRIPT,
                1,
                redis_key,
                expected_payload,
                next_payload,
                ttl_arg,
                time.time(),
            )
        )

    def delete(self, key: str) -> None:
        self._redis.delete(self._key(key))

    def count_prefix(self, prefix: str) -> int:
        return sum(1 for _ in self._scan_prefixed_keys(prefix))

    def delete_prefix(self, prefix: str) -> int:
        keys = list(self._scan_prefixed_keys(prefix))
        if not keys:
            return 0
        if self._redis_cluster:
            for key in keys:
                self._redis.delete(key)
            return len(keys)
        self._redis.delete(*keys)
        return len(keys)

    def close(self) -> None:
        self._redis.close()

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def _scan_prefixed_keys(self, prefix: str) -> Iterator[str]:
        match = self._key(f"{prefix}*")
        yield from cast("Iterator[str]", self._redis.scan_iter(match=match))

    @staticmethod
    def _ttl_ms(expires_at: float | None) -> int | None:
        if expires_at is None:
            return None
        remaining_s = expires_at - time.time()
        return math.ceil(remaining_s * 1000)


def _state_payload(value: StateValue, *, expires_at: float | None) -> str:
    return json.dumps({"value": value, "expires_at": expires_at}, ensure_ascii=False)


__all__ = ["RedisBackend"]
