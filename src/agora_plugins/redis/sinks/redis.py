"""
agora_plugins.redis.sinks.redis
=======================
``RedisSink[T]`` — write records to Redis (SET / LPUSH / XADD).

Requires: ``pip install 'agora-etl-plugins[redis]'``
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast
from urllib.parse import urlparse

import logstruct
from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")
logger = logstruct.getLogger(__name__)

_MODES = {"set", "lpush", "rpush", "xadd"}


def _redact_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        if parsed.password:
            return parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                + (f":{parsed.port}" if parsed.port else "")
            ).geturl()
    except Exception:
        pass
    return url


class RedisSink(BaseSink[T], Generic[T]):
    """Write records to Redis."""

    sink_name = "redis"

    def __init__(
        self,
        url: str,
        key_fn: Callable[[T], str],
        serializer: Callable[[T], Any] | None = None,
        mode: str = "set",
        ttl_seconds: int | None = None,
        maxlen: int | None = None,
    ) -> None:
        if mode not in _MODES:
            raise ValueError(f"RedisSink: invalid mode {mode!r}. Choose from {_MODES}")
        if ttl_seconds is not None and mode != "set":
            raise ValueError(
                f"RedisSink: ttl_seconds is only supported for mode='set', got {mode!r}"
            )
        self._url = url
        self._key_fn = key_fn
        self._serializer = serializer or _default_serializer
        self._mode = mode
        self._ttl = ttl_seconds
        self._maxlen = maxlen
        self._client: Any | None = None
        self._xadd_kwargs: dict[str, Any] = {}
        if maxlen is not None:
            self._xadd_kwargs = {"maxlen": maxlen, "approximate": True}

    async def open(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "RedisSink requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = cast("Any", aioredis.from_url(self._url))
        logger.info("redis_sink_ready", url=_redact_url(self._url), mode=self._mode)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.info("redis_sink_closed")

    def _pipe_command(self, pipe: Any, key: str, value: Any) -> None:
        """Queue the configured write command onto a pipeline (sync)."""
        if self._mode == "set":
            if self._ttl is not None:
                pipe.set(key, value, ex=self._ttl)
            else:
                pipe.set(key, value)
        elif self._mode == "lpush":
            pipe.lpush(key, value)
        elif self._mode == "rpush":
            pipe.rpush(key, value)
        else:  # xadd
            if not isinstance(value, dict):
                raise TypeError("RedisSink mode='xadd' requires serializer to return a dict")
            pipe.xadd(key, value, **self._xadd_kwargs)

    async def _client_command(self, key: str, value: Any) -> None:
        """Execute the configured write command directly on the async client."""
        client = self._require_client()
        if self._mode == "set":
            if self._ttl is not None:
                await client.set(key, value, ex=self._ttl)
            else:
                await client.set(key, value)
        elif self._mode == "lpush":
            await client.lpush(key, value)
        elif self._mode == "rpush":
            await client.rpush(key, value)
        elif self._mode == "xadd":
            if not isinstance(value, dict):
                raise TypeError("RedisSink mode='xadd' requires serializer to return a dict")
            xadd_kwargs: dict[str, Any] = {}
            if self._maxlen is not None:
                xadd_kwargs["maxlen"] = self._maxlen
                xadd_kwargs["approximate"] = True
            await client.xadd(key, value, **xadd_kwargs)

    async def write(self, record: T) -> None:
        if self._client is None:
            raise RuntimeError("RedisSink.open() was not called")
        key = self._key_fn(record)
        value = self._serializer(record)
        await self._client_command(key, value)
        logger.debug("redis_sink_write", mode=self._mode, key=key)

    async def write_batch(self, records: list[T]) -> None:
        client = self._require_client()
        if not records:
            return
        if self._mode == "set" and self._ttl is None:
            mapping = {self._key_fn(record): self._serializer(record) for record in records}
            await client.mset(mapping)
            logger.debug("redis_sink_write_batch_mset", count=len(mapping))
            return
        async with client.pipeline(transaction=False) as pipe:
            for record in records:
                key = self._key_fn(record)
                value = self._serializer(record)
                self._pipe_command(pipe, key, value)
            await pipe.execute()
        logger.debug("redis_sink_write_batch", mode=self._mode, count=len(records))

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisSink.open() was not called")
        return self._client


def _default_serializer(record: Any) -> Any:
    if hasattr(record, "model_dump_json"):
        return record.model_dump_json()
    if hasattr(record, "model_dump"):
        import json

        return json.dumps(record.model_dump(), ensure_ascii=False, default=str)
    if hasattr(record, "__dict__"):
        import json

        return json.dumps(record.__dict__, ensure_ascii=False, default=str)
    return str(record)


__all__ = ["RedisSink"]
