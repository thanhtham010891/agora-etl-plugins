"""
agora_plugins.redis.sinks.redis
=======================
``RedisSink[T]`` — write records to Redis (SET / LPUSH / XADD).

Requires: ``pip install 'agora-etl-plugins[redis]'``
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from urllib.parse import urlparse

import logstruct
from agora.core.retry import RetryPolicy, retry_async
from agora.core.sink import BaseSink

from agora_plugins.redis.connection import build_async_redis_client
from agora_plugins.redis.observability import (
    RedisEnterpriseAcceptanceGate,
    RedisPrometheusExporter,
    RedisSinkEnterpriseAcceptanceThresholds,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora_plugins.redis.connection import RedisClusterAddressRemap

T = TypeVar("T")
logger = logstruct.getLogger(__name__)

_MODES = {"set", "lpush", "rpush", "xadd"}
_LIST_IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60
_LIST_WRITE_SCRIPT = """
local marker_set = redis.call("SET", KEYS[2], "1", "NX", "EX", tonumber(ARGV[4]))
if marker_set == false then
    return 0
end

local maxlen = tonumber(ARGV[3])
if ARGV[1] == "lpush" then
    redis.call("LPUSH", KEYS[1], ARGV[2])
    if maxlen > 0 then
        redis.call("LTRIM", KEYS[1], 0, maxlen - 1)
    end
elseif ARGV[1] == "rpush" then
    redis.call("RPUSH", KEYS[1], ARGV[2])
    if maxlen > 0 then
        redis.call("LTRIM", KEYS[1], -maxlen, -1)
    end
else
    error("unsupported Redis list mode: " .. ARGV[1])
end
return 1
"""


def _redis_retry_exceptions() -> tuple[type[Exception], ...]:
    try:
        from redis.exceptions import BusyLoadingError, ConnectionError, ReadOnlyError, TimeoutError
    except ImportError:
        return ()
    return (BusyLoadingError, ConnectionError, TimeoutError, ReadOnlyError)


@dataclass(frozen=True, slots=True)
class RedisSinkMetricsSnapshot:
    """Operational snapshot for Redis sink write behavior."""

    target: str
    mode: str
    ttl_seconds: int | None
    maxlen: int | None
    connection_ready: bool = False
    write_call_count: int = 0
    write_batch_call_count: int = 0
    direct_write_count: int = 0
    mset_batch_count: int = 0
    pipeline_execute_count: int = 0
    written_record_count: int = 0
    accepted_record_count: int = 0
    redis_mutation_count: int = 0
    last_write_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "mode": self.mode,
            "ttl_seconds": self.ttl_seconds,
            "maxlen": self.maxlen,
            "connection_ready": self.connection_ready,
            "write_call_count": self.write_call_count,
            "write_batch_call_count": self.write_batch_call_count,
            "direct_write_count": self.direct_write_count,
            "mset_batch_count": self.mset_batch_count,
            "pipeline_execute_count": self.pipeline_execute_count,
            "written_record_count": self.written_record_count,
            "accepted_record_count": self.accepted_record_count,
            "redis_mutation_count": self.redis_mutation_count,
            "last_write_at": None if self.last_write_at is None else self.last_write_at.isoformat(),
        }


def _now_utc() -> datetime:
    return datetime.now(UTC)


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


def _metrics_target(url: str) -> str:
    parsed = urlparse(url)
    hostname = parsed.hostname or parsed.netloc or "unknown"
    port = f":{parsed.port}" if parsed.port else ""
    database = parsed.path.lstrip("/")
    return f"{hostname}{port}" + (f"/{database}" if database else "")


def _redis_hash_tag(key: str) -> str | None:
    start = key.find("{")
    if start < 0:
        return None
    end = key.find("}", start + 1)
    if end <= start + 1:
        return None
    return key[start + 1 : end]


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
        redis_cluster: bool = False,
        redis_cluster_address_remap: RedisClusterAddressRemap | None = None,
        sentinel_service_name: str | None = None,
        sentinel_urls: list[str] | None = None,
        retry_policy: RetryPolicy[Any] | None = None,
    ) -> None:
        if mode not in _MODES:
            raise ValueError(f"RedisSink: invalid mode {mode!r}. Choose from {_MODES}")
        if ttl_seconds is not None and mode != "set":
            raise ValueError(
                f"RedisSink: ttl_seconds is only supported for mode='set', got {mode!r}"
            )
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("RedisSink: ttl_seconds must be > 0 when provided.")
        if maxlen is not None and maxlen <= 0:
            raise ValueError("RedisSink: maxlen must be > 0 when provided.")
        self._url = url
        self._key_fn = key_fn
        self._serializer = serializer or _default_serializer
        self._mode = mode
        self._ttl = ttl_seconds
        self._maxlen = maxlen
        self._redis_cluster = redis_cluster
        self._redis_cluster_address_remap = redis_cluster_address_remap
        self._sentinel_service_name = sentinel_service_name
        self._sentinel_urls = list(sentinel_urls or [])
        self._retry_policy = retry_policy
        self._client: Any | None = None
        self._xadd_kwargs: dict[str, Any] = {}
        self._write_call_count = 0
        self._write_batch_call_count = 0
        self._direct_write_count = 0
        self._mset_batch_count = 0
        self._pipeline_execute_count = 0
        self._written_record_count = 0
        self._accepted_record_count = 0
        self._redis_mutation_count = 0
        self._last_write_at: datetime | None = None
        if maxlen is not None:
            self._xadd_kwargs = {"maxlen": maxlen, "approximate": True}

    async def open(self) -> None:
        try:
            __import__("redis.asyncio")
        except ImportError:
            raise ImportError(
                "RedisSink requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = await build_async_redis_client(
            url=self._url,
            decode_responses=False,
            redis_cluster=self._redis_cluster,
            redis_cluster_address_remap=self._redis_cluster_address_remap,
            sentinel_service_name=self._sentinel_service_name,
            sentinel_urls=self._sentinel_urls,
        )
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
        elif self._mode == "lpush" or self._mode == "rpush":
            self._pipe_list_command(pipe, key, value, operation_id=self._new_operation_id())
        else:  # xadd
            if not isinstance(value, dict):
                raise TypeError("RedisSink mode='xadd' requires serializer to return a dict")
            pipe.xadd(key, value, **self._xadd_kwargs)

    def _pipe_list_command(
        self,
        pipe: Any,
        key: str,
        value: Any,
        *,
        operation_id: str,
    ) -> None:
        pipe.eval(
            _LIST_WRITE_SCRIPT,
            2,
            key,
            self._list_idempotency_key(key, operation_id),
            self._mode,
            value,
            self._maxlen or 0,
            _LIST_IDEMPOTENCY_TTL_SECONDS,
        )

    async def _client_command(self, key: str, value: Any) -> None:
        """Execute the configured write command directly on the async client."""
        client = self._require_client()
        if self._mode == "set":
            if self._ttl is not None:
                await client.set(key, value, ex=self._ttl)
            else:
                await client.set(key, value)
        elif self._mode == "lpush" or self._mode == "rpush":
            await self._client_list_command(
                client, key, value, operation_id=self._new_operation_id()
            )
        elif self._mode == "xadd":
            if not isinstance(value, dict):
                raise TypeError("RedisSink mode='xadd' requires serializer to return a dict")
            xadd_kwargs: dict[str, Any] = {}
            if self._maxlen is not None:
                xadd_kwargs["maxlen"] = self._maxlen
                xadd_kwargs["approximate"] = True
            await client.xadd(key, value, **xadd_kwargs)

    async def _client_list_command(
        self,
        client: Any,
        key: str,
        value: Any,
        *,
        operation_id: str,
    ) -> None:
        await client.eval(
            _LIST_WRITE_SCRIPT,
            2,
            key,
            self._list_idempotency_key(key, operation_id),
            self._mode,
            value,
            self._maxlen or 0,
            _LIST_IDEMPOTENCY_TTL_SECONDS,
        )

    async def write(self, record: T) -> None:
        if self._client is None:
            raise RuntimeError("RedisSink.open() was not called")
        key = self._key_fn(record)
        value = self._serializer(record)
        self._write_call_count += 1
        if self._mode in {"lpush", "rpush"}:
            operation_id = self._new_operation_id()
            await self._retry_redis_command(
                lambda: self._client_list_command(
                    self._require_client(),
                    key,
                    value,
                    operation_id=operation_id,
                ),
                context="write",
            )
        else:
            await self._retry_redis_command(
                lambda: self._client_command(key, value),
                context="write",
            )
        self._direct_write_count += 1
        self._written_record_count += 1
        self._accepted_record_count += 1
        self._redis_mutation_count += 1
        self._last_write_at = _now_utc()
        logger.debug("redis_sink_write", mode=self._mode, key=key)

    async def write_batch(self, records: list[T]) -> None:
        self._require_client()
        self._write_batch_call_count += 1
        if not records:
            return
        if self._mode == "set" and self._ttl is None and not self._redis_cluster:
            mapping = {self._key_fn(record): self._serializer(record) for record in records}
            await self._retry_redis_command(
                lambda: self._require_client().mset(mapping),
                context="write_batch_mset",
            )
            self._mset_batch_count += 1
            self._written_record_count += len(records)
            self._accepted_record_count += len(records)
            self._redis_mutation_count += len(mapping)
            self._last_write_at = _now_utc()
            logger.debug("redis_sink_write_batch_mset", count=len(mapping))
            return

        prepared_records = [
            (
                self._key_fn(record),
                self._serializer(record),
                self._new_operation_id(),
            )
            for record in records
        ]

        async def _execute_pipeline() -> None:
            retry_client = self._require_client()
            async with retry_client.pipeline(transaction=False) as pipe:
                for key, value, operation_id in prepared_records:
                    if self._mode in {"lpush", "rpush"}:
                        self._pipe_list_command(
                            pipe,
                            key,
                            value,
                            operation_id=operation_id,
                        )
                    else:
                        self._pipe_command(pipe, key, value)
                await pipe.execute()

        await self._retry_redis_command(_execute_pipeline, context="write_batch_pipeline")
        self._pipeline_execute_count += 1
        self._written_record_count += len(records)
        self._accepted_record_count += len(records)
        self._redis_mutation_count += len(records)
        self._last_write_at = _now_utc()
        logger.debug("redis_sink_write_batch", mode=self._mode, count=len(records))

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisSink.open() was not called")
        return self._client

    @staticmethod
    def _new_operation_id() -> str:
        return uuid.uuid4().hex

    @staticmethod
    def _list_idempotency_key(key: str, operation_id: str) -> str:
        tag = _redis_hash_tag(key)
        if tag is not None:
            return f"{{{tag}}}:agora:list-write:{operation_id}"
        if "{" not in key and "}" not in key:
            return f"{{{key}}}:agora:list-write:{operation_id}"
        return f"{key}:agora:list-write:{operation_id}"

    def _effective_retry_policy(self) -> RetryPolicy[Any]:
        if self._retry_policy is not None:
            return self._retry_policy
        retry_exceptions = _redis_retry_exceptions()
        if not retry_exceptions:
            return RetryPolicy[Any](max_attempts=1)
        return RetryPolicy[Any](
            max_attempts=3,
            initial_backoff_s=0.1,
            backoff_multiplier=2.0,
            max_backoff_s=1.0,
            jitter_ratio=0.2,
            retry_exceptions=retry_exceptions,
        )

    async def _retry_redis_command(
        self,
        operation: Callable[[], Any],
        *,
        context: str,
    ) -> Any:
        async def _run() -> Any:
            return await operation()

        async def _on_retry(attempt: int, exc: Exception, delay: float) -> None:
            logger.warning(
                "redis_sink_retrying_write",
                mode=self._mode,
                context=context,
                attempt=attempt,
                delay_s=delay,
                error=str(exc),
            )

        return await retry_async(
            _run,
            policy=self._effective_retry_policy(),
            on_retry=_on_retry,
        )

    def metrics_snapshot(self) -> RedisSinkMetricsSnapshot:
        return RedisSinkMetricsSnapshot(
            target=_metrics_target(self._url),
            mode=self._mode,
            ttl_seconds=self._ttl,
            maxlen=self._maxlen,
            connection_ready=self._client is not None,
            write_call_count=self._write_call_count,
            write_batch_call_count=self._write_batch_call_count,
            direct_write_count=self._direct_write_count,
            mset_batch_count=self._mset_batch_count,
            pipeline_execute_count=self._pipeline_execute_count,
            written_record_count=self._written_record_count,
            accepted_record_count=self._accepted_record_count,
            redis_mutation_count=self._redis_mutation_count,
            last_write_at=self._last_write_at,
        )

    def acceptance_report(
        self,
        thresholds: RedisSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> Any:
        return RedisEnterpriseAcceptanceGate().evaluate_sink(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return RedisPrometheusExporter(namespace=namespace).render_sink(self.metrics_snapshot())


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


__all__ = ["RedisSink", "RedisSinkMetricsSnapshot"]
