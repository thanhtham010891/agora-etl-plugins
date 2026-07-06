"""Redis-backed DLQ sink/source implementations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import logstruct
from agora.core.dlq import DLQRecord, DLQSink, DLQSource

from agora_plugins.redis._dlq_keyspace import RedisDLQKeyspace
from agora_plugins.redis._dlq_payloads import (
    hash_to_record as _hash_to_record,
)
from agora_plugins.redis._dlq_runtime import RedisDLQSinkRuntime, RedisDLQSourceRuntime
from agora_plugins.redis._dlq_sink_surface import RedisDLQSinkSurface
from agora_plugins.redis._dlq_source_surface import RedisDLQSourceSurface
from agora_plugins.redis.connection import build_async_redis_client
from agora_plugins.redis.observability import (
    RedisDLQSinkEnterpriseAcceptanceThresholds,
    RedisDLQSinkMetricsSnapshot,
    RedisDLQSourceEnterpriseAcceptanceThresholds,
    RedisDLQSourceMetricsSnapshot,
    RedisEnterpriseAcceptanceGate,
    RedisEnterpriseAcceptanceReport,
    RedisPrometheusExporter,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora_plugins.dlq_policy import DLQPayloadPolicy

logger = logstruct.getLogger(__name__)

_DLQ_UPSERT_SCRIPT = """
-- REDIS_DLQ_UPSERT
local record_key = KEYS[1]
local primary_index_key = KEYS[2]
local pipeline_index_key = KEYS[3]
local stage_index_key = KEYS[4]
local pipeline_stage_index_key = KEYS[5]
local field_count = tonumber(ARGV[1])
local exists = redis.call("EXISTS", record_key) == 1
local old_pipeline_index_key = redis.call("HGET", record_key, "__pipeline_index_key")
local old_stage_index_key = redis.call("HGET", record_key, "__stage_index_key")
local old_pipeline_stage_index_key = redis.call("HGET", record_key, "__pipeline_stage_index_key")
local hash_args = {}
for idx = 1, field_count * 2 do
    hash_args[idx] = ARGV[idx + 1]
end
redis.call("HSET", record_key, unpack(hash_args))
if not exists then
    redis.call("RPUSH", primary_index_key, record_key)
    redis.call("RPUSH", pipeline_index_key, record_key)
    redis.call("RPUSH", stage_index_key, record_key)
    redis.call("RPUSH", pipeline_stage_index_key, record_key)
    return 1
end
local seen = {}
for _, index_key in ipairs({
    old_pipeline_index_key,
    old_stage_index_key,
    old_pipeline_stage_index_key,
    pipeline_index_key,
    stage_index_key,
    pipeline_stage_index_key
}) do
    if index_key and index_key ~= "" and not seen[index_key] then
        redis.call("LREM", index_key, 0, record_key)
        seen[index_key] = true
    end
end
redis.call("RPUSH", pipeline_index_key, record_key)
redis.call("RPUSH", stage_index_key, record_key)
redis.call("RPUSH", pipeline_stage_index_key, record_key)
return 0
"""

_DLQ_ACKNOWLEDGE_SCRIPT = """
-- REDIS_DLQ_ACKNOWLEDGE
local record_key = KEYS[1]
local primary_index_key = KEYS[2]
local pipeline_index_key = redis.call("HGET", record_key, "__pipeline_index_key") or KEYS[3]
local stage_index_key = redis.call("HGET", record_key, "__stage_index_key") or KEYS[4]
local pipeline_stage_index_key = redis.call("HGET", record_key, "__pipeline_stage_index_key") or KEYS[5]
redis.call("DEL", record_key)
redis.call("LREM", primary_index_key, 0, record_key)
if pipeline_index_key ~= "" then
    redis.call("LREM", pipeline_index_key, 0, record_key)
end
if stage_index_key ~= "" then
    redis.call("LREM", stage_index_key, 0, record_key)
end
if pipeline_stage_index_key ~= "" then
    redis.call("LREM", pipeline_stage_index_key, 0, record_key)
end
return 1
"""


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value)


def _has_redis_hash_tag(value: str) -> bool:
    open_brace = value.find("{")
    if open_brace == -1:
        return False
    close_brace = value.find("}", open_brace + 1)
    return close_brace > open_brace + 1


def _cluster_storage_prefix(key_prefix: str, *, redis_cluster: bool) -> str:
    if not redis_cluster or _has_redis_hash_tag(key_prefix):
        return key_prefix
    return f"{key_prefix}:{{{key_prefix}}}"


class RedisDLQSink(RedisDLQSinkRuntime, DLQSink):
    """Store DLQ records in Redis hashes plus an ordered index list."""

    sink_name = "redis_dlq"

    def __init__(
        self,
        url: str,
        key_prefix: str = "agora:dlq",
        *,
        redis_cluster: bool = False,
        sentinel_service_name: str | None = None,
        sentinel_urls: list[str] | None = None,
        payload_policy: DLQPayloadPolicy | None = None,
    ) -> None:
        self._url = url
        self._key_prefix = key_prefix.rstrip(":")
        self._storage_key_prefix = _cluster_storage_prefix(
            self._key_prefix,
            redis_cluster=redis_cluster,
        )
        self._redis_cluster = redis_cluster
        self._sentinel_service_name = sentinel_service_name
        self._sentinel_urls = list(sentinel_urls or [])
        self._payload_policy = payload_policy
        self._keyspace = RedisDLQKeyspace(
            key_prefix=self._key_prefix,
            storage_key_prefix=self._storage_key_prefix,
        )
        self._client: Any | None = None
        self._upsert_script: Any | None = None
        self._acknowledge_script: Any | None = None
        self._write_call_count = 0
        self._write_batch_call_count = 0
        self._inserted_record_count = 0
        self._upserted_record_count = 0
        self._updated_record_count = 0
        self._replay_count = 0
        self._replayed_record_count = 0
        self._acknowledge_count = 0
        self._acknowledged_record_count = 0
        self._last_write_at: datetime | None = None
        self._last_replay_at: datetime | None = None
        self._last_acknowledge_at: datetime | None = None
        self._surface = RedisDLQSinkSurface(
            self,
            now_utc=_now_utc,
            snapshot_cls=RedisDLQSinkMetricsSnapshot,
            acceptance_gate_cls=RedisEnterpriseAcceptanceGate,
            exporter_cls=RedisPrometheusExporter,
        )

    async def open(self) -> None:
        try:
            __import__("redis.asyncio")
        except ImportError:
            raise ImportError(
                "RedisDLQSink requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = await build_async_redis_client(
            url=self._url,
            decode_responses=True,
            redis_cluster=self._redis_cluster,
            sentinel_service_name=self._sentinel_service_name,
            sentinel_urls=self._sentinel_urls,
        )
        self._upsert_script = self._client.register_script(_DLQ_UPSERT_SCRIPT)
        self._acknowledge_script = self._client.register_script(_DLQ_ACKNOWLEDGE_SCRIPT)
        logger.info("redis_dlq_ready", prefix=self._key_prefix)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def write(self, record: DLQRecord) -> None:
        await self._surface.write(record)

    async def write_batch(self, records: list[DLQRecord]) -> None:
        await self._surface.write_batch(records)

    async def replay(self, record: DLQRecord) -> DLQRecord:
        return cast(
            "DLQRecord",
            await self._surface.replay(
                record,
                replay_record=lambda candidate: super(RedisDLQSink, self).replay(candidate),
            ),
        )

    async def acknowledge(self, record: DLQRecord) -> None:
        await self._surface.acknowledge(record)

    def metrics_snapshot(self) -> RedisDLQSinkMetricsSnapshot:
        return cast("RedisDLQSinkMetricsSnapshot", self._surface.metrics_snapshot())

    def acceptance_report(
        self,
        thresholds: RedisDLQSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        return cast("RedisEnterpriseAcceptanceReport", self._surface.acceptance_report(thresholds))

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return self._surface.render_prometheus_metrics(namespace=namespace)


class RedisDLQSource(RedisDLQSourceRuntime, DLQSource):
    """Read DLQ records from Redis hashes using an ordered index list."""

    source_name = "redis_dlq_source"

    def __init__(
        self,
        url: str,
        key_prefix: str = "agora:dlq",
        *,
        pipeline_id: str | None = None,
        stage: str | None = None,
        limit: int | None = None,
        redis_cluster: bool = False,
        sentinel_service_name: str | None = None,
        sentinel_urls: list[str] | None = None,
        payload_policy: DLQPayloadPolicy | None = None,
    ) -> None:
        self._url = url
        self._key_prefix = key_prefix.rstrip(":")
        self._storage_key_prefix = _cluster_storage_prefix(
            self._key_prefix,
            redis_cluster=redis_cluster,
        )
        self._pipeline_id = pipeline_id
        self._stage = stage
        self._limit = limit
        self._redis_cluster = redis_cluster
        self._sentinel_service_name = sentinel_service_name
        self._sentinel_urls = list(sentinel_urls or [])
        self._payload_policy = payload_policy
        self._keyspace = RedisDLQKeyspace(
            key_prefix=self._key_prefix,
            storage_key_prefix=self._storage_key_prefix,
        )
        self._client: Any | None = None
        self._scan_count = 0
        self._emitted_record_count = 0
        self._last_scan_at: datetime | None = None
        self._last_record_at: datetime | None = None
        self._surface = RedisDLQSourceSurface(
            self,
            now_utc=_now_utc,
            hash_to_record=_hash_to_record,
            snapshot_cls=RedisDLQSourceMetricsSnapshot,
            acceptance_gate_cls=RedisEnterpriseAcceptanceGate,
            exporter_cls=RedisPrometheusExporter,
        )

    async def open(self) -> None:
        try:
            __import__("redis.asyncio")
        except ImportError:
            raise ImportError(
                "RedisDLQSource requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = await build_async_redis_client(
            url=self._url,
            decode_responses=True,
            redis_cluster=self._redis_cluster,
            sentinel_service_name=self._sentinel_service_name,
            sentinel_urls=self._sentinel_urls,
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        async for record in self._surface.iter_records():
            yield record

    def metrics_snapshot(self) -> RedisDLQSourceMetricsSnapshot:
        return cast("RedisDLQSourceMetricsSnapshot", self._surface.metrics_snapshot())

    def acceptance_report(
        self,
        thresholds: RedisDLQSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        return cast("RedisEnterpriseAcceptanceReport", self._surface.acceptance_report(thresholds))

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return self._surface.render_prometheus_metrics(namespace=namespace)


__all__ = ["RedisDLQSink", "RedisDLQSource"]
