"""Redis-backed DLQ sink/source implementations."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

import logstruct
from agora.core.dlq import DLQRecord, DLQSink, DLQSource

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

_UPSERT_INDEX_PIPELINE_FIELD = "__pipeline_index_key"
_UPSERT_INDEX_STAGE_FIELD = "__stage_index_key"
_UPSERT_INDEX_PIPELINE_STAGE_FIELD = "__pipeline_stage_index_key"

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


def _index_part(value: str) -> str:
    return quote(value, safe="")


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


def _serialize_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _record_to_payload(record: DLQRecord) -> dict[str, Any]:
    return {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": record.record,
        "original_record": record.original_record,
        "processed_record": record.processed_record,
        "source": record.source or "",
        "checkpoint": record.checkpoint,
        "details": record.details,
        "middleware": record.middleware or "",
        "sink": record.sink or "",
        "created_at": record.created_at.isoformat(),
        "attempt": record.attempt,
        "max_attempts": record.max_attempts,
    }


def _payload_to_hash(payload: dict[str, Any]) -> dict[str, str]:
    return {
        "pipeline_id": str(payload["pipeline_id"]),
        "run_id": str(payload["run_id"]),
        "stage": str(payload["stage"]),
        "error_type": str(payload["error_type"]),
        "error_message": str(payload["error_message"]),
        "record": _serialize_value(payload.get("record")),
        "original_record": _serialize_value(payload.get("original_record")),
        "processed_record": _serialize_value(payload.get("processed_record")),
        "source": str(payload.get("source") or ""),
        "checkpoint": _serialize_value(payload.get("checkpoint")),
        "details": _serialize_value(payload.get("details")),
        "middleware": str(payload.get("middleware") or ""),
        "sink": str(payload.get("sink") or ""),
        "created_at": _coerce_datetime(cast("datetime | str", payload["created_at"])).isoformat(),
        "attempt": str(int(payload.get("attempt", 0))),
        "max_attempts": (
            "" if payload.get("max_attempts") is None else str(int(payload["max_attempts"]))
        ),
    }


def _record_to_hash(
    record: DLQRecord,
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> dict[str, str]:
    payload = _record_to_payload(record)
    if payload_policy is not None:
        payload = payload_policy.apply(payload)
    record_hash = _payload_to_hash(payload)
    if payload_policy is not None and payload_policy.mode == "encrypted":
        record_hash.update(
            {
                "record": _serialize_value(payload_policy.encrypt_payload(payload)),
                "original_record": "",
                "processed_record": "",
                "checkpoint": "",
                "details": "",
            }
        )
    return record_hash


def _decode_json(value: str | None) -> Any:
    if value is None or value == "":
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _hash_to_payload(payload: dict[str, str]) -> dict[str, Any]:
    return {
        "pipeline_id": payload["pipeline_id"],
        "run_id": payload["run_id"],
        "stage": payload["stage"],
        "error_type": payload["error_type"],
        "error_message": payload["error_message"],
        "record": _decode_json(payload.get("record")),
        "original_record": _decode_json(payload.get("original_record")),
        "processed_record": _decode_json(payload.get("processed_record")),
        "source": payload.get("source") or None,
        "checkpoint": _decode_json(payload.get("checkpoint")),
        "details": _decode_json(payload.get("details")),
        "middleware": payload.get("middleware") or None,
        "sink": payload.get("sink") or None,
        "created_at": _coerce_datetime(payload["created_at"]),
        "attempt": int(payload.get("attempt", "0") or 0),
        "max_attempts": (
            int(payload["max_attempts"]) if payload.get("max_attempts") not in (None, "") else None
        ),
    }


def _payload_to_record(payload: dict[str, Any], *, storage_key: str | None = None) -> DLQRecord:
    return DLQRecord(
        pipeline_id=str(payload["pipeline_id"]),
        run_id=str(payload["run_id"]),
        stage=str(payload["stage"]),
        error_type=str(payload["error_type"]),
        error_message=str(payload["error_message"]),
        record=payload.get("record"),
        original_record=payload.get("original_record"),
        processed_record=payload.get("processed_record"),
        source=(str(payload["source"]) if payload.get("source") is not None else None),
        checkpoint=payload.get("checkpoint"),
        details=payload.get("details"),
        middleware=(str(payload["middleware"]) if payload.get("middleware") is not None else None),
        sink=(str(payload["sink"]) if payload.get("sink") is not None else None),
        created_at=_coerce_datetime(cast("datetime | str", payload["created_at"])),
        attempt=int(payload.get("attempt", 0)),
        max_attempts=(
            int(payload["max_attempts"]) if payload.get("max_attempts") is not None else None
        ),
        _storage_id=cast("Any", storage_key),
    )


def _hash_to_record(
    payload: dict[str, str],
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> DLQRecord:
    record_payload = _decode_json(payload.get("record"))
    storage_key = payload.get("storage_key")
    if isinstance(record_payload, dict) and record_payload.get("payload_encoding") == "encrypted":
        if payload_policy is None:
            raise ValueError("Encrypted Redis DLQ payload requires a DLQPayloadPolicy.")
        return _payload_to_record(
            payload_policy.decrypt_payload(record_payload),
            storage_key=storage_key,
        )
    return _payload_to_record(_hash_to_payload(payload), storage_key=storage_key)


class RedisDLQSink(DLQSink):
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
        client = self._require_client()
        self._write_call_count += 1
        record_key = self._record_key(record)
        object.__setattr__(record, "_storage_id", record_key)
        should_index = await self._write_record(client, record, record_key)
        self._upserted_record_count += 1
        if should_index:
            self._inserted_record_count += 1
        else:
            self._updated_record_count += 1
        self._last_write_at = _now_utc()

    async def write_batch(self, records: list[DLQRecord]) -> None:
        client = self._require_client()
        if not records:
            return
        self._write_batch_call_count += 1
        inserted_count = 0
        for record in records:
            record_key = self._record_key(record)
            object.__setattr__(record, "_storage_id", record_key)
            if await self._write_record(client, record, record_key):
                inserted_count += 1
        self._inserted_record_count += inserted_count
        self._updated_record_count += len(records) - inserted_count
        self._upserted_record_count += len(records)
        self._last_write_at = _now_utc()

    async def replay(self, record: DLQRecord) -> DLQRecord:
        client = self._require_client()
        record_key = self._existing_record_key(record)
        updated = await super().replay(record)
        object.__setattr__(updated, "_storage_id", record_key)
        await client.hset(record_key, mapping={"attempt": str(updated.attempt)})
        self._replay_count += 1
        self._replayed_record_count += 1
        self._last_replay_at = _now_utc()
        return updated

    async def acknowledge(self, record: DLQRecord) -> None:
        client = self._require_client()
        record_key = self._existing_record_key(record)
        await self._acknowledge_record(client, record, record_key)
        self._acknowledge_count += 1
        self._acknowledged_record_count += 1
        self._last_acknowledge_at = _now_utc()

    def metrics_snapshot(self) -> RedisDLQSinkMetricsSnapshot:
        return RedisDLQSinkMetricsSnapshot(
            key_prefix=self._key_prefix,
            connection_ready=self._client is not None,
            write_call_count=self._write_call_count,
            write_batch_call_count=self._write_batch_call_count,
            inserted_record_count=self._inserted_record_count,
            upserted_record_count=self._upserted_record_count,
            updated_record_count=self._updated_record_count,
            replay_count=self._replay_count,
            replayed_record_count=self._replayed_record_count,
            acknowledge_count=self._acknowledge_count,
            acknowledged_record_count=self._acknowledged_record_count,
            last_write_at=self._last_write_at,
            last_replay_at=self._last_replay_at,
            last_acknowledge_at=self._last_acknowledge_at,
        )

    def acceptance_report(
        self,
        thresholds: RedisDLQSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        return RedisEnterpriseAcceptanceGate().evaluate_dlq_sink(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return RedisPrometheusExporter(namespace=namespace).render_dlq_sink(self.metrics_snapshot())

    @property
    def _index_key(self) -> str:
        return self._index_key_for_prefix(self._storage_key_prefix)

    def _pipeline_index_key(self, pipeline_id: str) -> str:
        return self._pipeline_index_key_for_prefix(self._storage_key_prefix, pipeline_id)

    def _stage_index_key(self, stage: str) -> str:
        return self._stage_index_key_for_prefix(self._storage_key_prefix, stage)

    def _pipeline_stage_index_key(self, pipeline_id: str, stage: str) -> str:
        return self._pipeline_stage_index_key_for_prefix(
            self._storage_key_prefix,
            pipeline_id,
            stage,
        )

    def _index_key_for_prefix(self, key_prefix: str) -> str:
        return f"{key_prefix}:__index__"

    def _pipeline_index_key_for_prefix(self, key_prefix: str, pipeline_id: str) -> str:
        return f"{key_prefix}:__index__:pipeline:{_index_part(pipeline_id)}"

    def _stage_index_key_for_prefix(self, key_prefix: str, stage: str) -> str:
        return f"{key_prefix}:__index__:stage:{_index_part(stage)}"

    def _pipeline_stage_index_key_for_prefix(
        self,
        key_prefix: str,
        pipeline_id: str,
        stage: str,
    ) -> str:
        return (
            f"{key_prefix}:__index__:pipeline_stage:{_index_part(pipeline_id)}:{_index_part(stage)}"
        )

    def _secondary_index_keys(
        self, record: DLQRecord, *, key_prefix: str | None = None
    ) -> set[str]:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return {
            self._pipeline_index_key_for_prefix(prefix, record.pipeline_id),
            self._stage_index_key_for_prefix(prefix, record.stage),
            self._pipeline_stage_index_key_for_prefix(prefix, record.pipeline_id, record.stage),
        }

    def _secondary_index_keys_from_payload(
        self,
        payload: dict[str, str],
        *,
        key_prefix: str | None = None,
    ) -> set[str]:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        explicit_index_keys = {
            payload.get(_UPSERT_INDEX_PIPELINE_FIELD) or "",
            payload.get(_UPSERT_INDEX_STAGE_FIELD) or "",
            payload.get(_UPSERT_INDEX_PIPELINE_STAGE_FIELD) or "",
        } - {""}
        if explicit_index_keys:
            return explicit_index_keys
        pipeline_id = payload.get("pipeline_id")
        stage = payload.get("stage")
        if not pipeline_id or not stage:
            return set()
        return {
            self._pipeline_index_key_for_prefix(prefix, pipeline_id),
            self._stage_index_key_for_prefix(prefix, stage),
            self._pipeline_stage_index_key_for_prefix(prefix, pipeline_id, stage),
        }

    def _record_key(self, record: DLQRecord) -> str:
        storage_id = record._storage_id
        if isinstance(storage_id, str) and storage_id:
            return storage_id
        return (
            f"{self._storage_key_prefix}:{record.pipeline_id}:{record.run_id}:"
            f"{record.stage}:{record.created_at.isoformat()}:{uuid.uuid4().hex}"
        )

    def _existing_record_key(self, record: DLQRecord) -> str:
        storage_id = record._storage_id
        if isinstance(storage_id, str) and storage_id:
            return storage_id
        raise ValueError("RedisDLQSink replay/acknowledge requires a persisted storage key.")

    @staticmethod
    async def _record_payload(client: Any, record_key: str) -> dict[str, str]:
        return cast("dict[str, str]", await client.hgetall(record_key))

    async def _write_record(self, client: Any, record: DLQRecord, record_key: str) -> bool:
        index_prefix = self._index_prefix_for_record_key(record_key)
        payload = self._record_hash(record, record_key, index_prefix=index_prefix)
        secondary_index_keys = self._secondary_index_keys(record, key_prefix=index_prefix)
        if self._upsert_script is not None and index_prefix == self._storage_key_prefix:
            ordered_secondary_keys = self._ordered_secondary_index_keys(record)
            inserted = await self._upsert_script(
                keys=[record_key, self._index_key, *ordered_secondary_keys],
                args=self._flatten_hash_mapping(payload),
            )
            return int(inserted or 0) == 1

        existing_payload = await self._record_payload(client, record_key)
        should_index = not existing_payload
        async with client.pipeline(transaction=not self._redis_cluster) as pipe:
            pipe.hset(record_key, mapping=payload)
            if should_index:
                pipe.rpush(self._index_key_for_prefix(index_prefix), record_key)
                for index_key in secondary_index_keys:
                    pipe.rpush(index_key, record_key)
            else:
                for index_key in self._secondary_index_keys_from_payload(existing_payload) - (
                    secondary_index_keys
                ):
                    pipe.lrem(index_key, 0, record_key)
                for index_key in secondary_index_keys:
                    pipe.lrem(index_key, 0, record_key)
                    pipe.rpush(index_key, record_key)
            await pipe.execute()
        return should_index

    async def _acknowledge_record(self, client: Any, record: DLQRecord, record_key: str) -> None:
        existing_payload = await self._record_payload(client, record_key)
        index_prefix = self._index_prefix_for_record_key(record_key)
        if (
            self._acknowledge_script is not None
            and index_prefix == self._storage_key_prefix
            and (
                not self._redis_cluster or bool(existing_payload.get(_UPSERT_INDEX_PIPELINE_FIELD))
            )
        ):
            await self._acknowledge_script(
                keys=[record_key, self._index_key, *self._ordered_secondary_index_keys(record)],
                args=[],
            )
            return

        secondary_index_keys = (
            self._secondary_index_keys_from_payload(existing_payload, key_prefix=index_prefix)
            if existing_payload
            else self._secondary_index_keys(record, key_prefix=index_prefix)
        )
        async with client.pipeline(transaction=not self._redis_cluster) as pipe:
            pipe.delete(record_key)
            pipe.lrem(self._index_key_for_prefix(index_prefix), 0, record_key)
            for index_key in secondary_index_keys:
                pipe.lrem(index_key, 0, record_key)
            await pipe.execute()

    def _record_hash(
        self,
        record: DLQRecord,
        record_key: str,
        *,
        index_prefix: str,
    ) -> dict[str, str]:
        payload = _record_to_hash(record, payload_policy=self._payload_policy)
        payload["storage_key"] = record_key
        payload.update(self._index_metadata(record, index_prefix=index_prefix))
        return payload

    def _index_metadata(self, record: DLQRecord, *, index_prefix: str) -> dict[str, str]:
        pipeline_index_key, stage_index_key, pipeline_stage_index_key = (
            self._ordered_secondary_index_keys(record, key_prefix=index_prefix)
        )
        return {
            _UPSERT_INDEX_PIPELINE_FIELD: pipeline_index_key,
            _UPSERT_INDEX_STAGE_FIELD: stage_index_key,
            _UPSERT_INDEX_PIPELINE_STAGE_FIELD: pipeline_stage_index_key,
        }

    def _ordered_secondary_index_keys(
        self,
        record: DLQRecord,
        *,
        key_prefix: str | None = None,
    ) -> tuple[str, str, str]:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return (
            self._pipeline_index_key_for_prefix(prefix, record.pipeline_id),
            self._stage_index_key_for_prefix(prefix, record.stage),
            self._pipeline_stage_index_key_for_prefix(prefix, record.pipeline_id, record.stage),
        )

    def _index_prefix_for_record_key(self, record_key: str) -> str:
        if record_key.startswith(f"{self._storage_key_prefix}:"):
            return self._storage_key_prefix
        return self._key_prefix

    @staticmethod
    def _flatten_hash_mapping(mapping: dict[str, str]) -> list[str]:
        flattened: list[str] = [str(len(mapping))]
        for key, value in mapping.items():
            flattened.extend((key, value))
        return flattened

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisDLQSink.open() was not called")
        return self._client


class RedisDLQSource(DLQSource):
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
        self._client: Any | None = None
        self._scan_count = 0
        self._emitted_record_count = 0
        self._last_scan_at: datetime | None = None
        self._last_record_at: datetime | None = None

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
        client = self._require_client()
        self._scan_count += 1
        self._last_scan_at = _now_utc()

        chunk_size = 100
        yielded = 0
        index_key = await self._resolve_index_key(client)
        index_snapshot = await client.lrange(index_key, 0, -1)
        for start in range(0, len(index_snapshot), chunk_size):
            keys = index_snapshot[start : start + chunk_size]

            # Batch fetch chunked hashes to avoid N+1 round trips without
            # offset-based pagination over a mutable index list.
            async with client.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.hgetall(key)
                payloads = await pipe.execute()

            for _record_key, payload in zip(keys, payloads, strict=True):
                if not payload:
                    continue
                record = _hash_to_record(payload, payload_policy=self._payload_policy)
                if self._pipeline_id is not None and record.pipeline_id != self._pipeline_id:
                    continue
                if self._stage is not None and record.stage != self._stage:
                    continue
                yield record
                yielded += 1
                self._emitted_record_count += 1
                self._last_record_at = _now_utc()
                if self._limit is not None and yielded >= self._limit:
                    return

    @property
    def _index_key(self) -> str:
        return self._index_key_for_prefix(self._storage_key_prefix)

    def _pipeline_index_key(self, pipeline_id: str) -> str:
        return self._pipeline_index_key_for_prefix(self._storage_key_prefix, pipeline_id)

    def _stage_index_key(self, stage: str) -> str:
        return self._stage_index_key_for_prefix(self._storage_key_prefix, stage)

    def _pipeline_stage_index_key(self, pipeline_id: str, stage: str) -> str:
        return self._pipeline_stage_index_key_for_prefix(
            self._storage_key_prefix,
            pipeline_id,
            stage,
        )

    def _index_key_for_prefix(self, key_prefix: str) -> str:
        return f"{key_prefix}:__index__"

    def _pipeline_index_key_for_prefix(self, key_prefix: str, pipeline_id: str) -> str:
        return f"{key_prefix}:__index__:pipeline:{_index_part(pipeline_id)}"

    def _stage_index_key_for_prefix(self, key_prefix: str, stage: str) -> str:
        return f"{key_prefix}:__index__:stage:{_index_part(stage)}"

    def _pipeline_stage_index_key_for_prefix(
        self,
        key_prefix: str,
        pipeline_id: str,
        stage: str,
    ) -> str:
        return (
            f"{key_prefix}:__index__:pipeline_stage:{_index_part(pipeline_id)}:{_index_part(stage)}"
        )

    async def _resolve_index_key(self, client: Any) -> str:
        preferred = self._preferred_index_key(self._storage_key_prefix)
        if await client.exists(preferred):
            return preferred
        legacy_preferred = self._preferred_index_key(self._key_prefix)
        if legacy_preferred != preferred and await client.exists(legacy_preferred):
            return legacy_preferred
        if preferred == self._index_key:
            return preferred
        if await client.exists(self._index_key):
            return self._index_key
        legacy_primary = self._index_key_for_prefix(self._key_prefix)
        if legacy_primary != self._index_key and await client.exists(legacy_primary):
            return legacy_primary
        return self._index_key

    def _preferred_index_key(self, key_prefix: str) -> str:
        if self._pipeline_id is not None and self._stage is not None:
            return self._pipeline_stage_index_key_for_prefix(
                key_prefix,
                self._pipeline_id,
                self._stage,
            )
        if self._pipeline_id is not None:
            return self._pipeline_index_key_for_prefix(key_prefix, self._pipeline_id)
        if self._stage is not None:
            return self._stage_index_key_for_prefix(key_prefix, self._stage)
        return self._index_key_for_prefix(key_prefix)

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisDLQSource.open() was not called")
        return self._client

    def metrics_snapshot(self) -> RedisDLQSourceMetricsSnapshot:
        return RedisDLQSourceMetricsSnapshot(
            key_prefix=self._key_prefix,
            pipeline_id=self._pipeline_id,
            stage=self._stage,
            limit=self._limit,
            connection_ready=self._client is not None,
            scan_count=self._scan_count,
            emitted_record_count=self._emitted_record_count,
            last_scan_at=self._last_scan_at,
            last_record_at=self._last_record_at,
        )

    def acceptance_report(
        self,
        thresholds: RedisDLQSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        return RedisEnterpriseAcceptanceGate().evaluate_dlq_source(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return RedisPrometheusExporter(namespace=namespace).render_dlq_source(
            self.metrics_snapshot()
        )


__all__ = ["RedisDLQSink", "RedisDLQSource"]
