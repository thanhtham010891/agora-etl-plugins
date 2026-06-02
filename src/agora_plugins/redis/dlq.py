"""Redis-backed DLQ sink/source implementations."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import logstruct
from agora.core.dlq import DLQRecord, DLQSink, DLQSource

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

logger = logstruct.getLogger(__name__)


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value)


def _serialize_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _record_to_hash(record: DLQRecord) -> dict[str, str]:
    return {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": _serialize_value(record.record),
        "original_record": _serialize_value(record.original_record),
        "processed_record": _serialize_value(record.processed_record),
        "source": record.source or "",
        "checkpoint": _serialize_value(record.checkpoint),
        "middleware": record.middleware or "",
        "sink": record.sink or "",
        "created_at": record.created_at.isoformat(),
        "attempt": str(record.attempt),
        "max_attempts": "" if record.max_attempts is None else str(record.max_attempts),
    }


def _decode_json(value: str | None) -> Any:
    if value is None or value == "":
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _hash_to_record(payload: dict[str, str]) -> DLQRecord:
    return DLQRecord(
        pipeline_id=payload["pipeline_id"],
        run_id=payload["run_id"],
        stage=payload["stage"],
        error_type=payload["error_type"],
        error_message=payload["error_message"],
        record=_decode_json(payload.get("record")),
        original_record=_decode_json(payload.get("original_record")),
        processed_record=_decode_json(payload.get("processed_record")),
        source=payload.get("source") or None,
        checkpoint=_decode_json(payload.get("checkpoint")),
        middleware=payload.get("middleware") or None,
        sink=payload.get("sink") or None,
        created_at=_coerce_datetime(payload["created_at"]),
        attempt=int(payload.get("attempt", "0") or 0),
        max_attempts=(
            int(payload["max_attempts"]) if payload.get("max_attempts") not in (None, "") else None
        ),
        _storage_id=cast("Any", payload.get("storage_key")),
    )


class RedisDLQSink(DLQSink):
    """Store DLQ records in Redis hashes plus an ordered index list."""

    sink_name = "redis_dlq"

    def __init__(
        self,
        url: str,
        key_prefix: str = "agora:dlq",
    ) -> None:
        self._url = url
        self._key_prefix = key_prefix.rstrip(":")
        self._client: Any | None = None

    async def open(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "RedisDLQSink requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = cast("Any", aioredis.from_url(self._url, decode_responses=True))
        logger.info("redis_dlq_ready", prefix=self._key_prefix)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def write(self, record: DLQRecord) -> None:
        client = self._require_client()
        record_key = self._record_key(record)
        object.__setattr__(record, "_storage_id", record_key)
        payload = _record_to_hash(record)
        payload["storage_key"] = record_key
        async with client.pipeline(transaction=False) as pipe:
            pipe.hset(record_key, mapping=payload)
            pipe.rpush(self._index_key, record_key)
            await pipe.execute()

    async def write_batch(self, records: list[DLQRecord]) -> None:
        client = self._require_client()
        if not records:
            return
        async with client.pipeline(transaction=False) as pipe:
            for record in records:
                record_key = self._record_key(record)
                object.__setattr__(record, "_storage_id", record_key)
                payload = _record_to_hash(record)
                payload["storage_key"] = record_key
                pipe.hset(record_key, mapping=payload)
                pipe.rpush(self._index_key, record_key)
            await pipe.execute()

    async def replay(self, record: DLQRecord) -> DLQRecord:
        client = self._require_client()
        updated = await super().replay(record)
        await client.hset(self._record_key(record), mapping={"attempt": str(updated.attempt)})
        return updated

    async def acknowledge(self, record: DLQRecord) -> None:
        client = self._require_client()
        record_key = self._record_key(record)
        await client.delete(record_key)
        await client.lrem(self._index_key, 0, record_key)

    @property
    def _index_key(self) -> str:
        return f"{self._key_prefix}:__index__"

    def _record_key(self, record: DLQRecord) -> str:
        storage_id = record._storage_id
        if isinstance(storage_id, str) and storage_id:
            return storage_id
        return (
            f"{self._key_prefix}:{record.pipeline_id}:{record.run_id}:"
            f"{record.stage}:{record.created_at.isoformat()}:{uuid.uuid4().hex}"
        )

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
    ) -> None:
        self._url = url
        self._key_prefix = key_prefix.rstrip(":")
        self._pipeline_id = pipeline_id
        self._stage = stage
        self._limit = limit
        self._client: Any | None = None

    async def open(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "RedisDLQSource requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = cast("Any", aioredis.from_url(self._url, decode_responses=True))

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        client = self._require_client()

        yielded = 0
        chunk_size = 100
        start = 0
        while True:
            end = start + chunk_size - 1
            keys = await client.lrange(self._index_key, start, end)
            if not keys:
                return

            # Batch fetch chunked hashes to avoid N+1 round trips without
            # materializing the entire DLQ index into memory first.
            async with client.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.hgetall(key)
                payloads = await pipe.execute()

            for _record_key, payload in zip(keys, payloads, strict=True):
                if not payload:
                    continue
                record = _hash_to_record(payload)
                if self._pipeline_id is not None and record.pipeline_id != self._pipeline_id:
                    continue
                if self._stage is not None and record.stage != self._stage:
                    continue
                yield record
                yielded += 1
                if self._limit is not None and yielded >= self._limit:
                    return

            start += chunk_size

    @property
    def _index_key(self) -> str:
        return f"{self._key_prefix}:__index__"

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisDLQSource.open() was not called")
        return self._client


__all__ = ["RedisDLQSink", "RedisDLQSource"]
