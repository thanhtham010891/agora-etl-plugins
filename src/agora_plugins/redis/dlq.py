"""Redis-backed DLQ sink/source implementations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

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
    if value in (None, ""):
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
        self._client = None

    async def open(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "RedisDLQSink requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = aioredis.from_url(self._url, decode_responses=True)
        logger.info("redis_dlq_ready", prefix=self._key_prefix)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def write(self, record: DLQRecord) -> None:
        if self._client is None:
            raise RuntimeError("RedisDLQSink.open() was not called")
        record_key = self._record_key(record)
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.hset(record_key, mapping=_record_to_hash(record))
            pipe.rpush(self._index_key, record_key)
            await pipe.execute()

    async def write_batch(self, records: list[DLQRecord]) -> None:
        if self._client is None:
            raise RuntimeError("RedisDLQSink.open() was not called")
        if not records:
            return
        async with self._client.pipeline(transaction=False) as pipe:
            for record in records:
                record_key = self._record_key(record)
                pipe.hset(record_key, mapping=_record_to_hash(record))
                pipe.rpush(self._index_key, record_key)
            await pipe.execute()

    async def replay(self, record: DLQRecord) -> DLQRecord:
        if self._client is None:
            raise RuntimeError("RedisDLQSink.open() was not called")
        updated = await super().replay(record)
        await self._client.hset(self._record_key(record), mapping={"attempt": str(updated.attempt)})
        return updated

    async def acknowledge(self, record: DLQRecord) -> None:
        if self._client is None:
            raise RuntimeError("RedisDLQSink.open() was not called")
        record_key = self._record_key(record)
        await self._client.delete(record_key)
        await self._client.lrem(self._index_key, 0, record_key)

    @property
    def _index_key(self) -> str:
        return f"{self._key_prefix}:__index__"

    def _record_key(self, record: DLQRecord) -> str:
        return (
            f"{self._key_prefix}:{record.pipeline_id}:{record.run_id}:"
            f"{record.stage}:{record.created_at.isoformat()}"
        )


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
        self._client = None

    async def open(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "RedisDLQSource requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        self._client = aioredis.from_url(self._url, decode_responses=True)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        if self._client is None:
            raise RuntimeError("RedisDLQSource.open() was not called")

        keys = await self._client.lrange(self._index_key, 0, -1)
        if not keys:
            return

        # Batch fetch all hashes in one pipeline — avoids N+1 round trips
        async with self._client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            payloads = await pipe.execute()

        yielded = 0
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
                break

    @property
    def _index_key(self) -> str:
        return f"{self._key_prefix}:__index__"


__all__ = ["RedisDLQSink", "RedisDLQSource"]
