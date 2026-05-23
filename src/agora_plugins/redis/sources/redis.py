"""
agora_plugins.redis.sources.redis
=========================
``RedisStreamSource[T]`` — async Redis Streams consumer (XREADGROUP).

Uses consumer groups for at-least-once delivery: messages are
acknowledged only after a successful ``yield``.

Requires: `pip install 'agora-etl-plugins[redis]'`

Usage::

    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="agora:ingest",
        group="pipeline-1",
        consumer="worker-1",
        deserializer=lambda fields: RawRecord(**fields),
        block_ms=2000,
    )
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct
from agora.core.source import BaseSource, SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from agora.core.checkpoint import Checkpoint

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class RedisStreamSource(BaseSource[T], Generic[T]):
    """Async Redis Streams consumer with XREADGROUP."""

    source_name = "redis_stream"
    supports_checkpoint = True

    def __init__(
        self,
        url: str,
        stream: str,
        group: str,
        consumer: str,
        deserializer: Callable[[dict[str, Any]], T] | None = None,
        block_ms: int = 2000,
        batch_size: int = 100,
        ack_on_success: bool = True,
        reclaim_idle_ms: int | None = None,
        reclaim_batch_size: int | None = None,
        on_deserialize_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
    ) -> None:
        self._url = url
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._deserializer: Callable[[dict[str, Any]], T] = (
            deserializer or (lambda d: d)  # type: ignore[return-value]
        )
        self._block_ms = block_ms
        self._batch_size = batch_size
        self._ack_on_success = ack_on_success
        self._reclaim_idle_ms = (
            max(1, int(reclaim_idle_ms)) if reclaim_idle_ms is not None else None
        )
        self._reclaim_batch_size = max(1, reclaim_batch_size or batch_size)
        self._on_deserialize_error = on_deserialize_error
        self._client = None
        self._last_message_id: str | None = None
        self._resume_cursor: str | None = None
        self._resume_pending = False
        self._reclaim_cursor = "0-0"
        self._record_error_count = 0
        self._record_drop_count = 0
        self._delivery_success_hook: Callable[[], Awaitable[None]] | None = None

    async def open(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "RedisStreamSource requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None

        self._client = aioredis.from_url(self._url, decode_responses=True)

        try:
            await self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
            logger.info(
                "redis_stream_group_created",
                stream=self._stream,
                group=self._group,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(
                    "redis_stream_group_exists",
                    stream=self._stream,
                    group=self._group,
                )
            else:
                raise

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.info("redis_stream_source_closed", stream=self._stream)

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        self._resume_pending = False
        self._resume_cursor = None
        if checkpoint is None or not isinstance(checkpoint.value, dict):
            return

        value = checkpoint.value
        message_id = value.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            return

        self._resume_cursor = message_id
        self._resume_pending = True

    async def stream(self) -> AsyncGenerator[T, None]:  # type: ignore[override]
        if self._client is None:
            raise RuntimeError("RedisStreamSource.open() was not called")
        self._record_error_count = 0
        self._record_drop_count = 0

        logger.info(
            "redis_stream_source_ready",
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
        )

        while True:
            reclaimed_entries = await self._read_reclaimed_messages()
            if reclaimed_entries:
                async for record in self._yield_entries(reclaimed_entries):
                    yield record
                continue

            stream_id = self._resume_cursor if self._resume_pending and self._resume_cursor else ">"
            try:
                entries = await self._client.xreadgroup(
                    self._group,
                    self._consumer,
                    {self._stream: stream_id},
                    count=self._batch_size,
                    block=self._block_ms,
                )
            except asyncio.CancelledError:
                logger.info("redis_stream_source_cancelled", stream=self._stream)
                raise
            except Exception:
                logger.exception("redis_stream_read_error", stream=self._stream)
                raise

            if not entries:
                if self._resume_pending:
                    self._resume_pending = False
                    continue
                continue

            async for record in self._yield_entries(entries):
                yield record

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._delivery_success_hook

    def current_checkpoint(self) -> dict[str, str] | None:
        if self._last_message_id is None:
            return None
        return {
            "stream": self._stream,
            "group": self._group,
            "consumer": self._consumer,
            "message_id": self._last_message_id,
        }

    async def _read_reclaimed_messages(
        self,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        if self._client is None or self._reclaim_idle_ms is None or self._resume_pending:
            return []

        xautoclaim = getattr(self._client, "xautoclaim", None)
        if not callable(xautoclaim):
            return []

        try:
            response = await xautoclaim(
                self._stream,
                self._group,
                self._consumer,
                self._reclaim_idle_ms,
                self._reclaim_cursor,
                count=self._reclaim_batch_size,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("redis_stream_reclaim_error", stream=self._stream)
            raise

        next_cursor, messages = self._parse_xautoclaim_response(response)
        self._reclaim_cursor = next_cursor
        if not messages:
            return []

        logger.info(
            "redis_stream_reclaimed_messages",
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
            count=len(messages),
        )
        return [(self._stream, messages)]

    async def _yield_entries(
        self,
        entries: list[tuple[str, list[tuple[str, dict[str, str]]]]],
    ) -> AsyncGenerator[T, None]:
        for _stream_name, messages in entries:
            for msg_id, fields in messages:
                self._last_message_id = msg_id
                if self._resume_pending:
                    self._resume_cursor = msg_id
                try:
                    record = self._deserializer(fields)
                except Exception as exc:
                    self._record_error_count += 1
                    logger.warning(
                        "redis_stream_deserialize_error",
                        stream=self._stream,
                        msg_id=msg_id,
                        error=str(exc),
                    )
                    if self._on_deserialize_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                        self._record_drop_count += 1
                        if self._ack_on_success:
                            await self._client.xack(self._stream, self._group, msg_id)
                        continue
                    raise SourceRecordError(
                        exc,
                        record={"message_id": msg_id, "fields": dict(fields)},
                        checkpoint=self.current_checkpoint(),
                        source=self.source_name,
                    ) from exc

                self._delivery_success_hook = self._build_ack_callback(msg_id)
                yield record

    def _build_ack_callback(self, msg_id: str) -> Callable[[], Awaitable[None]] | None:
        if not self._ack_on_success:
            return None

        async def _ack() -> None:
            if self._client is None:
                raise RuntimeError("RedisStreamSource client is closed — cannot ack message")
            await self._client.xack(self._stream, self._group, msg_id)

        return _ack

    @staticmethod
    def _parse_xautoclaim_response(
        response: object,
    ) -> tuple[str, list[tuple[str, dict[str, str]]]]:
        if not isinstance(response, (tuple, list)) or len(response) < 2:
            return "0-0", []

        next_cursor = response[0]
        messages = response[1]
        parsed_cursor = next_cursor if isinstance(next_cursor, str) and next_cursor else "0-0"
        if not isinstance(messages, list):
            return parsed_cursor, []
        return parsed_cursor, messages


__all__ = ["RedisStreamSource"]
