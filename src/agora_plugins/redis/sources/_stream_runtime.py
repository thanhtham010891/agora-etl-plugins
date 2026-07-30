"""Read-loop and reclaim helpers for Redis stream sources."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from agora.core.source import SourceRecordError
from agora.core.types import SourceRecordFailurePolicy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Mapping
    from datetime import datetime


class RedisReadLoopRuntime:
    """Owns the main stream loop, reclaim reads, and message-yield behavior."""

    def __init__(
        self,
        source: Any,
        *,
        now_utc: Callable[[], datetime],
    ) -> None:
        self._source = source
        self._now_utc = now_utc

    async def stream(self) -> AsyncGenerator[Any, None]:
        self._source._record_error_count = 0
        self._source._record_drop_count = 0
        await self._source._apply_resume_checkpoint(self._source._require_client())

        self._source.logger.info(
            "redis_stream_source_ready",
            stream=self._source._stream,
            group=self._source._group,
            consumer=self._source._consumer,
        )

        while True:
            if self.should_read_reclaimed_messages():
                reclaimed_entries = await self.read_reclaimed_messages()
                if reclaimed_entries:
                    async for record in self.yield_entries(reclaimed_entries):
                        yield record
                    continue

            stream_id = (
                self._source._resume_cursor
                if self._source._resume_pending and self._source._resume_cursor
                else ">"
            )
            try:
                client = self._source._require_client()
                self._source._read_call_count += 1
                entries = await client.xreadgroup(
                    self._source._group,
                    self._source._consumer,
                    {self._source._stream: stream_id},
                    count=self._source._batch_size,
                    block=self._source._block_ms,
                )
                self._source._last_read_at = self._now_utc()
            except asyncio.CancelledError:
                await self._source._flush_pending_acks()
                self._source.logger.info(
                    "redis_stream_source_cancelled",
                    stream=self._source._stream,
                )
                raise
            except Exception as exc:
                if await self._source._recover_from_connection_error(exc, context="read"):
                    continue
                self._source._remember_error(exc)
                self._source.logger.exception(
                    "redis_stream_read_error",
                    stream=self._source._stream,
                )
                raise

            if not entries:
                if self._source._resume_pending:
                    self._source._resume_pending = False
                    continue
                continue

            async for record in self.yield_entries(entries):
                yield record

    async def read_reclaimed_messages(
        self,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        if (
            self._source._client is None
            or self._source._reclaim_idle_ms is None
            or self._source._resume_pending
        ):
            self._source._consecutive_reclaim_batch_count = 0
            return []

        xautoclaim = getattr(self._source._client, "xautoclaim", None)
        if not callable(xautoclaim):
            self._source._consecutive_reclaim_batch_count = 0
            return []

        try:
            response = await xautoclaim(
                self._source._stream,
                self._source._group,
                self._source._consumer,
                self._source._reclaim_idle_ms,
                self._source._reclaim_cursor,
                count=self._source._reclaim_batch_size,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if await self._source._recover_from_connection_error(exc, context="reclaim"):
                self._source._consecutive_reclaim_batch_count = 0
                return []
            self._source.logger.exception(
                "redis_stream_reclaim_error",
                stream=self._source._stream,
            )
            raise

        next_cursor, messages = self._source._parse_xautoclaim_response(response)
        self._source._reclaim_cursor = next_cursor
        if not messages:
            self._source._consecutive_reclaim_batch_count = 0
            return []
        self._source._reclaimed_message_count += len(messages)
        self._source._consecutive_reclaim_batch_count += 1
        self._source._last_reclaim_at = self._now_utc()
        self._source._active_reclaimed_message_ids = {
            self._source._normalize_message_id(message_id) for message_id, _fields in messages
        }

        self._source.logger.info(
            "redis_stream_reclaimed_messages",
            stream=self._source._stream,
            group=self._source._group,
            consumer=self._source._consumer,
            count=len(messages),
        )
        return [(self._source._stream, messages)]

    def should_read_reclaimed_messages(self) -> bool:
        if self._source._max_consecutive_reclaim_batches is None:
            return True
        if self._source._reclaim_idle_ms is None or self._source._resume_pending:
            return True
        if (
            self._source._consecutive_reclaim_batch_count
            < self._source._max_consecutive_reclaim_batches
        ):
            return True
        self._source._consecutive_reclaim_batch_count = 0
        self._source._reclaim_fairness_yield_count += 1
        self._source.logger.info(
            "redis_stream_reclaim_fairness_yield",
            stream=self._source._stream,
            group=self._source._group,
            consumer=self._source._consumer,
            max_consecutive_reclaim_batches=self._source._max_consecutive_reclaim_batches,
            fairness_yield_count=self._source._reclaim_fairness_yield_count,
        )
        return False

    async def yield_entries(
        self,
        entries: list[tuple[str, list[tuple[str, dict[str, str]]]]],
    ) -> AsyncGenerator[Any, None]:
        for _stream_name, messages in entries:
            for msg_id, fields in messages:
                normalized_message_id = self._source._normalize_message_id(msg_id)
                was_reclaimed = normalized_message_id in self._source._active_reclaimed_message_ids
                self._source._active_reclaimed_message_ids.discard(normalized_message_id)
                self._source._last_message_id = normalized_message_id
                if self._source._resume_pending:
                    self._source._resume_cursor = normalized_message_id
                try:
                    record = self._source._deserializer(fields)
                except Exception as exc:
                    self._source._remember_error(exc)
                    self._source._record_error_count += 1
                    if was_reclaimed and not self._source._will_ack_failed_record():
                        self._source._poison_loop_count += 1
                        self._source._poison_loop_message_ids.add(normalized_message_id)
                        self._source._last_poison_loop_message_id = normalized_message_id
                        self._source._last_poison_loop_at = self._now_utc()
                    self._source.logger.warning(
                        "redis_stream_deserialize_error",
                        stream=self._source._stream,
                        msg_id=msg_id,
                        error=str(exc),
                    )
                    if (
                        self._source._on_deserialize_error
                        == SourceRecordFailurePolicy.LOG_AND_CONTINUE
                    ):
                        self._source._record_drop_count += 1
                        await self._source._enqueue_ack(msg_id)
                        continue
                    on_handled = (
                        self._source._build_failure_ack_callback(msg_id) if was_reclaimed else None
                    )
                    await self._source._flush_pending_acks()
                    source_error = SourceRecordError(
                        exc,
                        record={
                            "message_id": normalized_message_id,
                            "fields": self._source._normalize_error_fields(
                                cast("Mapping[str | bytes, Any]", fields)
                            ),
                        },
                        checkpoint=self._source.current_checkpoint(),
                        source=self._source.source_name,
                    )
                    if on_handled is not None:
                        object.__setattr__(source_error, "on_handled", on_handled)
                    raise source_error from exc

                self._source._delivery_success_hook = self._source._build_ack_callback(msg_id)
                self._source._delivery_context = self._source._build_delivery_context(
                    normalized_message_id
                )
                self._source._emitted_record_count += 1
                try:
                    yield record
                finally:
                    self._source._delivery_success_hook = None
                    self._source._delivery_context = None
