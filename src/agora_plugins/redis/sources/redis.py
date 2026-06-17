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
import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct
from agora.core.retry import RetryPolicy
from agora.core.source import BaseSource, SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.redis.connection import build_async_redis_client
from agora_plugins.redis.observability import (
    RedisEnterpriseAcceptanceGate,
    RedisPrometheusExporter,
    RedisSourceEnterpriseAcceptanceThresholds,
    RedisSourcePoisonLoopRiskSnapshot,
    RedisStreamSourceHealthSnapshot,
    RedisStreamSourceMetricsSnapshot,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping

    from agora.core.checkpoint import Checkpoint

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(UTC)


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
        ack_batch_size: int | None = None,
        decode_responses: bool = True,
        reclaim_idle_ms: int | None = None,
        reclaim_batch_size: int | None = None,
        max_consecutive_reclaim_batches: int | None = None,
        on_deserialize_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        redis_cluster: bool = False,
        sentinel_service_name: str | None = None,
        sentinel_urls: list[str] | None = None,
        reconnect_retry_policy: RetryPolicy[Any] | None = None,
    ) -> None:
        self._url = url
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._deserializer: Callable[[dict[str, Any]], T]
        if deserializer is None:

            def _identity(payload: dict[str, Any]) -> T:
                return cast("T", payload)

            self._deserializer = _identity
        else:
            self._deserializer = deserializer
        self._block_ms = block_ms
        self._batch_size = batch_size
        self._ack_on_success = ack_on_success
        self._ack_batch_size = max(1, ack_batch_size or batch_size)
        self._decode_responses = decode_responses
        self._reclaim_idle_ms = (
            max(1, int(reclaim_idle_ms)) if reclaim_idle_ms is not None else None
        )
        self._reclaim_batch_size = max(1, reclaim_batch_size or batch_size)
        self._max_consecutive_reclaim_batches = (
            max(1, int(max_consecutive_reclaim_batches))
            if max_consecutive_reclaim_batches is not None
            else None
        )
        self._on_deserialize_error = on_deserialize_error
        self._redis_cluster = redis_cluster
        self._sentinel_service_name = sentinel_service_name
        self._sentinel_urls = list(sentinel_urls or [])
        self._reconnect_retry_policy = reconnect_retry_policy or RetryPolicy[Any](
            max_attempts=20,
            initial_backoff_s=0.25,
            backoff_multiplier=2.0,
            max_backoff_s=2.0,
            jitter_ratio=0.2,
            retry_exceptions=(Exception,),
        )
        self._client: Any | None = None
        self._last_message_id: str | None = None
        self._resume_cursor: str | None = None
        self._resume_pending = False
        self._resume_group_seek_pending = False
        self._reclaim_cursor = "0-0"
        self._record_error_count = 0
        self._record_drop_count = 0
        self._delivery_success_hook: Callable[[], Awaitable[None]] | None = None
        self._pending_ack_ids: list[str | bytes] = []
        self._checkpoint_cache: dict[str, str] | None = None
        self._group_ready = False
        self._read_call_count = 0
        self._reconnect_count = 0
        self._reclaimed_message_count = 0
        self._consecutive_reclaim_batch_count = 0
        self._reclaim_fairness_yield_count = 0
        self._ack_flush_count = 0
        self._acked_message_count = 0
        self._emitted_record_count = 0
        self._last_read_at: datetime | None = None
        self._last_reconnect_at: datetime | None = None
        self._last_ack_at: datetime | None = None
        self._last_reclaim_at: datetime | None = None
        self._active_reclaimed_message_ids: set[str] = set()
        self._poison_loop_message_ids: set[str] = set()
        self._poison_loop_count = 0
        self._last_poison_loop_message_id: str | None = None
        self._last_poison_loop_at: datetime | None = None
        self._last_error: str | None = None
        self._last_error_at: datetime | None = None

    async def open(self) -> None:
        self._client = await self._build_client()
        await self._ensure_group(self._require_client())

    async def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                await self._flush_pending_acks()
            await self._client.aclose()
            self._client = None
            self._group_ready = False
            logger.info("redis_stream_source_closed", stream=self._stream)

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        """Prepare to resume from a checkpoint.

        Redis Streams resume uses ``XGROUP SETID`` and is therefore scoped to a
        single-consumer group. When the group has more than one consumer,
        ``stream()`` fails before rewinding the group cursor.
        """

        self._resume_pending = False
        self._resume_group_seek_pending = False
        self._resume_cursor = None
        if checkpoint is None or not isinstance(checkpoint.value, dict):
            return

        value = checkpoint.value
        message_id = value.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            return

        self._resume_cursor = message_id
        self._resume_group_seek_pending = True

    async def stream(self) -> AsyncGenerator[T, None]:
        self._record_error_count = 0
        self._record_drop_count = 0
        await self._apply_resume_checkpoint(self._require_client())

        logger.info(
            "redis_stream_source_ready",
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
        )

        while True:
            if self._should_read_reclaimed_messages():
                reclaimed_entries = await self._read_reclaimed_messages()
                if reclaimed_entries:
                    async for record in self._yield_entries(reclaimed_entries):
                        yield record
                    continue

            stream_id = self._resume_cursor if self._resume_pending and self._resume_cursor else ">"
            try:
                client = self._require_client()
                self._read_call_count += 1
                entries = await client.xreadgroup(
                    self._group,
                    self._consumer,
                    {self._stream: stream_id},
                    count=self._batch_size,
                    block=self._block_ms,
                )
                self._last_read_at = _now_utc()
            except asyncio.CancelledError:
                await self._flush_pending_acks()
                logger.info("redis_stream_source_cancelled", stream=self._stream)
                raise
            except Exception as exc:
                if await self._recover_from_connection_error(exc, context="read"):
                    continue
                self._remember_error(exc)
                logger.exception("redis_stream_read_error", stream=self._stream)
                raise

            if not entries:
                if self._resume_pending:
                    self._resume_pending = False
                    continue
                continue

            async for record in self._yield_entries(entries):
                yield record

    async def _build_client(self) -> Any:
        try:
            __import__("redis.asyncio")
        except ImportError:
            raise ImportError(
                "RedisStreamSource requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        return await build_async_redis_client(
            url=self._url,
            decode_responses=self._decode_responses,
            redis_cluster=self._redis_cluster,
            sentinel_service_name=self._sentinel_service_name,
            sentinel_urls=self._sentinel_urls,
        )

    async def _ensure_group(self, client: Any) -> None:
        try:
            await client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
            self._group_ready = True
            logger.info(
                "redis_stream_group_created",
                stream=self._stream,
                group=self._group,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                self._group_ready = True
                logger.debug(
                    "redis_stream_group_exists",
                    stream=self._stream,
                    group=self._group,
                )
            else:
                self._remember_error(exc)
                raise

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def metrics_snapshot(self) -> RedisStreamSourceMetricsSnapshot:
        return RedisStreamSourceMetricsSnapshot(
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
            block_ms=self._block_ms,
            batch_size=self._batch_size,
            ack_batch_size=self._ack_batch_size,
            ack_on_success=self._ack_on_success,
            reclaim_idle_ms=self._reclaim_idle_ms,
            reclaim_batch_size=self._reclaim_batch_size,
            max_consecutive_reclaim_batches=self._max_consecutive_reclaim_batches,
            health=self.health_snapshot(),
            poison_loop_risk=RedisSourcePoisonLoopRiskSnapshot(
                detected=self._poison_loop_count > 0,
                loop_count=self._poison_loop_count,
                distinct_message_count=len(self._poison_loop_message_ids),
                last_message_id=self._last_poison_loop_message_id,
                last_detected_at=self._last_poison_loop_at,
            ),
            read_call_count=self._read_call_count,
            reconnect_count=self._reconnect_count,
            reclaimed_message_count=self._reclaimed_message_count,
            consecutive_reclaim_batch_count=self._consecutive_reclaim_batch_count,
            reclaim_fairness_yield_count=self._reclaim_fairness_yield_count,
            ack_flush_count=self._ack_flush_count,
            acked_message_count=self._acked_message_count,
            emitted_record_count=self._emitted_record_count,
            pending_ack_count=len(self._pending_ack_ids),
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
            last_message_id=self._last_message_id,
            last_read_at=self._last_read_at,
            last_reconnect_at=self._last_reconnect_at,
            last_ack_at=self._last_ack_at,
            last_reclaim_at=self._last_reclaim_at,
            last_error_at=self._last_error_at,
        )

    def health_snapshot(self) -> RedisStreamSourceHealthSnapshot:
        connection_ready = self._client is not None
        return RedisStreamSourceHealthSnapshot(
            ready=connection_ready and self._group_ready,
            connection_ready=connection_ready,
            group_ready=self._group_ready,
            ack_enabled=self._ack_on_success,
            reclaim_enabled=self._reclaim_idle_ms is not None,
            last_error=self._last_error,
        )

    def acceptance_report(
        self,
        thresholds: RedisSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> Any:
        return RedisEnterpriseAcceptanceGate().evaluate_source(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return RedisPrometheusExporter(namespace=namespace).render_source(self.metrics_snapshot())

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._delivery_success_hook

    def current_checkpoint(self) -> dict[str, str] | None:
        if self._last_message_id is None:
            return None
        if (
            self._checkpoint_cache is not None
            and self._checkpoint_cache["message_id"] == self._last_message_id
        ):
            return self._checkpoint_cache
        self._checkpoint_cache = {
            "stream": self._stream,
            "group": self._group,
            "consumer": self._consumer,
            "message_id": self._last_message_id,
        }
        return self._checkpoint_cache

    async def _read_reclaimed_messages(
        self,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        if self._client is None or self._reclaim_idle_ms is None or self._resume_pending:
            self._consecutive_reclaim_batch_count = 0
            return []

        xautoclaim = getattr(self._client, "xautoclaim", None)
        if not callable(xautoclaim):
            self._consecutive_reclaim_batch_count = 0
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
        except Exception as exc:
            if await self._recover_from_connection_error(exc, context="reclaim"):
                self._consecutive_reclaim_batch_count = 0
                return []
            logger.exception("redis_stream_reclaim_error", stream=self._stream)
            raise

        next_cursor, messages = self._parse_xautoclaim_response(response)
        self._reclaim_cursor = next_cursor
        if not messages:
            self._consecutive_reclaim_batch_count = 0
            return []
        self._reclaimed_message_count += len(messages)
        self._consecutive_reclaim_batch_count += 1
        self._last_reclaim_at = _now_utc()
        self._active_reclaimed_message_ids = {
            self._normalize_message_id(message_id) for message_id, _fields in messages
        }

        logger.info(
            "redis_stream_reclaimed_messages",
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
            count=len(messages),
        )
        return [(self._stream, messages)]

    def _should_read_reclaimed_messages(self) -> bool:
        if self._max_consecutive_reclaim_batches is None:
            return True
        if self._reclaim_idle_ms is None or self._resume_pending:
            return True
        if self._consecutive_reclaim_batch_count < self._max_consecutive_reclaim_batches:
            return True
        self._consecutive_reclaim_batch_count = 0
        self._reclaim_fairness_yield_count += 1
        logger.info(
            "redis_stream_reclaim_fairness_yield",
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
            max_consecutive_reclaim_batches=self._max_consecutive_reclaim_batches,
            fairness_yield_count=self._reclaim_fairness_yield_count,
        )
        return False

    async def _yield_entries(
        self,
        entries: list[tuple[str, list[tuple[str, dict[str, str]]]]],
    ) -> AsyncGenerator[T, None]:
        for _stream_name, messages in entries:
            for msg_id, fields in messages:
                normalized_message_id = self._normalize_message_id(msg_id)
                was_reclaimed = normalized_message_id in self._active_reclaimed_message_ids
                self._active_reclaimed_message_ids.discard(normalized_message_id)
                self._last_message_id = normalized_message_id
                if self._resume_pending:
                    self._resume_cursor = normalized_message_id
                try:
                    record = self._deserializer(fields)
                except Exception as exc:
                    self._remember_error(exc)
                    self._record_error_count += 1
                    if was_reclaimed and not self._will_ack_failed_record():
                        self._poison_loop_count += 1
                        self._poison_loop_message_ids.add(normalized_message_id)
                        self._last_poison_loop_message_id = normalized_message_id
                        self._last_poison_loop_at = _now_utc()
                    logger.warning(
                        "redis_stream_deserialize_error",
                        stream=self._stream,
                        msg_id=msg_id,
                        error=str(exc),
                    )
                    if self._on_deserialize_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                        self._record_drop_count += 1
                        if self._ack_on_success:
                            await self._enqueue_ack(msg_id)
                        continue
                    await self._flush_pending_acks()
                    raise SourceRecordError(
                        exc,
                        record={
                            "message_id": normalized_message_id,
                            "fields": self._normalize_error_fields(
                                cast("Mapping[str | bytes, Any]", fields)
                            ),
                        },
                        checkpoint=self.current_checkpoint(),
                        source=self.source_name,
                    ) from exc

                self._delivery_success_hook = self._build_ack_callback(msg_id)
                self._emitted_record_count += 1
                yield record

    def _build_ack_callback(self, msg_id: str | bytes) -> Callable[[], Awaitable[None]] | None:
        if not self._ack_on_success:
            return None

        async def _ack() -> None:
            if self._client is None:
                raise RuntimeError("RedisStreamSource client is closed — cannot ack message")
            await self._enqueue_ack(msg_id)

        return _ack

    async def _enqueue_ack(self, msg_id: str | bytes) -> None:
        self._pending_ack_ids.append(msg_id)
        if len(self._pending_ack_ids) >= self._ack_batch_size:
            await self._flush_pending_acks()

    async def _flush_pending_acks(self) -> None:
        client = self._client
        if client is None or not self._pending_ack_ids:
            return
        pending_ids, self._pending_ack_ids = self._pending_ack_ids, []
        try:
            await client.xack(self._stream, self._group, *pending_ids)
        except Exception as exc:
            self._pending_ack_ids = pending_ids + self._pending_ack_ids
            if await self._recover_from_connection_error(exc, context="ack"):
                client = self._require_client()
                pending_ids, self._pending_ack_ids = self._pending_ack_ids, []
                try:
                    await client.xack(self._stream, self._group, *pending_ids)
                except Exception:
                    self._pending_ack_ids = pending_ids + self._pending_ack_ids
                    raise
            else:
                raise
        self._ack_flush_count += 1
        self._acked_message_count += len(pending_ids)
        self._last_ack_at = _now_utc()

    @staticmethod
    def _normalize_message_id(msg_id: str | bytes) -> str:
        if isinstance(msg_id, bytes):
            return msg_id.decode("utf-8")
        return msg_id

    @classmethod
    def _normalize_error_fields(
        cls,
        fields: Mapping[str | bytes, Any],
    ) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in fields.items():
            normalized_key = key.decode("utf-8") if isinstance(key, bytes) else key
            if isinstance(value, bytes):
                try:
                    normalized[normalized_key] = value.decode("utf-8")
                except UnicodeDecodeError:
                    normalized[normalized_key] = value.hex()
            else:
                normalized[normalized_key] = value
        return normalized

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

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisStreamSource.open() was not called")
        return self._client

    def _remember_error(self, exc: Exception) -> None:
        self._last_error = str(exc)
        self._last_error_at = _now_utc()

    def _is_retryable_connection_error(self, exc: Exception) -> bool:
        try:
            from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError
        except ImportError:
            return False
        return isinstance(exc, (ConnectionError, TimeoutError, ReadOnlyError))

    async def _recover_from_connection_error(self, exc: Exception, *, context: str) -> bool:
        if not self._is_retryable_connection_error(exc):
            return False
        self._remember_error(exc)
        logger.warning(
            "redis_stream_retryable_connection_error",
            stream=self._stream,
            group=self._group,
            consumer=self._consumer,
            context=context,
            error=str(exc),
        )
        await self._reconnect_client()
        return True

    async def _reconnect_client(self) -> None:
        previous_client = self._client
        self._client = None
        self._group_ready = False
        if previous_client is not None:
            with contextlib.suppress(Exception):
                await previous_client.aclose()

        last_error: Exception | None = None
        attempt = 1
        while True:
            try:
                client = await self._build_client()
                await self._ensure_group(client)
                self._client = client
                self._reconnect_count += 1
                self._last_reconnect_at = _now_utc()
                logger.info(
                    "redis_stream_source_reconnected",
                    stream=self._stream,
                    group=self._group,
                    consumer=self._consumer,
                    reconnect_count=self._reconnect_count,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as reconnect_error:
                last_error = reconnect_error
                self._remember_error(reconnect_error)
                if not self._reconnect_retry_policy.should_retry(
                    reconnect_error,
                    attempt=attempt,
                ):
                    break
                delay = self._reconnect_retry_policy.backoff_for(attempt=attempt)
                logger.warning(
                    "redis_stream_source_reconnect_retry",
                    stream=self._stream,
                    group=self._group,
                    consumer=self._consumer,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(reconnect_error),
                )
                if delay > 0:
                    await asyncio.sleep(delay)
                attempt += 1
        if last_error is not None:
            raise last_error

    def _will_ack_failed_record(self) -> bool:
        return (
            self._on_deserialize_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE
            and self._ack_on_success
        )

    async def _apply_resume_checkpoint(self, client: Any) -> None:
        if not self._resume_group_seek_pending or self._resume_cursor is None:
            return
        await self._ensure_resume_single_consumer_group(client)
        xgroup_setid = getattr(client, "xgroup_setid", None)
        if not callable(xgroup_setid):
            self._resume_group_seek_pending = False
            self._resume_pending = False
            raise TypeError(
                "RedisStreamSource resume requires a Redis client with xgroup_setid support. "
                "Upgrade redis-py or start without a checkpoint."
            )
        try:
            await xgroup_setid(self._stream, self._group, self._resume_cursor)
        except Exception as exc:
            self._remember_error(exc)
            raise
        self._resume_group_seek_pending = False
        logger.info(
            "redis_stream_group_seek_applied",
            stream=self._stream,
            group=self._group,
            message_id=self._resume_cursor,
        )

    async def _ensure_resume_single_consumer_group(self, client: Any) -> None:
        xinfo_consumers = getattr(client, "xinfo_consumers", None)
        if not callable(xinfo_consumers):
            self._resume_group_seek_pending = False
            self._resume_pending = False
            raise TypeError(
                "RedisStreamSource resume requires a Redis client with xinfo_consumers support "
                "so it can verify that XGROUP SETID will not rewind a multi-consumer group."
            )
        try:
            consumers = await xinfo_consumers(self._stream, self._group)
        except Exception as exc:
            self._remember_error(exc)
            raise
        consumer_count = len(consumers or [])
        if consumer_count > 1:
            self._resume_group_seek_pending = False
            self._resume_pending = False
            raise RuntimeError(
                "RedisStreamSource resume from checkpoint is only safe for a single-consumer "
                f"group; stream={self._stream!r} group={self._group!r} has "
                f"{consumer_count} consumers. Use a dedicated resume group or reset the group "
                "explicitly outside RedisStreamSource."
            )


__all__ = ["RedisStreamSource"]
