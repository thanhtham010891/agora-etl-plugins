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

import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct
from agora.core.retry import RetryPolicy
from agora.core.source import BaseSource, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.redis.observability import (
    RedisEnterpriseAcceptanceGate,
    RedisPrometheusExporter,
    RedisSourceEnterpriseAcceptanceThresholds,
    RedisSourcePoisonLoopRiskSnapshot,
    RedisStreamSourceHealthSnapshot,
    RedisStreamSourceMetricsSnapshot,
)
from agora_plugins.redis.sources._ack_runtime import RedisAckRuntime
from agora_plugins.redis.sources._connection_runtime import RedisConnectionRuntime
from agora_plugins.redis.sources._resume_runtime import RedisResumeRuntime
from agora_plugins.redis.sources._stream_runtime import RedisReadLoopRuntime

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
        self.logger = logger
        self._ack_runtime = RedisAckRuntime(self, now_utc=_now_utc)
        self._connection_runtime = RedisConnectionRuntime(self, now_utc=_now_utc)
        self._resume_runtime = RedisResumeRuntime(self)
        self._stream_runtime = RedisReadLoopRuntime(self, now_utc=_now_utc)

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

        await self._resume_runtime.prepare_resume(checkpoint)

    async def stream(self) -> AsyncGenerator[T, None]:
        async for record in self._stream_runtime.stream():
            yield record

    async def _build_client(self) -> Any:
        return await self._connection_runtime.build_client()

    async def _ensure_group(self, client: Any) -> None:
        await self._connection_runtime.ensure_group(client)

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
        return self._ack_runtime.delivery_success_callback()

    def current_checkpoint(self) -> dict[str, str] | None:
        return self._ack_runtime.current_checkpoint()

    async def _read_reclaimed_messages(
        self,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        return await self._stream_runtime.read_reclaimed_messages()

    def _should_read_reclaimed_messages(self) -> bool:
        return self._stream_runtime.should_read_reclaimed_messages()

    async def _yield_entries(
        self,
        entries: list[tuple[str, list[tuple[str, dict[str, str]]]]],
    ) -> AsyncGenerator[T, None]:
        async for record in self._stream_runtime.yield_entries(entries):
            yield cast("T", record)

    def _build_ack_callback(self, msg_id: str | bytes) -> Callable[[], Awaitable[None]] | None:
        return self._ack_runtime.build_ack_callback(msg_id)

    def _build_failure_ack_callback(self, msg_id: str | bytes) -> Callable[[], Awaitable[None]]:
        return self._ack_runtime.build_failure_ack_callback(msg_id)

    async def _enqueue_ack(self, msg_id: str | bytes) -> None:
        await self._ack_runtime.enqueue_ack(msg_id)

    async def _flush_pending_acks(self) -> None:
        await self._ack_runtime.flush_pending_acks()

    @staticmethod
    def _normalize_message_id(msg_id: str | bytes) -> str:
        return RedisAckRuntime.normalize_message_id(msg_id)

    @classmethod
    def _normalize_error_fields(
        cls,
        fields: Mapping[str | bytes, Any],
    ) -> dict[str, Any]:
        return RedisAckRuntime.normalize_error_fields(fields)

    @staticmethod
    def _parse_xautoclaim_response(
        response: object,
    ) -> tuple[str, list[tuple[str, dict[str, str]]]]:
        return RedisResumeRuntime.parse_xautoclaim_response(response)

    def _require_client(self) -> Any:
        return self._connection_runtime.require_client()

    def _remember_error(self, exc: Exception) -> None:
        self._connection_runtime.remember_error(exc)

    def _is_retryable_connection_error(self, exc: Exception) -> bool:
        return self._connection_runtime.is_retryable_connection_error(exc)

    async def _recover_from_connection_error(self, exc: Exception, *, context: str) -> bool:
        return await self._connection_runtime.recover_from_connection_error(
            exc,
            context=context,
            reconnect_client=self._reconnect_client,
        )

    async def _reconnect_client(self) -> None:
        await self._connection_runtime.reconnect_client(
            build_client=self._build_client,
            ensure_group=self._ensure_group,
            retry_policy=self._reconnect_retry_policy,
        )

    def _will_ack_failed_record(self) -> bool:
        return self._on_deserialize_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE

    async def _apply_resume_checkpoint(self, client: Any) -> None:
        await self._resume_runtime.apply_resume_checkpoint(client)

    async def _ensure_resume_single_consumer_group(self, client: Any) -> None:
        await self._resume_runtime.ensure_resume_single_consumer_group(client)


__all__ = ["RedisStreamSource"]
