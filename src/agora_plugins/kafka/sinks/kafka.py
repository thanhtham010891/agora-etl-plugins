"""
agora_plugins.kafka.sinks.kafka
===============================
``KafkaSink[T]`` — publish records to a Kafka topic using aiokafka.
"""

from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from inspect import isawaitable, iscoroutinefunction
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct
from agora.core.retry import RetryPolicy, retry_async
from agora.core.sink import BaseSink

from agora_plugins.kafka._lifecycle import call_lifecycle
from agora_plugins.kafka._security_posture import warn_if_insecure_plaintext
from agora_plugins.kafka.config import KafkaSecurityConfig
from agora_plugins.kafka.sinks._message import (
    UNSET as _UNSET,
)
from agora_plugins.kafka.sinks._message import (
    KafkaSinkMessage,
)
from agora_plugins.kafka.sinks._message import (
    ResolvedKafkaSinkMessage as _ResolvedKafkaSinkMessage,
)
from agora_plugins.kafka.sinks._message import (
    coerce_headers as _coerce_headers,
)
from agora_plugins.kafka.sinks._producer_options import (
    producer_supported_kwargs as _producer_supported_kwargs_for,
)
from agora_plugins.kafka.sinks._producer_options import (
    validate_producer_tuning as _validate_producer_tuning,
)
from agora_plugins.kafka.tracing import KafkaOpenTelemetryTracing

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable

try:
    from aiokafka import AIOKafkaProducer
    from aiokafka.errors import KafkaError

    _AIOKAFKA_AVAILABLE = True
except ImportError:
    _AIOKAFKA_AVAILABLE = False


T = TypeVar("T")
logger = logstruct.getLogger(__name__)


def _producer_supported_kwargs() -> set[str] | None:
    return _producer_supported_kwargs_for(AIOKafkaProducer)


_producer_supported_kwargs.cache_clear = _producer_supported_kwargs_for.cache_clear  # type: ignore[attr-defined]


class KafkaSink(BaseSink[T], Generic[T]):
    """Async Kafka producer sink."""

    sink_name = "kafka"

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        serializer: Callable[[T], bytes | Awaitable[bytes]],
        message_fn: Callable[[T], KafkaSinkMessage | Awaitable[KafkaSinkMessage]] | None = None,
        topic_fn: Callable[[T], str | None] | None = None,
        key_fn: Callable[[T], bytes | None] | None = None,
        partition_fn: Callable[[T], int | None] | None = None,
        headers_fn: Callable[[T], Iterable[tuple[str, bytes]] | None] | None = None,
        timestamp_ms_fn: Callable[[T], int | None] | None = None,
        security_protocol: str = "PLAINTEXT",
        security: KafkaSecurityConfig | None = None,
        max_pending_acks: int = 100,
        retry_policy: RetryPolicy[Any] | None = None,
        transactional_id: str | None = None,
        transaction_per_batch: bool = False,
        tracing: bool | KafkaOpenTelemetryTracing = False,
        **producer_kwargs: Any,
    ) -> None:
        if not _AIOKAFKA_AVAILABLE:
            raise ImportError(
                "aiokafka is required for KafkaSink. Install it: pip install 'agora-etl-plugins[kafka]'"
            )
        if max_pending_acks < 1:
            raise ValueError("max_pending_acks must be >= 1")
        if transactional_id is not None and not transactional_id:
            raise ValueError("KafkaSink transactional_id must be non-empty when provided.")
        if transaction_per_batch and transactional_id is None:
            raise ValueError("KafkaSink transaction_per_batch=True requires transactional_id.")
        self._topic = topic
        self._bootstrap = bootstrap_servers
        self._serializer = serializer
        self._message_fn = message_fn
        self._topic_fn = topic_fn
        self._key_fn = key_fn
        self._partition_fn = partition_fn
        self._headers_fn = headers_fn
        self._timestamp_ms_fn = timestamp_ms_fn
        self._security = self._resolve_security(security_protocol, security)
        self._security_protocol = (
            self._security.security_protocol if self._security is not None else security_protocol
        )
        warn_if_insecure_plaintext(
            subject=type(self).__name__,
            security_protocol=self._security_protocol,
            bootstrap_servers=bootstrap_servers,
        )
        self._producer_kwargs = dict(producer_kwargs)
        self._producer_kwargs.setdefault("linger_ms", 5)
        self._producer_kwargs.setdefault("compression_type", "gzip")
        self._producer_kwargs.setdefault("enable_idempotence", True)
        self._transactional_id = transactional_id
        self._transaction_per_batch = transaction_per_batch
        self._tracing = KafkaOpenTelemetryTracing.from_config(tracing)
        self._in_transaction = False
        if transactional_id is not None:
            self._producer_kwargs["transactional_id"] = transactional_id
            self._producer_kwargs["enable_idempotence"] = True
        _validate_producer_tuning(self._producer_kwargs)
        if self._producer_kwargs["enable_idempotence"]:
            self._producer_kwargs.setdefault("acks", "all")
            if self._supports_producer_kwarg("max_in_flight_requests_per_connection"):
                self._producer_kwargs.setdefault("max_in_flight_requests_per_connection", 5)
            if self._producer_kwargs.get("acks") not in {"all", -1}:
                raise ValueError("KafkaSink with idempotence enabled requires acks='all'")
        self._max_pending_acks = max_pending_acks
        self._retry_policy = retry_policy or RetryPolicy[Any](
            max_attempts=3,
            initial_backoff_s=0.25,
            backoff_multiplier=2.0,
            max_backoff_s=2.0,
            retry_exceptions=(KafkaError,),
        )
        self._producer: AIOKafkaProducer | None = None
        self._pending_acks: deque[Any] = deque()
        self._serializer_open = False
        serializer_call = type(serializer).__call__ if callable(serializer) else None
        self._serializer_is_async = iscoroutinefunction(serializer) or iscoroutinefunction(
            serializer_call
        )
        message_fn_call = type(message_fn).__call__ if callable(message_fn) else None
        self._message_fn_is_async = message_fn is not None and (
            iscoroutinefunction(message_fn) or iscoroutinefunction(message_fn_call)
        )

    async def startup(self) -> None:
        await self.open()

    async def shutdown(self) -> None:
        await self.close()

    async def open(self) -> None:
        try:
            await self._open_serializer()
            producer_kwargs = self._supported_producer_kwargs()
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap,
                **producer_kwargs,
            )
            await self._producer.start()
            logger.info(
                "kafka_sink_ready",
                topic=self._topic,
                brokers=self._bootstrap,
            )
        except Exception:
            producer = self._producer
            self._producer = None
            if producer is not None:
                try:
                    await producer.stop()
                except Exception:
                    logger.exception("kafka_sink_open_cleanup_error", topic=self._topic)
            await self._close_serializer()
            raise

    async def close(self) -> None:
        try:
            if self._producer is not None:
                producer = self._producer
                try:
                    if self._in_transaction:
                        await self.abort_transaction()
                    else:
                        await self.flush()
                finally:
                    self._producer = None
                    try:
                        await producer.stop()
                    finally:
                        self._pending_acks.clear()
                logger.info("kafka_sink_closed", topic=self._topic)
        finally:
            await self._close_serializer()

    async def write(self, record: T) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaSink.open() was not called")
        try:
            await self._enqueue_send(record)
        except KafkaError as exc:
            logger.exception("kafka_sink_send_error", topic=self._topic, error=str(exc))
            raise

    async def write_batch(self, records: list[T]) -> None:
        self._require_producer()
        if self._transaction_per_batch and not self._in_transaction:
            async with self.transaction():
                await self._write_batch_unwrapped(records)
            return

        await self._write_batch_unwrapped(records)

    async def _write_batch_unwrapped(self, records: list[T]) -> None:
        if self._message_fn is not None:
            for record in records:
                await self._enqueue_send(record)
            return
        # Serialize entire batch first to avoid per-record coroutine switching
        if self._serializer_is_async:
            values = [await self._serialize(record) for record in records]
        else:
            values = [self._serialize_sync(record) for record in records]
        for i, record in enumerate(records):
            topic = self._topic_for(record)
            key = self._key_for(record)
            partition = self._partition_for(record)
            headers = self._headers_for(record)
            timestamp_ms = self._timestamp_ms_for(record)
            delivery = await self._send_with_retry(
                topic,
                values[i],
                key=key,
                partition=partition,
                headers=headers,
                timestamp_ms=timestamp_ms,
            )
            self._pending_acks.append(delivery)
            if len(self._pending_acks) >= self._max_pending_acks:
                await self._await_oldest_ack()

    async def flush(self) -> None:
        if self._producer is None:
            return
        await self._drain_pending_acks()
        await retry_async(
            self._producer.flush,
            policy=self._retry_policy,
            on_retry=lambda attempt, exc, delay: logger.warning(
                "kafka_sink_flush_retry",
                topic=self._topic,
                attempt=attempt,
                wait_s=delay,
                error=str(exc),
            ),
        )

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[KafkaSink[T]]:
        await self.begin_transaction()
        try:
            yield self
        except Exception:
            await self.abort_transaction()
            raise
        else:
            await self.commit_transaction()

    async def begin_transaction(self) -> None:
        producer = self._require_producer()
        begin = getattr(producer, "begin_transaction", None)
        if not callable(begin):
            raise TypeError("Kafka producer does not support transactions.")
        result = begin()
        if isawaitable(result):
            await result
        self._in_transaction = True

    async def commit_transaction(self) -> None:
        producer = self._require_producer()
        commit = getattr(producer, "commit_transaction", None)
        if not callable(commit):
            raise TypeError("Kafka producer does not support transactions.")
        await self._drain_pending_acks()
        result = commit()
        if isawaitable(result):
            await result
        self._in_transaction = False

    async def abort_transaction(self) -> None:
        producer = self._require_producer()
        abort = getattr(producer, "abort_transaction", None)
        if not callable(abort):
            raise TypeError("Kafka producer does not support transactions.")
        self._pending_acks.clear()
        result = abort()
        if isawaitable(result):
            await result
        self._in_transaction = False

    async def send_offsets_to_transaction(self, offsets: Any, group_id: str) -> None:
        producer = self._require_producer()
        send_offsets = getattr(producer, "send_offsets_to_transaction", None)
        if not callable(send_offsets):
            raise TypeError("Kafka producer does not support send_offsets_to_transaction().")
        result = send_offsets(offsets, group_id)
        if isawaitable(result):
            await result

    async def _enqueue_send(self, record: T) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaSink.open() was not called")
        publish_message = await self._publish_message_for(record)
        delivery = await self._send_with_retry(
            publish_message.topic,
            publish_message.value,
            key=publish_message.key,
            partition=publish_message.partition,
            headers=publish_message.headers,
            timestamp_ms=publish_message.timestamp_ms,
        )
        self._pending_acks.append(delivery)
        if len(self._pending_acks) >= self._max_pending_acks:
            await self._await_oldest_ack()

    async def _send_with_retry(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None,
        partition: int | None,
        headers: list[tuple[str, bytes]] | None,
        timestamp_ms: int | None,
    ) -> Any:
        producer = self._require_producer()
        send_kwargs: dict[str, Any] = {
            "value": value,
            "key": key,
            "headers": headers,
        }
        if partition is not None:
            send_kwargs["partition"] = partition
        if timestamp_ms is not None:
            send_kwargs["timestamp_ms"] = timestamp_ms
        send_kwargs["headers"] = self._tracing.inject_headers(headers)

        async def _issue_send() -> Any:
            with self._tracing.start_span(
                "kafka.produce",
                kind="producer",
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination.name": topic,
                    "messaging.kafka.message.key_size": len(key) if key is not None else 0,
                    "messaging.message.body.size": len(value),
                },
            ):
                return await producer.send(topic, **send_kwargs)

        delivery = await retry_async(
            _issue_send,
            policy=self._retry_policy,
            on_retry=lambda attempt, exc, delay: logger.warning(
                "kafka_sink_send_retry",
                topic=topic,
                attempt=attempt,
                wait_s=delay,
                error=str(exc),
            ),
        )
        sink = self

        class _DeliveryConfirmation:
            def __await__(self) -> Any:
                return self._confirm().__await__()

            async def _confirm(self) -> Any:
                try:
                    return await delivery
                except Exception as exc:
                    if not sink._retry_policy.should_retry(exc, attempt=1):
                        raise

                    async def _send_and_confirm() -> Any:
                        retry_delivery = await _issue_send()
                        return await retry_delivery

                    return await retry_async(
                        _send_and_confirm,
                        policy=sink._retry_policy,
                        on_retry=lambda attempt, exc, delay: logger.warning(
                            "kafka_sink_delivery_retry",
                            topic=topic,
                            attempt=attempt,
                            wait_s=delay,
                            error=str(exc),
                        ),
                    )

        return _DeliveryConfirmation()

    async def wait_for_pending_acks(self) -> None:
        await self._drain_pending_acks()

    async def _await_oldest_ack(self) -> None:
        if not self._pending_acks:
            return
        delivery = self._pending_acks.popleft()
        await delivery

    async def _drain_pending_acks(self) -> None:
        while self._pending_acks:
            await self._await_oldest_ack()

    def _supported_producer_kwargs(self) -> dict[str, Any]:
        kwargs = {
            **self._security_kwargs(),
            **self._producer_kwargs,
        }
        supported = _producer_supported_kwargs()
        if supported is None:
            return kwargs

        unsupported = [key for key in kwargs if key not in supported]
        for key in unsupported:
            logger.warning("kafka_sink_unsupported_producer_kwarg", kwarg=key, topic=self._topic)
            kwargs.pop(key, None)
        return kwargs

    @staticmethod
    def _supports_producer_kwarg(name: str) -> bool:
        supported = _producer_supported_kwargs()
        return supported is None or name in supported

    def _security_kwargs(self) -> dict[str, Any]:
        if self._security is None:
            return {"security_protocol": self._security_protocol}
        return self._security.to_aiokafka_client_kwargs()

    @staticmethod
    def _resolve_security(
        security_protocol: str,
        security: KafkaSecurityConfig | None,
    ) -> KafkaSecurityConfig | None:
        if security is None:
            return (
                None
                if security_protocol == "PLAINTEXT"
                else KafkaSecurityConfig(security_protocol=security_protocol)
            )
        if security.security_protocol != security_protocol:
            raise ValueError(
                "KafkaSink security_protocol must match security.security_protocol when both are set."
            )
        return security

    async def _serialize(self, record: T) -> bytes:
        value = self._serializer(record)
        if self._serializer_is_async or isawaitable(value):
            awaited = cast("Awaitable[bytes]", value)
            return await awaited
        return value

    def _serialize_sync(self, record: T) -> bytes:
        value = self._serializer(record)
        if isawaitable(value):
            raise TypeError("KafkaSink serializer returned awaitable on the synchronous path.")
        return value

    def _headers_for(self, record: T) -> list[tuple[str, bytes]] | None:
        if self._headers_fn is None:
            return None
        headers = self._headers_fn(record)
        if headers is None:
            return None
        return list(headers)

    def _key_for(self, record: T) -> bytes | None:
        if self._key_fn is None:
            return None
        return self._key_fn(record)

    def _topic_for(self, record: T) -> str:
        if self._topic_fn is None:
            return self._topic
        topic = self._topic_fn(record)
        if topic:
            return topic
        return self._topic

    def _partition_for(self, record: T) -> int | None:
        if self._partition_fn is None:
            return None
        return self._partition_fn(record)

    def _timestamp_ms_for(self, record: T) -> int | None:
        if self._timestamp_ms_fn is None:
            return None
        return self._timestamp_ms_fn(record)

    async def _publish_message_for(self, record: T) -> _ResolvedKafkaSinkMessage:
        payload = await self._message_overrides_for(record)
        topic = self._topic_for(record)
        if payload.topic is not _UNSET:
            topic = cast("str", payload.topic)
        value = (
            await self._serialize(record)
            if payload.value is _UNSET
            else cast("bytes", payload.value)
        )
        key = self._key_for(record) if payload.key is _UNSET else cast("bytes | None", payload.key)
        partition = (
            self._partition_for(record)
            if payload.partition is _UNSET
            else cast("int | None", payload.partition)
        )
        headers = (
            self._headers_for(record)
            if payload.headers is _UNSET
            else _coerce_headers(payload.headers)
        )
        timestamp_ms = (
            self._timestamp_ms_for(record)
            if payload.timestamp_ms is _UNSET
            else cast("int | None", payload.timestamp_ms)
        )
        return _ResolvedKafkaSinkMessage(
            topic=topic,
            value=value,
            key=key,
            partition=partition,
            headers=headers,
            timestamp_ms=timestamp_ms,
        )

    async def _message_overrides_for(self, record: T) -> KafkaSinkMessage:
        if self._message_fn is None:
            return KafkaSinkMessage()
        message = self._message_fn(record)
        if self._message_fn_is_async or isawaitable(message):
            awaited = cast("Awaitable[KafkaSinkMessage]", message)
            return await awaited
        return message

    async def _open_serializer(self) -> None:
        await call_lifecycle(self._serializer, "open")
        self._serializer_open = True
        if self._message_fn is not None:
            await call_lifecycle(self._message_fn, "open")

    async def _close_serializer(self) -> None:
        if not self._serializer_open:
            return
        if self._message_fn is not None:
            await call_lifecycle(self._message_fn, "close")
        await call_lifecycle(self._serializer, "close")
        self._serializer_open = False

    def _require_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("KafkaSink.open() was not called")
        return self._producer


__all__ = ["KafkaSink", "KafkaSinkMessage"]
