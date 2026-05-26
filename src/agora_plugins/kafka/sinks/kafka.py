"""
agora_plugins.kafka.sinks.kafka
===============================
``KafkaSink[T]`` — publish records to a Kafka topic using aiokafka.
"""

from __future__ import annotations

from collections import deque
from inspect import Parameter, isawaitable, iscoroutinefunction, signature
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct
from agora.core.retry import RetryPolicy, retry_async
from agora.core.sink import BaseSink

from agora_plugins.kafka._lifecycle import call_lifecycle

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

try:
    from aiokafka import AIOKafkaProducer
    from aiokafka.errors import KafkaError

    _AIOKAFKA_AVAILABLE = True
except ImportError:
    _AIOKAFKA_AVAILABLE = False


T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class KafkaSink(BaseSink[T], Generic[T]):
    """Async Kafka producer sink."""

    sink_name = "kafka"

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        serializer: Callable[[T], bytes | Awaitable[bytes]],
        key_fn: Callable[[T], bytes | None] | None = None,
        security_protocol: str = "PLAINTEXT",
        max_pending_acks: int = 100,
        retry_policy: RetryPolicy[Any] | None = None,
        **producer_kwargs: Any,
    ) -> None:
        if not _AIOKAFKA_AVAILABLE:
            raise ImportError(
                "aiokafka is required for KafkaSink. Install it: pip install 'agora-etl-plugins[kafka]'"
            )
        if max_pending_acks < 1:
            raise ValueError("max_pending_acks must be >= 1")
        self._topic = topic
        self._bootstrap = bootstrap_servers
        self._serializer = serializer
        self._key_fn = key_fn
        self._security_protocol = security_protocol
        self._producer_kwargs = dict(producer_kwargs)
        self._producer_kwargs.setdefault("linger_ms", 5)
        self._producer_kwargs.setdefault("compression_type", "gzip")
        self._producer_kwargs.setdefault("enable_idempotence", True)
        if self._producer_kwargs["enable_idempotence"]:
            self._producer_kwargs.setdefault("acks", "all")
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
        serializer_call = type(serializer).__call__ if callable(serializer) else None
        self._serializer_is_async = iscoroutinefunction(serializer) or iscoroutinefunction(
            serializer_call
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
                security_protocol=self._security_protocol,
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
        if self._producer is not None:
            producer = self._producer
            try:
                await self.flush()
            finally:
                self._producer = None
                await producer.stop()
            logger.info("kafka_sink_closed", topic=self._topic)
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
        if self._producer is None:
            raise RuntimeError("KafkaSink.open() was not called")
        # Serialize entire batch first to avoid per-record coroutine switching
        if self._serializer_is_async:
            values = [await self._serialize(record) for record in records]
        else:
            values = [self._serializer(record) for record in records]
        for i, record in enumerate(records):
            key = self._key_fn(record) if self._key_fn else None
            delivery = await self._send_with_retry(values[i], key=key)
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

    async def _enqueue_send(self, record: T) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaSink.open() was not called")
        value = await self._serialize(record)
        key = self._key_fn(record) if self._key_fn else None
        delivery = await self._send_with_retry(value, key=key)
        self._pending_acks.append(delivery)
        if len(self._pending_acks) >= self._max_pending_acks:
            await self._await_oldest_ack()

    async def _send_with_retry(
        self,
        value: bytes,
        *,
        key: bytes | None,
    ) -> Any:
        try:
            return await self._producer.send(self._topic, value=value, key=key)
        except self._retry_policy.retry_exceptions:
            return await retry_async(
                lambda: self._producer.send(self._topic, value=value, key=key),
                policy=self._retry_policy,
                on_retry=lambda attempt, exc, delay: logger.warning(
                    "kafka_sink_send_retry",
                    topic=self._topic,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                ),
            )

    async def _await_oldest_ack(self) -> None:
        if not self._pending_acks:
            return
        delivery = self._pending_acks.popleft()
        await delivery

    async def _drain_pending_acks(self) -> None:
        while self._pending_acks:
            await self._await_oldest_ack()

    def _supported_producer_kwargs(self) -> dict[str, Any]:
        kwargs = dict(self._producer_kwargs)
        try:
            parameters = signature(AIOKafkaProducer.__init__).parameters
        except (TypeError, ValueError):  # pragma: no cover
            return kwargs

        if any(parameter.kind is Parameter.VAR_KEYWORD for parameter in parameters.values()):
            return kwargs

        supported = set(parameters)

        unsupported = [key for key in kwargs if key not in supported]
        for key in unsupported:
            logger.warning("kafka_sink_unsupported_producer_kwarg", kwarg=key, topic=self._topic)
            kwargs.pop(key, None)
        return kwargs

    async def _serialize(self, record: T) -> bytes:
        value = self._serializer(record)
        if self._serializer_is_async or isawaitable(value):
            return await value
        return value

    async def _open_serializer(self) -> None:
        await call_lifecycle(self._serializer, "open")

    async def _close_serializer(self) -> None:
        await call_lifecycle(self._serializer, "close")


__all__ = ["KafkaSink"]
