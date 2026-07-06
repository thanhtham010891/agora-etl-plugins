"""Deserializer and metadata helpers for Kafka sources."""

from __future__ import annotations

from inspect import Parameter, isawaitable, signature
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora_plugins.kafka._lifecycle import call_lifecycle
from agora_plugins.kafka.sources._models import BatchMessageContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from agora_plugins.kafka.tracing import KafkaOpenTelemetryTracing

T = TypeVar("T")


class KafkaDeserializerRuntime(Generic[T]):
    """Owns deserializer lifecycle plus Kafka message metadata normalization."""

    def __init__(
        self,
        *,
        topics: list[str],
        topic_pattern: str | None,
        group_id: str,
        bootstrap_servers: str,
        deserializer: Callable[..., T | Awaitable[T]],
        batch_deserializer: Callable[..., Iterable[T] | Awaitable[Iterable[T]]] | None,
        subscription_mode: Callable[[], str],
        active_assignment: Callable[[], Iterable[tuple[str, int]]],
        tracing: KafkaOpenTelemetryTracing,
    ) -> None:
        self._topics = list(topics)
        self._topic_pattern = topic_pattern
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._deserializer = deserializer
        self._batch_deserializer = batch_deserializer
        self._subscription_mode = subscription_mode
        self._active_assignment = active_assignment
        self._tracing = tracing
        self._deserializer_accepts_metadata = _callable_accepts_metadata(deserializer)
        self._batch_deserializer_accepts_context = _callable_accepts_metadata(batch_deserializer)

    async def open(self) -> None:
        await call_lifecycle(self._deserializer, "open")
        if self._batch_deserializer is not None:
            await call_lifecycle(self._batch_deserializer, "open")

    async def close(self) -> None:
        if self._batch_deserializer is not None:
            await call_lifecycle(self._batch_deserializer, "close")
        await call_lifecycle(self._deserializer, "close")

    async def deserialize(
        self,
        message: Any,
        metadata: dict[str, Any] | None = None,
    ) -> T:
        payload_metadata = metadata or self.message_metadata(message)
        with self._tracing.start_span(
            "kafka.consume",
            kind="consumer",
            headers=cast("list[tuple[str, bytes]]", payload_metadata.get("headers", [])),
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": str(payload_metadata["topic"]),
                "messaging.kafka.partition": int(payload_metadata["partition"]),
                "messaging.kafka.offset": int(payload_metadata["offset"]),
                "messaging.kafka.consumer.group": self._group_id,
            },
        ):
            if self._deserializer_accepts_metadata:
                record = self._deserializer(message.value, payload_metadata)
            else:
                record = self._deserializer(message.value)
            if isawaitable(record):
                return await record
            return record

    async def deserialize_batch(
        self,
        messages: list[Any],
        batch_contexts: list[BatchMessageContext],
    ) -> Iterable[T]:
        values = [message.value for message in messages]
        batch_context = {
            "topics": list(self._topics),
            "topic_pattern": self._topic_pattern,
            "assignments": [
                {"topic": topic, "partition": partition}
                for topic, partition in sorted(self._active_assignment())
            ],
            "consumer_group": self._group_id,
            "bootstrap_servers": self._bootstrap_servers,
            "subscription_mode": self._subscription_mode(),
            "batch_size": len(messages),
            "messages": [item.metadata for item in batch_contexts],
        }
        if self._batch_deserializer is None:
            raise RuntimeError("KafkaSource batch deserializer is not configured.")
        if self._batch_deserializer_accepts_context:
            records = self._batch_deserializer(values, batch_context)
        else:
            records = self._batch_deserializer(values)
        if isawaitable(records):
            return await records
        return records

    def build_batch_contexts(self, messages: list[Any]) -> list[BatchMessageContext]:
        batch_size = len(messages)
        return [
            BatchMessageContext(
                metadata=self.message_metadata(
                    message,
                    batch_size=batch_size,
                    batch_index=index,
                ),
                message=message,
            )
            for index, message in enumerate(messages)
        ]

    def message_metadata(
        self,
        message: Any,
        *,
        batch_size: int = 1,
        batch_index: int = 0,
    ) -> dict[str, Any]:
        return {
            "topic": str(message.topic),
            "partition": int(message.partition),
            "offset": int(message.offset),
            "key": getattr(message, "key", None),
            "headers": list(getattr(message, "headers", ()) or ()),
            "timestamp": getattr(message, "timestamp", None),
            "timestamp_type": getattr(message, "timestamp_type", None),
            "consumer_group": self._group_id,
            "bootstrap_servers": self._bootstrap_servers,
            "subscription_mode": self._subscription_mode(),
            "batch_size": batch_size,
            "batch_index": batch_index,
        }


def _callable_accepts_metadata(func: object) -> bool:
    try:
        parameters = signature(cast("Callable[..., Any]", func)).parameters.values()
    except (TypeError, ValueError):
        return False

    positional = [
        parameter
        for parameter in parameters
        if parameter.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if any(parameter.kind is Parameter.VAR_POSITIONAL for parameter in parameters):
        return True
    return len(positional) >= 2
