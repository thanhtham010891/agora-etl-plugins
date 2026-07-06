"""Kafka -> Redis envelope deserializer helpers."""

from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

T = TypeVar("T")


class KafkaRedisEnvelopeDeserializer(Generic[T]):
    """Wrap a payload deserializer and attach Kafka metadata for wedge transforms."""

    def __init__(
        self,
        inner: Callable[..., T | Awaitable[T]],
        *,
        metadata_aware: bool = False,
    ) -> None:
        self._inner = inner
        self._metadata_aware = metadata_aware

    async def open(self) -> None:
        open_hook = getattr(self._inner, "open", None)
        if callable(open_hook):
            result = open_hook()
            if isawaitable(result):
                await result

    async def close(self) -> None:
        close_hook = getattr(self._inner, "close", None)
        if callable(close_hook):
            result = close_hook()
            if isawaitable(result):
                await result

    async def __call__(
        self,
        value: bytes,
        metadata: dict[str, object],
    ) -> dict[str, object]:
        payload = self._inner(value, metadata) if self._metadata_aware else self._inner(value)
        if isawaitable(payload):
            payload = await payload
        return {
            "payload": payload,
            "metadata": metadata,
        }


def wrap_kafka_redis_deserializer(
    inner: Callable[..., T | Awaitable[T]],
    *,
    metadata_aware: bool = False,
) -> KafkaRedisEnvelopeDeserializer[T]:
    """Build the canonical Kafka payload+metadata envelope deserializer."""

    return KafkaRedisEnvelopeDeserializer(inner, metadata_aware=metadata_aware)
