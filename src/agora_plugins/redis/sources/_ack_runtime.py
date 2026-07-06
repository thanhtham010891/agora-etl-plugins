"""Ack, checkpoint, and error-field helpers for Redis stream sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from datetime import datetime


class RedisAckRuntime:
    """Owns ack batching and checkpoint bookkeeping for ``RedisStreamSource``."""

    def __init__(self, source: Any, *, now_utc: Callable[[], datetime]) -> None:
        self._source = source
        self._now_utc = now_utc

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._source._delivery_success_hook

    def current_checkpoint(self) -> dict[str, str] | None:
        if self._source._last_message_id is None:
            return None
        if (
            self._source._checkpoint_cache is not None
            and self._source._checkpoint_cache["message_id"] == self._source._last_message_id
        ):
            return self._source._checkpoint_cache
        self._source._checkpoint_cache = {
            "stream": self._source._stream,
            "group": self._source._group,
            "consumer": self._source._consumer,
            "message_id": self._source._last_message_id,
        }
        return self._source._checkpoint_cache

    def build_ack_callback(self, msg_id: str | bytes) -> Callable[[], Awaitable[None]] | None:
        if not self._source._ack_on_success:
            return None

        async def _ack() -> None:
            if self._source._client is None:
                raise RuntimeError("RedisStreamSource client is closed — cannot ack message")
            await self.enqueue_ack(msg_id)

        return _ack

    def build_failure_ack_callback(
        self,
        msg_id: str | bytes,
    ) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            client = self._source._client
            if client is None:
                raise RuntimeError("RedisStreamSource client is closed — cannot ack message")
            await client.xack(self._source._stream, self._source._group, msg_id)
            self._source._ack_flush_count += 1
            self._source._acked_message_count += 1
            self._source._last_ack_at = self._now_utc()

        return _ack

    async def enqueue_ack(self, msg_id: str | bytes) -> None:
        self._source._pending_ack_ids.append(msg_id)
        if len(self._source._pending_ack_ids) >= self._source._ack_batch_size:
            await self.flush_pending_acks()

    async def flush_pending_acks(self) -> None:
        client = self._source._client
        if client is None or not self._source._pending_ack_ids:
            return
        pending_ids, self._source._pending_ack_ids = self._source._pending_ack_ids, []
        try:
            await client.xack(self._source._stream, self._source._group, *pending_ids)
        except Exception as exc:
            self._source._pending_ack_ids = pending_ids + self._source._pending_ack_ids
            if await self._source._recover_from_connection_error(exc, context="ack"):
                client = self._source._require_client()
                pending_ids, self._source._pending_ack_ids = self._source._pending_ack_ids, []
                try:
                    await client.xack(self._source._stream, self._source._group, *pending_ids)
                except Exception:
                    self._source._pending_ack_ids = pending_ids + self._source._pending_ack_ids
                    raise
            else:
                raise
        self._source._ack_flush_count += 1
        self._source._acked_message_count += len(pending_ids)
        self._source._last_ack_at = self._now_utc()

    @staticmethod
    def normalize_message_id(msg_id: str | bytes) -> str:
        if isinstance(msg_id, bytes):
            return msg_id.decode("utf-8")
        return msg_id

    @classmethod
    def normalize_error_fields(
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
