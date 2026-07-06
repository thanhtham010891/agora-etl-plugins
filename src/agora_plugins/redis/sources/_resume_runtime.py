"""Resume and reclaim-checkpoint helpers for Redis stream sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.checkpoint import Checkpoint


class RedisResumeRuntime:
    """Owns resume cursor bookkeeping and checkpoint safety checks."""

    def __init__(self, source: Any) -> None:
        self._source = source

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        self._source._resume_pending = False
        self._source._resume_group_seek_pending = False
        self._source._resume_cursor = None
        if checkpoint is None or not isinstance(checkpoint.value, dict):
            return

        value = checkpoint.value
        message_id = value.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            return

        self._source._resume_cursor = message_id
        self._source._resume_group_seek_pending = True

    @staticmethod
    def parse_xautoclaim_response(
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

    async def apply_resume_checkpoint(self, client: Any) -> None:
        if not self._source._resume_group_seek_pending or self._source._resume_cursor is None:
            return
        await self.ensure_resume_single_consumer_group(client)
        xgroup_setid = getattr(client, "xgroup_setid", None)
        if not callable(xgroup_setid):
            self._source._resume_group_seek_pending = False
            self._source._resume_pending = False
            raise TypeError(
                "RedisStreamSource resume requires a Redis client with xgroup_setid support. "
                "Upgrade redis-py or start without a checkpoint."
            )
        try:
            await xgroup_setid(
                self._source._stream,
                self._source._group,
                self._source._resume_cursor,
            )
        except Exception as exc:
            self._source._remember_error(exc)
            raise
        self._source._resume_group_seek_pending = False
        self._source.logger.info(
            "redis_stream_group_seek_applied",
            stream=self._source._stream,
            group=self._source._group,
            message_id=self._source._resume_cursor,
        )

    async def ensure_resume_single_consumer_group(self, client: Any) -> None:
        xinfo_consumers = getattr(client, "xinfo_consumers", None)
        if not callable(xinfo_consumers):
            self._source._resume_group_seek_pending = False
            self._source._resume_pending = False
            raise TypeError(
                "RedisStreamSource resume requires a Redis client with xinfo_consumers support "
                "so it can verify that XGROUP SETID will not rewind a multi-consumer group."
            )
        try:
            consumers = await xinfo_consumers(self._source._stream, self._source._group)
        except Exception as exc:
            self._source._remember_error(exc)
            raise
        consumer_count = len(consumers or [])
        if consumer_count > 1:
            self._source._resume_group_seek_pending = False
            self._source._resume_pending = False
            raise RuntimeError(
                "RedisStreamSource resume from checkpoint is only safe for a single-consumer "
                f"group; stream={self._source._stream!r} group={self._source._group!r} has "
                f"{consumer_count} consumers. Use a dedicated resume group or reset the group "
                "explicitly outside RedisStreamSource."
            )
