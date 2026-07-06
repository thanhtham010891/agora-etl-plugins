"""Commit gating and broker flush helpers for Kafka sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora_plugins.kafka.sources._cursor_state import KafkaCursorState
    from agora_plugins.kafka.tracing import KafkaOpenTelemetryTracing


class KafkaCommitRuntime:
    """Owns tracked-offset commit semantics for ``KafkaSource``."""

    def __init__(
        self,
        *,
        group_id: str,
        enable_auto_commit: bool,
        commit_every: int,
        tracing: KafkaOpenTelemetryTracing,
        cursor_state: KafkaCursorState,
        build_topic_partition: Callable[[str, int], object],
        current_consumer: Callable[[], Any | None],
        on_commit_recorded: Callable[[], None],
    ) -> None:
        self._group_id = group_id
        self._enable_auto_commit = enable_auto_commit
        self._commit_every = commit_every
        self._tracing = tracing
        self._cursor_state = cursor_state
        self._build_topic_partition = build_topic_partition
        self._current_consumer = current_consumer
        self._on_commit_recorded = on_commit_recorded

    async def commit_if_needed(self, *, force: bool = False) -> None:
        consumer = self._current_consumer()
        if consumer is None or self._enable_auto_commit:
            return
        if self._cursor_state.pending_commit_count <= 0:
            return
        if not force and self._cursor_state.pending_commit_count < self._commit_every:
            return

        offsets = self._cursor_state.build_commit_offsets(self._build_topic_partition)
        with self._tracing.start_span(
            "kafka.commit",
            kind="client",
            attributes={
                "messaging.system": "kafka",
                "messaging.kafka.consumer.group": self._group_id,
                "messaging.kafka.commit.partition_count": len(offsets),
            },
        ):
            if offsets:
                await consumer.commit(offsets=offsets)
            else:
                await consumer.commit()
        self._cursor_state.clear_pending_commit_count()
        self._on_commit_recorded()
