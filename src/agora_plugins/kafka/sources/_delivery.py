"""Delivery-state and poison-progression helpers for Kafka sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import logstruct
from agora.core.source import SourceRecordError, SourceRuntimeMetrics

from agora_plugins.kafka.sources._models import (
    BatchMessageContext,
    KafkaDeliveryContext,
    KafkaPoisonRecordInfo,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora_plugins.kafka.sources._cursor_state import KafkaCursorState

logger = logstruct.getLogger("agora_plugins.kafka.sources.kafka")


class KafkaDeliveryController:
    """Owns active-delivery callbacks plus poison progression across records."""

    def __init__(
        self,
        *,
        cursor_state: KafkaCursorState,
        group_id: str,
        bootstrap_servers: str,
        source_name: str,
        enable_auto_commit: bool,
        subscription_mode: Callable[[], str],
        build_topic_partition: Callable[[str, int], object],
        commit_if_needed: Callable[[], Awaitable[None]],
        on_delivery_progress: Callable[[], None],
        on_commit_recorded: Callable[[], None],
        on_state_changed: Callable[[], None],
        current_checkpoint: Callable[[], dict[str, Any] | None],
        capture_poison_batch: Callable[..., Awaitable[None]],
        capture_poison_record: Callable[..., Awaitable[None]],
        observe_poison_records: Callable[[Exception, int], KafkaPoisonRecordInfo],
        should_continue_after_poison_record: Callable[[], bool],
    ) -> None:
        self._cursor_state = cursor_state
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._source_name = source_name
        self._enable_auto_commit = enable_auto_commit
        self._subscription_mode = subscription_mode
        self._build_topic_partition = build_topic_partition
        self._commit_if_needed = commit_if_needed
        self._on_delivery_progress = on_delivery_progress
        self._on_commit_recorded = on_commit_recorded
        self._on_state_changed = on_state_changed
        self._current_checkpoint = current_checkpoint
        self._capture_poison_batch = capture_poison_batch
        self._capture_poison_record = capture_poison_record
        self._observe_poison_records = observe_poison_records
        self._should_continue_after_poison_record = should_continue_after_poison_record
        self._record_error_count = 0
        self._record_drop_count = 0
        self._batch_deserialize_error_count = 0
        self._delivery_success_hook: Callable[[], Awaitable[None]] | None = None
        self._delivery_transaction_offsets_hook: (
            Callable[[], Awaitable[tuple[dict[Any, int], str]]] | None
        ) = None
        self._delivery_transaction_committed_hook: Callable[[], Awaitable[None]] | None = None
        self._delivery_context: KafkaDeliveryContext | None = None

    @property
    def record_error_count(self) -> int:
        return self._record_error_count

    @property
    def record_drop_count(self) -> int:
        return self._record_drop_count

    @property
    def batch_deserialize_error_count(self) -> int:
        return self._batch_deserialize_error_count

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def reset_run_state(self) -> None:
        self._record_error_count = 0
        self._record_drop_count = 0
        self.clear_active_delivery()

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._delivery_success_hook

    def delivery_transaction_offsets_callback(
        self,
    ) -> Callable[[], Awaitable[tuple[dict[Any, int], str]]] | None:
        return self._delivery_transaction_offsets_hook

    def delivery_transaction_committed_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._delivery_transaction_committed_hook

    def delivery_context(self) -> KafkaDeliveryContext | None:
        return self._delivery_context

    def mark_emitted_offset(self, topic: str, partition: int, offset: int) -> None:
        self._cursor_state.remember_processed_offset(topic, partition, offset)
        self._on_delivery_progress()

    def start_delivery(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        metadata: dict[str, Any],
    ) -> tuple[Callable[[], Awaitable[None]], Callable[[], bool]]:
        acknowledged = False

        async def _ack() -> None:
            nonlocal acknowledged
            if acknowledged:
                return
            acknowledged = True
            self._cursor_state.mark_delivered_message(
                topic,
                partition,
                offset,
                enable_auto_commit=self._enable_auto_commit,
            )
            self._on_delivery_progress()
            if not self._enable_auto_commit:
                await self._commit_if_needed()

        async def _offsets() -> tuple[dict[Any, int], str]:
            return {
                self._build_topic_partition(topic, partition): int(offset) + 1,
            }, self._group_id

        async def _committed() -> None:
            nonlocal acknowledged
            acknowledged = True
            self._cursor_state.mark_transaction_committed(topic, partition, offset)
            self._on_commit_recorded()

        self._delivery_context = self._delivery_context_from_metadata(metadata)
        self._delivery_success_hook = _ack
        self._delivery_transaction_offsets_hook = _offsets
        self._delivery_transaction_committed_hook = _committed

        def _was_acked() -> bool:
            return acknowledged

        return _ack, _was_acked

    def clear_active_delivery(self) -> None:
        self._delivery_success_hook = None
        self._delivery_transaction_offsets_hook = None
        self._delivery_transaction_committed_hook = None
        self._delivery_context = None

    async def handle_single_deserialize_error(
        self,
        *,
        exc: Exception,
        message: Any,
        metadata: dict[str, Any],
    ) -> bool:
        self._record_error_count += 1
        poison_info = self._observe_poison_records(exc, 1)
        self._on_state_changed()
        await self._capture_poison_record(
            exc,
            message,
            metadata,
            stage="kafka_deserialize",
            poison_info=poison_info,
        )
        logger.exception(
            "kafka_deserialize_error",
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
        )
        if self._should_continue_after_poison_record():
            self._record_drop_count += 1
            self._on_state_changed()
            await self._acknowledge_dropped_record(
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            return True
        raise SourceRecordError(
            exc,
            record=message.value,
            checkpoint=self._current_checkpoint(),
            source=self._source_name,
        ) from exc

    async def handle_batch_deserialize_error(
        self,
        *,
        messages: list[Any],
        batch_contexts: list[BatchMessageContext],
        exc: Exception,
    ) -> bool:
        self._batch_deserialize_error_count += 1
        self._record_error_count += len(messages)
        poison_info = self._observe_poison_records(exc, len(messages))
        self._on_state_changed()
        await self._capture_poison_batch(
            exc,
            messages,
            batch_contexts,
            stage="kafka_batch_deserialize",
            poison_info=poison_info,
        )
        logger.exception(
            "kafka_batch_deserialize_error",
            topics=sorted({str(message.topic) for message in messages}),
            batch_size=len(messages),
        )
        if self._should_continue_after_poison_record():
            await self._continue_after_batch_failure(messages)
            return True
        raise SourceRecordError(
            exc,
            record=[message.value for message in messages],
            checkpoint=self._current_checkpoint(),
            source=self._source_name,
        ) from exc

    async def handle_batch_deserializer_count_mismatch(
        self,
        *,
        messages: list[Any],
        batch_contexts: list[BatchMessageContext],
        output_count: int,
    ) -> bool:
        exc = ValueError(
            "Kafka batch_deserializer returned a different number of records than input "
            f"messages: input={len(messages)}, output={output_count}."
        )
        self._batch_deserialize_error_count += 1
        self._record_error_count += len(messages)
        poison_info = self._observe_poison_records(exc, len(messages))
        self._on_state_changed()
        await self._capture_poison_batch(
            exc,
            messages,
            batch_contexts,
            stage="kafka_batch_deserialize_count_mismatch",
            poison_info=poison_info,
        )
        logger.warning(
            "kafka_batch_deserializer_record_count_mismatch",
            input_messages=len(messages),
            output_records=output_count,
            group_id=self._group_id,
        )
        if self._should_continue_after_poison_record():
            await self._continue_after_batch_failure(messages)
            return True
        raise SourceRecordError(
            exc,
            record=[message.value for message in messages],
            checkpoint=self._current_checkpoint(),
            source=self._source_name,
        ) from exc

    async def _continue_after_batch_failure(self, messages: list[Any]) -> None:
        self._record_drop_count += len(messages)
        self._on_state_changed()
        for message in messages:
            self._cursor_state.remember_processed_offset(
                message.topic,
                message.partition,
                message.offset,
            )
            self._cursor_state.remember_committable_offset(
                message.topic,
                message.partition,
                message.offset,
            )
        self._on_delivery_progress()
        if not self._enable_auto_commit:
            self._cursor_state.increment_pending_commit_count(len(messages))
            self._on_state_changed()
            await self._commit_if_needed()

    async def _acknowledge_dropped_record(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        self._cursor_state.mark_delivered_message(
            topic,
            partition,
            offset,
            enable_auto_commit=self._enable_auto_commit,
        )
        self._on_delivery_progress()
        if not self._enable_auto_commit:
            await self._commit_if_needed()

    def _delivery_context_from_metadata(
        self,
        metadata: dict[str, Any],
    ) -> KafkaDeliveryContext:
        headers = metadata.get("headers", ())
        return KafkaDeliveryContext(
            topic=str(metadata["topic"]),
            partition=int(metadata["partition"]),
            offset=int(metadata["offset"]),
            consumer_group=str(metadata["consumer_group"]),
            bootstrap_servers=str(metadata["bootstrap_servers"]),
            subscription_mode=str(metadata["subscription_mode"]),
            batch_size=int(metadata.get("batch_size", 1)),
            batch_index=int(metadata.get("batch_index", 0)),
            key=cast("bytes | None", metadata.get("key")),
            headers=tuple(cast("list[tuple[str, bytes]]", list(headers))),
            timestamp=cast("int | None", metadata.get("timestamp")),
            timestamp_type=cast("int | None", metadata.get("timestamp_type")),
        )
