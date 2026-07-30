"""Poll, deserialize, and delivery orchestration for Kafka sources."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct
from agora.core.retry import RetryPolicy, retry_async

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from agora_plugins.kafka.sources._consumer_runtime import KafkaConsumerRuntime
    from agora_plugins.kafka.sources._cursor_state import KafkaCursorState
    from agora_plugins.kafka.sources._delivery import KafkaDeliveryController
    from agora_plugins.kafka.sources._deserializer_runtime import KafkaDeserializerRuntime
    from agora_plugins.kafka.sources._operator_controls import KafkaOperatorController
    from agora_plugins.kafka.sources._runtime_state import KafkaRuntimeState

T = TypeVar("T")

logger = logstruct.getLogger("agora_plugins.kafka.sources.kafka")


class KafkaStreamRuntime(Generic[T]):
    """Owns the streaming poll loop for ``KafkaSource``."""

    def __init__(
        self,
        *,
        group_id: str,
        topics: list[str],
        topic_pattern: str | None,
        assignments: list[tuple[str, int]],
        poll_timeout_ms: int,
        max_poll_records: int,
        max_idle_polls: int | None,
        has_batch_deserializer: bool,
        operator_controls: KafkaOperatorController,
        runtime_state: KafkaRuntimeState,
        cursor_state: KafkaCursorState,
        deserializer_runtime: KafkaDeserializerRuntime[T],
        delivery_controller: KafkaDeliveryController,
        consumer_runtime: KafkaConsumerRuntime,
        bootstrap_consumer_state: Callable[[Any | None], Awaitable[None]],
        commit_if_needed: Callable[..., Awaitable[None]],
        on_state_changed: Callable[[], None],
        retry_policy: RetryPolicy[Any],
    ) -> None:
        self._group_id = group_id
        self._topics = list(topics)
        self._topic_pattern = topic_pattern
        self._assignments = list(assignments)
        self._poll_timeout_ms = poll_timeout_ms
        self._max_poll_records = max_poll_records
        self._max_idle_polls = max_idle_polls
        self._has_batch_deserializer = has_batch_deserializer
        self._operator_controls = operator_controls
        self._runtime_state = runtime_state
        self._cursor_state = cursor_state
        self._deserializer_runtime = deserializer_runtime
        self._delivery_controller = delivery_controller
        self._consumer_runtime = consumer_runtime
        self._bootstrap_consumer_state = bootstrap_consumer_state
        self._commit_if_needed = commit_if_needed
        self._on_state_changed = on_state_changed
        self._retry_policy = retry_policy

    async def stream(self, *, consumer: Any) -> AsyncGenerator[T, None]:
        self._delivery_controller.reset_run_state()
        self._cursor_state.reset_for_stream()
        self._on_state_changed()

        await self._bootstrap_consumer_state(consumer)

        try:
            async for batch_messages in self._iter_message_batches(consumer):
                async for record in self._deliver_batch(batch_messages):
                    yield record
        except asyncio.CancelledError:
            await self._commit_if_needed(force=True)
            logger.info("kafka_source_cancelled", group_id=self._group_id)
            raise
        except Exception:
            await self._commit_if_needed(force=True)
            logger.exception("kafka_source_stream_error")
            raise
        finally:
            self._delivery_controller.clear_active_delivery()
            await self._commit_if_needed(force=True)

    async def _iter_message_batches(self, consumer: Any) -> AsyncGenerator[list[Any], None]:
        getmany = getattr(consumer, "getmany", None)
        if getmany is None:
            async for message in consumer:
                self._consumer_runtime.sync_assignment_from_consumer(consumer)
                yield [message]
            return

        idle_polls = 0
        while True:
            try:
                batches = await retry_async(
                    lambda: getmany(
                        timeout_ms=self._poll_timeout_ms,
                        max_records=self._max_poll_records,
                    ),
                    policy=self._retry_policy,
                )
                self._runtime_state.record_poll()
            except StopAsyncIteration:
                return
            if not any(batches.values()):
                if self._max_idle_polls is None:
                    self._runtime_state.idle_poll_count += 1
                    continue
                idle_polls += 1
                self._runtime_state.idle_poll_count = idle_polls
                if idle_polls >= self._max_idle_polls:
                    logger.info(
                        "kafka_source_idle_exit",
                        group_id=self._group_id,
                        topics=self._topics,
                        topic_pattern=self._topic_pattern,
                        assignments=self._assignments,
                        idle_polls=idle_polls,
                        poll_timeout_ms=self._poll_timeout_ms,
                    )
                    return
                continue

            idle_polls = 0
            self._runtime_state.idle_poll_count = 0
            self._operator_controls.sync_active_assignment(batches.keys())
            batch_messages: list[Any] = []
            for messages in batches.values():
                batch_messages.extend(messages)
            if batch_messages:
                yield batch_messages

    async def _deliver_batch(self, batch_messages: list[Any]) -> AsyncGenerator[T, None]:
        batch_contexts = self._deserializer_runtime.build_batch_contexts(batch_messages)

        if self._has_batch_deserializer:
            try:
                batch_records = list(
                    await self._deserializer_runtime.deserialize_batch(
                        batch_messages,
                        batch_contexts,
                    )
                )
            except Exception as exc:
                await self._delivery_controller.handle_batch_deserialize_error(
                    messages=batch_messages,
                    batch_contexts=batch_contexts,
                    exc=exc,
                )
                return

            if len(batch_records) == len(batch_messages):
                for record, message_context in zip(batch_records, batch_contexts, strict=False):
                    async for delivered in self._emit_record(
                        record,
                        message=message_context.message,
                        metadata=message_context.metadata,
                    ):
                        yield delivered
            else:
                self._delivery_controller.clear_active_delivery()
                await self._delivery_controller.handle_batch_deserializer_count_mismatch(
                    messages=batch_messages,
                    batch_contexts=batch_contexts,
                    output_count=len(batch_records),
                )
            return

        for index, message in enumerate(batch_messages):
            try:
                record = await self._deserializer_runtime.deserialize(
                    message,
                    batch_contexts[index].metadata,
                )
            except Exception as exc:
                if await self._delivery_controller.handle_single_deserialize_error(
                    exc=exc,
                    message=message,
                    metadata=batch_contexts[index].metadata,
                ):
                    continue
                raise
            async for delivered in self._emit_record(
                record,
                message=message,
                metadata=batch_contexts[index].metadata,
            ):
                yield delivered

    async def _emit_record(
        self,
        record: T,
        *,
        message: Any,
        metadata: dict[str, Any],
    ) -> AsyncGenerator[T, None]:
        # Checkpoint resume tracks the last emitted source offset.
        # Broker commits remain gated on handled delivery and resume
        # seeks from checkpoint offset + 1 on the next open.
        self._delivery_controller.mark_emitted_offset(
            message.topic,
            message.partition,
            message.offset,
        )
        ack_hook, was_acked = self._delivery_controller.start_delivery(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            metadata=metadata,
        )
        try:
            yield record
        finally:
            self._delivery_controller.clear_active_delivery()
        if not was_acked():
            await ack_hook()
