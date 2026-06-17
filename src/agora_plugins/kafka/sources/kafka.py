"""
agora_plugins.kafka.sources.kafka
=================================
Async Kafka source powered by ``aiokafka``.

Requires: ``pip install 'agora-etl-plugins[kafka]'``
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import time
from collections.abc import Iterable
from inspect import Parameter, isawaitable, signature
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct
from agora.core.source import BaseSource, SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.kafka._lifecycle import call_lifecycle
from agora_plugins.kafka.config import KafkaSecurityConfig
from agora_plugins.kafka.sources._models import (
    BatchMessageContext as _BatchMessageContext,
)
from agora_plugins.kafka.sources._models import (
    KafkaDeliveryContext,
    KafkaPartitionHealth,
    KafkaPoisonRecordPolicy,
    KafkaSourceHealthSnapshot,
    KafkaSourceOperationalMetrics,
)
from agora_plugins.kafka.sources._models import (
    KafkaPoisonRecordClassification as KafkaPoisonRecordClassification,
)
from agora_plugins.kafka.sources._models import (
    KafkaPoisonRecordInfo as KafkaPoisonRecordInfo,
)
from agora_plugins.kafka.sources._offsets import normalize_offset_map as _normalize_offset_map
from agora_plugins.kafka.sources._poison import (
    build_poison_dlq_record,
    capture_poison_batch,
    capture_poison_record,
    classify_poison_record,
    handle_poison_dlq_write_error,
    observe_poison_records,
    resolve_poison_record_policy,
    should_continue_after_poison_record,
)
from agora_plugins.kafka.sources._rebalance import (
    build_rebalance_listener as _build_rebalance_listener,
)
from agora_plugins.kafka.sources._rebalance import (
    normalize_assignment_items as _normalize_assignment_items,
)
from agora_plugins.kafka.sources._rebalance import (
    normalize_topic_partitions as _normalize_topic_partitions,
)
from agora_plugins.kafka.tracing import KafkaOpenTelemetryTracing

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from agora.core.dlq import DLQRecord, DLQSink

T = TypeVar("T")

logger = logstruct.getLogger(__name__)

_CONSUMER_POSITIVE_INT_CONFIGS = frozenset(
    {
        "auto_commit_interval_ms",
        "connections_max_idle_ms",
        "fetch_max_bytes",
        "heartbeat_interval_ms",
        "max_poll_interval_ms",
        "metadata_max_age_ms",
        "request_timeout_ms",
        "session_timeout_ms",
    }
)
_CONSUMER_NON_NEGATIVE_INT_CONFIGS = frozenset({"retry_backoff_ms"})


def _validate_int_config(
    name: str,
    value: object,
    *,
    minimum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"KafkaSource {name} must be an integer >= {minimum}.")
    if value < minimum:
        raise ValueError(f"KafkaSource {name} must be >= {minimum}.")
    return value


def _validate_extra_consumer_config(extra_config: dict[str, Any]) -> None:
    for name in sorted(_CONSUMER_POSITIVE_INT_CONFIGS):
        if name in extra_config:
            _validate_int_config(name, extra_config[name], minimum=1)
    for name in sorted(_CONSUMER_NON_NEGATIVE_INT_CONFIGS):
        if name in extra_config:
            _validate_int_config(name, extra_config[name], minimum=0)


class KafkaSource(BaseSource[T], Generic[T]):
    """Async Kafka consumer source."""

    source_name = "kafka"
    supports_checkpoint = True

    def __init__(
        self,
        topics: list[str] | None = None,
        topic_pattern: str | None = None,
        assignments: Iterable[tuple[str, int]] | None = None,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "agora-consumer",
        deserializer: Callable[..., T | Awaitable[T]] | None = None,
        batch_deserializer: Callable[..., Iterable[T] | Awaitable[Iterable[T]]] | None = None,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        commit_every: int = 100,
        poll_timeout_ms: int = 1000,
        max_idle_polls: int | None = None,
        max_poll_records: int = 500,
        fetch_min_bytes: int = 1,
        fetch_max_wait_ms: int = 500,
        max_partition_fetch_bytes: int = 1_048_576,
        security_protocol: str = "PLAINTEXT",
        security: KafkaSecurityConfig | None = None,
        extra_config: dict[str, Any] | None = None,
        start_offsets: dict[tuple[str, int], int] | None = None,
        rebalance_listener: object | None = None,
        on_deserialize_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        poison_record_policy: KafkaPoisonRecordPolicy | str | None = None,
        poison_record_sink: DLQSink | None = None,
        poison_record_pipeline_id: str | None = None,
        poison_record_max_attempts: int | None = None,
        health_snapshot_cache_ms: int = 250,
        tracing: bool | KafkaOpenTelemetryTracing = False,
    ) -> None:
        self._topics = list(topics or [])
        self._topic_pattern = topic_pattern
        self._assignments = _normalize_topic_partitions(assignments or ())
        if not self._topics and self._topic_pattern is None and not self._assignments:
            raise ValueError("KafkaSource requires `topics`, `topic_pattern`, or `assignments`.")
        if self._topics and self._topic_pattern is not None:
            raise ValueError("KafkaSource accepts either `topics` or `topic_pattern`, not both.")
        if self._assignments and (self._topics or self._topic_pattern is not None):
            raise ValueError(
                "KafkaSource accepts `assignments` only when `topics` and `topic_pattern` are unset."
            )
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._deserializer: Callable[..., T | Awaitable[T]] = deserializer or (lambda b: b)
        self._deserializer_accepts_metadata = _callable_accepts_metadata(self._deserializer)
        self._batch_deserializer = batch_deserializer
        self._batch_deserializer_accepts_context = _callable_accepts_metadata(
            self._batch_deserializer
        )
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._commit_every = _validate_int_config("commit_every", commit_every, minimum=1)
        self._poll_timeout_ms = _validate_int_config(
            "poll_timeout_ms",
            poll_timeout_ms,
            minimum=0,
        )
        self._max_idle_polls = (
            None
            if max_idle_polls is None
            else _validate_int_config("max_idle_polls", max_idle_polls, minimum=1)
        )
        self._max_poll_records = _validate_int_config(
            "max_poll_records",
            max_poll_records,
            minimum=1,
        )
        self._fetch_min_bytes = _validate_int_config(
            "fetch_min_bytes",
            fetch_min_bytes,
            minimum=1,
        )
        self._fetch_max_wait_ms = _validate_int_config(
            "fetch_max_wait_ms",
            fetch_max_wait_ms,
            minimum=0,
        )
        self._max_partition_fetch_bytes = _validate_int_config(
            "max_partition_fetch_bytes",
            max_partition_fetch_bytes,
            minimum=1,
        )
        self._security = self._resolve_security(security_protocol, security)
        self._security_protocol = (
            self._security.security_protocol if self._security is not None else security_protocol
        )
        self._extra_config = dict(extra_config or {})
        _validate_extra_consumer_config(self._extra_config)
        self._start_offsets = _normalize_offset_map(start_offsets or {})
        self._rebalance_listener = rebalance_listener
        self._wrapped_rebalance_listener: object | None = None
        self._on_deserialize_error = on_deserialize_error
        self._poison_record_policy = self._resolve_poison_record_policy(
            poison_record_policy,
            on_deserialize_error=on_deserialize_error,
        )
        self._poison_record_sink = poison_record_sink
        self._poison_record_pipeline_id = poison_record_pipeline_id or f"kafka:{group_id}"
        self._poison_record_max_attempts = poison_record_max_attempts
        if (
            self._poison_record_policy
            in {
                KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
                KafkaPoisonRecordPolicy.DLQ_AND_FAIL_CLOSED,
            }
            and self._poison_record_sink is None
        ):
            raise ValueError(
                "KafkaSource poison_record_policy requires poison_record_sink when using DLQ modes."
            )
        self._health_snapshot_cache_ms = _validate_int_config(
            "health_snapshot_cache_ms",
            health_snapshot_cache_ms,
            minimum=0,
        )
        self._tracing = KafkaOpenTelemetryTracing.from_config(tracing)
        self._consumer = None
        self._pending_commit_count = 0
        self._processed_offsets: dict[tuple[str, int], int] = {}
        self._committable_offsets: dict[tuple[str, int], int] = {}
        self._last_seen: tuple[str, int, int] | None = None
        self._resume_offsets: dict[tuple[str, int], int] = {}
        self._resume_applied = False
        self._topic_partition_cls = None
        self._record_error_count = 0
        self._record_drop_count = 0
        self._rebalance_count = 0
        self._batch_deserialize_error_count = 0
        self._poison_record_dlq_write_count = 0
        self._poison_record_dlq_write_failure_count = 0
        self._poison_record_log_only_count = 0
        self._poison_record_fail_closed_count = 0
        self._poison_record_classification_counts = dict.fromkeys(
            KafkaPoisonRecordClassification, 0
        )
        self._checkpoint_cache: dict[str, Any] | None = None
        self._checkpoint_dirty = False
        self._active_assignment: set[tuple[str, int]] = set(self._assignments)
        self._paused_partitions: set[tuple[str, int]] = set()
        self._pause_all_requested = False
        self._positioning_applied = False
        self._delivery_success_hook: Callable[[], Awaitable[None]] | None = None
        self._delivery_transaction_offsets_hook: (
            Callable[[], Awaitable[tuple[dict[Any, int], str]]] | None
        ) = None
        self._delivery_transaction_committed_hook: Callable[[], Awaitable[None]] | None = None
        self._delivery_context: KafkaDeliveryContext | None = None
        self._last_poll_monotonic: float | None = None
        self._last_message_monotonic: float | None = None
        self._last_commit_monotonic: float | None = None
        self._last_rebalance_monotonic: float | None = None
        self._idle_poll_count = 0
        self._health_snapshot_cache: KafkaSourceHealthSnapshot | None = None
        self._health_snapshot_cache_monotonic: float | None = None

    async def open(self) -> None:
        try:
            await self._open_poison_record_sink()
            await self._open_deserializer()
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            await self._close_poison_record_sink()
            await self._close_deserializer()
            raise ImportError(
                "KafkaSource requires aiokafka. Install via: pip install 'agora-etl-plugins[kafka]'"
            ) from None
        except Exception:
            await self._close_poison_record_sink()
            await self._close_deserializer()
            raise

        try:
            self._topic_partition_cls = getattr(
                importlib.import_module("aiokafka"), "TopicPartition", None
            )
            consumer_args = (
                self._topics if self._topic_pattern is None and not self._assignments else []
            )
            self._consumer = AIOKafkaConsumer(
                *consumer_args,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset=self._auto_offset_reset,
                enable_auto_commit=self._enable_auto_commit,
                max_poll_records=self._max_poll_records,
                fetch_min_bytes=self._fetch_min_bytes,
                fetch_max_wait_ms=self._fetch_max_wait_ms,
                max_partition_fetch_bytes=self._max_partition_fetch_bytes,
                **self._security_kwargs(),
                **self._extra_config,
            )
            consumer = cast("Any", self._consumer)
            self._subscribe_consumer(consumer)
            await consumer.start()
            self._bind_consumer(consumer)
            logger.info(
                "kafka_source_ready",
                topics=self._topics,
                topic_pattern=self._topic_pattern,
                assignments=self._assignments,
                group_id=self._group_id,
                bootstrap=self._bootstrap_servers,
            )
        except Exception:
            consumer = self._consumer
            self._consumer = None
            if consumer is not None:
                with contextlib.suppress(Exception):
                    await consumer.stop()
            await self._close_poison_record_sink()
            await self._close_deserializer()
            raise

    async def close(self) -> None:
        if self._consumer is not None:
            consumer = self._consumer
            try:
                await self._commit_if_needed(force=True)
            except Exception:
                logger.exception("kafka_source_close_error")
            finally:
                with contextlib.suppress(Exception):
                    await consumer.stop()
                self._consumer = None
                self._active_assignment = set()
                self._delivery_success_hook = None
                self._delivery_transaction_offsets_hook = None
                self._delivery_transaction_committed_hook = None
                self._delivery_context = None
                self._invalidate_health_snapshot_cache()
                logger.info("kafka_source_closed", group_id=self._group_id)
        await self._close_poison_record_sink()
        await self._close_deserializer()

    async def _commit_if_needed(self, *, force: bool = False) -> None:
        if self._consumer is None or self._enable_auto_commit:
            return
        if self._pending_commit_count <= 0:
            return
        if not force and self._pending_commit_count < self._commit_every:
            return

        offsets = {
            self._build_topic_partition(topic, partition): offset + 1
            for (topic, partition), offset in self._committable_offsets.items()
        }
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
                await self._consumer.commit(offsets=offsets)
            else:
                await self._consumer.commit()
        self._pending_commit_count = 0
        self._last_commit_monotonic = time.monotonic()
        self._invalidate_health_snapshot_cache()

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def operational_metrics(self) -> KafkaSourceOperationalMetrics:
        return KafkaSourceOperationalMetrics(
            rebalance_count=self._rebalance_count,
            batch_deserialize_error_count=self._batch_deserialize_error_count,
            manual_assign_partition_count=len(self._assignments),
            paused_partition_count=self._paused_partition_count(),
            poison_record_dlq_write_count=self._poison_record_dlq_write_count,
            poison_record_dlq_write_failure_count=self._poison_record_dlq_write_failure_count,
            poison_record_log_only_count=self._poison_record_log_only_count,
            poison_record_fail_closed_count=self._poison_record_fail_closed_count,
            poison_record_deserialization_count=self._poison_record_classification_counts[
                KafkaPoisonRecordClassification.DESERIALIZATION
            ],
            poison_record_schema_evolution_count=self._poison_record_classification_counts[
                KafkaPoisonRecordClassification.SCHEMA_EVOLUTION
            ],
            poison_record_schema_validation_count=self._poison_record_classification_counts[
                KafkaPoisonRecordClassification.SCHEMA_VALIDATION
            ],
            poison_record_schema_registry_binding_mismatch_count=(
                self._poison_record_classification_counts[
                    KafkaPoisonRecordClassification.SCHEMA_REGISTRY_BINDING_MISMATCH
                ]
            ),
            poison_record_unknown_count=self._poison_record_classification_counts[
                KafkaPoisonRecordClassification.UNKNOWN
            ],
        )

    async def commit_now(self) -> None:
        """Flush tracked offsets immediately for operator-driven handoff/control."""

        consumer = self._require_open_consumer()
        offsets = {
            self._build_topic_partition(topic, partition): offset + 1
            for (topic, partition), offset in self._committable_offsets.items()
        }
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
        self._pending_commit_count = 0
        self._last_commit_monotonic = time.monotonic()
        self._invalidate_health_snapshot_cache()

    async def seek_to_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        """Reposition assigned partitions to exact Kafka offsets."""

        normalized = _normalize_offset_map(offsets)
        if not normalized:
            return

        consumer = self._require_open_consumer()
        await self._refresh_assignment_state(bootstrap=True)
        self._validate_assigned_partitions(set(normalized))

        seek = getattr(consumer, "seek", None)
        if not callable(seek):
            raise TypeError("Kafka consumer does not support seek().")

        for (topic, partition), offset in sorted(normalized.items()):
            result = seek(self._build_topic_partition(topic, partition), offset)
            if isawaitable(result):
                await result

        self._start_offsets = normalized
        self._resume_offsets = {}
        self._positioning_applied = True
        self._discard_operator_tracking_state()

    async def seek_to_beginning(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        """Rewind assigned partitions to the earliest available offsets."""

        await self._seek_with_consumer_method("seek_to_beginning", partitions)

    async def seek_to_end(
        self,
        partitions: Iterable[tuple[str, int]] | None = None,
    ) -> None:
        """Advance assigned partitions to the latest available offsets."""

        await self._seek_with_consumer_method("seek_to_end", partitions)

    async def stream(self) -> AsyncGenerator[T, None]:
        if self._consumer is None:
            raise RuntimeError(
                "KafkaSource must be used as an async context manager "
                "or call open() before stream()."
            )
        self._record_error_count = 0
        self._record_drop_count = 0
        self._pending_commit_count = 0
        self._processed_offsets = {}
        self._committable_offsets = {}
        self._last_seen = None
        self._positioning_applied = False
        self._delivery_success_hook = None
        self._delivery_transaction_offsets_hook = None
        self._delivery_transaction_committed_hook = None
        self._delivery_context = None
        self._invalidate_health_snapshot_cache()

        await self._bootstrap_consumer_state()
        consumer = self._consumer

        async def _iter_message_batches() -> AsyncGenerator[list[Any], None]:
            getmany = getattr(consumer, "getmany", None)
            if getmany is None:
                async for message in consumer:
                    self._sync_active_assignment()
                    yield [message]
                return

            idle_polls = 0
            while True:
                try:
                    batches = await getmany(
                        timeout_ms=self._poll_timeout_ms,
                        max_records=self._max_poll_records,
                    )
                    self._last_poll_monotonic = time.monotonic()
                    self._invalidate_health_snapshot_cache()
                except StopAsyncIteration:
                    return
                if not any(batches.values()):
                    if self._max_idle_polls is None:
                        self._idle_poll_count += 1
                        self._invalidate_health_snapshot_cache()
                        continue
                    idle_polls += 1
                    self._idle_poll_count = idle_polls
                    self._invalidate_health_snapshot_cache()
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
                self._idle_poll_count = 0
                self._invalidate_health_snapshot_cache()
                self._sync_active_assignment(batches.keys())
                batch_messages: list[Any] = []
                for messages in batches.values():
                    batch_messages.extend(messages)
                if batch_messages:
                    yield batch_messages

        try:
            async for batch_messages in _iter_message_batches():
                batch_contexts = self._build_batch_contexts(batch_messages)

                if self._batch_deserializer is not None:
                    try:
                        batch_records = list(
                            await self._deserialize_batch(batch_messages, batch_contexts)
                        )
                    except Exception as exc:
                        await self._handle_batch_deserialize_error(batch_messages, exc)
                        continue

                    if len(batch_records) == len(batch_messages):
                        for record, message_context in zip(
                            batch_records, batch_contexts, strict=False
                        ):
                            message = message_context.message
                            self._remember_processed_offset(
                                message.topic,
                                message.partition,
                                message.offset,
                            )
                            (
                                ack_hook,
                                was_acked,
                                mark_acknowledged,
                            ) = self._prepare_delivery_success_callback(
                                message.topic,
                                message.partition,
                                message.offset,
                            )
                            tx_offsets_hook, tx_committed_hook = (
                                self._prepare_delivery_transaction_callbacks(
                                    message.topic,
                                    message.partition,
                                    message.offset,
                                    mark_acknowledged=mark_acknowledged,
                                )
                            )
                            self._delivery_context = self._delivery_context_from_metadata(
                                message_context.metadata
                            )
                            self._delivery_success_hook = ack_hook
                            self._delivery_transaction_offsets_hook = tx_offsets_hook
                            self._delivery_transaction_committed_hook = tx_committed_hook
                            try:
                                yield record
                            finally:
                                self._delivery_success_hook = None
                                self._delivery_transaction_offsets_hook = None
                                self._delivery_transaction_committed_hook = None
                                self._delivery_context = None
                            if not was_acked():
                                await ack_hook()
                    else:
                        self._delivery_success_hook = None
                        self._delivery_transaction_offsets_hook = None
                        self._delivery_transaction_committed_hook = None
                        self._delivery_context = None
                        await self._handle_batch_deserializer_count_mismatch(
                            batch_messages,
                            output_count=len(batch_records),
                        )
                    continue

                for index, msg in enumerate(batch_messages):
                    try:
                        record = await self._deserialize(msg, batch_contexts[index].metadata)
                    except Exception as exc:
                        self._record_error_count += 1
                        poison_info = self._observe_poison_records(exc, count=1)
                        self._invalidate_health_snapshot_cache()
                        await self._capture_poison_record(
                            exc,
                            msg,
                            batch_contexts[index].metadata,
                            stage="kafka_deserialize",
                            poison_info=poison_info,
                        )
                        logger.exception(
                            "kafka_deserialize_error",
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                        )
                        if self._should_continue_after_poison_record():
                            self._record_drop_count += 1
                            self._invalidate_health_snapshot_cache()
                            await self._mark_delivered_message(
                                msg.topic,
                                msg.partition,
                                msg.offset,
                            )
                            continue
                        raise SourceRecordError(
                            exc,
                            record=msg.value,
                            checkpoint=self.current_checkpoint(),
                            source=self.source_name,
                        ) from exc

                    ack_hook, was_acked, mark_acknowledged = (
                        self._prepare_delivery_success_callback(
                            msg.topic,
                            msg.partition,
                            msg.offset,
                        )
                    )
                    tx_offsets_hook, tx_committed_hook = (
                        self._prepare_delivery_transaction_callbacks(
                            msg.topic,
                            msg.partition,
                            msg.offset,
                            mark_acknowledged=mark_acknowledged,
                        )
                    )
                    self._remember_processed_offset(msg.topic, msg.partition, msg.offset)
                    self._delivery_context = self._delivery_context_from_metadata(
                        batch_contexts[index].metadata
                    )
                    self._delivery_success_hook = ack_hook
                    self._delivery_transaction_offsets_hook = tx_offsets_hook
                    self._delivery_transaction_committed_hook = tx_committed_hook
                    try:
                        yield record
                    finally:
                        self._delivery_success_hook = None
                        self._delivery_transaction_offsets_hook = None
                        self._delivery_transaction_committed_hook = None
                        self._delivery_context = None
                    if not was_acked():
                        await ack_hook()

        except asyncio.CancelledError:
            await self._commit_if_needed(force=True)
            logger.info("kafka_source_cancelled", group_id=self._group_id)
            raise
        except Exception:
            await self._commit_if_needed(force=True)
            logger.exception("kafka_source_stream_error")
            raise
        finally:
            await self._commit_if_needed(force=True)

    async def prepare_resume(self, checkpoint: Any) -> None:
        self._positioning_applied = False
        self._resume_offsets = {}
        self._invalidate_health_snapshot_cache()
        if checkpoint is None or not isinstance(checkpoint.value, dict):
            return

        self._resume_offsets = self._normalize_checkpoint_offsets(checkpoint.value)

    async def _bootstrap_consumer_state(self) -> None:
        await self._refresh_assignment_state(bootstrap=True)
        await self._apply_initial_positioning()
        self._apply_pause_state()

    async def _apply_initial_positioning(self) -> None:
        if self._consumer is None or self._positioning_applied:
            return

        target_offsets = self._start_offsets or self._resume_offsets
        if not target_offsets:
            self._positioning_applied = True
            return

        assignment = getattr(self._consumer, "assignment", None)
        assigned = assignment() if callable(assignment) else None
        if callable(assignment) and not self._assignment_contains_all(assigned, target_offsets):
            getmany = getattr(self._consumer, "getmany", None)
            if callable(getmany):
                with contextlib.suppress(Exception):
                    await getmany(timeout_ms=0, max_records=1)
            assigned = assignment() if callable(assignment) else None
            self._sync_active_assignment(assigned)
        seek = getattr(self._consumer, "seek", None)
        if not callable(seek):
            logger.warning("kafka_resume_seek_unsupported", group_id=self._group_id)
            self._positioning_applied = True
            return

        resume_count = 0
        is_exact_positioning = bool(self._start_offsets)
        for (topic, partition), offset in target_offsets.items():
            if (
                callable(assignment)
                and assigned
                and not self._assignment_contains(assigned, topic, partition)
            ):
                logger.warning(
                    "kafka_resume_partition_unassigned",
                    topic=topic,
                    partition=partition,
                    group_id=self._group_id,
                )
                continue

            seek(
                self._build_topic_partition(topic, partition),
                offset if is_exact_positioning else offset + 1,
            )
            resume_count += 1
        self._positioning_applied = True
        self._invalidate_health_snapshot_cache()
        logger.info(
            "kafka_resume_applied",
            partitions=resume_count,
            group_id=self._group_id,
            exact_offsets=is_exact_positioning,
        )

    @staticmethod
    def _assignment_contains(
        assignments: object,
        topic: str,
        partition: int,
    ) -> bool:
        if not assignments:
            return False
        for item in cast("Iterable[Any]", assignments):
            item_topic = getattr(item, "topic", None)
            item_partition = getattr(item, "partition", None)
            if item_topic is None and isinstance(item, tuple) and len(item) >= 2:
                item_topic = item[0]
                item_partition = item[1]
            if item_partition is None:
                continue
            if item_topic == topic and int(item_partition) == partition:
                return True
        return False

    @classmethod
    def _assignment_contains_all(
        cls,
        assignments: object,
        offsets: dict[tuple[str, int], int],
    ) -> bool:
        if not offsets:
            return True
        return all(
            cls._assignment_contains(assignments, topic, partition) for topic, partition in offsets
        )

    def _remember_processed_offset(self, topic: str, partition: int, offset: int) -> None:
        self._processed_offsets[(str(topic), int(partition))] = int(offset)
        self._last_seen = (str(topic), int(partition), int(offset))
        self._checkpoint_dirty = True
        self._last_message_monotonic = time.monotonic()
        self._invalidate_health_snapshot_cache()

    def _remember_committable_offset(self, topic: str, partition: int, offset: int) -> None:
        self._committable_offsets[(str(topic), int(partition))] = int(offset)
        self._invalidate_health_snapshot_cache()

    def current_checkpoint(self) -> dict[str, Any] | None:
        if not self._processed_offsets or self._last_seen is None:
            return None
        if not self._checkpoint_dirty and self._checkpoint_cache is not None:
            return self._checkpoint_cache
        last_topic, last_partition, last_offset = self._last_seen
        self._checkpoint_cache = {
            "topic": last_topic,
            "partition": last_partition,
            "offset": last_offset,
            "offsets": [
                {"topic": t, "partition": p, "offset": o}
                for (t, p), o in sorted(self._processed_offsets.items())
            ],
        }
        self._checkpoint_dirty = False
        return self._checkpoint_cache

    def _build_topic_partition(self, topic: str, partition: int) -> object:
        if self._topic_partition_cls is not None:
            return self._topic_partition_cls(topic, partition)
        return (topic, partition)

    async def health_snapshot(
        self,
        *,
        force_refresh: bool = False,
    ) -> KafkaSourceHealthSnapshot:
        cached = self._cached_health_snapshot(force_refresh=force_refresh)
        if cached is not None:
            return cached

        if self._consumer is not None:
            await self._refresh_assignment_state()

        positions = await self._partition_positions()
        committed_offsets = await self._partition_committed_offsets()
        end_offsets = await self._partition_end_offsets()
        partitions: list[KafkaPartitionHealth] = []
        total_lag = 0
        any_lag = False
        lagging_partition_count = 0
        max_lag = 0
        total_commit_lag = 0
        any_commit_lag = False
        max_commit_lag = 0
        for topic, partition in sorted(self._active_assignment):
            key = (topic, partition)
            current_offset = positions.get(key)
            committed_offset = committed_offsets.get(key)
            processed_offset = self._processed_offsets.get(key)
            committable_offset = self._committable_offsets.get(key)
            end_offset = end_offsets.get(key)
            lag = None
            if current_offset is not None and end_offset is not None:
                lag = max(0, end_offset - current_offset)
                total_lag += lag
                any_lag = True
                if lag > 0:
                    lagging_partition_count += 1
                max_lag = max(max_lag, lag)
            commit_lag = None
            if committed_offset is not None and end_offset is not None:
                commit_lag = max(0, end_offset - committed_offset)
                total_commit_lag += commit_lag
                any_commit_lag = True
                max_commit_lag = max(max_commit_lag, commit_lag)
            delivery_gap = None
            if processed_offset is not None and committable_offset is not None:
                delivery_gap = max(0, processed_offset - committable_offset)
            commit_gap = None
            if committable_offset is not None and committed_offset is not None:
                commit_gap = max(0, (committable_offset + 1) - committed_offset)
            partitions.append(
                KafkaPartitionHealth(
                    topic=topic,
                    partition=partition,
                    current_offset=current_offset,
                    committed_offset=committed_offset,
                    processed_offset=processed_offset,
                    committable_offset=committable_offset,
                    end_offset=end_offset,
                    lag=lag,
                    commit_lag=commit_lag,
                    delivery_gap=delivery_gap,
                    commit_gap=commit_gap,
                    paused=self._pause_all_requested or key in self._paused_partitions,
                )
            )

        now = time.monotonic()
        snapshot = KafkaSourceHealthSnapshot(
            ready=self._consumer is not None and bool(self._active_assignment),
            stalled=(
                self._consumer is not None
                and self._max_idle_polls is not None
                and self._idle_poll_count >= self._max_idle_polls
            ),
            consumer_group=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            subscription_mode=self._subscription_mode(),
            assignment_count=len(self._active_assignment),
            paused_partition_count=self._paused_partition_count(),
            pending_commit_count=self._pending_commit_count,
            rebalance_count=self._rebalance_count,
            idle_poll_count=self._idle_poll_count,
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
            last_poll_age_ms=_age_ms(self._last_poll_monotonic, now),
            last_message_age_ms=_age_ms(self._last_message_monotonic, now),
            last_commit_age_ms=_age_ms(self._last_commit_monotonic, now),
            last_rebalance_age_ms=_age_ms(self._last_rebalance_monotonic, now),
            total_lag=total_lag if any_lag else None,
            lagging_partition_count=lagging_partition_count,
            max_lag=max_lag if any_lag else None,
            total_commit_lag=total_commit_lag if any_commit_lag else None,
            max_commit_lag=max_commit_lag if any_commit_lag else None,
            partitions=tuple(partitions),
        )
        self._health_snapshot_cache = snapshot
        self._health_snapshot_cache_monotonic = now
        return snapshot

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

    def _require_open_consumer(self) -> Any:
        if self._consumer is None:
            raise RuntimeError(
                "KafkaSource operator controls require an open consumer. "
                "Call open() or use the source inside a running pipeline first."
            )
        return self._consumer

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
                "KafkaSource security_protocol must match security.security_protocol when both are set."
            )
        return security

    async def _partition_positions(self) -> dict[tuple[str, int], int]:
        consumer = self._consumer
        if consumer is None:
            return {}
        position = getattr(consumer, "position", None)
        if not callable(position):
            return {}
        assignments = sorted(self._active_assignment)
        values = await asyncio.gather(
            *[
                self._resolve_partition_value(
                    position,
                    self._build_topic_partition(topic, partition),
                )
                for topic, partition in assignments
            ]
        )
        return {
            assignment: int(value)
            for assignment, value in zip(assignments, values, strict=False)
            if value is not None
        }

    async def _partition_end_offsets(self) -> dict[tuple[str, int], int]:
        consumer = self._consumer
        if consumer is None:
            return {}
        end_offsets = getattr(consumer, "end_offsets", None)
        if not callable(end_offsets) or not self._active_assignment:
            return {}
        topic_partitions = [
            self._build_topic_partition(topic, partition)
            for topic, partition in sorted(self._active_assignment)
        ]
        values = end_offsets(topic_partitions)
        if isawaitable(values):
            values = await values
        normalized: dict[tuple[str, int], int] = {}
        for key, value in cast("dict[Any, Any]", values or {}).items():
            topic = getattr(key, "topic", None)
            partition = getattr(key, "partition", None)
            if topic is None and isinstance(key, tuple) and len(key) >= 2:
                topic = key[0]
                partition = key[1]
            if topic is None or partition is None or value is None:
                continue
            normalized[(str(topic), int(partition))] = int(value)
        return normalized

    async def _partition_committed_offsets(self) -> dict[tuple[str, int], int]:
        consumer = self._consumer
        if consumer is None:
            return {}
        committed = getattr(consumer, "committed", None)
        if not callable(committed):
            return {}
        assignments = sorted(self._active_assignment)
        values = await asyncio.gather(
            *[
                self._resolve_partition_value(
                    committed,
                    self._build_topic_partition(topic, partition),
                )
                for topic, partition in assignments
            ]
        )
        return {
            assignment: int(value)
            for assignment, value in zip(assignments, values, strict=False)
            if value is not None
        }

    async def _seek_with_consumer_method(
        self,
        method_name: str,
        partitions: Iterable[tuple[str, int]] | None,
    ) -> None:
        consumer = self._require_open_consumer()
        await self._refresh_assignment_state(bootstrap=True)
        targets = self._resolve_seek_targets(partitions)

        method = getattr(consumer, method_name, None)
        if not callable(method):
            raise TypeError(f"Kafka consumer does not support {method_name}().")

        result = method(
            *[self._build_topic_partition(topic, partition) for topic, partition in sorted(targets)]
        )
        if isawaitable(result):
            await result

        self._start_offsets = {}
        self._resume_offsets = {}
        self._positioning_applied = True
        self._discard_operator_tracking_state()

    def _resolve_seek_targets(
        self,
        partitions: Iterable[tuple[str, int]] | None,
    ) -> set[tuple[str, int]]:
        if partitions is None:
            if not self._active_assignment:
                raise RuntimeError(
                    "KafkaSource has no active assignment yet, so operator seek controls "
                    "cannot resolve target partitions."
                )
            return set(self._active_assignment)

        targets = set(_normalize_topic_partitions(partitions))
        self._validate_assigned_partitions(targets)
        return targets

    def _validate_assigned_partitions(self, targets: set[tuple[str, int]]) -> None:
        unassigned = sorted(targets.difference(self._active_assignment))
        if unassigned:
            raise ValueError(
                "KafkaSource operator controls can only target assigned partitions. "
                f"Unassigned targets: {unassigned!r}"
            )

    def _discard_operator_tracking_state(self) -> None:
        self._pending_commit_count = 0
        self._processed_offsets = {}
        self._committable_offsets = {}
        self._last_seen = None
        self._checkpoint_cache = None
        self._checkpoint_dirty = False
        self._invalidate_health_snapshot_cache()

    def _prepare_delivery_success_callback(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> tuple[Callable[[], Awaitable[None]], Callable[[], bool], Callable[[], None]]:
        acknowledged = False

        async def _ack() -> None:
            nonlocal acknowledged
            if acknowledged:
                return
            acknowledged = True
            await self._mark_delivered_message(topic, partition, offset)

        def _was_acked() -> bool:
            return acknowledged

        def _mark_acknowledged() -> None:
            nonlocal acknowledged
            acknowledged = True

        return _ack, _was_acked, _mark_acknowledged

    def _prepare_delivery_transaction_callbacks(
        self,
        topic: str,
        partition: int,
        offset: int,
        *,
        mark_acknowledged: Callable[[], None],
    ) -> tuple[Callable[[], Awaitable[tuple[dict[Any, int], str]]], Callable[[], Awaitable[None]]]:
        async def _offsets() -> tuple[dict[Any, int], str]:
            return {
                self._build_topic_partition(topic, partition): int(offset) + 1,
            }, self._group_id

        async def _committed() -> None:
            mark_acknowledged()
            self._remember_processed_offset(topic, partition, offset)
            self._remember_committable_offset(topic, partition, offset)
            self._pending_commit_count = 0
            self._last_commit_monotonic = time.monotonic()
            self._invalidate_health_snapshot_cache()

        return _offsets, _committed

    async def _mark_delivered_message(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        self._remember_processed_offset(topic, partition, offset)
        self._remember_committable_offset(topic, partition, offset)
        if not self._enable_auto_commit:
            self._pending_commit_count += 1
            self._invalidate_health_snapshot_cache()
            await self._commit_if_needed()

    async def _deserialize(self, message: Any, metadata: dict[str, Any] | None = None) -> T:
        payload_metadata = metadata or self._message_metadata(message)
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

    async def _deserialize_batch(
        self,
        messages: list[Any],
        batch_contexts: list[_BatchMessageContext],
    ) -> Iterable[T]:
        values = [message.value for message in messages]
        batch_context = {
            "topics": list(self._topics),
            "topic_pattern": self._topic_pattern,
            "assignments": [
                {"topic": topic, "partition": partition}
                for topic, partition in sorted(self._active_assignment)
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

    def _message_metadata(
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

    async def _open_deserializer(self) -> None:
        await call_lifecycle(self._deserializer, "open")
        if self._batch_deserializer is not None:
            await call_lifecycle(self._batch_deserializer, "open")

    async def _close_deserializer(self) -> None:
        if self._batch_deserializer is not None:
            await call_lifecycle(self._batch_deserializer, "close")
        await call_lifecycle(self._deserializer, "close")

    async def _open_poison_record_sink(self) -> None:
        await call_lifecycle(self._poison_record_sink, "open")

    async def _close_poison_record_sink(self) -> None:
        await call_lifecycle(self._poison_record_sink, "close")

    def _subscribe_consumer(self, consumer: Any) -> None:
        subscribe = getattr(consumer, "subscribe", None)
        if self._assignments:
            return
        if not callable(subscribe):
            return
        self._wrapped_rebalance_listener = _build_rebalance_listener(
            self,
            self._rebalance_listener,
        )
        if self._topic_pattern is not None:
            subscribe(pattern=self._topic_pattern, listener=self._wrapped_rebalance_listener)
            return
        subscribe(topics=self._topics, listener=self._wrapped_rebalance_listener)

    def _bind_consumer(self, consumer: Any) -> None:
        if self._assignments:
            assign = getattr(consumer, "assign", None)
            if callable(assign):
                topic_partitions = [
                    self._build_topic_partition(topic, partition)
                    for topic, partition in self._assignments
                ]
                assign(topic_partitions)
                self._active_assignment = set(self._assignments)
                self._invalidate_health_snapshot_cache()

    async def _refresh_assignment_state(
        self,
        *,
        bootstrap: bool = False,
    ) -> None:
        if self._consumer is None:
            return
        assignment = getattr(self._consumer, "assignment", None)
        if not callable(assignment):
            return
        assigned = assignment()
        if not assigned and bootstrap:
            getmany = getattr(self._consumer, "getmany", None)
            if callable(getmany):
                with contextlib.suppress(Exception):
                    await getmany(timeout_ms=0, max_records=1)
                assigned = assignment()
        self._sync_active_assignment(assigned)

    def _sync_active_assignment(self, assignments: object | None = None) -> None:
        assignments = self._active_assignment if assignments is None else assignments
        normalized: set[tuple[str, int]] = set()
        for item in cast("Iterable[Any]", assignments or ()):
            topic = getattr(item, "topic", None)
            partition = getattr(item, "partition", None)
            if topic is None and isinstance(item, tuple) and len(item) >= 2:
                topic = item[0]
                partition = item[1]
            if topic is None or partition is None:
                continue
            normalized.add((str(topic), int(partition)))
        if normalized and normalized != self._active_assignment:
            self._active_assignment = normalized
            self._invalidate_health_snapshot_cache()

    def _sync_assignment_from_consumer(self) -> None:
        if self._consumer is None:
            return
        assignment = getattr(self._consumer, "assignment", None)
        if callable(assignment):
            self._sync_active_assignment(assignment())

    async def _handle_partitions_assigned(self, partitions: object) -> None:
        self._rebalance_count += 1
        self._last_rebalance_monotonic = time.monotonic()
        self._sync_active_assignment(partitions)
        self._apply_pause_state(partitions)
        self._invalidate_health_snapshot_cache()

    async def _handle_partitions_revoked(self, partitions: object) -> None:
        await self._commit_if_needed(force=True)
        revoked = {
            (str(topic), int(partition))
            for topic, partition in _normalize_assignment_items(partitions)
        }
        if not revoked:
            return
        for key in revoked:
            self._committable_offsets.pop(key, None)
            self._processed_offsets.pop(key, None)
            self._paused_partitions.discard(key)
        if self._last_seen is not None and (self._last_seen[0], self._last_seen[1]) in revoked:
            if self._processed_offsets:
                (topic, partition), offset = sorted(self._processed_offsets.items())[-1]
                self._last_seen = (topic, partition, offset)
            else:
                self._last_seen = None
        self._active_assignment.difference_update(revoked)
        self._checkpoint_dirty = True
        self._invalidate_health_snapshot_cache()

    async def _handle_batch_deserialize_error(
        self,
        messages: list[Any],
        exc: Exception,
    ) -> None:
        self._batch_deserialize_error_count += 1
        self._record_error_count += len(messages)
        poison_info = self._observe_poison_records(exc, count=len(messages))
        self._invalidate_health_snapshot_cache()
        batch_contexts = self._build_batch_contexts(messages)
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
            self._record_drop_count += len(messages)
            self._invalidate_health_snapshot_cache()
            for message in messages:
                self._remember_processed_offset(
                    message.topic,
                    message.partition,
                    message.offset,
                )
                self._remember_committable_offset(
                    message.topic,
                    message.partition,
                    message.offset,
                )
            if not self._enable_auto_commit:
                self._pending_commit_count += len(messages)
                self._invalidate_health_snapshot_cache()
                await self._commit_if_needed()
            return
        raise SourceRecordError(
            exc,
            record=[message.value for message in messages],
            checkpoint=self.current_checkpoint(),
            source=self.source_name,
        ) from exc

    async def _handle_batch_deserializer_count_mismatch(
        self,
        messages: list[Any],
        *,
        output_count: int,
    ) -> None:
        exc = ValueError(
            "Kafka batch_deserializer returned a different number of records than input "
            f"messages: input={len(messages)}, output={output_count}."
        )
        self._batch_deserialize_error_count += 1
        self._record_error_count += len(messages)
        poison_info = self._observe_poison_records(exc, count=len(messages))
        self._invalidate_health_snapshot_cache()
        batch_contexts = self._build_batch_contexts(messages)
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
            self._record_drop_count += len(messages)
            for message in messages:
                self._remember_processed_offset(
                    message.topic,
                    message.partition,
                    message.offset,
                )
                self._remember_committable_offset(
                    message.topic,
                    message.partition,
                    message.offset,
                )
            if not self._enable_auto_commit:
                self._pending_commit_count += len(messages)
                await self._commit_if_needed()
            self._invalidate_health_snapshot_cache()
            return
        raise SourceRecordError(
            exc,
            record=[message.value for message in messages],
            checkpoint=self.current_checkpoint(),
            source=self.source_name,
        ) from exc

    def pause(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self._sync_assignment_from_consumer()
        if partitions is None:
            self._pause_all_requested = True
            targets = set(self._active_assignment)
        else:
            targets = set(_normalize_topic_partitions(partitions))
            self._paused_partitions.update(targets)
        if partitions is None and targets:
            self._paused_partitions.update(targets)
        self._apply_pause_state(targets if targets else None)
        self._invalidate_health_snapshot_cache()

    def resume(self, partitions: Iterable[tuple[str, int]] | None = None) -> None:
        self._sync_assignment_from_consumer()
        if partitions is None:
            self._pause_all_requested = False
            targets = set(self._paused_partitions or self._active_assignment)
            self._paused_partitions.clear()
        else:
            targets = set(_normalize_topic_partitions(partitions))
            if self._pause_all_requested:
                self._pause_all_requested = False
                self._paused_partitions = set(self._active_assignment).difference(targets)
            self._paused_partitions.difference_update(targets)
        self._apply_resume_state(targets if targets else None)
        self._invalidate_health_snapshot_cache()

    def _apply_pause_state(self, partitions: object | None = None) -> None:
        if self._consumer is None:
            return
        pause = getattr(self._consumer, "pause", None)
        if not callable(pause):
            return
        targets = (
            set(self._active_assignment)
            if self._pause_all_requested
            else set(self._paused_partitions)
        )
        if partitions is not None:
            requested = {
                (str(topic), int(partition))
                for topic, partition in _normalize_assignment_items(partitions)
            }
            if requested:
                targets = targets.intersection(requested)
        if not targets:
            return
        pause(
            *[self._build_topic_partition(topic, partition) for topic, partition in sorted(targets)]
        )

    def _apply_resume_state(self, partitions: object | None = None) -> None:
        if self._consumer is None:
            return
        resume = getattr(self._consumer, "resume", None)
        if not callable(resume):
            return
        targets = (
            {
                (str(topic), int(partition))
                for topic, partition in _normalize_assignment_items(partitions)
            }
            if partitions is not None
            else set(self._active_assignment)
        )
        if not targets:
            return
        resume(
            *[self._build_topic_partition(topic, partition) for topic, partition in sorted(targets)]
        )

    def _build_batch_contexts(self, messages: list[Any]) -> list[_BatchMessageContext]:
        batch_size = len(messages)
        return [
            _BatchMessageContext(
                metadata=self._message_metadata(
                    message,
                    batch_size=batch_size,
                    batch_index=index,
                ),
                message=message,
            )
            for index, message in enumerate(messages)
        ]

    def _subscription_mode(self) -> str:
        if self._assignments:
            return "manual_assign"
        if self._topic_pattern is not None:
            return "topic_pattern"
        return "topics"

    def _paused_partition_count(self) -> int:
        if self._pause_all_requested:
            return len(self._active_assignment)
        return len(self._paused_partitions)

    def _should_continue_after_poison_record(self) -> bool:
        return should_continue_after_poison_record(self)

    @staticmethod
    def _resolve_poison_record_policy(
        policy: KafkaPoisonRecordPolicy | str | None,
        *,
        on_deserialize_error: SourceRecordFailurePolicy,
    ) -> KafkaPoisonRecordPolicy:
        return resolve_poison_record_policy(
            policy,
            on_deserialize_error=on_deserialize_error,
        )

    async def _capture_poison_batch(
        self,
        exc: Exception,
        messages: list[Any],
        batch_contexts: list[_BatchMessageContext],
        *,
        stage: str,
        poison_info: KafkaPoisonRecordInfo,
    ) -> None:
        await capture_poison_batch(
            self,
            exc,
            messages,
            batch_contexts,
            stage=stage,
            poison_info=poison_info,
        )

    async def _capture_poison_record(
        self,
        exc: Exception,
        message: Any,
        metadata: dict[str, Any],
        *,
        stage: str,
        poison_info: KafkaPoisonRecordInfo,
    ) -> None:
        await capture_poison_record(
            self,
            exc,
            message,
            metadata,
            stage=stage,
            poison_info=poison_info,
        )

    def _handle_poison_dlq_write_error(
        self,
        exc: Exception,
        *,
        record_count: int,
        stage: str,
    ) -> None:
        handle_poison_dlq_write_error(
            self,
            exc,
            record_count=record_count,
            stage=stage,
        )

    def _build_poison_dlq_record(
        self,
        exc: Exception,
        message: Any,
        metadata: dict[str, Any],
        *,
        stage: str,
        poison_info: KafkaPoisonRecordInfo,
    ) -> DLQRecord:
        return build_poison_dlq_record(
            self,
            exc,
            message,
            metadata,
            stage=stage,
            poison_info=poison_info,
        )

    def _observe_poison_records(
        self,
        exc: Exception,
        *,
        count: int,
    ) -> KafkaPoisonRecordInfo:
        return observe_poison_records(self, exc, count=count)

    def _classify_poison_record(
        self,
        exc: Exception,
    ) -> KafkaPoisonRecordClassification:
        return classify_poison_record(exc)

    def _invalidate_health_snapshot_cache(self) -> None:
        self._health_snapshot_cache = None
        self._health_snapshot_cache_monotonic = None

    def _cached_health_snapshot(
        self,
        *,
        force_refresh: bool,
    ) -> KafkaSourceHealthSnapshot | None:
        if (
            force_refresh
            or self._health_snapshot_cache_ms <= 0
            or self._health_snapshot_cache is None
            or self._health_snapshot_cache_monotonic is None
        ):
            return None
        age_ms = (time.monotonic() - self._health_snapshot_cache_monotonic) * 1000.0
        if age_ms > self._health_snapshot_cache_ms:
            return None
        return self._health_snapshot_cache

    async def _resolve_partition_value(
        self,
        resolver: Any,
        partition: object,
    ) -> int | None:
        value = resolver(partition)
        if isawaitable(value):
            value = await value
        if value is None:
            return None
        return int(value)

    @staticmethod
    def _normalize_checkpoint_offsets(value: dict[str, Any]) -> dict[tuple[str, int], int]:
        offsets: dict[tuple[str, int], int] = {}
        raw_offsets = value.get("offsets")
        if isinstance(raw_offsets, Iterable) and not isinstance(raw_offsets, (str, bytes, dict)):
            for item in raw_offsets:
                if not isinstance(item, dict):
                    continue
                if {"topic", "partition", "offset"} - set(item):
                    continue
                offsets[(str(item["topic"]), int(item["partition"]))] = int(item["offset"])

        if offsets:
            return offsets

        if {"topic", "partition", "offset"} - set(value):
            return {}
        return {
            (str(value["topic"]), int(value["partition"])): int(value["offset"]),
        }


__all__ = [
    "KafkaDeliveryContext",
    "KafkaPartitionHealth",
    "KafkaPoisonRecordPolicy",
    "KafkaSource",
    "KafkaSourceHealthSnapshot",
    "KafkaSourceOperationalMetrics",
]


def _age_ms(start: float | None, end: float) -> float | None:
    if start is None:
        return None
    return max(0.0, (end - start) * 1000.0)


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
