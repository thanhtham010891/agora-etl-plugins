"""
agora_plugins.kafka.sources.kafka
=================================
Async Kafka source powered by ``aiokafka``.

Requires: ``pip install 'agora-etl-plugins[kafka]'``
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.core.source import BaseSource
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.kafka._security_posture import warn_if_insecure_plaintext
from agora_plugins.kafka.sources._commit_runtime import KafkaCommitRuntime
from agora_plugins.kafka.sources._consumer_runtime import KafkaConsumerRuntime
from agora_plugins.kafka.sources._cursor_state import KafkaCursorState
from agora_plugins.kafka.sources._delivery import KafkaDeliveryController
from agora_plugins.kafka.sources._deserializer_runtime import KafkaDeserializerRuntime
from agora_plugins.kafka.sources._facade import KafkaSourceFacade
from agora_plugins.kafka.sources._health import KafkaHealthMonitor
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
from agora_plugins.kafka.sources._operator_api import KafkaOperatorAPI
from agora_plugins.kafka.sources._operator_controls import KafkaOperatorController
from agora_plugins.kafka.sources._poison import resolve_poison_record_policy
from agora_plugins.kafka.sources._poison_controller import KafkaPoisonController
from agora_plugins.kafka.sources._public_api import KafkaSourcePublicAPI
from agora_plugins.kafka.sources._rebalance import (
    normalize_topic_partitions as _normalize_topic_partitions,
)
from agora_plugins.kafka.sources._runtime_state import KafkaRuntimeState
from agora_plugins.kafka.sources._session_runtime import KafkaConsumerSession
from agora_plugins.kafka.sources._source_config import (
    resolve_security,
    validate_extra_consumer_config,
    validate_int_config,
)
from agora_plugins.kafka.sources._source_surface import KafkaSourceSurface
from agora_plugins.kafka.sources._stream_runtime import KafkaStreamRuntime
from agora_plugins.kafka.tracing import KafkaOpenTelemetryTracing

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from agora.core.dlq import DLQSink

    from agora_plugins.kafka.config import KafkaSecurityConfig

T = TypeVar("T")


class KafkaSource(KafkaSourceFacade, BaseSource[T], Generic[T]):
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
        self._batch_deserializer = batch_deserializer
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._commit_every = validate_int_config("commit_every", commit_every, minimum=1)
        self._poll_timeout_ms = validate_int_config(
            "poll_timeout_ms",
            poll_timeout_ms,
            minimum=0,
        )
        self._max_idle_polls = (
            None
            if max_idle_polls is None
            else validate_int_config("max_idle_polls", max_idle_polls, minimum=1)
        )
        self._max_poll_records = validate_int_config(
            "max_poll_records",
            max_poll_records,
            minimum=1,
        )
        self._fetch_min_bytes = validate_int_config(
            "fetch_min_bytes",
            fetch_min_bytes,
            minimum=1,
        )
        self._fetch_max_wait_ms = validate_int_config(
            "fetch_max_wait_ms",
            fetch_max_wait_ms,
            minimum=0,
        )
        self._max_partition_fetch_bytes = validate_int_config(
            "max_partition_fetch_bytes",
            max_partition_fetch_bytes,
            minimum=1,
        )
        self._security = resolve_security(security_protocol, security)
        self._security_protocol = (
            self._security.security_protocol if self._security is not None else security_protocol
        )
        warn_if_insecure_plaintext(
            subject=type(self).__name__,
            security_protocol=self._security_protocol,
            bootstrap_servers=bootstrap_servers,
        )
        self._extra_config = dict(extra_config or {})
        validate_extra_consumer_config(self._extra_config)
        self._cursor_state = KafkaCursorState(
            start_offsets=start_offsets or {},
            on_change=self._invalidate_health_snapshot_cache,
        )
        poison_record_policy = resolve_poison_record_policy(
            poison_record_policy,
            on_deserialize_error=on_deserialize_error,
        )
        if (
            poison_record_policy
            in {
                KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
                KafkaPoisonRecordPolicy.DLQ_AND_FAIL_CLOSED,
            }
            and poison_record_sink is None
        ):
            raise ValueError(
                "KafkaSource poison_record_policy requires poison_record_sink when using DLQ modes."
            )
        self._health_snapshot_cache_ms = validate_int_config(
            "health_snapshot_cache_ms",
            health_snapshot_cache_ms,
            minimum=0,
        )
        self._tracing = KafkaOpenTelemetryTracing.from_config(tracing)
        self._consumer = None
        self._topic_partition_cls = None
        self._operator_controls = KafkaOperatorController(
            assignments=self._assignments,
            on_change=self._invalidate_health_snapshot_cache,
        )
        self._runtime_state = KafkaRuntimeState(
            on_change=self._invalidate_health_snapshot_cache,
        )
        self._poison_controller = KafkaPoisonController(
            source_name=self.source_name,
            policy=poison_record_policy,
            sink=poison_record_sink,
            pipeline_id=poison_record_pipeline_id or f"kafka:{group_id}",
            max_attempts=poison_record_max_attempts,
            on_change=self._invalidate_health_snapshot_cache,
        )
        self._deserializer_runtime = KafkaDeserializerRuntime(
            topics=self._topics,
            topic_pattern=self._topic_pattern,
            group_id=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            deserializer=self._deserializer,
            batch_deserializer=self._batch_deserializer,
            subscription_mode=self._subscription_mode,
            active_assignment=lambda: self._operator_controls.active_assignment,
            tracing=self._tracing,
        )
        self._consumer_runtime = KafkaConsumerRuntime(
            topics=self._topics,
            topic_pattern=self._topic_pattern,
            assignments=self._assignments,
            group_id=self._group_id,
            rebalance_listener=rebalance_listener,
            operator_controls=self._operator_controls,
            cursor_state=self._cursor_state,
            commit_if_needed=self._commit_if_needed,
            on_rebalance=self._runtime_state.record_rebalance,
            build_topic_partition=self._build_topic_partition,
            on_change=self._invalidate_health_snapshot_cache,
        )
        self._commit_runtime = KafkaCommitRuntime(
            group_id=self._group_id,
            enable_auto_commit=self._enable_auto_commit,
            commit_every=self._commit_every,
            tracing=self._tracing,
            cursor_state=self._cursor_state,
            build_topic_partition=self._build_topic_partition,
            current_consumer=lambda: self._consumer,
            on_commit_recorded=self._runtime_state.record_commit_progress,
        )
        self._delivery_controller = KafkaDeliveryController(
            cursor_state=self._cursor_state,
            group_id=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            source_name=self.source_name,
            enable_auto_commit=self._enable_auto_commit,
            subscription_mode=self._subscription_mode,
            build_topic_partition=self._build_topic_partition,
            commit_if_needed=self._commit_runtime.commit_if_needed,
            on_delivery_progress=self._runtime_state.record_delivery_progress,
            on_commit_recorded=self._runtime_state.record_commit_progress,
            on_state_changed=self._invalidate_health_snapshot_cache,
            current_checkpoint=self.current_checkpoint,
            capture_poison_batch=self._poison_controller.capture_batch,
            capture_poison_record=self._poison_controller.capture_record,
            observe_poison_records=self._poison_controller.observe_records,
            should_continue_after_poison_record=self._poison_controller.should_continue,
        )
        self._operator_api = KafkaOperatorAPI(
            group_id=self._group_id,
            tracing=self._tracing,
            cursor_state=self._cursor_state,
            operator_controls=self._operator_controls,
            consumer_runtime=self._consumer_runtime,
            build_topic_partition=self._build_topic_partition,
            current_consumer=lambda: self._consumer,
            require_open_consumer=self._require_open_consumer,
            on_commit_recorded=self._runtime_state.record_commit_progress,
        )
        self._session_runtime = KafkaConsumerSession(
            topics=self._topics,
            topic_pattern=self._topic_pattern,
            assignments=self._assignments,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=self._enable_auto_commit,
            max_poll_records=self._max_poll_records,
            fetch_min_bytes=self._fetch_min_bytes,
            fetch_max_wait_ms=self._fetch_max_wait_ms,
            max_partition_fetch_bytes=self._max_partition_fetch_bytes,
            extra_config=self._extra_config,
            rebalance_owner=self,
            consumer_runtime=self._consumer_runtime,
            poison_controller=self._poison_controller,
            deserializer_runtime=self._deserializer_runtime,
            security_kwargs=self._security_kwargs,
            current_consumer=lambda: self._consumer,
            set_consumer=self._set_consumer,
            set_topic_partition_cls=self._set_topic_partition_cls,
            commit_if_needed=self._commit_runtime.commit_if_needed,
            on_consumer_closed=self._handle_consumer_closed,
        )
        self._source_surface = KafkaSourceSurface(
            cursor_state=self._cursor_state,
            delivery_controller=self._delivery_controller,
            poison_controller=self._poison_controller,
            operator_controls=self._operator_controls,
            rebalance_count=lambda: self._runtime_state.rebalance_count,
            manual_assign_partition_count=lambda: len(self._assignments),
        )
        self._stream_runtime = KafkaStreamRuntime(
            group_id=self._group_id,
            topics=self._topics,
            topic_pattern=self._topic_pattern,
            assignments=self._assignments,
            poll_timeout_ms=self._poll_timeout_ms,
            max_poll_records=self._max_poll_records,
            max_idle_polls=self._max_idle_polls,
            has_batch_deserializer=self._batch_deserializer is not None,
            operator_controls=self._operator_controls,
            runtime_state=self._runtime_state,
            cursor_state=self._cursor_state,
            deserializer_runtime=self._deserializer_runtime,
            delivery_controller=self._delivery_controller,
            consumer_runtime=self._consumer_runtime,
            bootstrap_consumer_state=self._consumer_runtime.bootstrap_consumer_state,
            commit_if_needed=self._commit_runtime.commit_if_needed,
            on_state_changed=self._invalidate_health_snapshot_cache,
        )
        self._health_monitor = KafkaHealthMonitor(
            cache_ms=self._health_snapshot_cache_ms,
            group_id=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            subscription_mode=self._subscription_mode,
            cursor_state=self._cursor_state,
            operator_controls=self._operator_controls,
            delivery_controller=self._delivery_controller,
            active_assignment=lambda: self._operator_controls.active_assignment,
            refresh_assignment_state=lambda consumer: (
                self._consumer_runtime.refresh_assignment_state(consumer)
            ),
            probe_partitions=self._consumer_runtime.probe_partitions,
        )
        self._public_api = KafkaSourcePublicAPI(
            source_surface=self._source_surface,
            operator_api=self._operator_api,
            health_monitor=self._health_monitor,
            stream_runtime=self._stream_runtime,
            current_consumer=lambda: self._consumer,
            runtime_state=self._runtime_state,
            max_idle_polls=self._max_idle_polls,
        )

    async def open(self) -> None:
        await self._session_runtime.open()

    async def close(self) -> None:
        await self._session_runtime.close()

    async def _commit_if_needed(self, *, force: bool = False) -> None:
        await self._commit_runtime.commit_if_needed(force=force)


__all__ = [
    "KafkaDeliveryContext",
    "KafkaPartitionHealth",
    "KafkaPoisonRecordClassification",
    "KafkaPoisonRecordInfo",
    "KafkaPoisonRecordPolicy",
    "KafkaSource",
    "KafkaSourceHealthSnapshot",
    "KafkaSourceOperationalMetrics",
]
