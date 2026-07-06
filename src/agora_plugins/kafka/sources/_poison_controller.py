"""Public-facing poison-record policy controller for Kafka sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora_plugins.kafka._lifecycle import call_lifecycle
from agora_plugins.kafka.sources._models import KafkaPoisonRecordClassification
from agora_plugins.kafka.sources._poison import (
    capture_poison_batch,
    capture_poison_record,
    observe_poison_records,
    should_continue_after_poison_record,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.dlq import DLQSink

    from agora_plugins.kafka.sources._models import (
        BatchMessageContext,
        KafkaPoisonRecordInfo,
        KafkaPoisonRecordPolicy,
    )


class KafkaPoisonController:
    """Owns poison policy state, counters, and sink lifecycle."""

    def __init__(
        self,
        *,
        source_name: str,
        policy: KafkaPoisonRecordPolicy,
        sink: DLQSink | None,
        pipeline_id: str,
        max_attempts: int | None,
        on_change: Callable[[], None],
    ) -> None:
        self.source_name = source_name
        self._poison_record_policy = policy
        self._poison_record_sink = sink
        self._poison_record_pipeline_id = pipeline_id
        self._poison_record_max_attempts = max_attempts
        self._poison_record_dlq_write_count = 0
        self._poison_record_dlq_write_failure_count = 0
        self._poison_record_log_only_count = 0
        self._poison_record_fail_closed_count = 0
        self._poison_record_classification_counts = dict.fromkeys(
            KafkaPoisonRecordClassification,
            0,
        )
        self._on_change = on_change

    async def open(self) -> None:
        await call_lifecycle(self._poison_record_sink, "open")

    async def close(self) -> None:
        await call_lifecycle(self._poison_record_sink, "close")

    def should_continue(self) -> bool:
        return should_continue_after_poison_record(self)

    def observe_records(self, exc: Exception, count: int) -> KafkaPoisonRecordInfo:
        return observe_poison_records(self, exc, count=count)

    async def capture_batch(
        self,
        exc: Exception,
        messages: list[Any],
        batch_contexts: list[BatchMessageContext],
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

    async def capture_record(
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

    def classification_count(self, classification: KafkaPoisonRecordClassification) -> int:
        return self._poison_record_classification_counts[classification]

    @property
    def dlq_write_count(self) -> int:
        return self._poison_record_dlq_write_count

    @property
    def dlq_write_failure_count(self) -> int:
        return self._poison_record_dlq_write_failure_count

    @property
    def log_only_count(self) -> int:
        return self._poison_record_log_only_count

    @property
    def fail_closed_count(self) -> int:
        return self._poison_record_fail_closed_count

    def _invalidate_health_snapshot_cache(self) -> None:
        self._on_change()
