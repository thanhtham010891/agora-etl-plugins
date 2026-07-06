"""Public-facing runtime surface for Kafka sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora_plugins.kafka.sources._cursor_state import (
    normalize_checkpoint_offsets as normalize_checkpoint_offsets_from_checkpoint,
)
from agora_plugins.kafka.sources._models import (
    KafkaPoisonRecordClassification,
    KafkaSourceOperationalMetrics,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.source import SourceRuntimeMetrics

    from agora_plugins.kafka.sources._cursor_state import KafkaCursorState
    from agora_plugins.kafka.sources._delivery import KafkaDeliveryController
    from agora_plugins.kafka.sources._models import KafkaDeliveryContext
    from agora_plugins.kafka.sources._operator_controls import KafkaOperatorController
    from agora_plugins.kafka.sources._poison_controller import KafkaPoisonController


class KafkaSourceSurface:
    """Owns the public runtime/delivery API surfaced by ``KafkaSource``."""

    def __init__(
        self,
        *,
        cursor_state: KafkaCursorState,
        delivery_controller: KafkaDeliveryController,
        poison_controller: KafkaPoisonController,
        operator_controls: KafkaOperatorController,
        rebalance_count: Callable[[], int],
        manual_assign_partition_count: Callable[[], int],
    ) -> None:
        self._cursor_state = cursor_state
        self._delivery_controller = delivery_controller
        self._poison_controller = poison_controller
        self._operator_controls = operator_controls
        self._rebalance_count = rebalance_count
        self._manual_assign_partition_count = manual_assign_partition_count

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return self._delivery_controller.runtime_metrics()

    def operational_metrics(self) -> KafkaSourceOperationalMetrics:
        return KafkaSourceOperationalMetrics(
            rebalance_count=self._rebalance_count(),
            batch_deserialize_error_count=self._delivery_controller.batch_deserialize_error_count,
            manual_assign_partition_count=self._manual_assign_partition_count(),
            paused_partition_count=self._operator_controls.paused_partition_count(),
            poison_record_dlq_write_count=self._poison_controller.dlq_write_count,
            poison_record_dlq_write_failure_count=self._poison_controller.dlq_write_failure_count,
            poison_record_log_only_count=self._poison_controller.log_only_count,
            poison_record_fail_closed_count=self._poison_controller.fail_closed_count,
            poison_record_deserialization_count=self._poison_controller.classification_count(
                KafkaPoisonRecordClassification.DESERIALIZATION
            ),
            poison_record_schema_evolution_count=self._poison_controller.classification_count(
                KafkaPoisonRecordClassification.SCHEMA_EVOLUTION
            ),
            poison_record_schema_validation_count=self._poison_controller.classification_count(
                KafkaPoisonRecordClassification.SCHEMA_VALIDATION
            ),
            poison_record_schema_registry_binding_mismatch_count=(
                self._poison_controller.classification_count(
                    KafkaPoisonRecordClassification.SCHEMA_REGISTRY_BINDING_MISMATCH
                )
            ),
            poison_record_unknown_count=self._poison_controller.classification_count(
                KafkaPoisonRecordClassification.UNKNOWN
            ),
        )

    async def prepare_resume(self, checkpoint: Any) -> None:
        if checkpoint is None or not isinstance(checkpoint.value, dict):
            self._cursor_state.prepare_resume({})
            return
        self._cursor_state.prepare_resume(
            normalize_checkpoint_offsets_from_checkpoint(checkpoint.value)
        )

    def current_checkpoint(self) -> dict[str, Any] | None:
        return self._cursor_state.current_checkpoint()

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._delivery_controller.delivery_success_callback()

    def delivery_transaction_offsets_callback(
        self,
    ) -> Callable[[], Awaitable[tuple[dict[Any, int], str]]] | None:
        return self._delivery_controller.delivery_transaction_offsets_callback()

    def delivery_transaction_committed_callback(self) -> Callable[[], Awaitable[None]] | None:
        return self._delivery_controller.delivery_transaction_committed_callback()

    def delivery_context(self) -> KafkaDeliveryContext | None:
        return self._delivery_controller.delivery_context()
