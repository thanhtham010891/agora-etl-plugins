"""Typed public models shared by the Kafka source implementation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from agora.core.failures import PoisonRecordClassification, PoisonRecordInfo
from agora.core.health import ComponentHealthSnapshot

try:
    from agora.core.failures import PoisonRecordPolicy as _CorePoisonRecordPolicy
except ImportError:
    # Keep the Kafka plugin wheel compatible with older published core builds
    # that do not yet export PoisonRecordPolicy.
    class KafkaPoisonRecordPolicy(StrEnum):
        """Source-side poison-record handling policy for Kafka ingestion."""

        FAIL_CLOSED = "fail_closed"
        LOG_AND_CONTINUE = "log_and_continue"
        DLQ_AND_CONTINUE = "dlq_and_continue"
        DLQ_AND_FAIL_CLOSED = "dlq_and_fail_closed"


else:
    KafkaPoisonRecordPolicy = _CorePoisonRecordPolicy


@dataclass(frozen=True, slots=True)
class BatchMessageContext:
    """Internal pairing of a broker message and its normalized metadata."""

    metadata: dict[str, Any]
    message: Any


@dataclass(frozen=True, slots=True)
class KafkaSourceOperationalMetrics:
    """Kafka-specific operational counters that do not belong in core metrics."""

    rebalance_count: int = 0
    batch_deserialize_error_count: int = 0
    manual_assign_partition_count: int = 0
    paused_partition_count: int = 0
    poison_record_dlq_write_count: int = 0
    poison_record_dlq_write_failure_count: int = 0
    poison_record_log_only_count: int = 0
    poison_record_fail_closed_count: int = 0
    poison_record_deserialization_count: int = 0
    poison_record_schema_evolution_count: int = 0
    poison_record_schema_validation_count: int = 0
    poison_record_schema_registry_binding_mismatch_count: int = 0
    poison_record_unknown_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "rebalance_count": self.rebalance_count,
            "batch_deserialize_error_count": self.batch_deserialize_error_count,
            "manual_assign_partition_count": self.manual_assign_partition_count,
            "paused_partition_count": self.paused_partition_count,
            "poison_record_dlq_write_count": self.poison_record_dlq_write_count,
            "poison_record_dlq_write_failure_count": self.poison_record_dlq_write_failure_count,
            "poison_record_log_only_count": self.poison_record_log_only_count,
            "poison_record_fail_closed_count": self.poison_record_fail_closed_count,
            "poison_record_deserialization_count": self.poison_record_deserialization_count,
            "poison_record_schema_evolution_count": self.poison_record_schema_evolution_count,
            "poison_record_schema_validation_count": self.poison_record_schema_validation_count,
            "poison_record_schema_registry_binding_mismatch_count": (
                self.poison_record_schema_registry_binding_mismatch_count
            ),
            "poison_record_unknown_count": self.poison_record_unknown_count,
        }


@dataclass(frozen=True, slots=True)
class KafkaDeliveryContext:
    """Stable per-record delivery coordinates for downstream idempotency."""

    topic: str
    partition: int
    offset: int
    consumer_group: str
    bootstrap_servers: str
    subscription_mode: str
    batch_size: int = 1
    batch_index: int = 0
    key: bytes | None = None
    headers: tuple[tuple[str, bytes], ...] = ()
    timestamp: int | None = None
    timestamp_type: int | None = None

    @property
    def delivery_id(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "consumer_group": self.consumer_group,
            "bootstrap_servers": self.bootstrap_servers,
            "subscription_mode": self.subscription_mode,
            "batch_size": self.batch_size,
            "batch_index": self.batch_index,
            "key": self.key,
            "headers": list(self.headers),
            "timestamp": self.timestamp,
            "timestamp_type": self.timestamp_type,
            "delivery_id": self.delivery_id,
        }


KafkaPoisonRecordClassification = PoisonRecordClassification


@dataclass(frozen=True, slots=True, kw_only=True)
class KafkaPoisonRecordInfo(PoisonRecordInfo):
    """Structured poison metadata persisted alongside DLQ payloads."""

    classification: KafkaPoisonRecordClassification
    policy: KafkaPoisonRecordPolicy

    def to_dict(self) -> dict[str, str]:
        return {
            "classification": self.classification.value,
            "policy": self.policy.value,
        }


@dataclass(frozen=True, slots=True)
class KafkaPartitionHealth:
    topic: str
    partition: int
    current_offset: int | None = None
    committed_offset: int | None = None
    processed_offset: int | None = None
    committable_offset: int | None = None
    end_offset: int | None = None
    lag: int | None = None
    commit_lag: int | None = None
    delivery_gap: int | None = None
    commit_gap: int | None = None
    paused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "partition": self.partition,
            "current_offset": self.current_offset,
            "committed_offset": self.committed_offset,
            "processed_offset": self.processed_offset,
            "committable_offset": self.committable_offset,
            "end_offset": self.end_offset,
            "lag": self.lag,
            "commit_lag": self.commit_lag,
            "delivery_gap": self.delivery_gap,
            "commit_gap": self.commit_gap,
            "paused": self.paused,
        }


@dataclass(frozen=True, slots=True)
class KafkaSourceHealthSnapshot(ComponentHealthSnapshot):
    stalled: bool
    consumer_group: str
    bootstrap_servers: str
    subscription_mode: str
    assignment_count: int
    paused_partition_count: int
    pending_commit_count: int
    rebalance_count: int
    idle_poll_count: int
    record_error_count: int
    record_drop_count: int
    last_poll_age_ms: float | None = None
    last_message_age_ms: float | None = None
    last_commit_age_ms: float | None = None
    last_rebalance_age_ms: float | None = None
    total_lag: int | None = None
    lagging_partition_count: int = 0
    max_lag: int | None = None
    total_commit_lag: int | None = None
    max_commit_lag: int | None = None
    partitions: tuple[KafkaPartitionHealth, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "stalled": self.stalled,
            "consumer_group": self.consumer_group,
            "bootstrap_servers": self.bootstrap_servers,
            "subscription_mode": self.subscription_mode,
            "assignment_count": self.assignment_count,
            "paused_partition_count": self.paused_partition_count,
            "pending_commit_count": self.pending_commit_count,
            "rebalance_count": self.rebalance_count,
            "idle_poll_count": self.idle_poll_count,
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
            "last_poll_age_ms": self.last_poll_age_ms,
            "last_message_age_ms": self.last_message_age_ms,
            "last_commit_age_ms": self.last_commit_age_ms,
            "last_rebalance_age_ms": self.last_rebalance_age_ms,
            "total_lag": self.total_lag,
            "lagging_partition_count": self.lagging_partition_count,
            "max_lag": self.max_lag,
            "total_commit_lag": self.total_commit_lag,
            "max_commit_lag": self.max_commit_lag,
            "partitions": [partition.to_dict() for partition in self.partitions],
        }


__all__ = [
    "KafkaDeliveryContext",
    "KafkaPartitionHealth",
    "KafkaPoisonRecordClassification",
    "KafkaPoisonRecordInfo",
    "KafkaPoisonRecordPolicy",
    "KafkaSourceHealthSnapshot",
    "KafkaSourceOperationalMetrics",
]
