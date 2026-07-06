"""Redis observability snapshot models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.health import ComponentHealthSnapshot

if TYPE_CHECKING:
    from datetime import datetime


def isoformat_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


@dataclass(frozen=True, slots=True)
class RedisStreamSourceHealthSnapshot(ComponentHealthSnapshot):
    """Operator-facing readiness snapshot for RedisStreamSource."""

    connection_ready: bool
    group_ready: bool
    ack_enabled: bool
    reclaim_enabled: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "connection_ready": self.connection_ready,
            "group_ready": self.group_ready,
            "ack_enabled": self.ack_enabled,
            "reclaim_enabled": self.reclaim_enabled,
            "last_error": self.last_error,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class RedisSourcePoisonLoopRiskSnapshot:
    """Operator-facing snapshot for reclaimed poison messages looping unacked."""

    detected: bool
    loop_count: int = 0
    distinct_message_count: int = 0
    last_message_id: str | None = None
    last_detected_at: datetime | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "detected": self.detected,
            "loop_count": self.loop_count,
            "distinct_message_count": self.distinct_message_count,
            "last_message_id": self.last_message_id,
            "last_detected_at": isoformat_or_none(self.last_detected_at),
        }


@dataclass(frozen=True, slots=True)
class RedisStreamSourceMetricsSnapshot:
    """Operational metrics for RedisStreamSource activity."""

    stream: str
    group: str
    consumer: str
    block_ms: int
    batch_size: int
    ack_batch_size: int
    ack_on_success: bool
    reclaim_idle_ms: int | None
    reclaim_batch_size: int
    max_consecutive_reclaim_batches: int | None
    health: RedisStreamSourceHealthSnapshot
    poison_loop_risk: RedisSourcePoisonLoopRiskSnapshot
    read_call_count: int = 0
    reconnect_count: int = 0
    reclaimed_message_count: int = 0
    consecutive_reclaim_batch_count: int = 0
    reclaim_fairness_yield_count: int = 0
    ack_flush_count: int = 0
    acked_message_count: int = 0
    emitted_record_count: int = 0
    pending_ack_count: int = 0
    record_error_count: int = 0
    record_drop_count: int = 0
    last_message_id: str | None = None
    last_read_at: datetime | None = None
    last_reconnect_at: datetime | None = None
    last_ack_at: datetime | None = None
    last_reclaim_at: datetime | None = None
    last_error_at: datetime | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "stream": self.stream,
            "group": self.group,
            "consumer": self.consumer,
            "block_ms": self.block_ms,
            "batch_size": self.batch_size,
            "ack_batch_size": self.ack_batch_size,
            "ack_on_success": self.ack_on_success,
            "reclaim_idle_ms": self.reclaim_idle_ms,
            "reclaim_batch_size": self.reclaim_batch_size,
            "max_consecutive_reclaim_batches": self.max_consecutive_reclaim_batches,
            "health": self.health.to_dict(),
            "poison_loop_risk": self.poison_loop_risk.to_dict(),
            "read_call_count": self.read_call_count,
            "reconnect_count": self.reconnect_count,
            "reclaimed_message_count": self.reclaimed_message_count,
            "consecutive_reclaim_batch_count": self.consecutive_reclaim_batch_count,
            "reclaim_fairness_yield_count": self.reclaim_fairness_yield_count,
            "ack_flush_count": self.ack_flush_count,
            "acked_message_count": self.acked_message_count,
            "emitted_record_count": self.emitted_record_count,
            "pending_ack_count": self.pending_ack_count,
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
            "last_message_id": self.last_message_id,
            "last_read_at": isoformat_or_none(self.last_read_at),
            "last_reconnect_at": isoformat_or_none(self.last_reconnect_at),
            "last_ack_at": isoformat_or_none(self.last_ack_at),
            "last_reclaim_at": isoformat_or_none(self.last_reclaim_at),
            "last_error_at": isoformat_or_none(self.last_error_at),
        }


@dataclass(frozen=True, slots=True)
class RedisDLQSinkMetricsSnapshot:
    """Operational metrics for Redis-backed DLQ sink activity."""

    key_prefix: str
    connection_ready: bool
    write_call_count: int = 0
    write_batch_call_count: int = 0
    inserted_record_count: int = 0
    upserted_record_count: int = 0
    updated_record_count: int = 0
    replay_count: int = 0
    replayed_record_count: int = 0
    acknowledge_count: int = 0
    acknowledged_record_count: int = 0
    last_write_at: datetime | None = None
    last_replay_at: datetime | None = None
    last_acknowledge_at: datetime | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "key_prefix": self.key_prefix,
            "connection_ready": self.connection_ready,
            "write_call_count": self.write_call_count,
            "write_batch_call_count": self.write_batch_call_count,
            "inserted_record_count": self.inserted_record_count,
            "upserted_record_count": self.upserted_record_count,
            "updated_record_count": self.updated_record_count,
            "replay_count": self.replay_count,
            "replayed_record_count": self.replayed_record_count,
            "acknowledge_count": self.acknowledge_count,
            "acknowledged_record_count": self.acknowledged_record_count,
            "last_write_at": isoformat_or_none(self.last_write_at),
            "last_replay_at": isoformat_or_none(self.last_replay_at),
            "last_acknowledge_at": isoformat_or_none(self.last_acknowledge_at),
        }


@dataclass(frozen=True, slots=True)
class RedisDLQSourceMetricsSnapshot:
    """Operational metrics for Redis-backed DLQ source scans."""

    key_prefix: str
    pipeline_id: str | None
    stage: str | None
    limit: int | None
    connection_ready: bool
    scan_count: int = 0
    emitted_record_count: int = 0
    last_scan_at: datetime | None = None
    last_record_at: datetime | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "key_prefix": self.key_prefix,
            "pipeline_id": self.pipeline_id,
            "stage": self.stage,
            "limit": self.limit,
            "connection_ready": self.connection_ready,
            "scan_count": self.scan_count,
            "emitted_record_count": self.emitted_record_count,
            "last_scan_at": isoformat_or_none(self.last_scan_at),
            "last_record_at": isoformat_or_none(self.last_record_at),
        }
