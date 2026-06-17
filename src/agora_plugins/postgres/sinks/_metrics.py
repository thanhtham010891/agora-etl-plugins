"""Typed PostgreSQL sink metric snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(frozen=True, slots=True)
class PostgresLatencyHistogramSnapshot:
    operation: str
    outcome: str
    buckets: tuple[tuple[float, int], ...]
    count: int
    sum_s: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "outcome": self.outcome,
            "buckets": [{"le": le, "count": count} for le, count in self.buckets],
            "count": self.count,
            "sum_s": self.sum_s,
        }


@dataclass(frozen=True, slots=True)
class PostgresSinkMetricsSnapshot:
    """Operational snapshot for PostgreSQL sink buffering and flush behavior."""

    table: str
    conflict_keys: tuple[str, ...]
    batch_size: int
    upsert: bool
    insert_mode: str
    pool_size: int
    max_rows_per_statement: int | None
    max_parameters_per_statement: int
    write_safety_policy: str
    buffered_row_count: int = 0
    write_call_count: int = 0
    write_batch_call_count: int = 0
    enqueue_count: int = 0
    flush_count: int = 0
    flushed_row_count: int = 0
    retry_count: int = 0
    schema_refresh_count: int = 0
    schema_drift_detected_count: int = 0
    schema_drift_aligned_count: int = 0
    poison_record_count: int = 0
    poison_record_schema_drift_count: int = 0
    poison_record_constraint_violation_count: int = 0
    poison_record_type_mismatch_count: int = 0
    poison_record_unknown_count: int = 0
    connection_ready: bool = False
    pooled_connection_count: int = 0
    pooled_available_count: int = 0
    last_flush_at: datetime | None = None
    latency_histograms: tuple[PostgresLatencyHistogramSnapshot, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "conflict_keys": list(self.conflict_keys),
            "batch_size": self.batch_size,
            "upsert": self.upsert,
            "insert_mode": self.insert_mode,
            "pool_size": self.pool_size,
            "max_rows_per_statement": self.max_rows_per_statement,
            "max_parameters_per_statement": self.max_parameters_per_statement,
            "write_safety_policy": self.write_safety_policy,
            "buffered_row_count": self.buffered_row_count,
            "write_call_count": self.write_call_count,
            "write_batch_call_count": self.write_batch_call_count,
            "enqueue_count": self.enqueue_count,
            "flush_count": self.flush_count,
            "flushed_row_count": self.flushed_row_count,
            "retry_count": self.retry_count,
            "schema_refresh_count": self.schema_refresh_count,
            "schema_drift_detected_count": self.schema_drift_detected_count,
            "schema_drift_aligned_count": self.schema_drift_aligned_count,
            "poison_record_count": self.poison_record_count,
            "poison_record_schema_drift_count": self.poison_record_schema_drift_count,
            "poison_record_constraint_violation_count": (
                self.poison_record_constraint_violation_count
            ),
            "poison_record_type_mismatch_count": self.poison_record_type_mismatch_count,
            "poison_record_unknown_count": self.poison_record_unknown_count,
            "connection_ready": self.connection_ready,
            "pooled_connection_count": self.pooled_connection_count,
            "pooled_available_count": self.pooled_available_count,
            "last_flush_at": (
                None if self.last_flush_at is None else self.last_flush_at.isoformat()
            ),
            "latency_histograms": [histogram.to_dict() for histogram in self.latency_histograms],
        }


__all__ = ["PostgresLatencyHistogramSnapshot", "PostgresSinkMetricsSnapshot"]
