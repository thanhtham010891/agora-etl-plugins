"""Metrics and latency surface for PostgreSQL sinks."""

from __future__ import annotations

from agora_plugins.postgres.sinks._metrics import (
    PostgresLatencyHistogramSnapshot,
    PostgresSinkMetricsSnapshot,
)
from agora_plugins.postgres.sinks._sink_types import PostgresPoisonRecordClassification

POSTGRES_LATENCY_BUCKETS_S = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


class PostgresSinkMetricsSurface:
    """Owns metric snapshot assembly and latency histogram bookkeeping."""

    def __init__(self, sink: object) -> None:
        self._sink = sink

    def observe_retry(self) -> None:
        self._sink._retry_count += 1

    def observe_latency(self, operation: str, outcome: str, duration_s: float) -> None:
        key = (operation, outcome)
        buckets = self._sink._latency_bucket_counts.setdefault(
            key, [0 for _ in POSTGRES_LATENCY_BUCKETS_S]
        )
        for index, upper_bound_s in enumerate(POSTGRES_LATENCY_BUCKETS_S):
            if duration_s <= upper_bound_s:
                buckets[index] += 1
        self._sink._latency_counts[key] = self._sink._latency_counts.get(key, 0) + 1
        self._sink._latency_sums[key] = self._sink._latency_sums.get(key, 0.0) + duration_s

    def latency_histogram_snapshots(self) -> tuple[PostgresLatencyHistogramSnapshot, ...]:
        snapshots: list[PostgresLatencyHistogramSnapshot] = []
        for operation, outcome in sorted(self._sink._latency_counts):
            key = (operation, outcome)
            snapshots.append(
                PostgresLatencyHistogramSnapshot(
                    operation=operation,
                    outcome=outcome,
                    buckets=tuple(
                        zip(
                            POSTGRES_LATENCY_BUCKETS_S,
                            self._sink._latency_bucket_counts[key],
                            strict=True,
                        )
                    ),
                    count=self._sink._latency_counts[key],
                    sum_s=self._sink._latency_sums[key],
                )
            )
        return tuple(snapshots)

    def snapshot(self) -> PostgresSinkMetricsSnapshot:
        sink = self._sink
        return PostgresSinkMetricsSnapshot(
            table=str(sink._table),
            conflict_keys=tuple(str(key) for key in sink._conflict_keys),
            batch_size=sink._batch_size,
            upsert=sink._upsert,
            insert_mode=sink._insert_mode,
            pool_size=sink._pool_size,
            max_rows_per_statement=sink._max_rows_per_statement,
            max_parameters_per_statement=sink._max_parameters_per_statement,
            write_safety_policy=sink._write_safety_policy.value,
            buffered_row_count=len(sink._buffer),
            write_call_count=sink._write_call_count,
            write_batch_call_count=sink._write_batch_call_count,
            enqueue_count=sink._enqueue_count,
            flush_count=sink._flush_count,
            flushed_row_count=sink._flushed_row_count,
            retry_count=sink._retry_count,
            schema_refresh_count=sink._schema_refresh_count,
            schema_drift_detected_count=sink._schema_drift_detected_count,
            schema_drift_aligned_count=sink._schema_drift_aligned_count,
            poison_record_count=sink._poison_record_count,
            poison_record_schema_drift_count=sink._poison_record_classification_counts[
                PostgresPoisonRecordClassification.SCHEMA_DRIFT
            ],
            poison_record_constraint_violation_count=sink._poison_record_classification_counts[
                PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION
            ],
            poison_record_type_mismatch_count=sink._poison_record_classification_counts[
                PostgresPoisonRecordClassification.TYPE_MISMATCH
            ],
            poison_record_unknown_count=sink._poison_record_classification_counts[
                PostgresPoisonRecordClassification.UNKNOWN
            ],
            connection_ready=sink._conn is not None or sink._write_pool_open_connections > 0,
            pooled_connection_count=sink._write_pool_open_connections,
            pooled_available_count=(0 if sink._write_pool is None else sink._write_pool.qsize()),
            last_flush_at=sink._last_flush_at,
            latency_histograms=self.latency_histogram_snapshots(),
        )
