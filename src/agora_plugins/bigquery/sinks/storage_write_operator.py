"""Operator-facing surface for BigQuery Storage Write sinks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora_plugins.bigquery.observability import (
    BigQueryEnterpriseAcceptanceGate,
    BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds,
    BigQueryStorageWriteSinkHealthSnapshot,
    BigQueryStorageWriteSinkMetricsSnapshot,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class BigQueryStorageWriteSinkOperatorSurface:
    """Public-facing observability surface for BigQuery Storage Write sinks."""

    def __init__(
        self,
        *,
        sink_name: str,
        table: str,
        connection_ready: Callable[[], bool],
        stream_name: Callable[[], str | None],
        batch_size: Callable[[], int],
        max_request_bytes: Callable[[], int],
        buffered_row_count: Callable[[], int],
        flush_count: Callable[[], int],
        append_error_count: Callable[[], int],
        submitted_row_count: Callable[[], int],
        appended_row_count: Callable[[], int],
        last_append_offset: Callable[[], int | None],
        last_append_at: Callable[[], Any | None],
        last_append_succeeded: Callable[[], bool | None],
        last_error: Callable[[], str | None],
        acceptance_gate_factory: Callable[[], BigQueryEnterpriseAcceptanceGate],
    ) -> None:
        self._sink_name = sink_name
        self._table = table
        self._connection_ready = connection_ready
        self._stream_name = stream_name
        self._batch_size = batch_size
        self._max_request_bytes = max_request_bytes
        self._buffered_row_count = buffered_row_count
        self._flush_count = flush_count
        self._append_error_count = append_error_count
        self._submitted_row_count = submitted_row_count
        self._appended_row_count = appended_row_count
        self._last_append_offset = last_append_offset
        self._last_append_at = last_append_at
        self._last_append_succeeded = last_append_succeeded
        self._last_error = last_error
        self._acceptance_gate_factory = acceptance_gate_factory

    def metrics_snapshot(self) -> BigQueryStorageWriteSinkMetricsSnapshot:
        return BigQueryStorageWriteSinkMetricsSnapshot(
            table=self._table,
            stream_name=self._stream_name(),
            batch_size=self._batch_size(),
            max_request_bytes=self._max_request_bytes(),
            connection_ready=self._connection_ready(),
            buffered_row_count=self._buffered_row_count(),
            flush_count=self._flush_count(),
            append_error_count=self._append_error_count(),
            submitted_row_count=self._submitted_row_count(),
            appended_row_count=self._appended_row_count(),
            last_append_offset=self._last_append_offset(),
            last_append_at=self._last_append_at(),
            last_append_succeeded=self._last_append_succeeded(),
            last_error=self._last_error(),
        )

    def health_snapshot(self) -> BigQueryStorageWriteSinkHealthSnapshot:
        connection_ready = self._connection_ready()
        return BigQueryStorageWriteSinkHealthSnapshot(
            ready=connection_ready
            and self._buffered_row_count() == 0
            and self._last_error() is None,
            component=self._sink_name,
            connection_ready=connection_ready,
            last_error=self._last_error(),
            table=self._table,
            stream_name=self._stream_name(),
            buffered_row_count=self._buffered_row_count(),
            flush_count=self._flush_count(),
            last_append_succeeded=self._last_append_succeeded(),
        )

    def acceptance_report(
        self,
        thresholds: BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> Any:
        return self._acceptance_gate_factory().evaluate_storage_write_sink(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )


__all__ = ["BigQueryStorageWriteSinkOperatorSurface"]
