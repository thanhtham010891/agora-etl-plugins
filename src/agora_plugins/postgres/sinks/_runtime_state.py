"""Internal shared state facade for PostgreSQL sink collaborators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncio
    from datetime import datetime

    from agora.core.dlq import DLQSink


class PostgresSinkRuntimeState:
    """Groups mutable sink state so collaborators don't need many tiny accessors."""

    def __init__(self, sink: Any) -> None:
        self._sink = sink

    def observe_schema_drift_detected(self, count: int = 1) -> None:
        self._sink._schema_drift_detected_count += count

    def observe_schema_drift_aligned(self, count: int) -> None:
        self._sink._schema_drift_aligned_count += count

    def current_buffer(self) -> list[dict[str, Any]]:
        return self._sink._buffer

    def clear_buffer_prefix(self, count: int) -> None:
        del self._sink._buffer[:count]

    def current_poison_sink(self) -> DLQSink | None:
        return self._sink._poison_record_sink

    def current_conn(self) -> Any | None:
        return self._sink._conn

    def set_conn(self, conn: Any | None) -> None:
        self._sink._conn = conn

    def current_write_pool(self) -> asyncio.LifoQueue[Any] | None:
        return self._sink._write_pool

    def set_write_pool(self, write_pool: asyncio.LifoQueue[Any] | None) -> None:
        self._sink._write_pool = write_pool

    def current_external_write_pool(self) -> Any | None:
        return self._sink._external_write_pool

    def set_external_write_pool(self, write_pool: Any | None) -> None:
        self._sink._external_write_pool = write_pool

    def current_write_pool_open_connections(self) -> int:
        return self._sink._write_pool_open_connections

    def set_write_pool_open_connections(self, count: int) -> None:
        self._sink._write_pool_open_connections = count

    def should_defer_upsert_constraint_preflight(self) -> bool:
        return self._sink._defer_upsert_constraint_preflight

    def current_upsert_constraint_preflight_complete(self) -> bool:
        return self._sink._upsert_constraint_preflight_complete

    def set_upsert_constraint_preflight_complete(self, complete: bool) -> None:
        self._sink._upsert_constraint_preflight_complete = complete

    def note_flush_success(self, count: int, flushed_at: datetime) -> None:
        self._sink._flush_count += 1
        self._sink._flushed_row_count += count
        self._sink._last_flush_at = flushed_at

    def observe_poison_record(self, error: Any) -> None:
        self._sink._poison_record_count += 1
        self._sink._poison_record_classification_counts[error.poison_info.classification] += 1
