"""Write and write-batch enqueue orchestration for PostgreSQL sinks."""

from __future__ import annotations

from typing import Any

from agora_plugins.postgres.sinks._sink_types import (
    PostgresSinkWriteError,
    PostgresWriteSafetyPolicy,
)


class PostgresSinkWriteBuffer:
    """Keeps PostgresSink write/write_batch focused on the public facade."""

    def __init__(self, sink: Any) -> None:
        self._sink = sink

    async def write(self, record: Any) -> None:
        sink = self._sink
        row = sink._row_mapper(record)
        if sink._write_safety_policy == PostgresWriteSafetyPolicy.ALIGN_TO_TARGET:
            target_columns = await sink._load_target_columns()
            row = sink._normalize_row_to_target(row, target_columns, row_index=0)
        sink._write_call_count += 1
        await self._enqueue_row(row)

    async def write_batch(self, records: list[Any]) -> None:
        if not records:
            return
        sink = self._sink
        sink._write_batch_call_count += 1
        mapped_rows = [sink._row_mapper(record) for record in records]
        if sink._write_safety_policy == PostgresWriteSafetyPolicy.ALIGN_TO_TARGET:
            target_columns = await sink._load_target_columns()
            mapped_rows = [
                sink._normalize_row_to_target(row, target_columns, row_index=index)
                for index, row in enumerate(mapped_rows)
            ]
        for row in mapped_rows:
            await self._enqueue_row(row)

    async def _enqueue_row(self, row: dict[str, Any]) -> None:
        sink = self._sink
        sink._enqueue_count += 1
        sink._buffer.append(row)
        if len(sink._buffer) >= sink._batch_size:
            try:
                await sink.flush()
            except PostgresSinkWriteError as exc:
                await sink._route_failed_buffer_to_dlq(exc)
                raise
