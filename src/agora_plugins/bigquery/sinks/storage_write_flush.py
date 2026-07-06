"""Flush/runtime collaborator for BigQuery Storage Write sinks."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import datetime

    from agora_plugins.bigquery.sinks.storage_write_session import BigQueryStorageWriteSession

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class BigQueryStorageWriteFlushRuntime(Generic[T]):
    """Public-facing buffer and flush orchestration collaborator."""

    def __init__(
        self,
        *,
        table: str,
        batch_size: int,
        max_request_bytes: int,
        append_timeout_s: float | None,
        session: BigQueryStorageWriteSession,
        ensure_open: Callable[[], Awaitable[None]],
        coerce_row: Callable[[T], dict[str, object]],
        oversized_row_error: Callable[[], Exception],
        now_utc: Callable[[], datetime],
    ) -> None:
        self._table = table
        self._batch_size = batch_size
        self._max_request_bytes = max_request_bytes
        self._append_timeout_s = append_timeout_s
        self._session = session
        self._ensure_open = ensure_open
        self._coerce_row = coerce_row
        self._oversized_row_error = oversized_row_error
        self._now_utc = now_utc
        self._buffer: list[T] = []
        self._flush_count = 0
        self._append_error_count = 0
        self._submitted_row_count = 0
        self._appended_row_count = 0
        self._last_append_offset: int | None = None
        self._last_append_at: datetime | None = None
        self._last_append_succeeded: bool | None = None
        self._last_error: str | None = None

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def max_request_bytes(self) -> int:
        return self._max_request_bytes

    @max_request_bytes.setter
    def max_request_bytes(self, value: int) -> None:
        self._max_request_bytes = value

    @property
    def buffer(self) -> list[T]:
        return self._buffer

    @buffer.setter
    def buffer(self, value: list[T]) -> None:
        self._buffer = value

    @property
    def flush_count(self) -> int:
        return self._flush_count

    @flush_count.setter
    def flush_count(self, value: int) -> None:
        self._flush_count = value

    @property
    def append_error_count(self) -> int:
        return self._append_error_count

    @append_error_count.setter
    def append_error_count(self, value: int) -> None:
        self._append_error_count = value

    @property
    def submitted_row_count(self) -> int:
        return self._submitted_row_count

    @submitted_row_count.setter
    def submitted_row_count(self, value: int) -> None:
        self._submitted_row_count = value

    @property
    def appended_row_count(self) -> int:
        return self._appended_row_count

    @appended_row_count.setter
    def appended_row_count(self, value: int) -> None:
        self._appended_row_count = value

    @property
    def last_append_offset(self) -> int | None:
        return self._last_append_offset

    @last_append_offset.setter
    def last_append_offset(self, value: int | None) -> None:
        self._last_append_offset = value

    @property
    def last_append_at(self) -> datetime | None:
        return self._last_append_at

    @last_append_at.setter
    def last_append_at(self, value: datetime | None) -> None:
        self._last_append_at = value

    @property
    def last_append_succeeded(self) -> bool | None:
        return self._last_append_succeeded

    @last_append_succeeded.setter
    def last_append_succeeded(self, value: bool | None) -> None:
        self._last_append_succeeded = value

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @last_error.setter
    def last_error(self, value: str | None) -> None:
        self._last_error = value

    async def write(self, record: T) -> None:
        self._buffer.append(record)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def write_batch(self, records: list[T]) -> None:
        self._buffer.extend(records)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        if (
            self._session.stream is None
            or self._session.serializer is None
            or self._session.write_client is None
        ):
            try:
                await self._ensure_open()
            except Exception as exc:
                self._append_error_count += 1
                self._last_append_succeeded = False
                self._last_error = str(exc)
                raise
        assert self._session.stream is not None
        assert self._session.serializer is not None
        batch = list(self._buffer)
        try:
            rows = [self._coerce_row(record) for record in batch]
            serialized_rows = [self._session.serializer.serialize_row(row) for row in rows]
            for chunk_index, serialized_chunk in enumerate(
                self.chunk_serialized_rows(serialized_rows),
                start=1,
            ):
                append_offset = await asyncio.to_thread(
                    self._session.stream.append_serialized_rows,
                    serialized_chunk,
                    timeout=self._append_timeout_s,
                )
                del self._buffer[: len(serialized_chunk)]
                self._flush_count += 1
                self._submitted_row_count += len(serialized_chunk)
                self._appended_row_count += len(serialized_chunk)
                self._last_append_offset = append_offset
                self._last_append_at = self._now_utc()
                self._last_append_succeeded = True
                self._last_error = None
                logger.info(
                    "bigquery_storage_write_sink_flush",
                    table=self._table,
                    stream_name=self._session.stream_name,
                    chunk_index=chunk_index,
                    row_count=len(serialized_chunk),
                    append_offset=append_offset,
                )
        except Exception as exc:
            self._append_error_count += 1
            self._last_append_succeeded = False
            self._last_error = str(exc)
            raise

    def chunk_serialized_rows(self, serialized_rows: list[bytes]) -> list[list[bytes]]:
        batches: list[list[bytes]] = []
        current_batch: list[bytes] = []
        current_size = 0
        for payload in serialized_rows:
            payload_size = len(payload)
            if payload_size > self._max_request_bytes:
                raise self._oversized_row_error()
            if current_batch and current_size + payload_size > self._max_request_bytes:
                batches.append(current_batch)
                current_batch = []
                current_size = 0
            current_batch.append(payload)
            current_size += payload_size
        if current_batch:
            batches.append(current_batch)
        return batches


__all__ = ["BigQueryStorageWriteFlushRuntime"]
