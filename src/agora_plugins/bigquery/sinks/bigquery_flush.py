"""Flush/runtime collaborator for BigQuery dataset sinks."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import datetime

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class BigQuerySinkFlushRuntime(Generic[T]):
    """Public-facing buffer and flush orchestration runtime for BigQuery sinks."""

    def __init__(
        self,
        *,
        table: str,
        batch_size: int,
        initial_truncate_pending: bool,
        ensure_open: Callable[[], Awaitable[None]],
        client_provider: Callable[[], Any | None],
        coerce_row: Callable[[T], dict[str, Any]],
        submit_load_job: Callable[[list[dict[str, Any]], str], tuple[str | None, int]],
        now_utc: Callable[[], datetime],
    ) -> None:
        self._table = table
        self._batch_size = batch_size
        self._ensure_open = ensure_open
        self._client_provider = client_provider
        self._coerce_row = coerce_row
        self._submit_load_job = submit_load_job
        self._now_utc = now_utc
        self._buffer: list[T] = []
        self._truncate_pending = initial_truncate_pending
        self._flush_count = 0
        self._flush_error_count = 0
        self._submitted_row_count = 0
        self._loaded_row_count = 0
        self._last_job_id: str | None = None
        self._last_flush_at: datetime | None = None
        self._last_flush_succeeded: bool | None = None
        self._last_error: str | None = None

    @property
    def buffer(self) -> list[T]:
        return self._buffer

    @buffer.setter
    def buffer(self, value: list[T]) -> None:
        self._buffer = value

    @property
    def truncate_pending(self) -> bool:
        return self._truncate_pending

    @truncate_pending.setter
    def truncate_pending(self, value: bool) -> None:
        self._truncate_pending = value

    @property
    def flush_count(self) -> int:
        return self._flush_count

    @flush_count.setter
    def flush_count(self, value: int) -> None:
        self._flush_count = value

    @property
    def flush_error_count(self) -> int:
        return self._flush_error_count

    @flush_error_count.setter
    def flush_error_count(self, value: int) -> None:
        self._flush_error_count = value

    @property
    def submitted_row_count(self) -> int:
        return self._submitted_row_count

    @submitted_row_count.setter
    def submitted_row_count(self, value: int) -> None:
        self._submitted_row_count = value

    @property
    def loaded_row_count(self) -> int:
        return self._loaded_row_count

    @loaded_row_count.setter
    def loaded_row_count(self, value: int) -> None:
        self._loaded_row_count = value

    @property
    def last_job_id(self) -> str | None:
        return self._last_job_id

    @last_job_id.setter
    def last_job_id(self, value: str | None) -> None:
        self._last_job_id = value

    @property
    def last_flush_at(self) -> datetime | None:
        return self._last_flush_at

    @last_flush_at.setter
    def last_flush_at(self, value: datetime | None) -> None:
        self._last_flush_at = value

    @property
    def last_flush_succeeded(self) -> bool | None:
        return self._last_flush_succeeded

    @last_flush_succeeded.setter
    def last_flush_succeeded(self, value: bool | None) -> None:
        self._last_flush_succeeded = value

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
        if self._client_provider() is None:
            await self._ensure_open()
        batch = list(self._buffer)
        try:
            rows = [self._coerce_row(record) for record in batch]
            effective_write_disposition = "truncate" if self._truncate_pending else "append"
            job_id, loaded_rows = await asyncio.to_thread(
                self._submit_load_job,
                rows,
                effective_write_disposition,
            )
        except Exception as exc:
            self._flush_error_count += 1
            self._last_flush_succeeded = False
            self._last_error = str(exc)
            raise
        self._buffer = self._buffer[len(batch) :]
        self._truncate_pending = False
        self._flush_count += 1
        self._submitted_row_count += len(rows)
        self._loaded_row_count += loaded_rows
        self._last_job_id = job_id
        self._last_flush_at = self._now_utc()
        self._last_flush_succeeded = True
        self._last_error = None
        logger.info(
            "bigquery_sink_flush",
            table=self._table,
            job_id=job_id,
            row_count=len(rows),
            write_disposition=effective_write_disposition,
        )


__all__ = ["BigQuerySinkFlushRuntime"]
