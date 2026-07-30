"""BigQuery sink for dataset-oriented ETL pipelines."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

import logstruct
from agora.core.delivery import IdempotencyMode, SinkDeliveryCapability
from agora.core.sink import BaseSink

from agora_plugins.bigquery.config import (
    BigQueryConnectionConfig,
    build_bigquery_client,
    coerce_connection_config,
)
from agora_plugins.bigquery.observability import (
    BigQueryEnterpriseAcceptanceGate,
    BigQuerySinkEnterpriseAcceptanceThresholds,
    BigQuerySinkHealthSnapshot,
    BigQuerySinkMetricsSnapshot,
)
from agora_plugins.bigquery.sinks.bigquery_flush import BigQuerySinkFlushRuntime
from agora_plugins.bigquery.sinks.bigquery_load_job import BigQuerySinkLoadJobRuntime

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")
logger = logstruct.getLogger(__name__)
__all__ = [
    "BigQuerySink",
    "BigQuerySinkFlushRuntime",
    "BigQuerySinkLoadJobRuntime",
    "BigQuerySinkMetricsSnapshot",
    "BigQuerySinkWriteError",
]


def _identity(record: Any) -> Any:
    return record


class BigQuerySinkWriteError(RuntimeError):
    """Structured sink error carrying BigQuery job metadata."""

    def __init__(
        self, message: str, *, job_id: str | None, errors: list[dict[str, Any]] | None
    ) -> None:
        super().__init__(message)
        self.job_id = job_id
        self.errors = list(errors or [])


class BigQuerySink(BaseSink[T], Generic[T]):
    """Write mapped rows into a BigQuery table via load jobs."""

    sink_name = "bigquery"

    def delivery_capability(self) -> SinkDeliveryCapability:
        """Load jobs are append/truncate operations, not replay-safe upserts."""
        return SinkDeliveryCapability(
            sink_name=self.sink_name,
            idempotency=IdempotencyMode.NONE,
            replay_safe=False,
            notes=("Batch load jobs do not provide merge or upsert semantics.",),
        )

    def __init__(
        self,
        *,
        table: str,
        row_mapper: Callable[[T], dict[str, Any]] | None = None,
        batch_size: int = 500,
        write_disposition: Literal["append", "truncate"] = "append",
        create_disposition: Literal["create_if_needed", "create_never"] = "create_if_needed",
        project: str | None = None,
        location: str | None = None,
        credentials_path: str | None = None,
        credentials: Any | None = None,
        connection: BigQueryConnectionConfig | None = None,
        client: Any | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1.")
        if write_disposition not in {"append", "truncate"}:
            raise ValueError("write_disposition must be 'append' or 'truncate'.")
        if create_disposition not in {"create_if_needed", "create_never"}:
            raise ValueError("create_disposition must be 'create_if_needed' or 'create_never'.")
        self._table = table
        self._row_mapper = row_mapper or _identity
        self._batch_size = batch_size
        self._write_disposition = write_disposition
        self._create_disposition = create_disposition
        self._connection = coerce_connection_config(
            project=project,
            location=location,
            credentials_path=credentials_path,
            credentials=credentials,
            connection=connection,
        )
        self._client = client
        self._load_job_runtime = BigQuerySinkLoadJobRuntime(
            table=self._table,
            create_disposition=self._create_disposition,
        )
        self._flush_runtime = BigQuerySinkFlushRuntime(
            table=self._table,
            batch_size=self._batch_size,
            initial_truncate_pending=write_disposition == "truncate",
            ensure_open=self.open,
            client_provider=lambda: self._client,
            coerce_row=self._coerce_row,
            submit_load_job=self._submit_load_job,
            now_utc=lambda: datetime.now(UTC),
        )

    @property
    def _buffer(self) -> list[T]:
        return self._flush_runtime.buffer

    @_buffer.setter
    def _buffer(self, value: list[T]) -> None:
        self._flush_runtime.buffer = value

    @property
    def _truncate_pending(self) -> bool:
        return self._flush_runtime.truncate_pending

    @_truncate_pending.setter
    def _truncate_pending(self, value: bool) -> None:
        self._flush_runtime.truncate_pending = value

    @property
    def _flush_count(self) -> int:
        return self._flush_runtime.flush_count

    @_flush_count.setter
    def _flush_count(self, value: int) -> None:
        self._flush_runtime.flush_count = value

    @property
    def _flush_error_count(self) -> int:
        return self._flush_runtime.flush_error_count

    @_flush_error_count.setter
    def _flush_error_count(self, value: int) -> None:
        self._flush_runtime.flush_error_count = value

    @property
    def _submitted_row_count(self) -> int:
        return self._flush_runtime.submitted_row_count

    @_submitted_row_count.setter
    def _submitted_row_count(self, value: int) -> None:
        self._flush_runtime.submitted_row_count = value

    @property
    def _loaded_row_count(self) -> int:
        return self._flush_runtime.loaded_row_count

    @_loaded_row_count.setter
    def _loaded_row_count(self, value: int) -> None:
        self._flush_runtime.loaded_row_count = value

    @property
    def _last_job_id(self) -> str | None:
        return self._flush_runtime.last_job_id

    @_last_job_id.setter
    def _last_job_id(self, value: str | None) -> None:
        self._flush_runtime.last_job_id = value

    @property
    def _last_flush_at(self) -> datetime | None:
        return self._flush_runtime.last_flush_at

    @_last_flush_at.setter
    def _last_flush_at(self, value: datetime | None) -> None:
        self._flush_runtime.last_flush_at = value

    @property
    def _last_flush_succeeded(self) -> bool | None:
        return self._flush_runtime.last_flush_succeeded

    @_last_flush_succeeded.setter
    def _last_flush_succeeded(self, value: bool | None) -> None:
        self._flush_runtime.last_flush_succeeded = value

    @property
    def _last_error(self) -> str | None:
        return self._flush_runtime.last_error

    @_last_error.setter
    def _last_error(self, value: str | None) -> None:
        self._flush_runtime.last_error = value

    async def open(self) -> None:
        if self._client is None:
            self._client = build_bigquery_client(self._connection)

    async def close(self) -> None:
        await self.flush()
        close = getattr(self._client, "close", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result

    async def write(self, record: T) -> None:
        await self._flush_runtime.write(record)

    async def write_batch(self, records: list[T]) -> None:
        await self._flush_runtime.write_batch(records)

    async def flush(self) -> None:
        await self._flush_runtime.flush()

    def metrics_snapshot(self) -> BigQuerySinkMetricsSnapshot:
        return BigQuerySinkMetricsSnapshot(
            table=self._table,
            batch_size=self._batch_size,
            connection_ready=self._client is not None,
            buffered_row_count=len(self._buffer),
            flush_count=self._flush_count,
            flush_error_count=self._flush_error_count,
            submitted_row_count=self._submitted_row_count,
            loaded_row_count=self._loaded_row_count,
            write_disposition=self._write_disposition,
            create_disposition=self._create_disposition,
            last_job_id=self._last_job_id,
            last_flush_at=self._last_flush_at,
            last_flush_succeeded=self._last_flush_succeeded,
            last_error=self._last_error,
        )

    def health_snapshot(self) -> BigQuerySinkHealthSnapshot:
        connection_ready = self._client is not None
        return BigQuerySinkHealthSnapshot(
            ready=connection_ready and not self._buffer and self._last_error is None,
            component=self.sink_name,
            connection_ready=connection_ready,
            last_error=self._last_error,
            table=self._table,
            buffered_row_count=len(self._buffer),
            flush_count=self._flush_count,
            last_flush_succeeded=self._last_flush_succeeded,
        )

    def acceptance_report(
        self,
        thresholds: BigQuerySinkEnterpriseAcceptanceThresholds | None = None,
    ) -> Any:
        return BigQueryEnterpriseAcceptanceGate().evaluate_sink(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    def _coerce_row(self, record: T) -> dict[str, Any]:
        mapped = self._row_mapper(record)
        if not isinstance(mapped, dict):
            raise TypeError(
                "BigQuerySink row_mapper must return dict[str, Any] for load jobs. "
                f"Got {type(mapped).__name__}."
            )
        return mapped

    def _submit_load_job(
        self,
        rows: list[dict[str, Any]],
        effective_write_disposition: Literal["append", "truncate"],
    ) -> tuple[str | None, int]:
        assert self._client is not None
        return self._load_job_runtime.submit_load_job(
            client=self._client,
            rows=rows,
            effective_write_disposition=effective_write_disposition,
        )
