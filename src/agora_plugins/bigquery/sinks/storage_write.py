"""BigQuery Storage Write API sink for bounded append-only dataset ETL."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Generic, TypeVar

from agora.core.data_plane import DataPlane
from agora.core.sink import BaseSink

from agora_plugins.bigquery.config import (
    BigQueryConnectionConfig,
    coerce_connection_config,
)
from agora_plugins.bigquery.observability import (
    BigQueryEnterpriseAcceptanceGate,
    BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds,
    BigQueryStorageWriteSinkHealthSnapshot,
    BigQueryStorageWriteSinkMetricsSnapshot,
)
from agora_plugins.bigquery.sinks.storage_write_compat import BigQueryStorageWriteCompatMixin
from agora_plugins.bigquery.sinks.storage_write_flush import BigQueryStorageWriteFlushRuntime
from agora_plugins.bigquery.sinks.storage_write_operator import (
    BigQueryStorageWriteSinkOperatorSurface,
)
from agora_plugins.bigquery.sinks.storage_write_serializer import BigQueryStorageWriteRowSerializer
from agora_plugins.bigquery.sinks.storage_write_session import BigQueryStorageWriteSession

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from google.protobuf import descriptor_pb2

T = TypeVar("T")

__all__ = [
    "BigQueryStorageWriteFlushRuntime",
    "BigQueryStorageWriteRowSerializer",
    "BigQueryStorageWriteSession",
    "BigQueryStorageWriteSink",
    "BigQueryStorageWriteSinkError",
    "BigQueryStorageWriteSinkMetricsSnapshot",
    "BigQueryStorageWriteSinkOperatorSurface",
]

_MAX_REQUEST_LIMIT = 10_000_000


def _identity(record: object) -> object:
    return record


def _resolve_table_path(table: str, *, default_project: str | None) -> tuple[str, str, str]:
    normalized = table.replace(":", ".", 1)
    parts = normalized.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2 and default_project:
        return default_project, parts[0], parts[1]
    raise ValueError(
        "BigQueryStorageWriteSink table must be 'project.dataset.table' or "
        "'dataset.table' with connection.project/client.project available."
    )


def _extract_append_offset(response: object) -> int | None:
    append_result = getattr(response, "append_result", None)
    if append_result is None:
        return None
    offset = getattr(append_result, "offset", None)
    if offset is None:
        return None
    return getattr(offset, "value", offset)


def _status_message(status: object) -> str | None:
    if status is None:
        return None
    return getattr(status, "message", None) or str(status)


class BigQueryStorageWriteSinkError(RuntimeError):
    """Structured Storage Write API sink error carrying stream metadata."""

    def __init__(
        self,
        message: str,
        *,
        stream_name: str | None,
        status_code: int | None = None,
        row_errors: list[object] | None = None,
    ) -> None:
        super().__init__(message)
        self.stream_name = stream_name
        self.status_code = status_code
        self.row_errors = list(row_errors or [])


_DynamicProtoRowSerializer = BigQueryStorageWriteRowSerializer


class _ManagedStorageWriteStream:
    """Thin wrapper around google-cloud-bigquery-storage managed writer."""

    def __init__(
        self,
        *,
        write_client: object,
        stream_name: str,
        descriptor_proto: descriptor_pb2.DescriptorProto,
    ) -> None:
        try:
            from google.cloud.bigquery_storage_v1 import exceptions as storage_exceptions
            from google.cloud.bigquery_storage_v1 import types
            from google.cloud.bigquery_storage_v1.writer import AppendRowsStream
        except ImportError:
            raise ImportError(
                "BigQuery Storage Write plugins require google-cloud-bigquery-storage. "
                "Install via: pip install 'agora-etl-plugins[bigquery]'"
            ) from None
        self._stream_name = stream_name
        self._types = types
        self._stream_closed_error_type = storage_exceptions.StreamClosedError
        initial_request = types.AppendRowsRequest(
            write_stream=stream_name,
            proto_rows=types.AppendRowsRequest.ProtoData(
                writer_schema=types.ProtoSchema(proto_descriptor=descriptor_proto)
            ),
        )
        self._stream = AppendRowsStream(write_client, initial_request)

    @property
    def stream_name(self) -> str:
        return self._stream_name

    def append_serialized_rows(
        self, serialized_rows: list[bytes], *, timeout: float | None
    ) -> int | None:
        request = self._types.AppendRowsRequest(
            write_stream=self._stream_name,
            proto_rows=self._types.AppendRowsRequest.ProtoData(
                rows=self._types.ProtoRows(serialized_rows=serialized_rows)
            ),
        )
        response = self._stream.send(request).result(timeout=timeout)
        status = getattr(response, "error", None)
        if status is not None and getattr(status, "code", 0):
            raise BigQueryStorageWriteSinkError(
                _status_message(status) or "BigQuery Storage Write append failed.",
                stream_name=self._stream_name,
                status_code=getattr(status, "code", None),
            )
        row_errors = list(getattr(response, "row_errors", ()) or ())
        if row_errors:
            raise BigQueryStorageWriteSinkError(
                "BigQuery Storage Write append reported row errors.",
                stream_name=self._stream_name,
                row_errors=row_errors,
            )
        return _extract_append_offset(response)

    def close(self) -> None:
        close = getattr(self._stream, "close", None)
        if callable(close):
            try:
                close()
            except self._stream_closed_error_type:
                return


class BigQueryStorageWriteSink(BigQueryStorageWriteCompatMixin[T], BaseSink[T], Generic[T]):
    """Append rows to the default stream; duplicate rows remain possible after ambiguous failure."""

    sink_name = "bigquery_storage_write"
    accepted_data_planes = (DataPlane.PYTHON_ROWS, DataPlane.PYTHON_BATCHES)
    native_data_planes = accepted_data_planes

    def __init__(
        self,
        *,
        table: str,
        row_mapper: Callable[[T], dict[str, object]] | None = None,
        batch_size: int = 500,
        max_request_bytes: int = 8_000_000,
        append_timeout_s: float | None = 30.0,
        table_schema: Sequence[object] | None = None,
        project: str | None = None,
        location: str | None = None,
        credentials_path: str | None = None,
        credentials: object | None = None,
        connection: BigQueryConnectionConfig | None = None,
        client: object | None = None,
        write_client: object | None = None,
        stream_factory: Callable[[object, str, descriptor_pb2.DescriptorProto], object]
        | None = None,
        serializer: BigQueryStorageWriteRowSerializer | None = None,
        validate_table_access: bool | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1.")
        if max_request_bytes < 1 or max_request_bytes >= _MAX_REQUEST_LIMIT:
            raise ValueError("max_request_bytes must be between 1 and 9_999_999.")
        self._table = table
        self._row_mapper = row_mapper or _identity
        self._batch_size = batch_size
        self._max_request_bytes = max_request_bytes
        self._append_timeout_s = append_timeout_s
        self._table_schema = tuple(table_schema or ())
        self._connection = coerce_connection_config(
            project=project,
            location=location,
            credentials_path=credentials_path,
            credentials=credentials,
            connection=connection,
        )
        self._stream_factory = stream_factory or self._default_stream_factory
        self._validate_table_access = (
            validate_table_access if validate_table_access is not None else stream_factory is None
        )
        self._session = BigQueryStorageWriteSession(
            table=self._table,
            table_schema=self._table_schema,
            connection=self._connection,
            validate_table_access=self._validate_table_access,
            client=client,
            write_client=write_client,
            stream=None,
            stream_name=None,
            serializer=serializer,
            stream_factory=self._stream_factory,
            serializer_factory=BigQueryStorageWriteRowSerializer,
            resolve_table_path=self._resolve_table_path,
        )
        self._flush_runtime = BigQueryStorageWriteFlushRuntime(
            table=self._table,
            batch_size=self._batch_size,
            max_request_bytes=self._max_request_bytes,
            append_timeout_s=self._append_timeout_s,
            session=self._session,
            ensure_open=self.open,
            coerce_row=self._coerce_row,
            oversized_row_error=self._oversized_row_error,
            now_utc=lambda: datetime.now(UTC),
        )
        self._operator_surface = BigQueryStorageWriteSinkOperatorSurface(
            sink_name=self.sink_name,
            table=self._table,
            connection_ready=self._session.connection_ready,
            stream_name=lambda: self._stream_name,
            batch_size=lambda: self._batch_size,
            max_request_bytes=lambda: self._max_request_bytes,
            buffered_row_count=lambda: len(self._buffer),
            flush_count=lambda: self._flush_count,
            append_error_count=lambda: self._append_error_count,
            submitted_row_count=lambda: self._submitted_row_count,
            appended_row_count=lambda: self._appended_row_count,
            last_append_offset=lambda: self._last_append_offset,
            last_append_at=lambda: self._last_append_at,
            last_append_succeeded=lambda: self._last_append_succeeded,
            last_error=lambda: self._last_error,
            acceptance_gate_factory=BigQueryEnterpriseAcceptanceGate,
        )

    async def open(self) -> None:
        await self._session.open()

    async def close(self) -> None:
        await self._session.close(flush=self.flush)

    async def write(self, record: T) -> None:
        await self._flush_runtime.write(record)

    async def write_batch(self, records: list[T]) -> None:
        await self._flush_runtime.write_batch(records)

    async def flush(self) -> None:
        await self._flush_runtime.flush()

    def metrics_snapshot(self) -> BigQueryStorageWriteSinkMetricsSnapshot:
        return self._operator_surface.metrics_snapshot()

    def health_snapshot(self) -> BigQueryStorageWriteSinkHealthSnapshot:
        return self._operator_surface.health_snapshot()

    def acceptance_report(
        self,
        thresholds: BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> object:
        return self._operator_surface.acceptance_report(thresholds=thresholds)

    def _coerce_row(self, record: T) -> dict[str, object]:
        mapped = self._row_mapper(record)
        if not isinstance(mapped, dict):
            raise TypeError(
                "BigQueryStorageWriteSink row_mapper must return dict[str, object] for append rows. "
                f"Got {type(mapped).__name__}."
            )
        return mapped

    def _chunk_serialized_rows(self, serialized_rows: list[bytes]) -> list[list[bytes]]:
        return self._flush_runtime.chunk_serialized_rows(serialized_rows)

    def _oversized_row_error(self) -> BigQueryStorageWriteSinkError:
        return BigQueryStorageWriteSinkError(
            "BigQuery Storage Write flush contains a single row that exceeds the configured "
            "request-size guard. Reduce payload size for this phase-2 sink.",
            stream_name=self._stream_name,
        )

    def _resolve_table_path(self, table: str) -> tuple[str, str, str]:
        return _resolve_table_path(
            table,
            default_project=self._connection.project or getattr(self._client, "project", None),
        )

    @staticmethod
    def _default_stream_factory(
        write_client: object,
        stream_name: str,
        descriptor_proto: descriptor_pb2.DescriptorProto,
    ) -> _ManagedStorageWriteStream:
        return _ManagedStorageWriteStream(
            write_client=write_client,
            stream_name=stream_name,
            descriptor_proto=descriptor_proto,
        )
