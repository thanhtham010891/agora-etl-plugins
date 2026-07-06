"""Upload runtime for S3 dataset sinks."""

from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sinks.file.parquet import ParquetSink

from agora_plugins.s3.observability import S3SinkMetricsSnapshot

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from typing import Literal

    from agora.core.sink import BaseSink

T = TypeVar("T")


class S3SinkUploadRuntime(Generic[T]):
    """Public-facing file lifecycle and upload runtime for S3 sinks."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        run_id: str,
        format: Literal["jsonl", "csv", "parquet"],
        flush_every: int,
        file_prefix: str,
        row_mapper: Callable[[T], Any],
        csv_fieldnames: list[str],
        csv_delimiter: str,
        encoding: str,
        client_provider: Callable[[], Any | None],
        now_utc: Callable[[], datetime],
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._run_id = run_id
        self._format = format
        self._flush_every = flush_every
        self._file_prefix = file_prefix
        self._row_mapper = row_mapper
        self._csv_fieldnames = csv_fieldnames
        self._csv_delimiter = csv_delimiter
        self._encoding = encoding
        self._client_provider = client_provider
        self._now_utc = now_utc
        self.file_index = 0
        self.uploaded_object_count = 0
        self.uploaded_record_count = 0
        self.last_uploaded_key: str | None = None
        self.last_upload_at: datetime | None = None
        self.last_error: str | None = None
        self.active_partition: str | None = None
        self.active_record_count = 0
        self.active_sink: BaseSink[Any] | None = None
        self.active_path: Path | None = None
        self.active_key: str | None = None

    def connection_ready(self) -> bool:
        return self._client_provider() is not None

    @property
    def run_id(self) -> str:
        return self._run_id

    def set_run_id(self, run_id: str) -> None:
        if self.active_sink is not None:
            raise RuntimeError("S3Sink run_id cannot change while a dataset file is open.")
        self._run_id = run_id

    def metrics_snapshot(self, *, max_records_per_file: int) -> S3SinkMetricsSnapshot:
        return S3SinkMetricsSnapshot(
            bucket=self._bucket,
            prefix=self._prefix,
            run_id=self._run_id,
            format=self._format,
            flush_every=self._flush_every,
            max_records_per_file=max_records_per_file,
            connection_ready=self.connection_ready(),
            uploaded_object_count=self.uploaded_object_count,
            uploaded_record_count=self.uploaded_record_count,
            last_uploaded_key=self.last_uploaded_key,
            last_upload_at=self.last_upload_at,
            last_error=self.last_error,
        )

    async def open_active_file(self, partition: str | None) -> None:
        object_key = self.build_object_key(partition)
        fd, raw_path = tempfile.mkstemp(suffix=f".{self._format}")
        os.close(fd)
        temp_path = Path(raw_path)
        sink = self.build_local_sink(temp_path)
        await sink.open()
        self.active_partition = partition
        self.active_sink = sink
        self.active_path = temp_path
        self.active_key = object_key
        self.active_record_count = 0

    async def finalize_active_file(self) -> None:
        assert self.active_sink is not None
        assert self.active_path is not None
        assert self.active_key is not None
        await self.active_sink.close()
        body = await asyncio.to_thread(self.active_path.read_bytes)
        client = self._client_provider()
        assert client is not None
        try:
            await asyncio.to_thread(
                client.put_object,
                Bucket=self._bucket,
                Key=self.active_key,
                Body=body,
                IfNoneMatch="*",
            )
        except Exception as exc:
            self.last_error = str(exc)
            raise
        self.uploaded_object_count += 1
        self.uploaded_record_count += self.active_record_count
        self.last_uploaded_key = self.active_key
        self.last_upload_at = self._now_utc()
        self.last_error = None
        with contextlib.suppress(FileNotFoundError):
            self.active_path.unlink()
        self.active_partition = None
        self.active_sink = None
        self.active_path = None
        self.active_key = None
        self.active_record_count = 0
        self.file_index += 1

    async def flush_active_file(self) -> None:
        if self.active_sink is not None:
            await self.active_sink.flush()

    def build_local_sink(self, temp_path: Path) -> BaseSink[Any]:
        if self._format == "jsonl":
            return JsonLinesSink(
                temp_path,
                serializer=lambda record: self._row_mapper(record),
                flush_every=self._flush_every,
                encoding=self._encoding,
            )
        if self._format == "csv":
            return CsvSink(
                temp_path,
                row_mapper=self.coerce_mapping_row,
                fieldnames=self._csv_fieldnames or None,
                flush_every=self._flush_every,
                delimiter=self._csv_delimiter,
                encoding=self._encoding,
            )
        return ParquetSink(
            temp_path,
            row_mapper=self.coerce_mapping_row,
            batch_size=self._flush_every,
        )

    def coerce_mapping_row(self, record: T) -> dict[str, Any]:
        mapped = self._row_mapper(record)
        if not isinstance(mapped, dict):
            raise TypeError(
                "S3Sink row_mapper must return dict[str, Any] for csv/parquet dataset files. "
                f"Got {type(mapped).__name__}."
            )
        return mapped

    @staticmethod
    def normalize_partition(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip("/")
        return cleaned or None

    def build_object_key(self, partition: str | None) -> str:
        pieces = [piece for piece in [self._prefix, f"run_id={self._run_id}", partition] if piece]
        filename = f"{self._file_prefix}-{self.file_index:05d}.{self._format}"
        if pieces:
            return "/".join([*pieces, filename])
        return filename


__all__ = ["S3SinkUploadRuntime"]
