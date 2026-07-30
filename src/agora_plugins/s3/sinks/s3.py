"""S3 dataset sink that reuses core file codecs before object upload."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar
from uuid import uuid4

from agora.core.data_plane import DataPlane
from agora.core.delivery import IdempotencyMode, SinkDeliveryCapability
from agora.core.sink import BaseSink

from agora_plugins.s3.config import S3ConnectionConfig, build_s3_client, coerce_connection_config
from agora_plugins.s3.observability import S3SinkMetricsSnapshot
from agora_plugins.s3.sinks.upload_runtime import S3SinkUploadRuntime

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")
__all__ = ["S3Sink", "S3SinkMetricsSnapshot", "S3SinkUploadRuntime"]


class S3Sink(BaseSink[T], Generic[T]):
    """Write partitioned dataset files to S3-compatible object storage."""

    sink_name = "s3"
    accepted_data_planes = (DataPlane.PYTHON_ROWS, DataPlane.PYTHON_BATCHES)
    native_data_planes = accepted_data_planes

    def delivery_capability(self) -> SinkDeliveryCapability:
        """Conditional object creation prevents overwrite but not new-run duplicates."""
        return SinkDeliveryCapability(
            sink_name=self.sink_name,
            idempotency=IdempotencyMode.NONE,
            replay_safe=False,
            notes=(
                "Run-scoped conditional creates fail on a same-run collision; a new run id "
                "creates distinct objects during replay.",
            ),
        )

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        format: Literal["jsonl", "csv", "parquet"],
        row_mapper: Callable[[T], Any] | None = None,
        partition_path_fn: Callable[[T], str | None] | None = None,
        max_records_per_file: int = 10_000,
        flush_every: int = 500,
        csv_fieldnames: list[str] | None = None,
        csv_delimiter: str = ",",
        encoding: str = "utf-8",
        file_prefix: str = "part",
        run_id: str | None = None,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        addressing_style: Literal["auto", "path", "virtual"] = "auto",
        connection: S3ConnectionConfig | None = None,
        client: Any | None = None,
    ) -> None:
        if max_records_per_file < 1:
            raise ValueError("max_records_per_file must be >= 1.")
        if flush_every < 1:
            raise ValueError("flush_every must be >= 1.")
        resolved_run_id = run_id or str(uuid4())
        if not resolved_run_id or "/" in resolved_run_id:
            raise ValueError("S3Sink run_id must be non-empty and must not contain '/'.")
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._format = format
        self._row_mapper = row_mapper or (lambda record: record)
        self._partition_path_fn = partition_path_fn or (lambda record: None)
        self._max_records_per_file = max_records_per_file
        self._flush_every = flush_every
        self._csv_fieldnames = list(csv_fieldnames or [])
        self._csv_delimiter = csv_delimiter
        self._encoding = encoding
        self._file_prefix = file_prefix
        self._connection = coerce_connection_config(
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            addressing_style=addressing_style,
            connection=connection,
        )
        self._client = client
        self._upload_runtime = S3SinkUploadRuntime(
            bucket=self._bucket,
            prefix=self._prefix,
            run_id=resolved_run_id,
            format=self._format,
            flush_every=self._flush_every,
            file_prefix=self._file_prefix,
            row_mapper=self._row_mapper,
            csv_fieldnames=self._csv_fieldnames,
            csv_delimiter=self._csv_delimiter,
            encoding=self._encoding,
            client_provider=lambda: self._client,
            now_utc=lambda: datetime.now(UTC),
        )

    def bind_context(self, ctx: Any) -> None:
        """Use the pipeline run identity in every object key."""
        run_id = str(ctx.run_id)
        if not run_id or "/" in run_id:
            raise ValueError(
                "Pipeline run_id for S3Sink must be non-empty and must not contain '/'."
            )
        self._upload_runtime.set_run_id(run_id)

    async def open(self) -> None:
        if self._client is None:
            self._client = build_s3_client(self._connection)

    async def close(self) -> None:
        if self._upload_runtime.active_sink is not None:
            await self._upload_runtime.finalize_active_file()
        close = getattr(self._client, "close", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result

    async def write(self, record: T) -> None:
        if self._client is None:
            await self.open()
        partition = self._upload_runtime.normalize_partition(self._partition_path_fn(record))
        if (
            self._upload_runtime.active_sink is not None
            and self._upload_runtime.active_partition != partition
            and self._upload_runtime.active_record_count > 0
        ):
            await self._upload_runtime.finalize_active_file()
        if self._upload_runtime.active_sink is None:
            await self._upload_runtime.open_active_file(partition)
        assert self._upload_runtime.active_sink is not None
        await self._upload_runtime.active_sink.write(record)
        self._upload_runtime.active_record_count += 1
        if self._upload_runtime.active_record_count >= self._max_records_per_file:
            await self._upload_runtime.finalize_active_file()

    async def write_batch(self, records: list[T]) -> None:
        for record in records:
            await self.write(record)

    async def flush(self) -> None:
        await self._upload_runtime.flush_active_file()

    def metrics_snapshot(self) -> S3SinkMetricsSnapshot:
        return self._upload_runtime.metrics_snapshot(
            max_records_per_file=self._max_records_per_file,
        )
