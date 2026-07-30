"""S3 dataset source for object-boundary replay semantics."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

import logstruct
from agora.core.checkpoint import Checkpoint, SourceIdentity, SourceIdentityMismatchPolicy
from agora.core.source import BaseSource, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins._source_identity import (
    fingerprint_source_identity,
    redact_url_password,
    validate_resume_checkpoint_identity,
)
from agora_plugins.s3.config import S3ConnectionConfig, build_s3_client, coerce_connection_config
from agora_plugins.s3.observability import (
    S3SourceMetricsSnapshot,
    S3SourceRecoveryContractSnapshot,
    S3SourceRecoveryMode,
)
from agora_plugins.s3.sources.object_runtime import S3SourceObjectRuntime

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Sequence

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


def _identity(row: dict[str, Any]) -> Any:
    return row


class S3Source(BaseSource[T], Generic[T]):
    """Read a lexically ordered dataset from S3-compatible object storage."""

    source_name = "s3"
    supports_checkpoint = True

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        format: Literal["jsonl", "csv", "parquet"],
        row_mapper: Callable[[dict[str, Any]], T | None] | None = None,
        delimiter: str = ",",
        has_header: bool = True,
        fieldnames: Sequence[str] | None = None,
        encoding: str = "utf-8",
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        addressing_style: Literal["auto", "path", "virtual"] = "auto",
        connection: S3ConnectionConfig | None = None,
        client: Any | None = None,
        source_identity_mismatch_policy: SourceIdentityMismatchPolicy
        | str = SourceIdentityMismatchPolicy.FAIL_CLOSED,
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._format = format
        self._row_mapper = cast("Callable[[dict[str, Any]], T | None]", row_mapper or _identity)
        self._delimiter = delimiter
        self._has_header = has_header
        self._fieldnames = list(fieldnames or [])
        self._encoding = encoding
        self._on_record_error = on_record_error
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
        self._source_identity_mismatch_policy = SourceIdentityMismatchPolicy(
            source_identity_mismatch_policy
        )
        self._resume_after_key: str | None = None
        self._listed_object_count = 0
        self._completed_object_count = 0
        self._emitted_record_count = 0
        self._record_error_count = 0
        self._record_drop_count = 0
        self._last_listed_key: str | None = None
        self._last_completed_key: str | None = None
        self._last_error: str | None = None
        self._object_runtime = S3SourceObjectRuntime(
            bucket=self._bucket,
            prefix=self._prefix,
            format=self._format,
            row_mapper=self._row_mapper,
            delimiter=self._delimiter,
            has_header=self._has_header,
            fieldnames=self._fieldnames,
            encoding=self._encoding,
            on_record_error=self._on_record_error,
            client_provider=lambda: self._client,
        )

    async def open(self) -> None:
        if self._client is None:
            self._client = build_s3_client(self._connection)

    async def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        self._resume_after_key = None
        self._listed_object_count = 0
        self._completed_object_count = 0
        self._emitted_record_count = 0
        self._record_error_count = 0
        self._record_drop_count = 0
        self._last_listed_key = None
        self._last_completed_key = None
        self._last_error = None
        checkpoint = validate_resume_checkpoint_identity(
            checkpoint,
            current_identity=self.checkpoint_source_identity(),
            policy=self._source_identity_mismatch_policy,
            source_name=self.source_name,
        )
        if checkpoint is None:
            return
        value = checkpoint.value if isinstance(checkpoint.value, dict) else {}
        checkpoint_key = value.get("object_key")
        if checkpoint_key is not None:
            self._resume_after_key = str(checkpoint_key)

    def checkpoint_source_identity(self) -> SourceIdentity:
        """Return a secret-free identity for this ordered S3 dataset input."""
        return fingerprint_source_identity(
            "s3-dataset",
            {
                "bucket": self._bucket,
                "prefix": self._prefix,
                "format": self._format,
                "delimiter": self._delimiter,
                "has_header": self._has_header,
                "fieldnames": self._fieldnames,
                "encoding": self._encoding,
                "region_name": self._connection.region_name,
                "endpoint_url": (
                    None
                    if self._connection.endpoint_url is None
                    else redact_url_password(self._connection.endpoint_url)
                ),
                "addressing_style": self._connection.addressing_style,
            },
        )

    def current_checkpoint(self) -> dict[str, Any] | None:
        if self._last_completed_key is None:
            return None
        return {"object_key": self._last_completed_key}

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def recovery_contract(self) -> S3SourceRecoveryContractSnapshot:
        return S3SourceRecoveryContractSnapshot(
            mode=S3SourceRecoveryMode.CHECKPOINT_RERUN,
            supports_checkpoint=True,
            checkpoint_fields=("object_key",),
            checkpoint_params={"object_key": "object_key"},
            on_record_error=str(self._on_record_error),
        )

    def metrics_snapshot(self) -> S3SourceMetricsSnapshot:
        return S3SourceMetricsSnapshot(
            bucket=self._bucket,
            prefix=self._prefix,
            format=self._format,
            supports_checkpoint=True,
            listed_object_count=self._listed_object_count,
            completed_object_count=self._completed_object_count,
            emitted_record_count=self._emitted_record_count,
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
            last_listed_key=self._last_listed_key,
            last_completed_key=self._last_completed_key,
            last_error=self._last_error,
            recovery_contract=self.recovery_contract(),
        )

    async def stream(self) -> AsyncGenerator[T, None]:
        if self._client is None:
            await self.open()
        async for object_key in self._object_runtime.iter_object_keys():
            if self._resume_after_key is not None and object_key <= self._resume_after_key:
                continue
            self._listed_object_count += 1
            self._last_listed_key = object_key
            logger.info(
                "s3_source_object_started",
                bucket=self._bucket,
                key=object_key,
                format=self._format,
            )
            temp_path = await self._object_runtime.download_to_tempfile(object_key)
            local_source = self._object_runtime.build_local_source(temp_path)
            try:
                async for record in local_source.stream():
                    self._emitted_record_count += 1
                    yield record
                metrics = local_source.runtime_metrics()
                self._record_error_count += metrics.record_error_count
                self._record_drop_count += metrics.record_drop_count
                self._completed_object_count += 1
                self._last_completed_key = object_key
            except Exception as exc:
                metrics = local_source.runtime_metrics()
                self._record_error_count += metrics.record_error_count
                self._record_drop_count += metrics.record_drop_count
                self._last_error = str(exc)
                raise
            finally:
                with contextlib.suppress(Exception):
                    await local_source.close()
                with contextlib.suppress(FileNotFoundError):
                    temp_path.unlink()
            logger.info(
                "s3_source_object_completed",
                bucket=self._bucket,
                key=object_key,
                emitted_record_count=self._emitted_record_count,
            )
