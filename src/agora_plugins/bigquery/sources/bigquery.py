"""BigQuery source for dataset-oriented ETL pipelines."""

from __future__ import annotations

from datetime import UTC, datetime
from inspect import Parameter, isawaitable, signature
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

import logstruct
from agora.core.checkpoint import Checkpoint, SourceIdentity, SourceIdentityMismatchPolicy
from agora.core.source import BaseSource, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins._source_identity import (
    fingerprint_source_identity,
    validate_resume_checkpoint_identity,
)
from agora_plugins.bigquery.config import (
    BigQueryConnectionConfig,
    build_bigquery_client,
    coerce_connection_config,
)
from agora_plugins.bigquery.observability import (
    BigQueryEnterpriseAcceptanceGate,
    BigQuerySourceEnterpriseAcceptanceThresholds,
    BigQuerySourceHealthSnapshot,
    BigQuerySourceMetricsSnapshot,
    BigQuerySourceRecoveryContractSnapshot,
    BigQuerySourceRecoveryMode,
)
from agora_plugins.bigquery.sources.query_runtime import BigQuerySourceQueryRuntime
from agora_plugins.bigquery.sources.stream_runtime import BigQuerySourceStreamRuntime

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence

    from agora.core.context import PipelineContext

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


def _callable_accepts_context(fn: Callable[..., Any]) -> bool:
    try:
        params = signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    positional = [
        param
        for param in params
        if param.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    ]
    return len(positional) >= 2


def _identity(row: dict[str, Any]) -> Any:
    return row


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    items = getattr(row, "items", None)
    if callable(items):
        return dict(items())
    return dict(row)


class BigQuerySource(BaseSource[T], Generic[T]):
    """Read rows from a BigQuery table or query."""

    source_name = "bigquery"

    def __init__(
        self,
        *,
        table: str | None = None,
        query: str | None = None,
        row_mapper: Callable[..., T | Awaitable[T] | None] | None = None,
        query_parameters: dict[str, Any] | None = None,
        columns: Sequence[str] | None = None,
        order_by: Sequence[str] | None = None,
        checkpoint_column: str | None = None,
        checkpoint_tiebreaker_column: str | None = None,
        checkpoint_column_is_unique: bool = False,
        batch_size: int = 500,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        project: str | None = None,
        location: str | None = None,
        credentials_path: str | None = None,
        credentials: Any | None = None,
        connection: BigQueryConnectionConfig | None = None,
        client: Any | None = None,
        source_identity_mismatch_policy: SourceIdentityMismatchPolicy
        | str = SourceIdentityMismatchPolicy.FAIL_CLOSED,
    ) -> None:
        if (table is None) == (query is None):
            raise ValueError("Provide exactly one of table=... or query=....")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1.")
        if checkpoint_column is not None and table is None:
            raise ValueError("checkpoint_column is only supported for table mode.")
        if checkpoint_column is not None:
            BigQuerySourceQueryRuntime.validate_field_name(checkpoint_column)
        if checkpoint_tiebreaker_column is not None:
            BigQuerySourceQueryRuntime.validate_field_name(checkpoint_tiebreaker_column)
        if checkpoint_tiebreaker_column is not None and checkpoint_column is None:
            raise ValueError("checkpoint_tiebreaker_column requires checkpoint_column.")
        if (
            checkpoint_tiebreaker_column is not None
            and checkpoint_tiebreaker_column == checkpoint_column
        ):
            raise ValueError("checkpoint_tiebreaker_column must differ from checkpoint_column.")
        if (
            checkpoint_column is not None
            and checkpoint_tiebreaker_column is None
            and not checkpoint_column_is_unique
        ):
            raise ValueError(
                "BigQuery checkpoint_column requires either "
                "checkpoint_column_is_unique=True or checkpoint_tiebreaker_column=... "
                "to prevent data loss when cursor values repeat."
            )
        if checkpoint_column_is_unique and checkpoint_tiebreaker_column is not None:
            raise ValueError(
                "Choose either checkpoint_column_is_unique=True or "
                "checkpoint_tiebreaker_column, not both."
            )
        validated_columns = tuple(
            BigQuerySourceQueryRuntime.validate_field_name(name) for name in (columns or ())
        )
        validated_order_by = tuple(
            BigQuerySourceQueryRuntime.validate_field_name(name) for name in (order_by or ())
        )
        checkpoint_order = tuple(
            value
            for value in (checkpoint_column, checkpoint_tiebreaker_column)
            if value is not None
        )
        if (
            checkpoint_order
            and validated_order_by
            and validated_order_by[: len(checkpoint_order)] != checkpoint_order
        ):
            raise ValueError(
                "order_by must start with checkpoint_column and checkpoint_tiebreaker_column "
                "when checkpointing is enabled."
            )

        self._mode: Literal["table", "query"] = "table" if table is not None else "query"
        self._table = table
        self._query = query
        self._row_mapper = row_mapper or _identity
        self._row_mapper_accepts_context = _callable_accepts_context(self._row_mapper)
        self._query_parameters = dict(query_parameters or {})
        self._columns = validated_columns
        self._order_by = validated_order_by or checkpoint_order
        self._checkpoint_column = checkpoint_column
        self._checkpoint_tiebreaker_column = checkpoint_tiebreaker_column
        self._batch_size = batch_size
        self._on_record_error = on_record_error
        self._connection = coerce_connection_config(
            project=project,
            location=location,
            credentials_path=credentials_path,
            credentials=credentials,
            connection=connection,
        )
        self._client = client
        self._source_identity_mismatch_policy = SourceIdentityMismatchPolicy(
            source_identity_mismatch_policy
        )
        self._resume_cursor: Any | None = None
        self._rows_seen = 0
        self._emitted_record_count = 0
        self._record_error_count = 0
        self._record_drop_count = 0
        self._query_execution_count = 0
        self._stream_run_count = 0
        self._active_stream_count = 0
        self._last_checkpoint_cursor: Any | None = None
        self._last_job_id: str | None = None
        self._last_stream_started_at: datetime | None = None
        self._last_stream_completed_at: datetime | None = None
        self._last_stream_succeeded: bool | None = None
        self._last_error: str | None = None
        self._bound_ctx: PipelineContext | None = None
        self.supports_checkpoint = self._mode == "table" and self._checkpoint_column is not None
        self._query_runtime = BigQuerySourceQueryRuntime(
            mode=self._mode,
            table=self._table,
            query=self._query,
            query_parameters=self._query_parameters,
            columns=self._columns,
            order_by=self._order_by,
            checkpoint_column=self._checkpoint_column,
            checkpoint_tiebreaker_column=self._checkpoint_tiebreaker_column,
            batch_size=self._batch_size,
            supports_checkpoint=self.supports_checkpoint,
        )
        self._stream_runtime = BigQuerySourceStreamRuntime(
            query_runtime=self._query_runtime,
            ensure_open=self.open,
            client_provider=lambda: self._client,
            row_to_dict=_row_to_dict,
            apply_row_mapper=self._apply_row_mapper,
            current_checkpoint=self.current_checkpoint,
            on_stream_started=self._handle_stream_started,
            on_query_started=self._handle_query_started,
            on_row_seen=self._handle_row_seen,
            on_record_error=self._handle_record_error,
            on_record_dropped=self._handle_record_dropped,
            on_checkpoint_cursor=self._handle_checkpoint_cursor,
            on_record_emitted=self._handle_record_emitted,
            on_stream_failed=self._handle_stream_failed,
            on_stream_completed=self._handle_stream_completed,
            logger=logger,
            query_mode=self._mode,
            supports_checkpoint=self.supports_checkpoint,
            checkpoint_column=self._checkpoint_column,
            checkpoint_tiebreaker_column=self._checkpoint_tiebreaker_column,
            on_record_error_policy=self._on_record_error,
            source_name=self.source_name,
            metrics_provider=self._stream_log_metrics,
        )

    def bind_context(self, ctx: PipelineContext) -> None:
        self._bound_ctx = ctx

    async def open(self) -> None:
        if self._client is None:
            self._client = build_bigquery_client(self._connection)

    async def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            result = close()
            if isawaitable(result):
                await result

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        self._resume_cursor = None
        self._rows_seen = 0
        self._emitted_record_count = 0
        self._last_checkpoint_cursor = None
        if checkpoint is None or not self.supports_checkpoint:
            return
        checkpoint = validate_resume_checkpoint_identity(
            checkpoint,
            current_identity=self.checkpoint_source_identity(),
            policy=self._source_identity_mismatch_policy,
            source_name=self.source_name,
        )
        if checkpoint is None:
            return
        value = checkpoint.value if isinstance(checkpoint.value, dict) else {}
        self._resume_cursor = value.get("cursor")
        if (
            self._checkpoint_tiebreaker_column is not None
            and self._resume_cursor is not None
            and not isinstance(self._resume_cursor, dict)
        ):
            raise ValueError(
                "Cannot resume BigQuery composite checkpoint from a legacy scalar cursor. "
                "Start a new run or migrate the saved checkpoint."
            )

    def checkpoint_source_identity(self) -> SourceIdentity:
        """Return a secret-free identity for the configured BigQuery input."""
        return fingerprint_source_identity(
            "bigquery",
            {
                "mode": self._mode,
                "table": self._table,
                "query": self._query,
                "query_parameters": self._query_parameters,
                "columns": self._columns,
                "order_by": self._order_by,
                "checkpoint_column": self._checkpoint_column,
                "checkpoint_tiebreaker_column": self._checkpoint_tiebreaker_column,
                "project": self._connection.project,
                "location": self._connection.location,
            },
        )

    def current_checkpoint(self) -> dict[str, Any] | None:
        if self._rows_seen <= 0 and self._last_checkpoint_cursor is None:
            return None
        payload: dict[str, Any] = {"row_number": self._rows_seen}
        if self._last_checkpoint_cursor is not None:
            payload["cursor"] = self._last_checkpoint_cursor
        return payload

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def recovery_contract(self) -> BigQuerySourceRecoveryContractSnapshot:
        return BigQuerySourceRecoveryContractSnapshot(
            mode=(
                BigQuerySourceRecoveryMode.CHECKPOINT_RERUN
                if self.supports_checkpoint
                else BigQuerySourceRecoveryMode.FULL_RERUN
            ),
            supports_checkpoint=self.supports_checkpoint,
            checkpoint_fields=tuple(
                value
                for value in (self._checkpoint_column, self._checkpoint_tiebreaker_column)
                if value is not None
            ),
            checkpoint_params=(
                {
                    self._checkpoint_column: "checkpoint_cursor",
                    **(
                        {self._checkpoint_tiebreaker_column: "checkpoint_tiebreaker_cursor"}
                        if self._checkpoint_tiebreaker_column
                        else {}
                    ),
                }
                if self._checkpoint_column
                else {}
            ),
            on_record_error=str(self._on_record_error),
        )

    def metrics_snapshot(self) -> BigQuerySourceMetricsSnapshot:
        return BigQuerySourceMetricsSnapshot(
            mode=self._mode,
            connection_ready=self._client is not None,
            supports_checkpoint=self.supports_checkpoint,
            stream_run_count=self._stream_run_count,
            active_stream_count=self._active_stream_count,
            rows_seen=self._rows_seen,
            row_mapper_accepts_context=self._row_mapper_accepts_context,
            query_execution_count=self._query_execution_count,
            emitted_record_count=self._emitted_record_count,
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
            last_job_id=self._last_job_id,
            last_checkpoint_cursor=self._last_checkpoint_cursor,
            last_stream_started_at=self._last_stream_started_at,
            last_stream_completed_at=self._last_stream_completed_at,
            last_stream_succeeded=self._last_stream_succeeded,
            last_error=self._last_error,
            recovery_contract=self.recovery_contract(),
        )

    def health_snapshot(self) -> BigQuerySourceHealthSnapshot:
        connection_ready = self._client is not None
        return BigQuerySourceHealthSnapshot(
            ready=connection_ready and self._active_stream_count == 0 and self._last_error is None,
            component=self.source_name,
            connection_ready=connection_ready,
            last_error=self._last_error,
            mode=self._mode,
            supports_checkpoint=self.supports_checkpoint,
            query_executed=self._query_execution_count > 0,
            active_stream_count=self._active_stream_count,
            last_stream_succeeded=self._last_stream_succeeded,
        )

    def acceptance_report(
        self,
        thresholds: BigQuerySourceEnterpriseAcceptanceThresholds | None = None,
    ) -> Any:
        return BigQueryEnterpriseAcceptanceGate().evaluate_source(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    async def stream(self) -> AsyncGenerator[T, None]:
        async for record in self._stream_runtime.stream(resume_cursor=self._resume_cursor):
            yield record

    def _handle_stream_started(self) -> None:
        self._last_error = None
        self._last_stream_started_at = datetime.now(UTC)
        self._last_stream_completed_at = None
        self._last_stream_succeeded = None
        self._stream_run_count += 1
        self._active_stream_count += 1

    def _handle_query_started(self, job_id: str | None) -> None:
        self._last_job_id = job_id
        self._query_execution_count += 1

    def _handle_row_seen(self) -> None:
        self._rows_seen += 1

    def _handle_record_error(self, exc: Exception) -> None:
        self._record_error_count += 1
        self._last_error = str(exc)

    def _handle_record_dropped(self) -> None:
        self._record_drop_count += 1

    def _handle_checkpoint_cursor(self, value: Any) -> None:
        self._last_checkpoint_cursor = value

    def _handle_record_emitted(self) -> None:
        self._emitted_record_count += 1

    def _handle_stream_failed(self, exc: Exception) -> None:
        self._last_error = str(exc)

    def _handle_stream_completed(self, success: bool) -> None:
        self._active_stream_count = max(0, self._active_stream_count - 1)
        self._last_stream_completed_at = datetime.now(UTC)
        self._last_stream_succeeded = success

    def _stream_log_metrics(self) -> dict[str, Any]:
        return {
            "job_id": self._last_job_id,
            "rows_seen": self._rows_seen,
            "emitted_record_count": self._emitted_record_count,
            "record_error_count": self._record_error_count,
            "record_drop_count": self._record_drop_count,
        }

    async def _apply_row_mapper(self, row: dict[str, Any]) -> T | None:
        mapper = self._row_mapper
        if self._row_mapper_accepts_context:
            mapped = cast(
                "Callable[[dict[str, Any], PipelineContext | None], T | Awaitable[T] | None]",
                mapper,
            )(row, self._bound_ctx)
        else:
            mapped = cast("Callable[[dict[str, Any]], T | Awaitable[T] | None]", mapper)(row)
        if isawaitable(mapped):
            return await mapped
        return mapped
