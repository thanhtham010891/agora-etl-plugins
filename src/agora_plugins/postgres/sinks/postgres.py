"""
agora_plugins.postgres.sinks.postgres
=====================================
Generic async PostgreSQL sink with batch upsert plus schema adapter helpers.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from agora.core.delivery import IdempotencyMode, SinkDeliveryCapability
from agora.core.sink import BaseSink

from agora_plugins.postgres.connection import (
    PostgresConnectionConfig,
    coerce_connection_config,
)
from agora_plugins.postgres.sinks._close_runtime import PostgresCloseRuntime
from agora_plugins.postgres.sinks._connection_runtime import PostgresSinkConnectionRuntime
from agora_plugins.postgres.sinks._flush_runtime import PostgresFlushRuntime
from agora_plugins.postgres.sinks._identifiers import (
    QuotedIdentifier,
)
from agora_plugins.postgres.sinks._identifiers import (
    _postgres_type as _postgres_type,
)
from agora_plugins.postgres.sinks._identifiers import (
    _quote_identifier as _quote_identifier,
)
from agora_plugins.postgres.sinks._identifiers import (
    _schema_advisory_lock_key as _schema_advisory_lock_key,
)
from agora_plugins.postgres.sinks._identifiers import (
    _table_lookup_condition as _table_lookup_condition,
)
from agora_plugins.postgres.sinks._lifecycle_runtime import PostgresLifecycleRuntime
from agora_plugins.postgres.sinks._metrics import (
    PostgresSinkMetricsSnapshot,
)
from agora_plugins.postgres.sinks._metrics_surface import PostgresSinkMetricsSurface
from agora_plugins.postgres.sinks._poison_runtime import PostgresPoisonRuntime
from agora_plugins.postgres.sinks._runtime_state import PostgresSinkRuntimeState
from agora_plugins.postgres.sinks._sink_compat import PostgresSinkCompatMixin
from agora_plugins.postgres.sinks._sink_types import (
    PostgresPoisonRecordClassification,
    PostgresPoisonRecordInfo,
    PostgresSinkWriteError,
    PostgresWriteSafetyPolicy,
)
from agora_plugins.postgres.sinks._sink_types import (
    now_utc as _now_utc,
)
from agora_plugins.postgres.sinks._sink_types import (
    resolve_write_safety_policy as _resolve_write_safety_policy,
)
from agora_plugins.postgres.sinks._write_buffer import PostgresSinkWriteBuffer
from agora_plugins.postgres.sinks._write_plan import PostgresWritePlanner
from agora_plugins.postgres.sinks._write_preparation import (
    PostgresWritePreparation,
    _TargetColumn,
)
from agora_plugins.postgres.sinks.schema_adapter import PostgresSchemaAdapter

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from agora.core.dlq import DLQSink
    from agora.core.retry import RetryPolicy

T = TypeVar("T")


class PostgresSink(PostgresSinkCompatMixin, BaseSink[T], Generic[T]):
    """Generic async batch-upsert PostgreSQL sink."""

    sink_name = "postgres"

    def __init__(
        self,
        dsn: str | None,
        table: str | QuotedIdentifier,
        row_mapper: Callable[[T], dict[str, Any]],
        conflict_key: str | QuotedIdentifier | list[str | QuotedIdentifier],
        batch_size: int = 100,
        upsert: bool = True,
        insert_mode: Literal["sql", "copy", "copy_merge"] = "sql",
        pool_size: int = 1,
        max_rows_per_statement: int | None = None,
        max_parameters_per_statement: int = 32_000,
        retry_policy: RetryPolicy[Any] | None = None,
        write_safety_policy: PostgresWriteSafetyPolicy | str = PostgresWriteSafetyPolicy.STRICT,
        replay_safe_key_contract: bool = False,
        connection: PostgresConnectionConfig | None = None,
        poison_record_sink: DLQSink | None = None,
        poison_record_pipeline_id: str | None = None,
        poison_record_max_attempts: int | None = None,
        pool_acquire_timeout_s: float | None = 30.0,
        pool_health_check: bool = True,
        pool_max_lifetime_s: float = 3600.0,
        pool_max_idle_s: float = 600.0,
        allow_quoted_identifiers: bool = False,
    ) -> None:
        if insert_mode not in {"sql", "copy", "copy_merge"}:
            raise ValueError("insert_mode must be 'sql', 'copy', or 'copy_merge'")
        if upsert and insert_mode == "copy":
            raise ValueError("insert_mode='copy' is only supported when upsert=False")
        if not isinstance(replay_safe_key_contract, bool):
            raise TypeError("replay_safe_key_contract must be a bool")
        if replay_safe_key_contract and not upsert:
            raise ValueError("replay_safe_key_contract=True requires upsert=True")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")
        if max_rows_per_statement is not None and max_rows_per_statement < 1:
            raise ValueError("max_rows_per_statement must be >= 1 when provided")
        if max_parameters_per_statement < 1:
            raise ValueError("max_parameters_per_statement must be >= 1")
        self._connection = coerce_connection_config(dsn=dsn, connection=connection)
        self._table = table
        self._row_mapper = row_mapper
        self._conflict_keys = (
            [conflict_key] if isinstance(conflict_key, (str, QuotedIdentifier)) else conflict_key
        )
        self._allow_quoted_identifiers = allow_quoted_identifiers
        self._batch_size = batch_size
        self._upsert = upsert
        self._write_planner = PostgresWritePlanner(
            table=table,
            conflict_keys=tuple(self._conflict_keys),
            upsert=upsert,
            allow_quoted_identifiers=allow_quoted_identifiers,
        )
        self._insert_mode = insert_mode
        self._pool_size = pool_size
        self._max_rows_per_statement = max_rows_per_statement
        self._max_parameters_per_statement = max_parameters_per_statement
        self._retry_policy = retry_policy
        self._write_safety_policy = _resolve_write_safety_policy(write_safety_policy)
        self._replay_safe_key_contract = replay_safe_key_contract
        self._poison_record_sink = poison_record_sink
        self._poison_record_pipeline_id = poison_record_pipeline_id or f"postgres:{table}"
        self._poison_record_max_attempts = poison_record_max_attempts
        if pool_acquire_timeout_s is not None and pool_acquire_timeout_s <= 0:
            raise ValueError("pool_acquire_timeout_s must be > 0 when provided")
        if pool_max_lifetime_s <= 0:
            raise ValueError("pool_max_lifetime_s must be > 0")
        if pool_max_idle_s <= 0:
            raise ValueError("pool_max_idle_s must be > 0")
        self._pool_acquire_timeout_s = pool_acquire_timeout_s
        self._pool_health_check = pool_health_check
        self._pool_max_lifetime_s = pool_max_lifetime_s
        self._pool_max_idle_s = pool_max_idle_s
        self._buffer: list[dict[str, Any]] = []
        self._conn: Any | None = None
        self._write_pool: asyncio.LifoQueue[Any] | None = None
        self._write_pool_open_connections = 0
        self._write_pool_lock = asyncio.Lock()
        self._external_write_pool: Any | None = None
        self._external_write_pool_conn_ids: set[int] = set()
        self._external_write_pool_unavailable = False
        self._psycopg: Any | None = None
        self._connection_runtime = PostgresSinkConnectionRuntime(self)
        self._latency_bucket_counts: dict[tuple[str, str], list[int]] = {}
        self._latency_counts: dict[tuple[str, str], int] = {}
        self._latency_sums: dict[tuple[str, str], float] = {}
        self._metrics_surface = PostgresSinkMetricsSurface(self)
        self._write_call_count = 0
        self._write_batch_call_count = 0
        self._enqueue_count = 0
        self._flush_count = 0
        self._flushed_row_count = 0
        self._retry_count = 0
        self._schema_refresh_count = 0
        self._schema_drift_detected_count = 0
        self._schema_drift_aligned_count = 0
        self._poison_record_count = 0
        self._poison_record_classification_counts = dict.fromkeys(
            PostgresPoisonRecordClassification,
            0,
        )
        self._last_flush_at: datetime | None = None
        self._target_columns_cache: list[_TargetColumn] | None = None
        self._defer_upsert_constraint_preflight = False
        self._upsert_constraint_preflight_complete = False
        self._runtime_state = PostgresSinkRuntimeState(self)
        self._write_buffer = PostgresSinkWriteBuffer(self)
        self._write_preparation = PostgresWritePreparation(
            table=self._table,
            conflict_keys=tuple(self._conflict_keys),
            write_safety_policy=self._write_safety_policy,
            max_rows_per_statement=self._max_rows_per_statement,
            max_parameters_per_statement=self._max_parameters_per_statement,
            on_schema_drift_detected=self._runtime_state.observe_schema_drift_detected,
            on_schema_drift_aligned=self._runtime_state.observe_schema_drift_aligned,
            make_write_error=self._make_write_error,
            schema_drift_classification=PostgresPoisonRecordClassification.SCHEMA_DRIFT,
        )
        self._poison_runtime = PostgresPoisonRuntime(
            table=self._table,
            sink_name=self.sink_name,
            poison_record_pipeline_id=self._poison_record_pipeline_id,
            poison_record_max_attempts=self._poison_record_max_attempts,
            current_buffer=self._runtime_state.current_buffer,
            clear_buffer_prefix=self._runtime_state.clear_buffer_prefix,
            current_poison_sink=self._runtime_state.current_poison_sink,
            now_utc=_now_utc,
            build_write_error=self._build_write_error,
            on_poison_record_observed=self._runtime_state.observe_poison_record,
            on_schema_drift_detected=self._runtime_state.observe_schema_drift_detected,
            classification_schema_drift=PostgresPoisonRecordClassification.SCHEMA_DRIFT,
            classification_constraint_violation=PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION,
            classification_type_mismatch=PostgresPoisonRecordClassification.TYPE_MISMATCH,
            classification_unknown=PostgresPoisonRecordClassification.UNKNOWN,
        )
        self._close_runtime = PostgresCloseRuntime(
            table=self._table,
            current_conn=self._runtime_state.current_conn,
            set_conn=self._runtime_state.set_conn,
            current_write_pool=self._runtime_state.current_write_pool,
            set_write_pool=self._runtime_state.set_write_pool,
            current_external_write_pool=self._runtime_state.current_external_write_pool,
            set_external_write_pool=self._runtime_state.set_external_write_pool,
            external_write_pool_conn_ids=self._external_write_pool_conn_ids,
            current_write_pool_open_connections=self._runtime_state.current_write_pool_open_connections,
            set_write_pool_open_connections=self._runtime_state.set_write_pool_open_connections,
            current_poison_sink=self._runtime_state.current_poison_sink,
            route_failed_buffer_to_dlq=self._route_failed_buffer_to_dlq,
        )
        self._lifecycle_runtime = PostgresLifecycleRuntime(
            table=self._table,
            conflict_keys=tuple(self._conflict_keys),
            upsert=self._upsert,
            allow_quoted_identifiers=self._allow_quoted_identifiers,
            current_poison_sink=self._runtime_state.current_poison_sink,
            should_defer_upsert_constraint_preflight=self._runtime_state.should_defer_upsert_constraint_preflight,
            upsert_constraint_preflight_complete=self._runtime_state.current_upsert_constraint_preflight_complete,
            set_upsert_constraint_preflight_complete=self._runtime_state.set_upsert_constraint_preflight_complete,
            write_connection=self._write_connection,
        )
        self._flush_runtime = PostgresFlushRuntime(
            sink=self,
            table=self._table,
            insert_mode=self._insert_mode,
            write_safety_policy=self._write_safety_policy,
            current_buffer=self._runtime_state.current_buffer,
            load_psycopg=self._load_psycopg,
            effective_retry_policy=self._effective_retry_policy,
            prepared_write_batches=self._prepared_write_batches,
            wrap_write_error=self._wrap_write_error,
            observe_latency=self._observe_latency,
            discard_buffer_indexes=self._discard_buffer_indexes,
            observe_retry=self._observe_retry,
            write_connection=self._write_connection,
            note_flush_success=self._runtime_state.note_flush_success,
            now_utc=_now_utc,
        )

    def delivery_capability(self) -> SinkDeliveryCapability:
        """Declare replay safety only after an explicit stable-key contract."""
        if self._upsert:
            return SinkDeliveryCapability(
                sink_name=self.sink_name,
                idempotency=IdempotencyMode.APPLICATION_MANAGED,
                replay_safe=self._replay_safe_key_contract,
                notes=(
                    "Upsert preflight validates the configured conflict key. "
                    + (
                        "The caller explicitly guarantees that its row mapper emits a stable "
                        "conflict key, so replay updates the same target identity."
                        if self._replay_safe_key_contract
                        else "The row mapper's conflict-key stability is not machine-verifiable; "
                        "set replay_safe_key_contract=True only after establishing that contract."
                    ),
                ),
            )
        return SinkDeliveryCapability(
            sink_name=self.sink_name,
            idempotency=IdempotencyMode.NONE,
            replay_safe=False,
            notes=("Append-only insert and COPY modes can duplicate rows on replay.",),
        )

    def invalidate_target_columns_cache(self) -> None:
        """Clear cached target-table metadata after an external schema change."""

        self._target_columns_cache = None

    def defer_upsert_constraint_preflight_until_schema_applied(self) -> None:
        """Let a schema adapter run DDL before validating upsert constraints."""

        self._defer_upsert_constraint_preflight = True
        self._upsert_constraint_preflight_complete = False

    async def connection(self) -> Any:
        """Public API for obtaining the underlying connection (used by schema adapters)."""
        return await self._get_conn()

    async def open(self) -> None:
        await self._lifecycle_runtime.open()

    async def validate_upsert_constraint(self) -> None:
        await self._lifecycle_runtime.validate_upsert_constraint()

    async def write(self, record: T) -> None:
        await self._write_buffer.write(record)

    async def write_batch(self, records: list[T]) -> None:
        await self._write_buffer.write_batch(records)

    async def flush(self) -> None:
        await self._flush_runtime.flush()

    def metrics_snapshot(self) -> PostgresSinkMetricsSnapshot:
        return self._metrics_surface.snapshot()

    def render_prometheus_metrics(self, namespace: str = "agora_postgres") -> str:
        from agora_plugins.postgres.observability import PostgresPrometheusExporter

        return PostgresPrometheusExporter(namespace=namespace).render_sink(self.metrics_snapshot())

    async def close(self) -> None:
        await self._close_runtime.close(flush=self.flush)


__all__ = [
    "PostgresPoisonRecordClassification",
    "PostgresPoisonRecordInfo",
    "PostgresSchemaAdapter",
    "PostgresSink",
    "PostgresSinkMetricsSnapshot",
    "PostgresSinkWriteError",
    "PostgresWriteSafetyPolicy",
    "QuotedIdentifier",
    "_postgres_type",
    "_quote_identifier",
    "_schema_advisory_lock_key",
    "_table_lookup_condition",
]
