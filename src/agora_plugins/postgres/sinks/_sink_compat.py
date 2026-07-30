"""Compatibility/proxy surface for PostgreSQL sinks.

These methods are still part of the de facto white-box surface used by tests and
schema/runtime collaborators. Keeping them in a mixin lets the main sink facade
stay focused on construction and public operations without breaking callers.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from agora.core.retry import RetryPolicy

from agora_plugins.postgres.failures import classify_postgres_failure
from agora_plugins.postgres.sinks._identifiers import QuotedIdentifier, _table_lookup_condition
from agora_plugins.postgres.sinks._sink_types import (
    PostgresPoisonRecordClassification,
    PostgresPoisonRecordInfo,
    PostgresSinkWriteError,
)
from agora_plugins.postgres.sinks._write_preparation import _PreparedWriteBatch, _TargetColumn
from agora_plugins.postgres.sinks._write_strategies import (
    flush_via_copy,
    flush_via_copy_merge,
    flush_via_sql,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from agora_plugins.postgres.sinks._metrics import PostgresLatencyHistogramSnapshot


class PostgresSinkCompatMixin:
    """Legacy private-method surface delegated to extracted collaborators."""

    async def _load_psycopg(self) -> Any:
        return await self._connection_runtime.load_psycopg()

    async def _create_connection(self) -> Any:
        return await self._connection_runtime.create_connection()

    async def _get_conn(self) -> Any:
        return await self._connection_runtime.get_conn()

    async def _acquire_write_conn(self) -> tuple[Any, bool]:
        return await self._connection_runtime.acquire_write_conn()

    async def _pooled_connection_ready(self, conn: Any) -> bool:
        return await self._connection_runtime.pooled_connection_ready(conn)

    async def _discard_pooled_connection(self, conn: Any) -> None:
        await self._connection_runtime.discard_pooled_connection(conn)

    async def _release_write_conn(self, conn: Any, *, pooled: bool, discard: bool = False) -> None:
        await self._connection_runtime.release_write_conn(conn, pooled=pooled, discard=discard)

    async def _ensure_external_write_pool(self) -> Any | None:
        return await self._connection_runtime.ensure_external_write_pool()

    def _write_connection(self) -> Any:
        return self._connection_runtime.write_connection()

    async def _has_matching_upsert_constraint(self, conn: Any) -> bool:
        return await self._lifecycle_runtime.has_matching_upsert_constraint(conn)

    def _effective_retry_policy(self) -> RetryPolicy[Any]:
        if self._retry_policy is not None:
            return self._retry_policy
        if self._psycopg is None:
            return RetryPolicy[Any](max_attempts=1)
        return RetryPolicy[Any](
            max_attempts=3,
            initial_backoff_s=0.25,
            backoff_multiplier=2.0,
            max_backoff_s=2.0,
            retry_exceptions=(self._psycopg.OperationalError, self._psycopg.InterfaceError),
            failure_classifier=classify_postgres_failure,
        )

    def _build_upsert_sql(self, columns: Sequence[str | QuotedIdentifier]) -> str:
        return self._write_planner.build_upsert_sql(columns)

    def _build_batch_upsert_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        *,
        row_count: int,
    ) -> str:
        return self._write_planner.build_batch_upsert_sql(
            columns,
            row_count=row_count,
        )

    def _build_copy_sql(self, columns: Sequence[str | QuotedIdentifier]) -> str:
        return self._write_planner.build_copy_sql(columns)

    def _build_copy_sql_for_table(
        self,
        table: str | QuotedIdentifier,
        columns: Sequence[str | QuotedIdentifier],
    ) -> str:
        return self._write_planner.build_copy_sql_for_table(table, columns)

    def _build_copy_merge_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        staging_table: str,
    ) -> str:
        return self._write_planner.build_copy_merge_sql(columns, staging_table)

    def _build_create_temp_table_sql(self, staging_table: str) -> str:
        return self._write_planner.build_create_temp_table_sql(staging_table)

    def _build_stage_table_name(self) -> str:
        return f"agora_stage_{uuid.uuid4().hex[:12]}"

    def _flatten_rows(self, rows: list[dict[str, Any]], columns: list[str]) -> list[Any]:
        return self._write_preparation.flatten_rows(rows, columns)

    def _statement_row_limit(self, column_count: int) -> int:
        return self._write_preparation.statement_row_limit(column_count)

    def _iter_sql_chunks(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> Iterator[list[dict[str, Any]]]:
        yield from self._write_preparation.iter_sql_chunks(rows, columns)

    async def _prepared_write_batches(
        self,
        rows: list[dict[str, Any]],
    ) -> list[_PreparedWriteBatch]:
        return await self._write_preparation.prepared_write_batches(
            rows,
            load_target_columns=self._load_target_columns,
        )

    async def _load_target_columns(self, *, force_refresh: bool = False) -> list[_TargetColumn]:
        if self._target_columns_cache is not None and not force_refresh:
            return list(self._target_columns_cache)

        conn = await self._get_conn()
        where_sql, where_params = _table_lookup_condition(
            self._table,
            allow_quoted=self._allow_quoted_identifiers,
        )
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT column_name, is_nullable, column_default, is_generated "
                "FROM information_schema.columns "
                f"WHERE {where_sql} "
                "ORDER BY ordinal_position",
                where_params,
            )
            rows = await cur.fetchall()
        self._schema_refresh_count += 1
        if not rows:
            raise self._make_write_error(
                "Postgres target table is missing.",
                classification=PostgresPoisonRecordClassification.SCHEMA_DRIFT,
                reason="undefined_table",
                details={"table": self._table},
            )
        target_columns = [
            _TargetColumn(
                name=str(column_name),
                nullable=str(is_nullable).upper() == "YES",
                has_default=(column_default is not None),
                writable=str(is_generated).upper() == "NEVER",
            )
            for column_name, is_nullable, column_default, is_generated in rows
        ]
        self._target_columns_cache = target_columns
        return list(target_columns)

    def _align_rows_to_target(
        self,
        rows: list[dict[str, Any]],
        target_columns: list[_TargetColumn],
    ) -> list[_PreparedWriteBatch]:
        return self._write_preparation.align_rows_to_target(rows, target_columns)

    def _normalize_row_to_target(
        self,
        row: dict[str, Any],
        target_columns: list[_TargetColumn],
        *,
        row_index: int,
    ) -> dict[str, Any]:
        return self._write_preparation.normalize_row_to_target(
            row,
            target_columns,
            row_index=row_index,
        )

    def _build_write_error(
        self,
        message: str,
        *,
        classification: PostgresPoisonRecordClassification,
        reason: str,
        details: dict[str, Any],
    ) -> PostgresSinkWriteError:
        return PostgresSinkWriteError(
            message,
            poison_info=PostgresPoisonRecordInfo(
                classification=classification,
                reason=reason,
                details=details,
            ),
        )

    def _make_write_error(
        self,
        message: str,
        *,
        classification: PostgresPoisonRecordClassification,
        reason: str,
        details: dict[str, Any],
    ) -> PostgresSinkWriteError:
        return self._poison_runtime.make_write_error(
            message,
            classification=classification,
            reason=reason,
            details=details,
        )

    def _wrap_write_error(
        self,
        exc: Exception,
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> PostgresSinkWriteError:
        return self._poison_runtime.wrap_write_error(
            exc,
            rows=rows,
            columns=columns,
        )

    def _discard_buffer_indexes(self, indexes: set[int]) -> None:
        for index in sorted(indexes, reverse=True):
            if index < len(self._buffer):
                del self._buffer[index]

    async def _flush_via_sql(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        policy: RetryPolicy[Any],
    ) -> None:
        await flush_via_sql(self, rows, columns, count, policy)

    async def _flush_via_copy(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        _policy: RetryPolicy[Any],
    ) -> None:
        await flush_via_copy(self, rows, columns, count)

    async def _flush_via_copy_merge(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        count: int,
        policy: RetryPolicy[Any],
    ) -> None:
        await flush_via_copy_merge(self, rows, columns, count, policy)

    async def _flush_aligned_batches_atomically(
        self,
        batches: list[_PreparedWriteBatch],
        rows: list[dict[str, Any]],
        policy: RetryPolicy[Any],
    ) -> None:
        await self._flush_runtime.flush_aligned_batches_atomically(
            batches,
            rows,
            policy,
        )

    def _observe_retry(self) -> None:
        self._metrics_surface.observe_retry()

    def _observe_latency(self, operation: str, outcome: str, duration_s: float) -> None:
        self._metrics_surface.observe_latency(operation, outcome, duration_s)

    def _latency_histogram_snapshots(self) -> tuple[PostgresLatencyHistogramSnapshot, ...]:
        return self._metrics_surface.latency_histogram_snapshots()

    async def _route_failed_buffer_to_dlq(self, error: PostgresSinkWriteError) -> None:
        await self._poison_runtime.route_failed_buffer_to_dlq(error)


__all__ = ["PostgresSinkCompatMixin"]
