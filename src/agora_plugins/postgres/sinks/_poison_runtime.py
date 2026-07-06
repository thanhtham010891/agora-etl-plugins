"""Poison-record and DLQ helpers for PostgreSQL sinks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.dlq import DLQRecord

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from agora.core.dlq import DLQSink

    from agora_plugins.postgres.sinks.postgres import (
        PostgresPoisonRecordClassification,
        PostgresSinkWriteError,
    )


class PostgresPoisonRuntime:
    """Owns poison classification bookkeeping and DLQ routing for ``PostgresSink``."""

    def __init__(
        self,
        *,
        table: str | object,
        sink_name: str,
        poison_record_pipeline_id: str,
        poison_record_max_attempts: int | None,
        current_buffer: Callable[[], list[dict[str, Any]]],
        clear_buffer_prefix: Callable[[int], None],
        current_poison_sink: Callable[[], DLQSink | None],
        now_utc: Callable[[], datetime],
        build_write_error: Callable[..., PostgresSinkWriteError],
        on_poison_record_observed: Callable[[PostgresSinkWriteError], None],
        on_schema_drift_detected: Callable[[int], None],
        classification_schema_drift: PostgresPoisonRecordClassification,
        classification_constraint_violation: PostgresPoisonRecordClassification,
        classification_type_mismatch: PostgresPoisonRecordClassification,
        classification_unknown: PostgresPoisonRecordClassification,
    ) -> None:
        self._table = table
        self._sink_name = sink_name
        self._poison_record_pipeline_id = poison_record_pipeline_id
        self._poison_record_max_attempts = poison_record_max_attempts
        self._current_buffer = current_buffer
        self._clear_buffer_prefix = clear_buffer_prefix
        self._current_poison_sink = current_poison_sink
        self._now_utc = now_utc
        self._build_write_error = build_write_error
        self._on_poison_record_observed = on_poison_record_observed
        self._on_schema_drift_detected = on_schema_drift_detected
        self._classification_schema_drift = classification_schema_drift
        self._classification_constraint_violation = classification_constraint_violation
        self._classification_type_mismatch = classification_type_mismatch
        self._classification_unknown = classification_unknown

    def make_write_error(
        self,
        message: str,
        *,
        classification: PostgresPoisonRecordClassification,
        reason: str,
        details: dict[str, Any],
    ) -> PostgresSinkWriteError:
        error = self._build_write_error(
            message,
            classification=classification,
            reason=reason,
            details=details,
        )
        self.observe_poison_record(error)
        return error

    def wrap_write_error(
        self,
        exc: Exception,
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> PostgresSinkWriteError:
        if hasattr(exc, "poison_info") and hasattr(exc, "dlq_details"):
            return exc  # type: ignore[return-value]

        classification = self._classification_unknown
        reason = type(exc).__name__
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate in {"42P01", "42703"}:
            classification = self._classification_schema_drift
            reason = "undefined_table" if sqlstate == "42P01" else "undefined_column"
            self._on_schema_drift_detected(1)
        elif sqlstate in {"23502", "23505", "23503", "23514"}:
            classification = self._classification_constraint_violation
            reason = {
                "23502": "not_null_violation",
                "23505": "unique_violation",
                "23503": "foreign_key_violation",
                "23514": "check_violation",
            }[sqlstate]
        elif sqlstate in {"22P02", "42804"}:
            classification = self._classification_type_mismatch
            reason = "invalid_text_representation" if sqlstate == "22P02" else "datatype_mismatch"

        return self.make_write_error(
            f"Postgres sink write failed: {exc}",
            classification=classification,
            reason=reason,
            details={
                "table": self._table,
                "sqlstate": sqlstate,
                "columns": list(columns),
                "row_count": len(rows),
            },
        )

    def observe_poison_record(self, error: PostgresSinkWriteError) -> None:
        self._on_poison_record_observed(error)

    async def route_failed_buffer_to_dlq(self, error: PostgresSinkWriteError) -> None:
        poison_sink = self._current_poison_sink()
        failed_rows = list(self._current_buffer())
        if poison_sink is None or not failed_rows:
            return

        run_id = f"postgres-flush-{self._now_utc().isoformat()}"
        records = [
            DLQRecord(
                pipeline_id=self._poison_record_pipeline_id,
                run_id=run_id,
                stage="postgres_sink_flush",
                error_type=type(error).__name__,
                error_message=str(error),
                record=row,
                processed_record=row,
                details=error.dlq_details,
                sink=self._sink_name,
                max_attempts=self._poison_record_max_attempts,
            )
            for row in failed_rows
        ]
        await poison_sink.write_batch(records)
        self._clear_buffer_prefix(len(failed_rows))
