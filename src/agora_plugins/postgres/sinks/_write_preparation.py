"""Row preparation and schema-alignment helpers for PostgreSQL sinks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator, Sequence

    from agora_plugins.postgres.sinks.postgres import (
        PostgresPoisonRecordClassification,
        PostgresSinkWriteError,
        PostgresWriteSafetyPolicy,
    )


@dataclass(frozen=True, slots=True)
class _TargetColumn:
    name: str
    nullable: bool
    has_default: bool
    writable: bool


@dataclass(frozen=True, slots=True)
class _PreparedWriteBatch:
    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    row_indexes: tuple[int, ...]


class PostgresWritePreparation:
    """Owns batch shaping and target-schema alignment for ``PostgresSink``."""

    def __init__(
        self,
        *,
        table: str | object,
        conflict_keys: Sequence[str | object],
        write_safety_policy: PostgresWriteSafetyPolicy,
        max_rows_per_statement: int | None,
        max_parameters_per_statement: int,
        on_schema_drift_detected: Callable[[int], None],
        on_schema_drift_aligned: Callable[[int], None],
        make_write_error: Callable[..., PostgresSinkWriteError],
        schema_drift_classification: PostgresPoisonRecordClassification,
    ) -> None:
        self._table = table
        self._conflict_keys = list(conflict_keys)
        self._write_safety_policy = write_safety_policy
        self._max_rows_per_statement = max_rows_per_statement
        self._max_parameters_per_statement = max_parameters_per_statement
        self._on_schema_drift_detected = on_schema_drift_detected
        self._on_schema_drift_aligned = on_schema_drift_aligned
        self._make_write_error = make_write_error
        self._schema_drift_classification = schema_drift_classification

    async def prepared_write_batches(
        self,
        rows: list[dict[str, Any]],
        *,
        load_target_columns: Callable[[], Awaitable[list[_TargetColumn]]],
    ) -> list[_PreparedWriteBatch]:
        if not rows:
            return []
        if self._write_safety_policy.value == "strict":
            return [
                _PreparedWriteBatch(
                    columns=tuple(rows[0].keys()),
                    rows=tuple(rows),
                    row_indexes=tuple(range(len(rows))),
                )
            ]

        target_columns = await load_target_columns()
        return self.align_rows_to_target(rows, target_columns)

    def align_rows_to_target(
        self,
        rows: list[dict[str, Any]],
        target_columns: list[_TargetColumn],
    ) -> list[_PreparedWriteBatch]:
        writable_order = [column.name for column in target_columns if column.writable]
        grouped: dict[tuple[str, ...], list[tuple[int, dict[str, Any]]]] = {}
        order: list[tuple[str, ...]] = []

        for index, row in enumerate(rows):
            normalized_row = self.normalize_row_to_target(row, target_columns, row_index=index)
            ordered_columns = tuple(column for column in writable_order if column in normalized_row)
            if ordered_columns not in grouped:
                grouped[ordered_columns] = []
                order.append(ordered_columns)
            grouped[ordered_columns].append((index, normalized_row))

        return [
            _PreparedWriteBatch(
                columns=columns,
                rows=tuple(row for _index, row in grouped[columns]),
                row_indexes=tuple(index for index, _row in grouped[columns]),
            )
            for columns in order
        ]

    def normalize_row_to_target(
        self,
        row: dict[str, Any],
        target_columns: list[_TargetColumn],
        *,
        row_index: int,
    ) -> dict[str, Any]:
        target_by_name = {column.name: column for column in target_columns}
        unknown_columns = [
            key for key in row if key not in target_by_name or not target_by_name[key].writable
        ]
        if unknown_columns:
            self._on_schema_drift_detected(1)
            self._on_schema_drift_aligned(len(unknown_columns))

        normalized = {
            key: value
            for key, value in row.items()
            if key in target_by_name and target_by_name[key].writable
        }

        missing_conflict_keys = [key for key in self._conflict_keys if key not in normalized]
        if missing_conflict_keys:
            raise self._make_write_error(
                "Postgres row is missing conflict keys after schema alignment.",
                classification=self._schema_drift_classification,
                reason="missing_conflict_keys",
                details={
                    "table": self._table,
                    "row_index": row_index,
                    "missing_conflict_keys": missing_conflict_keys,
                    "input_columns": list(row.keys()),
                },
            )

        missing_required = [
            column.name
            for column in target_columns
            if (
                column.writable
                and column.name not in normalized
                and not column.nullable
                and not column.has_default
            )
        ]
        if missing_required:
            self._on_schema_drift_detected(1)
            raise self._make_write_error(
                "Postgres target schema requires non-null columns that are missing from the row.",
                classification=self._schema_drift_classification,
                reason="missing_required_columns",
                details={
                    "table": self._table,
                    "row_index": row_index,
                    "missing_required_columns": missing_required,
                    "input_columns": list(row.keys()),
                },
            )

        if not normalized:
            raise self._make_write_error(
                "Postgres row has no writable target columns after schema alignment.",
                classification=self._schema_drift_classification,
                reason="no_writable_columns",
                details={
                    "table": self._table,
                    "row_index": row_index,
                    "input_columns": list(row.keys()),
                },
            )
        return normalized

    def flatten_rows(self, rows: list[dict[str, Any]], columns: list[str]) -> list[Any]:
        params: list[Any] = []
        expected_columns = tuple(columns)
        for row in rows:
            row_columns = tuple(row.keys())
            if set(row_columns) != set(expected_columns):
                raise ValueError(
                    "PostgresSink rows in the same batch must have identical column sets. "
                    f"Expected {expected_columns!r}, got {row_columns!r}."
                )
            params.extend(row[column] for column in columns)
        return params

    def statement_row_limit(self, column_count: int) -> int:
        if column_count <= 0:
            return 1
        by_params = max(1, self._max_parameters_per_statement // column_count)
        if self._max_rows_per_statement is None:
            return by_params
        return max(1, min(self._max_rows_per_statement, by_params))

    def iter_sql_chunks(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> Iterator[list[dict[str, Any]]]:
        chunk_size = self.statement_row_limit(len(columns))
        for start in range(0, len(rows), chunk_size):
            yield rows[start : start + chunk_size]
