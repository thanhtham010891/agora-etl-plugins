"""SQL planning for PostgreSQL sink write strategies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora_plugins.postgres.sinks._identifiers import QuotedIdentifier, _quote_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, slots=True)
class PostgresWritePlanner:
    """Build SQL without owning connections, buffers, retries, or metrics."""

    table: str | QuotedIdentifier
    conflict_keys: tuple[str | QuotedIdentifier, ...]
    upsert: bool
    allow_quoted_identifiers: bool

    def build_upsert_sql(self, columns: Sequence[str | QuotedIdentifier]) -> str:
        return self.build_batch_upsert_sql(columns, row_count=1)

    def build_batch_upsert_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        *,
        row_count: int,
    ) -> str:
        quoted_table = _quote_identifier(
            self.table,
            allow_path=True,
            allow_quoted=self.allow_quoted_identifiers,
        )
        col_list = ", ".join(
            _quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)
            for column in columns
        )
        row_placeholder = f"({', '.join(['%s'] * len(columns))})"
        val_list = ", ".join([row_placeholder] * row_count)
        conflict_cols = ", ".join(
            _quote_identifier(key, allow_quoted=self.allow_quoted_identifiers)
            for key in self.conflict_keys
        )
        update_set = ", ".join(
            f"{_quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)} = "
            f"EXCLUDED.{_quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)}"
            for column in columns
            if column not in self.conflict_keys
        )

        if self.upsert and update_set:
            return (
                f"INSERT INTO {quoted_table} ({col_list}) "
                f"VALUES {val_list} "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
            )
        return (
            f"INSERT INTO {quoted_table} ({col_list}) "
            f"VALUES {val_list} "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )

    def build_copy_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
    ) -> str:
        return self.build_copy_sql_for_table(self.table, columns)

    def build_copy_sql_for_table(
        self,
        table: str | QuotedIdentifier,
        columns: Sequence[str | QuotedIdentifier],
    ) -> str:
        quoted_table = _quote_identifier(
            table,
            allow_path=(("." in table) if isinstance(table, str) else len(table.parts) > 1),
            allow_quoted=self.allow_quoted_identifiers,
        )
        col_list = ", ".join(
            _quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)
            for column in columns
        )
        return f"COPY {quoted_table} ({col_list}) FROM STDIN"

    def build_copy_merge_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        staging_table: str,
    ) -> str:
        quoted_table = _quote_identifier(
            self.table,
            allow_path=True,
            allow_quoted=self.allow_quoted_identifiers,
        )
        quoted_staging = _quote_identifier(staging_table)
        col_list = ", ".join(
            _quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)
            for column in columns
        )
        select_list = ", ".join(
            f"{quoted_staging}."
            f"{_quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)}"
            for column in columns
        )
        conflict_cols = ", ".join(
            _quote_identifier(key, allow_quoted=self.allow_quoted_identifiers)
            for key in self.conflict_keys
        )
        update_set = ", ".join(
            f"{_quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)} = "
            f"EXCLUDED.{_quote_identifier(column, allow_quoted=self.allow_quoted_identifiers)}"
            for column in columns
            if column not in self.conflict_keys
        )

        if self.upsert and update_set:
            return (
                f"INSERT INTO {quoted_table} ({col_list}) "
                f"SELECT {select_list} FROM {quoted_staging} "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
            )
        return (
            f"INSERT INTO {quoted_table} ({col_list}) "
            f"SELECT {select_list} FROM {quoted_staging} "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )

    def build_create_temp_table_sql(self, staging_table: str) -> str:
        quoted_table = _quote_identifier(
            self.table,
            allow_path=True,
            allow_quoted=self.allow_quoted_identifiers,
        )
        quoted_staging = _quote_identifier(staging_table)
        return (
            f"CREATE TEMP TABLE {quoted_staging} "
            f"(LIKE {quoted_table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
