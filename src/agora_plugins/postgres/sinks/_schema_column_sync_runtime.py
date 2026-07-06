"""Column synchronization helpers for PostgreSQL schema adapter."""

from __future__ import annotations

from typing import Any

from agora_plugins.postgres.sinks._identifiers import (
    _postgres_type,
    _quote_identifier,
    _table_lookup_condition,
)


class PostgresSchemaColumnSyncRuntime:
    """Owns existing-column discovery and ALTER TABLE column sync."""

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    async def alter_table_add_columns(self, ctx: Any) -> bool:
        schema = self._adapter._schema
        if schema is None or not self._adapter._table_created:
            return True

        if not self._adapter._existing_columns:
            await self.load_existing_columns(ctx)

        new_columns = set(schema.columns.keys()) - self._adapter._existing_columns
        if not new_columns:
            return True

        conn = await self._adapter._get_connection()
        if conn is None:
            return False

        table_name = schema.table
        table_has_rows = await self.table_has_rows(conn, table_name)
        for col_name in sorted(new_columns):
            col = schema.columns[col_name]
            pg_type = _postgres_type(col.data_type)
            nullable = "NULL" if col.nullable or table_has_rows else "NOT NULL"
            alter_sql = (
                "ALTER TABLE "
                f"{_quote_identifier(table_name, allow_path=True, allow_quoted=self._adapter._allow_quoted_identifiers)} "
                "ADD COLUMN IF NOT EXISTS "
                f"{_quote_identifier(col_name, allow_quoted=self._adapter._allow_quoted_identifiers)} "
                f"{pg_type} {nullable}"
            )

            try:
                await self._adapter._prepare_schema_ddl_transaction(conn, table_name)
                async with conn.cursor() as cur:
                    await cur.execute(alter_sql)
                await conn.commit()
                ctx.log.info(
                    "postgres_column_added",
                    table=table_name,
                    column=col_name,
                    type=pg_type,
                    nullable=(nullable == "NULL"),
                    requested_nullable=col.nullable,
                    sink=self._adapter.sink_name,
                )
                if table_has_rows and not col.nullable:
                    ctx.log.warning(
                        "postgres_column_added_nullable_for_existing_rows",
                        table=table_name,
                        column=col_name,
                        message=(
                            "Added new non-null schema column as nullable because the table "
                            "already contains rows and no default/backfill value was provided."
                        ),
                        sink=self._adapter.sink_name,
                    )
                self._adapter._existing_columns.add(col_name)
            except Exception as exc:
                await conn.rollback()
                ctx.log.exception(
                    "postgres_add_column_failed",
                    table=table_name,
                    column=col_name,
                    error=str(exc),
                    sink=self._adapter.sink_name,
                )
                raise
        return True

    async def table_has_rows(self, conn: Any, table_name: str) -> bool:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM "
                    f"{_quote_identifier(table_name, allow_path=True, allow_quoted=self._adapter._allow_quoted_identifiers)} "
                    "LIMIT 1)"
                )
                result = await cur.fetchone()
            await conn.commit()
            return bool(result[0]) if result else False
        except Exception:
            await conn.rollback()
            raise

    async def load_existing_columns(self, ctx: Any) -> None:
        schema = self._adapter._schema
        if schema is None:
            return

        conn = await self._adapter._get_connection()
        if conn is None:
            return

        table_name = schema.table
        where_sql, where_params = _table_lookup_condition(
            table_name,
            allow_quoted=self._adapter._allow_quoted_identifiers,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE {where_sql}",
                    where_params,
                )
                rows = await cur.fetchall()
                await conn.commit()
                self._adapter._existing_columns = {row[0] for row in rows}
                ctx.log.debug(
                    "postgres_loaded_columns",
                    table=table_name,
                    columns=len(self._adapter._existing_columns),
                    sink=self._adapter.sink_name,
                )
        except Exception as exc:
            await conn.rollback()
            ctx.log.warning(
                "postgres_load_columns_failed",
                table=table_name,
                error=str(exc),
                sink=self._adapter.sink_name,
            )
