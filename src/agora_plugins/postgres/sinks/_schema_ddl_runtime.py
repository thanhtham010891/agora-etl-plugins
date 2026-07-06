"""DDL helpers for PostgreSQL schema adapter."""

from __future__ import annotations

from typing import Any

from agora_plugins.postgres.sinks._identifiers import (
    _postgres_type,
    _quote_identifier,
    _schema_advisory_lock_key,
    _table_lookup_condition,
)


class PostgresSchemaDDLRuntime:
    """Owns create-table and DDL lock orchestration for ``PostgresSchemaAdapter``."""

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    async def prepare_schema_ddl_transaction(self, conn: Any, table_name: str) -> None:
        if (
            self._adapter._schema_lock_timeout_ms is None
            and not self._adapter._schema_advisory_lock
        ):
            return
        async with conn.cursor() as cur:
            if self._adapter._schema_lock_timeout_ms is not None:
                await cur.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (f"{self._adapter._schema_lock_timeout_ms}ms",),
                )
            if self._adapter._schema_advisory_lock:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (
                        _schema_advisory_lock_key(
                            table_name,
                            allow_quoted=self._adapter._allow_quoted_identifiers,
                        ),
                    ),
                )

    async def create_table_if_not_exists(self, ctx: Any) -> bool:
        schema = self._adapter._schema
        if schema is None:
            return True

        conn = await self._adapter._get_connection()
        if conn is None:
            ctx.log.warning(
                "postgres_schema_adapter_no_connection",
                message="Cannot get connection from wrapped sink",
                sink=self._adapter.sink_name,
            )
            return False

        table_name = schema.table
        try:
            where_sql, where_params = _table_lookup_condition(
                table_name,
                allow_quoted=self._adapter._allow_quoted_identifiers,
            )
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE {where_sql})",
                    where_params,
                )
                result = await cur.fetchone()
                table_exists = result[0] if result else False

            if table_exists:
                ctx.log.info(
                    "postgres_table_exists", table=table_name, sink=self._adapter.sink_name
                )
                self._adapter._table_created = True
                await conn.commit()
                await self._adapter._load_existing_columns(ctx)
                return True
        except Exception:
            await conn.rollback()
            raise

        columns_sql: list[str] = []
        for col_name in sorted(schema.columns.keys()):
            col = schema.columns[col_name]
            pg_type = _postgres_type(col.data_type)
            nullable = "NULL" if col.nullable else "NOT NULL"
            columns_sql.append(
                f"{_quote_identifier(col_name, allow_quoted=self._adapter._allow_quoted_identifiers)} "
                f"{pg_type} {nullable}"
            )
        conflict_keys = self._adapter._conflict_keys_for_create()
        if conflict_keys:
            missing_conflict_keys = [
                str(key) for key in conflict_keys if str(key) not in schema.columns
            ]
            if missing_conflict_keys:
                raise ValueError(
                    "PostgresSchemaAdapter cannot auto-create a table for a sink whose "
                    f"conflict_keys are missing from the schema: {missing_conflict_keys!r}."
                )
            constraint_cols = ", ".join(
                _quote_identifier(
                    key,
                    allow_quoted=self._adapter._allow_quoted_identifiers,
                )
                for key in conflict_keys
            )
            columns_sql.append(f"UNIQUE ({constraint_cols})")

        create_sql = (
            "CREATE TABLE IF NOT EXISTS "
            f"{_quote_identifier(table_name, allow_path=True, allow_quoted=self._adapter._allow_quoted_identifiers)} (\n"
            f"  {', '.join(columns_sql)}\n"
            f")"
        )

        try:
            await self.prepare_schema_ddl_transaction(conn, table_name)
            async with conn.cursor() as cur:
                await cur.execute(create_sql)
            await conn.commit()
            ctx.log.info(
                "postgres_table_created",
                table=table_name,
                columns=len(schema.columns),
                sink=self._adapter.sink_name,
            )
            self._adapter._table_created = True
            self._adapter._existing_columns = set(schema.columns.keys())
            return True
        except Exception as exc:
            await conn.rollback()
            ctx.log.exception(
                "postgres_create_table_failed",
                table=table_name,
                error=str(exc),
                sink=self._adapter.sink_name,
            )
            raise
