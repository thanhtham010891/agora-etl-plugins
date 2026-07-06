"""Open-time lifecycle and upsert preflight helpers for PostgreSQL sinks."""

from __future__ import annotations

import logstruct

from agora_plugins.postgres.sinks._identifiers import _identifier_parts, _table_catalog_condition

logger = logstruct.getLogger("agora_plugins.postgres.sinks.postgres")


class PostgresLifecycleRuntime:
    """Owns sink open-time lifecycle and upsert-constraint preflight orchestration."""

    def __init__(
        self,
        *,
        table: str | object,
        conflict_keys: tuple[str | object, ...],
        upsert: bool,
        allow_quoted_identifiers: bool,
        current_poison_sink,
        should_defer_upsert_constraint_preflight,
        upsert_constraint_preflight_complete,
        set_upsert_constraint_preflight_complete,
        write_connection,
    ) -> None:
        self._table = table
        self._conflict_keys = conflict_keys
        self._upsert = upsert
        self._allow_quoted_identifiers = allow_quoted_identifiers
        self._current_poison_sink = current_poison_sink
        self._should_defer_upsert_constraint_preflight = should_defer_upsert_constraint_preflight
        self._upsert_constraint_preflight_complete = upsert_constraint_preflight_complete
        self._set_upsert_constraint_preflight_complete = set_upsert_constraint_preflight_complete
        self._write_connection = write_connection

    async def open(self) -> None:
        poison_opened = False
        try:
            poison_sink = self._current_poison_sink()
            if poison_sink is not None:
                await poison_sink.open()
                poison_opened = True
            if self._upsert and not self._should_defer_upsert_constraint_preflight():
                await self.validate_upsert_constraint()
        except Exception:
            if poison_opened:
                poison_sink = self._current_poison_sink()
                if poison_sink is not None:
                    try:
                        await poison_sink.close()
                    except Exception:
                        logger.exception("postgres_open_cleanup_error", table=self._table)
            raise

    async def validate_upsert_constraint(self) -> None:
        if not self._upsert or self._upsert_constraint_preflight_complete():
            return
        async with self._write_connection() as conn:
            has_constraint = await self.has_matching_upsert_constraint(conn)
            await conn.rollback()
            if not has_constraint:
                conflict_keys = [str(key) for key in self._conflict_keys]
                raise ValueError(
                    "PostgresSink upsert requires a PRIMARY KEY or UNIQUE constraint "
                    "whose key columns exactly match conflict_keys. "
                    f"table={self._table!r} conflict_keys={conflict_keys!r}. "
                    "Create the constraint or wrap the sink in PostgresSchemaAdapter."
                )
        self._set_upsert_constraint_preflight_complete(True)

    async def has_matching_upsert_constraint(self, conn) -> bool:
        where_sql, where_params = _table_catalog_condition(
            self._table,
            namespace_alias="ns",
            table_alias="tbl",
            allow_quoted=self._allow_quoted_identifiers,
        )
        expected = sorted(
            _identifier_parts(
                key,
                allow_quoted=self._allow_quoted_identifiers,
            )[0]
            for key in self._conflict_keys
        )
        sql = f"""
            SELECT array_agg(att.attname ORDER BY att.attname)
            FROM pg_index idx
            JOIN pg_class tbl ON tbl.oid = idx.indrelid
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            JOIN unnest(idx.indkey) WITH ORDINALITY AS keycols(attnum, ordinality)
                ON keycols.ordinality <= idx.indnkeyatts
            JOIN pg_attribute att
                ON att.attrelid = tbl.oid
                AND att.attnum = keycols.attnum
            WHERE idx.indisunique
              AND idx.indpred IS NULL
              AND idx.indexprs IS NULL
              AND {where_sql}
            GROUP BY idx.indexrelid, idx.indnkeyatts
            HAVING count(*) = %s
        """
        async with conn.cursor() as cur:
            await cur.execute(sql, (*where_params, len(expected)))
            rows = await cur.fetchall()
        for row in rows:
            columns = row[0] if row else None
            if sorted(str(column) for column in columns or []) == expected:
                return True
        return False
