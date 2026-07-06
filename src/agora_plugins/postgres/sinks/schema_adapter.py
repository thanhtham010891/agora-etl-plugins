from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.core.sink import BaseSink

from agora_plugins.postgres.sinks._identifiers import QuotedIdentifier
from agora_plugins.postgres.sinks._schema_column_sync_runtime import (
    PostgresSchemaColumnSyncRuntime,
)
from agora_plugins.postgres.sinks._schema_ddl_runtime import PostgresSchemaDDLRuntime

if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.schema.types import Schema

T = TypeVar("T")


class PostgresSchemaAdapter(BaseSink[T], Generic[T]):
    """Schema-aware wrapper for PostgresSink."""

    sink_name = "postgres_schema_adapter"

    def __init__(
        self,
        sink: BaseSink[T],
        auto_create: bool = True,
        auto_alter: bool = True,
        allow_quoted_identifiers: bool = False,
        schema_lock_timeout_ms: int | None = 5_000,
        schema_advisory_lock: bool = True,
    ) -> None:
        if schema_lock_timeout_ms is not None and schema_lock_timeout_ms <= 0:
            raise ValueError("schema_lock_timeout_ms must be > 0 when provided.")
        self._sink = sink
        self._auto_create = auto_create
        self._auto_alter = auto_alter
        self._allow_quoted_identifiers = allow_quoted_identifiers
        self._schema_lock_timeout_ms = schema_lock_timeout_ms
        self._schema_advisory_lock = schema_advisory_lock
        self._schema_apply_lock = asyncio.Lock()
        self._ctx: PipelineContext | None = None
        self._schema: Schema | None = None
        self._table_created = False
        self._existing_columns: set[str] = set()
        self._applied_schema_hash: str | None = None
        self._ddl_runtime = PostgresSchemaDDLRuntime(self)
        self._column_sync_runtime = PostgresSchemaColumnSyncRuntime(self)

    def _conflict_keys_for_create(self) -> list[str | QuotedIdentifier]:
        keys = getattr(self._sink, "_conflict_keys", None)
        if keys is None:
            return []
        if isinstance(keys, (str, QuotedIdentifier)):
            return [keys]
        if isinstance(keys, list | tuple):
            return list(keys)
        return []

    def bind_context(self, ctx: PipelineContext) -> None:
        self._ctx = ctx
        bind_sink = getattr(self._sink, "bind_context", None)
        if callable(bind_sink):
            bind_sink(ctx)

    async def open(self) -> None:
        defer_upsert_preflight = getattr(
            self._sink,
            "defer_upsert_constraint_preflight_until_schema_applied",
            None,
        )
        if callable(defer_upsert_preflight):
            defer_upsert_preflight()
        await self._sink.open()
        await self._ensure_schema_applied()
        validate_upsert_constraint = getattr(self._sink, "validate_upsert_constraint", None)
        if callable(validate_upsert_constraint):
            await validate_upsert_constraint()

    async def write(self, record: T) -> None:
        await self._ensure_schema_applied()
        await self._sink.write(record)

    async def write_batch(self, records: list[T]) -> None:
        await self._ensure_schema_applied()
        await self._sink.write_batch(records)

    async def flush(self) -> None:
        await self._sink.flush()

    async def close(self) -> None:
        await self._sink.close()

    async def _ensure_schema_applied(self) -> None:
        ctx = self._ctx
        if ctx is None:
            return

        schema = ctx.extras.get("schema")
        if schema is None:
            ctx.log.warning(
                "postgres_schema_adapter_no_schema",
                message="No schema found in ctx.extras — SchemaMiddleware not used?",
                sink=self.sink_name,
            )
            return

        if self._applied_schema_hash == schema.hash:
            return

        async with self._schema_apply_lock:
            if self._applied_schema_hash == schema.hash:
                return

            self._schema = schema

            create_applied = True
            if self._auto_create:
                create_applied = await self._create_table_if_not_exists(ctx)
            alter_applied = True
            if self._auto_alter:
                alter_applied = await self._alter_table_add_columns(ctx)

            if create_applied and alter_applied:
                self._invalidate_wrapped_sink_target_columns_cache()
                self._applied_schema_hash = schema.hash

    def _invalidate_wrapped_sink_target_columns_cache(self) -> None:
        invalidate = getattr(self._sink, "invalidate_target_columns_cache", None)
        if callable(invalidate):
            invalidate()

    async def _prepare_schema_ddl_transaction(self, conn: Any, table_name: str) -> None:
        await self._ddl_runtime.prepare_schema_ddl_transaction(conn, table_name)

    async def _create_table_if_not_exists(self, ctx: PipelineContext) -> bool:
        return await self._ddl_runtime.create_table_if_not_exists(ctx)

    async def _alter_table_add_columns(self, ctx: PipelineContext) -> bool:
        return await self._column_sync_runtime.alter_table_add_columns(ctx)

    async def _table_has_rows(self, conn: Any, table_name: str) -> bool:
        return await self._column_sync_runtime.table_has_rows(conn, table_name)

    async def _load_existing_columns(self, ctx: PipelineContext) -> None:
        await self._column_sync_runtime.load_existing_columns(ctx)

    async def _get_connection(self) -> Any:
        if hasattr(self._sink, "connection"):
            return await self._sink.connection()
        raise TypeError(
            f"PostgresSchemaAdapter requires a sink with a public `connection()` method. "
            f"Got {type(self._sink).__name__!r}. Use PostgresSink or a compatible sink."
        )


__all__ = ["PostgresSchemaAdapter"]
