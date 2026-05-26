from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from agora.core import IterableSource, Pipeline
from agora.core.context import PipelineContext
from agora.core.metrics import PipelineMetrics
from agora.core.sink import BaseSink
from agora.schema.middleware import SchemaMiddleware
from agora.schema.store import InMemorySchemaStore
from agora.schema.types import Column, DataType, Schema, SchemaContract

from agora_plugins.postgres.sinks.postgres import (
    PostgresSchemaAdapter,
    _postgres_type,
    _quote_identifier,
)


def _make_ctx() -> PipelineContext:
    return PipelineContext(pipeline_id="test_pipeline", metrics=PipelineMetrics())


class FakePostgresSink(BaseSink[dict[str, object]]):
    sink_name = "fake_postgres"

    def __init__(self, conn: MagicMock | None = None) -> None:
        self._conn = conn
        self.records: list[dict[str, object]] = []
        self.open_calls = 0
        self.flush_calls = 0
        self.close_calls = 0

    async def open(self) -> None:
        self.open_calls += 1

    async def write(self, record: dict[str, object]) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        self.flush_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def connection(self):
        return self._conn

    async def _get_conn(self):
        return self._conn


def _make_conn(
    *, table_exists: list[tuple[bool]], existing_columns: list[tuple[str]] | None = None
):
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(side_effect=table_exists)
    cursor.fetchall = AsyncMock(return_value=existing_columns or [])
    cursor.__aenter__ = AsyncMock(return_value=cursor)
    cursor.__aexit__ = AsyncMock()

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    return conn, cursor


def test_postgres_type_mapping() -> None:
    assert _postgres_type(DataType.STRING) == "TEXT"
    assert _postgres_type(DataType.INTEGER) == "BIGINT"
    assert _postgres_type(DataType.FLOAT) == "DOUBLE PRECISION"
    assert _postgres_type(DataType.BOOLEAN) == "BOOLEAN"
    assert _postgres_type(DataType.TIMESTAMP) == "TIMESTAMPTZ"
    assert _postgres_type(DataType.JSON) == "JSONB"
    assert _postgres_type(DataType.BYTES) == "BYTEA"
    assert _postgres_type(DataType.NULL) == "TEXT"


def test_quote_identifier_supports_schema_paths() -> None:
    assert _quote_identifier("users") == '"users"'
    assert _quote_identifier("public.users", allow_path=True) == '"public"."users"'


@pytest.mark.asyncio
async def test_adapter_no_schema_in_context() -> None:
    inner = FakePostgresSink()
    adapter = PostgresSchemaAdapter(inner)
    ctx = _make_ctx()

    adapter.bind_context(ctx)
    await adapter.open()

    assert inner.open_calls == 1


@pytest.mark.asyncio
async def test_adapter_passthrough_write() -> None:
    inner = FakePostgresSink()
    adapter = PostgresSchemaAdapter(inner)
    ctx = _make_ctx()

    adapter.bind_context(ctx)
    await adapter.open()
    await adapter.write({"id": 1})
    await adapter.flush()
    await adapter.close()

    assert inner.records == [{"id": 1}]
    assert inner.flush_calls == 1
    assert inner.close_calls == 1


@pytest.mark.asyncio
async def test_adapter_create_table_if_not_exists() -> None:
    conn, cursor = _make_conn(table_exists=[(False,)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=False)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="public.users",
        columns={
            "id": Column("id", DataType.INTEGER, nullable=False),
            "name": Column("name", DataType.STRING, nullable=True),
        },
    )

    adapter.bind_context(ctx)
    await adapter.open()

    create_sql = cursor.execute.call_args_list[-1][0][0]
    assert "CREATE TABLE IF NOT EXISTS" in create_sql
    assert '"public"."users"' in create_sql
    assert '"id" BIGINT NOT NULL' in create_sql
    assert '"name" TEXT NULL' in create_sql


@pytest.mark.asyncio
async def test_adapter_alter_table_add_columns() -> None:
    conn, cursor = _make_conn(table_exists=[(True,)], existing_columns=[("id",)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=True)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="users",
        columns={
            "id": Column("id", DataType.INTEGER, nullable=False),
            "name": Column("name", DataType.STRING, nullable=True),
        },
    )

    adapter.bind_context(ctx)
    await adapter.open()

    alter_sql = [
        call[0][0] for call in cursor.execute.call_args_list if "ALTER TABLE" in call[0][0]
    ]
    assert len(alter_sql) == 1
    assert 'ADD COLUMN "name" TEXT NULL' in alter_sql[0]


@pytest.mark.asyncio
async def test_adapter_schema_introspection_uses_schema_qualified_lookup() -> None:
    conn, cursor = _make_conn(table_exists=[(True,)], existing_columns=[("id",)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=True)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="analytics.users",
        columns={
            "id": Column("id", DataType.INTEGER, nullable=False),
            "name": Column("name", DataType.STRING, nullable=True),
        },
    )

    adapter.bind_context(ctx)
    await adapter.open()

    first_query, first_params = cursor.execute.call_args_list[0][0]
    second_query, second_params = cursor.execute.call_args_list[1][0]

    assert "table_schema = %s AND table_name = %s" in first_query
    assert first_params == ("analytics", "users")
    assert "table_schema = %s AND table_name = %s" in second_query
    assert second_params == ("analytics", "users")


@pytest.mark.asyncio
async def test_adapter_auto_create_disabled() -> None:
    conn, cursor = _make_conn(table_exists=[])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(inner, auto_create=False, auto_alter=False)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
    )

    adapter.bind_context(ctx)
    await adapter.open()

    assert cursor.execute.call_args_list == []


@pytest.mark.asyncio
async def test_adapter_applies_schema_during_same_pipeline_run() -> None:
    conn, cursor = _make_conn(table_exists=[(False,), (True,)], existing_columns=[("id",)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=True)
    store = InMemorySchemaStore()

    pipeline = (
        Pipeline(
            IterableSource(
                [
                    {"id": 1},
                    {"id": 2, "name": "Alice"},
                ]
            ),
            id="test_pipeline",
        )
        .pipe(SchemaMiddleware(table="users", store=store, contract=SchemaContract.EVOLVE))
        .build(adapter)
    )

    summary = await pipeline.run()

    executed_sql = [call[0][0] for call in cursor.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS" in sql for sql in executed_sql)
    assert any('ALTER TABLE "users" ADD COLUMN "name" TEXT NULL' in sql for sql in executed_sql)
    assert inner.records == [{"id": 1}, {"id": 2, "name": "Alice"}]
    assert summary.records_written == 2
