from __future__ import annotations

import asyncio
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
    PostgresSink,
    QuotedIdentifier,
    _postgres_type,
    _quote_identifier,
    _schema_advisory_lock_key,
    _table_lookup_condition,
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
        self.invalidate_target_columns_cache_calls = 0

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

    def invalidate_target_columns_cache(self) -> None:
        self.invalidate_target_columns_cache_calls += 1


class FakePreflightPostgresSink(FakePostgresSink):
    def __init__(self, conn: MagicMock | None = None) -> None:
        super().__init__(conn)
        self.call_order: list[str] = []

    def defer_upsert_constraint_preflight_until_schema_applied(self) -> None:
        self.call_order.append("defer")

    async def open(self) -> None:
        self.call_order.append("open")
        await super().open()

    async def validate_upsert_constraint(self) -> None:
        self.call_order.append("validate")


def _make_conn(
    *, table_exists: list[tuple[bool]], existing_columns: list[tuple[str]] | None = None
):
    fetchone_results = list(table_exists)

    async def _fetchone():
        if fetchone_results:
            return fetchone_results.pop(0)
        return (False,)

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(side_effect=_fetchone)
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


def test_quote_identifier_keeps_conservative_default() -> None:
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _quote_identifier("order-events")
    with pytest.raises(ValueError, match="allow_quoted_identifiers"):
        _quote_identifier('"order-events"')


def test_quote_identifier_supports_explicit_quoted_identifier_wrapper() -> None:
    assert _quote_identifier(QuotedIdentifier("order-events")) == '"order-events"'
    assert _quote_identifier(QuotedIdentifier('tenant "vip"')) == '"tenant ""vip"""'
    assert (
        _quote_identifier(
            QuotedIdentifier("tenant-schema", "order.events"),
            allow_path=True,
        )
        == '"tenant-schema"."order.events"'
    )


def test_quote_identifier_supports_valid_prequoted_strings_when_opted_in() -> None:
    assert (
        _quote_identifier(
            '"tenant-schema"."order.events"',
            allow_path=True,
            allow_quoted=True,
        )
        == '"tenant-schema"."order.events"'
    )
    assert (
        _quote_identifier(
            '"tenant ""vip"""',
            allow_quoted=True,
        )
        == '"tenant ""vip"""'
    )


def test_quote_identifier_rejects_malformed_prequoted_strings() -> None:
    for identifier in ('"tenant"suffix', '"tenant"."events"; DROP TABLE users; --'):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _quote_identifier(identifier, allow_path=True, allow_quoted=True)


def test_table_lookup_condition_unquotes_explicit_identifier_names() -> None:
    assert _table_lookup_condition(
        '"tenant-schema"."order.events"',
        allow_quoted=True,
    ) == (
        "table_schema = %s AND table_name = %s",
        ("tenant-schema", "order.events"),
    )
    assert _table_lookup_condition(
        QuotedIdentifier("tenant-schema", "order.events"),
        allow_quoted=True,
    ) == (
        "table_schema = %s AND table_name = %s",
        ("tenant-schema", "order.events"),
    )


def test_schema_advisory_lock_key_is_based_on_canonical_identifier_parts() -> None:
    raw_key = _schema_advisory_lock_key("public.users")
    quoted_key = _schema_advisory_lock_key('"public"."users"', allow_quoted=True)
    wrapper_key = _schema_advisory_lock_key(QuotedIdentifier("public", "users"))

    assert raw_key == quoted_key == wrapper_key


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
async def test_adapter_auto_create_adds_unique_constraint_for_sink_conflict_key() -> None:
    conn, cursor = _make_conn(table_exists=[(False,)])
    cursor.fetchall = AsyncMock(return_value=[(["id"],)])
    inner = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="public.users",
        row_mapper=lambda row: row,
        conflict_key="id",
    )
    inner._conn = conn  # type: ignore[attr-defined]
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

    create_sql = next(
        call[0][0]
        for call in cursor.execute.call_args_list
        if "CREATE TABLE IF NOT EXISTS" in call[0][0]
    )
    assert 'UNIQUE ("id")' in create_sql


@pytest.mark.asyncio
async def test_adapter_auto_create_rejects_missing_sink_conflict_key() -> None:
    conn, _cursor = _make_conn(table_exists=[(False,)])
    inner = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="public.users",
        row_mapper=lambda row: row,
        conflict_key="slug",
    )
    inner._conn = conn  # type: ignore[attr-defined]
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=False)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="public.users",
        columns={"id": Column("id", DataType.INTEGER, nullable=False)},
    )

    adapter.bind_context(ctx)
    with pytest.raises(ValueError, match="conflict_keys are missing"):
        await adapter.open()


@pytest.mark.asyncio
async def test_adapter_create_table_supports_opt_in_quoted_identifiers() -> None:
    conn, cursor = _make_conn(table_exists=[(False,)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(
        inner,
        auto_create=True,
        auto_alter=False,
        allow_quoted_identifiers=True,
    )
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table='"tenant-schema"."events-2026"',
        columns={
            '"Order ID"': Column('"Order ID"', DataType.STRING, nullable=False),
            "payload": Column("payload", DataType.JSON, nullable=True),
        },
    )

    adapter.bind_context(ctx)
    await adapter.open()

    lookup_params = cursor.execute.call_args_list[0][0][1]
    create_sql = cursor.execute.call_args_list[-1][0][0]
    assert lookup_params == ("tenant-schema", "events-2026")
    assert 'CREATE TABLE IF NOT EXISTS "tenant-schema"."events-2026"' in create_sql
    assert '"Order ID" TEXT NOT NULL' in create_sql
    assert '"payload" JSONB NULL' in create_sql


@pytest.mark.asyncio
async def test_adapter_create_table_sets_schema_lock_guards_before_ddl() -> None:
    conn, cursor = _make_conn(table_exists=[(False,)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=False)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="public.users",
        columns={"id": Column("id", DataType.INTEGER, nullable=False)},
    )

    adapter.bind_context(ctx)
    await adapter.open()

    executed = [call[0][0] for call in cursor.execute.call_args_list]
    create_index = next(index for index, sql in enumerate(executed) if "CREATE TABLE" in sql)
    assert executed[create_index - 2] == "SELECT set_config('lock_timeout', %s, true)"
    assert executed[create_index - 1] == "SELECT pg_advisory_xact_lock(%s)"
    assert cursor.execute.call_args_list[create_index - 2][0][1] == ("5000ms",)
    advisory_params = cursor.execute.call_args_list[create_index - 1][0][1]
    assert isinstance(advisory_params[0], int)
    assert advisory_params[0] > 0


@pytest.mark.asyncio
async def test_adapter_create_table_can_disable_schema_lock_guards() -> None:
    conn, cursor = _make_conn(table_exists=[(False,)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(
        inner,
        auto_create=True,
        auto_alter=False,
        schema_lock_timeout_ms=None,
        schema_advisory_lock=False,
    )
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER, nullable=False)},
    )

    adapter.bind_context(ctx)
    await adapter.open()

    executed = [call[0][0] for call in cursor.execute.call_args_list]
    assert "SELECT set_config('lock_timeout', %s, true)" not in executed
    assert "SELECT pg_advisory_xact_lock(%s)" not in executed


def test_adapter_rejects_invalid_schema_lock_timeout() -> None:
    with pytest.raises(ValueError, match="schema_lock_timeout_ms"):
        PostgresSchemaAdapter(FakePostgresSink(), schema_lock_timeout_ms=0)


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
    assert 'ADD COLUMN IF NOT EXISTS "name" TEXT NULL' in alter_sql[0]


@pytest.mark.asyncio
async def test_adapter_alter_table_sets_schema_lock_guards_before_ddl() -> None:
    conn, cursor = _make_conn(table_exists=[(True,), (False,)], existing_columns=[("id",)])
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

    executed = [call[0][0] for call in cursor.execute.call_args_list]
    alter_index = next(index for index, sql in enumerate(executed) if "ALTER TABLE" in sql)
    assert executed[alter_index - 2] == "SELECT set_config('lock_timeout', %s, true)"
    assert executed[alter_index - 1] == "SELECT pg_advisory_xact_lock(%s)"
    assert cursor.execute.call_args_list[alter_index - 2][0][1] == ("5000ms",)
    assert isinstance(cursor.execute.call_args_list[alter_index - 1][0][1][0], int)


@pytest.mark.asyncio
async def test_adapter_invalidates_target_column_cache_after_schema_apply() -> None:
    conn, _cursor = _make_conn(table_exists=[(True,)], existing_columns=[("id",)])
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
    await adapter.write({"id": 1, "name": "Alice"})

    assert inner.invalidate_target_columns_cache_calls == 1


@pytest.mark.asyncio
async def test_adapter_adds_new_non_null_column_as_nullable_when_table_has_rows() -> None:
    conn, cursor = _make_conn(table_exists=[(True,), (True,)], existing_columns=[("id",)])
    inner = FakePostgresSink(conn)
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=True)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="users",
        columns={
            "id": Column("id", DataType.INTEGER, nullable=False),
            "tenant_id": Column("tenant_id", DataType.STRING, nullable=False),
        },
    )

    adapter.bind_context(ctx)
    await adapter.open()

    alter_sql = [
        call[0][0] for call in cursor.execute.call_args_list if "ALTER TABLE" in call[0][0]
    ]
    assert alter_sql == ['ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "tenant_id" TEXT NULL']


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
    assert any(
        'ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "name" TEXT NULL' in sql
        for sql in executed_sql
    )
    assert inner.records == [{"id": 1}, {"id": 2, "name": "Alice"}]
    assert summary.records_written == 2


@pytest.mark.asyncio
async def test_adapter_serializes_concurrent_schema_application() -> None:
    inner = FakePostgresSink()
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=True)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER, nullable=False)},
    )
    create_calls = 0
    alter_calls = 0

    async def _create_table_if_not_exists(_ctx: PipelineContext) -> bool:
        nonlocal create_calls
        create_calls += 1
        await asyncio.sleep(0)
        return True

    async def _alter_table_add_columns(_ctx: PipelineContext) -> bool:
        nonlocal alter_calls
        alter_calls += 1
        await asyncio.sleep(0)
        return True

    adapter._create_table_if_not_exists = AsyncMock(side_effect=_create_table_if_not_exists)  # type: ignore[method-assign]
    adapter._alter_table_add_columns = AsyncMock(side_effect=_alter_table_add_columns)  # type: ignore[method-assign]

    adapter.bind_context(ctx)
    await asyncio.gather(adapter.write({"id": 1}), adapter.write({"id": 2}))

    assert create_calls == 1
    assert alter_calls == 1
    assert inner.records == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_adapter_retries_schema_application_after_failed_alter() -> None:
    conn, _cursor = _make_conn(table_exists=[(True,), (True,)], existing_columns=[("id",)])
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

    alter_attempts = 0

    async def _create_table_if_not_exists(_ctx: PipelineContext) -> bool:
        adapter._table_created = True
        adapter._existing_columns = {"id"}
        return True

    async def _alter_table_add_columns(_ctx: PipelineContext) -> bool:
        nonlocal alter_attempts
        alter_attempts += 1
        if alter_attempts == 1:
            raise RuntimeError("ddl failed once")
        adapter._existing_columns.add("name")
        return True

    adapter._create_table_if_not_exists = AsyncMock(side_effect=_create_table_if_not_exists)  # type: ignore[method-assign]
    adapter._alter_table_add_columns = AsyncMock(side_effect=_alter_table_add_columns)  # type: ignore[method-assign]

    adapter.bind_context(ctx)
    with pytest.raises(RuntimeError, match="ddl failed once"):
        await adapter.open()

    await adapter.write({"id": 1, "name": "Alice"})

    assert alter_attempts == 2
    assert inner.records == [{"id": 1, "name": "Alice"}]


@pytest.mark.asyncio
async def test_adapter_defers_upsert_constraint_validation_until_schema_is_applied() -> None:
    inner = FakePreflightPostgresSink()
    adapter = PostgresSchemaAdapter(inner, auto_create=True, auto_alter=True)
    ctx = _make_ctx()
    ctx.extras["schema"] = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER, nullable=False)},
    )

    async def _ensure_schema_applied() -> None:
        inner.call_order.append("schema")

    adapter._ensure_schema_applied = AsyncMock(side_effect=_ensure_schema_applied)  # type: ignore[method-assign]

    adapter.bind_context(ctx)
    await adapter.open()

    assert inner.call_order == ["defer", "open", "schema", "validate"]
