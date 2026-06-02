from __future__ import annotations

from typing import Any

import pytest
from agora.core.retry import RetryPolicy

from agora_plugins.postgres.sinks import PostgresSink


def _make_sink(table: str = "events") -> PostgresSink[dict]:
    return PostgresSink(
        dsn="postgresql://example.invalid/db",
        table=table,
        row_mapper=lambda row: row,
        conflict_key="slug",
    )


def test_build_upsert_sql_quotes_identifiers() -> None:
    sink = _make_sink(table="public.events")

    sql = sink._build_upsert_sql(["slug", "display_name"])

    assert 'INSERT INTO "public"."events"' in sql
    assert '("slug", "display_name")' in sql
    assert 'ON CONFLICT ("slug")' in sql
    assert '"display_name" = EXCLUDED."display_name"' in sql


def test_build_upsert_sql_rejects_invalid_table_identifier() -> None:
    sink = _make_sink(table="events; DROP TABLE users; --")

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        sink._build_upsert_sql(["slug", "display_name"])


def test_build_upsert_sql_rejects_invalid_column_identifier() -> None:
    sink = _make_sink()

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        sink._build_upsert_sql(["slug", 'name") VALUES (1); --'])


def test_copy_mode_requires_insert_only() -> None:
    with pytest.raises(ValueError, match="only supported when upsert=False"):
        PostgresSink(
            dsn="postgresql://example.invalid/db",
            table="events",
            row_mapper=lambda row: row,
            conflict_key="slug",
            upsert=True,
            insert_mode="copy",
        )


def test_build_copy_merge_sql_uses_staging_select_and_conflict_update() -> None:
    sink = _make_sink(table="public.events")

    sql = sink._build_copy_merge_sql(["slug", "display_name"], "agora_stage_abcd")

    assert 'INSERT INTO "public"."events" ("slug", "display_name")' in sql
    assert 'SELECT "agora_stage_abcd"."slug", "agora_stage_abcd"."display_name"' in sql
    assert 'FROM "agora_stage_abcd"' in sql
    assert 'ON CONFLICT ("slug") DO UPDATE SET "display_name" = EXCLUDED."display_name"' in sql


def test_statement_row_limit_uses_parameter_budget() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        max_parameters_per_statement=5,
    )

    assert sink._statement_row_limit(2) == 2
    assert sink._statement_row_limit(3) == 1


@pytest.mark.asyncio
async def test_flush_uses_single_multirow_execute_for_buffered_rows() -> None:
    sink = _make_sink()
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b", "display_name": "B"},
    ]

    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[Any]]] = []

        async def execute(self, sql: str, params: list[Any]) -> None:
            self.calls.append((sql, params))

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            self.rollback_calls += 1

    conn = FakeConn()
    sink._conn = conn  # type: ignore[attr-defined]

    await sink.flush()

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert len(conn.cursor_obj.calls) == 1
    sql, params = conn.cursor_obj.calls[0]
    assert 'INSERT INTO "events"' in sql
    assert sql.count("(%s, %s)") == 2
    assert params == ["a", "A", "b", "B"]


@pytest.mark.asyncio
async def test_flush_uses_copy_for_insert_only_rows() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        upsert=False,
        insert_mode="copy",
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b", "display_name": "B"},
    ]

    class FakeCopy:
        def __init__(self) -> None:
            self.rows: list[list[Any]] = []

        async def write_row(self, row: list[Any]) -> None:
            self.rows.append(list(row))

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeCursor:
        def __init__(self) -> None:
            self.copy_sql: str | None = None
            self.copy_obj = FakeCopy()

        def copy(self, sql: str) -> FakeCopy:
            self.copy_sql = sql
            return self.copy_obj

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            self.rollback_calls += 1

    conn = FakeConn()
    sink._conn = conn  # type: ignore[attr-defined]

    await sink.flush()

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert conn.cursor_obj.copy_sql == 'COPY "events" ("slug", "display_name") FROM STDIN'
    assert conn.cursor_obj.copy_obj.rows == [["a", "A"], ["b", "B"]]


@pytest.mark.asyncio
async def test_flush_uses_copy_merge_for_upsert_rows() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        insert_mode="copy_merge",
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b", "display_name": "B"},
    ]
    sink._build_stage_table_name = lambda: "agora_stage_fixed"  # type: ignore[method-assign]

    class FakeCopy:
        def __init__(self) -> None:
            self.rows: list[list[Any]] = []

        async def write_row(self, row: list[Any]) -> None:
            self.rows.append(list(row))

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeCursor:
        def __init__(self) -> None:
            self.execute_calls: list[str] = []
            self.copy_sql: str | None = None
            self.copy_obj = FakeCopy()

        async def execute(self, sql: str, params: list[Any] | None = None) -> None:
            del params
            self.execute_calls.append(sql)

        def copy(self, sql: str) -> FakeCopy:
            self.copy_sql = sql
            return self.copy_obj

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self) -> None:
            self.cursors: list[FakeCursor] = []
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self) -> FakeCursor:
            cursor = FakeCursor()
            self.cursors.append(cursor)
            return cursor

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            self.rollback_calls += 1

    conn = FakeConn()
    sink._conn = conn  # type: ignore[attr-defined]

    await sink.flush()

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert conn.cursors[0].execute_calls == [
        'CREATE TEMP TABLE "agora_stage_fixed" (LIKE "events" INCLUDING DEFAULTS) ON COMMIT DROP'
    ]
    assert (
        conn.cursors[1].copy_sql == 'COPY "agora_stage_fixed" ("slug", "display_name") FROM STDIN'
    )
    assert conn.cursors[1].copy_obj.rows == [["a", "A"], ["b", "B"]]
    assert conn.cursors[2].execute_calls == [
        'INSERT INTO "events" ("slug", "display_name") '
        'SELECT "agora_stage_fixed"."slug", "agora_stage_fixed"."display_name" FROM "agora_stage_fixed" '
        'ON CONFLICT ("slug") DO UPDATE SET "display_name" = EXCLUDED."display_name"'
    ]


@pytest.mark.asyncio
async def test_flush_chunks_sql_statements_when_parameter_budget_is_small() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        max_parameters_per_statement=4,
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b", "display_name": "B"},
        {"slug": "c", "display_name": "C"},
    ]

    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[Any]]] = []

        async def execute(self, sql: str, params: list[Any]) -> None:
            self.calls.append((sql, params))

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            self.rollback_calls += 1

    conn = FakeConn()
    sink._conn = conn  # type: ignore[attr-defined]

    await sink.flush()

    assert conn.commit_calls == 1
    assert len(conn.cursor_obj.calls) == 2
    assert conn.cursor_obj.calls[0][1] == ["a", "A", "b", "B"]
    assert conn.cursor_obj.calls[1][1] == ["c", "C"]


@pytest.mark.asyncio
async def test_write_batch_defers_flush_until_internal_batch_size_is_reached() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=3,
    )

    flush_calls = 0

    async def _fake_flush() -> None:
        nonlocal flush_calls
        flush_calls += 1

    sink.flush = _fake_flush  # type: ignore[method-assign]

    await sink.write_batch(
        [
            {"slug": "a", "display_name": "A"},
            {"slug": "b", "display_name": "B"},
        ]
    )
    assert flush_calls == 0
    assert sink._buffer == [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b", "display_name": "B"},
    ]

    await sink.write_batch([{"slug": "c", "display_name": "C"}])
    assert flush_calls == 1


@pytest.mark.asyncio
async def test_flush_keeps_buffer_when_database_write_fails() -> None:
    sink = _make_sink()
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b", "display_name": "B"},
    ]

    class FakeCursor:
        async def execute(self, sql: str, params: list[Any]) -> None:
            raise RuntimeError("db unavailable")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self) -> None:
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            self.rollback_calls += 1

    conn = FakeConn()
    sink._conn = conn  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="db unavailable"):
        await sink.flush()

    assert conn.commit_calls == 0
    assert conn.rollback_calls == 1
    assert sink._buffer == [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b", "display_name": "B"},
    ]


@pytest.mark.asyncio
async def test_flush_retries_transient_database_errors() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        retry_policy=RetryPolicy[Any](
            max_attempts=2,
            initial_backoff_s=0.0,
            retry_exceptions=(RuntimeError,),
        ),
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
    ]

    class FakeCursor:
        def __init__(self) -> None:
            self.execute_calls = 0

        async def execute(self, sql: str, params: list[Any]) -> None:
            self.execute_calls += 1
            if self.execute_calls == 1:
                raise RuntimeError("transient db error")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            self.rollback_calls += 1

    conn = FakeConn()

    async def _fake_get_conn():
        sink._conn = conn  # type: ignore[attr-defined]
        return conn

    sink._get_conn = _fake_get_conn  # type: ignore[method-assign]

    await sink.flush()

    assert conn.cursor_obj.execute_calls == 2
    assert conn.rollback_calls == 1
    assert conn.commit_calls == 1
    assert sink._buffer == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_flush_reuses_internal_write_pool_connection_across_flushes() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        pool_size=2,
        retry_policy=RetryPolicy[Any](max_attempts=1),
    )

    class FakeCursor:
        async def execute(self, sql: str, params: list[Any]) -> None:
            del sql, params

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self, name: str) -> None:
            self.name = name
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        async def commit(self) -> None:
            self.commit_calls += 1

        async def rollback(self) -> None:
            self.rollback_calls += 1

        async def close(self) -> None:
            return None

    created: list[FakeConn] = []

    async def _fake_create_connection():
        conn = FakeConn(f"conn-{len(created)}")
        created.append(conn)
        return conn

    sink._create_connection = _fake_create_connection  # type: ignore[method-assign]
    sink._buffer = [{"slug": "a", "display_name": "A"}]  # type: ignore[attr-defined]
    await sink.flush()
    sink._buffer = [{"slug": "b", "display_name": "B"}]  # type: ignore[attr-defined]
    await sink.flush()

    assert len(created) == 1
    assert created[0].commit_calls == 2


def test_flatten_rows_accepts_mismatched_column_order_with_same_columns() -> None:
    sink = _make_sink()

    params = sink._flatten_rows(
        [
            {"slug": "a", "display_name": "A"},
            {"display_name": "B", "slug": "b"},
        ],
        ["slug", "display_name"],
    )

    assert params == ["a", "A", "b", "B"]
