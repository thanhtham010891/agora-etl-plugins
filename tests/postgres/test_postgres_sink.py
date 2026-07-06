from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from agora.core.failures import PoisonRecordClassification, PoisonRecordInfo
from agora.core.retry import RetryPolicy

from agora_plugins.postgres import (
    PostgresAuthConfig,
    PostgresConnectionConfig,
    PostgresTLSConfig,
)
from agora_plugins.postgres.sinks import (
    PostgresPoisonRecordClassification,
    PostgresSink,
    PostgresSinkWriteError,
    PostgresWriteSafetyPolicy,
    QuotedIdentifier,
)
from agora_plugins.postgres.sinks import postgres as sink_module


def _make_sink(table: str = "events") -> PostgresSink[dict]:
    return PostgresSink(
        dsn="postgresql://example.invalid/db",
        table=table,
        row_mapper=lambda row: row,
        conflict_key="slug",
    )


class _CollectDLQSink:
    def __init__(self) -> None:
        self.records: list[Any] = []
        self.open_calls = 0
        self.close_calls = 0

    async def open(self) -> None:
        self.open_calls += 1

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def write_batch(self, records: list[Any]) -> None:
        self.records.extend(records)

    async def close(self) -> None:
        self.close_calls += 1


class _ConstraintCursor:
    def __init__(self, rows: list[tuple[list[str]]]) -> None:
        self.rows = rows
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self.execute_calls.append((sql, params))

    async def fetchall(self) -> list[tuple[list[str]]]:
        return self.rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _ConstraintConn:
    def __init__(self, rows: list[tuple[list[str]]]) -> None:
        self.cursor_obj = _ConstraintCursor(rows)
        self.close_calls = 0
        self.rollback_calls = 0

    def cursor(self) -> _ConstraintCursor:
        return self.cursor_obj

    async def rollback(self) -> None:
        self.rollback_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


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


def test_build_upsert_sql_supports_explicit_quoted_identifiers() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table=QuotedIdentifier("tenant-schema", "events-2026"),
        row_mapper=lambda row: row,
        conflict_key=QuotedIdentifier("Order ID"),
    )

    sql = sink._build_upsert_sql([QuotedIdentifier("Order ID"), "payload"])

    assert 'INSERT INTO "tenant-schema"."events-2026"' in sql
    assert '("Order ID", "payload")' in sql
    assert 'ON CONFLICT ("Order ID")' in sql
    assert '"payload" = EXCLUDED."payload"' in sql


def test_build_upsert_sql_supports_prequoted_identifiers_only_when_opted_in() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table='"tenant-schema"."events-2026"',
        row_mapper=lambda row: row,
        conflict_key='"Order ID"',
        allow_quoted_identifiers=True,
    )

    sql = sink._build_upsert_sql(['"Order ID"', "payload"])

    assert 'INSERT INTO "tenant-schema"."events-2026"' in sql
    assert 'ON CONFLICT ("Order ID")' in sql


def test_build_upsert_sql_rejects_prequoted_identifiers_by_default() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table='"tenant-schema"."events-2026"',
        row_mapper=lambda row: row,
        conflict_key='"Order ID"',
    )

    with pytest.raises(ValueError, match="allow_quoted_identifiers"):
        sink._build_upsert_sql(['"Order ID"', "payload"])


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


def test_postgres_sink_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        PostgresSink(
            dsn="postgresql://example.invalid/db",
            table="events",
            row_mapper=lambda row: row,
            conflict_key="slug",
            batch_size=0,
        )


@pytest.mark.asyncio
async def test_open_validates_upsert_constraint_before_first_flush() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
    )
    conn = _ConstraintConn([])
    sink._conn = conn  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="conflict_keys"):
        await sink.open()

    assert conn.cursor_obj.execute_calls
    assert conn.rollback_calls == 1
    assert conn.close_calls == 1


@pytest.mark.asyncio
async def test_open_closes_poison_sink_when_upsert_preflight_fails() -> None:
    poison_sink = _CollectDLQSink()
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        poison_record_sink=poison_sink,
    )
    sink._conn = _ConstraintConn([])  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="conflict_keys"):
        await sink.open()

    assert poison_sink.open_calls == 1
    assert poison_sink.close_calls == 1


@pytest.mark.asyncio
async def test_open_accepts_matching_unique_constraint() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="analytics.events",
        row_mapper=lambda row: row,
        conflict_key="slug",
    )
    conn = _ConstraintConn([(["slug"],)])
    sink._conn = conn  # type: ignore[attr-defined]

    await sink.open()

    _sql, params = conn.cursor_obj.execute_calls[0]
    assert params == ("analytics", "events", 1)
    assert conn.rollback_calls == 1
    assert conn.close_calls == 0
    assert sink.metrics_snapshot().connection_ready is True


@pytest.mark.asyncio
async def test_open_accepts_matching_composite_unique_constraint_in_any_order() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key=["slug", "tenant_id"],
    )
    conn = _ConstraintConn([(["tenant_id", "slug"],)])
    sink._conn = conn  # type: ignore[attr-defined]

    await sink.open()

    assert conn.cursor_obj.execute_calls[0][1] == ("events", 2)


@pytest.mark.asyncio
async def test_open_rejects_constraint_superset_for_upsert() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
    )
    conn = _ConstraintConn([(["slug", "tenant_id"],)])
    sink._conn = conn  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="exactly match"):
        await sink.open()


@pytest.mark.asyncio
async def test_open_skips_upsert_constraint_preflight_when_insert_only() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        upsert=False,
    )
    conn = _ConstraintConn([])
    sink._conn = conn  # type: ignore[attr-defined]

    await sink.open()

    assert conn.cursor_obj.execute_calls == []


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


def test_write_safety_policy_rejects_unknown_value() -> None:
    with pytest.raises(ValueError, match="write_safety_policy"):
        PostgresSink(
            dsn="postgresql://example.invalid/db",
            table="events",
            row_mapper=lambda row: row,
            conflict_key="slug",
            write_safety_policy="boom",
        )


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

    metrics = sink.metrics_snapshot().to_dict()
    assert metrics["buffered_row_count"] == 0
    assert metrics["flush_count"] == 1
    assert metrics["flushed_row_count"] == 2
    assert metrics["connection_ready"] is True
    assert metrics["last_flush_at"] is not None


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
async def test_postgres_sink_rereads_password_file_on_reconnect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    connect_calls: list[dict[str, object]] = []
    password_file = tmp_path / "postgres-password.txt"
    password_file.write_text("secret-one\n", encoding="utf-8")

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args
            connect_calls.append(dict(kwargs))
            return SimpleNamespace()

    fake_psycopg = SimpleNamespace(
        AsyncConnection=_AsyncConnection,
        OperationalError=RuntimeError,
        InterfaceError=RuntimeError,
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)

    sink = PostgresSink(
        dsn=None,
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        connection=PostgresConnectionConfig(
            dsn="postgresql://example.invalid/db",
            auth=PostgresAuthConfig(password_file=str(password_file)),
            tls=PostgresTLSConfig(sslmode="require"),
            application_name="agora-sink",
        ),
    )

    with pytest.warns(UserWarning, match="without full server identity verification"):
        await sink._create_connection()
    password_file.write_text("secret-two\n", encoding="utf-8")
    with pytest.warns(UserWarning, match="without full server identity verification"):
        await sink._create_connection()

    assert connect_calls[0]["password"] == "secret-one"
    assert connect_calls[1]["password"] == "secret-two"
    assert connect_calls[0]["sslmode"] == "require"
    assert connect_calls[1]["application_name"] == "agora-sink"


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
async def test_write_batch_keeps_failed_auto_flush_buffer() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=1,
    )

    async def _boom_flush() -> None:
        raise RuntimeError("db unavailable")

    sink.flush = _boom_flush  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="db unavailable"):
        await sink.write_batch([{"slug": "a", "display_name": "A"}])

    assert sink._buffer == [{"slug": "a", "display_name": "A"}]  # type: ignore[attr-defined]


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
async def test_flush_uses_default_psycopg_retry_policy_on_first_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeOperationalError(Exception):
        pass

    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
    ]

    class FakeCursor:
        def __init__(self) -> None:
            self.execute_calls = 0

        async def execute(self, sql: str, params: list[Any]) -> None:
            del sql, params
            self.execute_calls += 1
            if self.execute_calls == 1:
                raise FakeOperationalError("transient db error")

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

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return conn

    fake_psycopg = SimpleNamespace(
        AsyncConnection=_AsyncConnection,
        OperationalError=FakeOperationalError,
        InterfaceError=RuntimeError,
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)

    await sink.flush()

    assert sink._psycopg is fake_psycopg  # type: ignore[attr-defined]
    assert conn.cursor_obj.execute_calls == 2
    assert conn.rollback_calls == 1
    assert conn.commit_calls == 1
    assert sink.metrics_snapshot().retry_count == 1
    assert sink._buffer == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_flush_aligns_rows_to_live_target_schema_and_drops_unknown_columns() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        write_safety_policy=PostgresWriteSafetyPolicy.ALIGN_TO_TARGET,
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A", "legacy_note": "drop-me"},
        {"slug": "b", "display_name": "B"},
    ]

    async def _fake_load_target_columns():
        return [
            sink_module._TargetColumn("slug", nullable=False, has_default=False, writable=True),
            sink_module._TargetColumn(
                "display_name",
                nullable=False,
                has_default=False,
                writable=True,
            ),
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
    sink._load_target_columns = _fake_load_target_columns  # type: ignore[method-assign]

    await sink.flush()

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert conn.cursor_obj.calls[0][1] == ["a", "A", "b", "B"]
    metrics = sink.metrics_snapshot()
    assert metrics.write_safety_policy == "align_to_target"
    assert metrics.schema_drift_detected_count == 1
    assert metrics.schema_drift_aligned_count == 1


@pytest.mark.asyncio
async def test_flush_align_to_target_raises_structured_error_for_missing_required_columns() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        write_safety_policy=PostgresWriteSafetyPolicy.ALIGN_TO_TARGET,
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
    ]

    async def _fake_load_target_columns():
        return [
            sink_module._TargetColumn("slug", nullable=False, has_default=False, writable=True),
            sink_module._TargetColumn(
                "display_name",
                nullable=False,
                has_default=False,
                writable=True,
            ),
            sink_module._TargetColumn(
                "tenant_id", nullable=False, has_default=False, writable=True
            ),
        ]

    sink._load_target_columns = _fake_load_target_columns  # type: ignore[method-assign]

    with pytest.raises(PostgresSinkWriteError) as exc_info:
        await sink.flush()

    error = exc_info.value
    assert error.poison_info.classification is PostgresPoisonRecordClassification.SCHEMA_DRIFT
    assert PostgresPoisonRecordClassification is PoisonRecordClassification
    assert isinstance(error.poison_info, PoisonRecordInfo)
    assert error.poison_info.reason == "missing_required_columns"
    assert error.dlq_details["postgres"]["details"]["missing_required_columns"] == ["tenant_id"]
    metrics = sink.metrics_snapshot()
    assert metrics.poison_record_count == 1
    assert metrics.poison_record_schema_drift_count == 1


@pytest.mark.asyncio
async def test_auto_flush_failure_keeps_buffer_when_no_dlq_is_configured() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=1,
    )
    error = sink._make_write_error(  # type: ignore[attr-defined]
        "boom",
        classification=PostgresPoisonRecordClassification.UNKNOWN,
        reason="unit_test",
        details={"table": "events"},
    )

    async def _fail_flush() -> None:
        raise error

    sink.flush = _fail_flush  # type: ignore[method-assign]

    with pytest.raises(PostgresSinkWriteError):
        await sink.write({"slug": "a", "display_name": "A"})

    assert sink._buffer == [{"slug": "a", "display_name": "A"}]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_auto_flush_failure_routes_failed_buffer_to_dlq() -> None:
    dlq = _CollectDLQSink()
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=1,
        poison_record_sink=dlq,
        poison_record_pipeline_id="orders-postgres",
    )
    error = sink._make_write_error(  # type: ignore[attr-defined]
        "boom",
        classification=PostgresPoisonRecordClassification.UNKNOWN,
        reason="unit_test",
        details={"table": "events"},
    )

    async def _fail_flush() -> None:
        raise error

    sink.flush = _fail_flush  # type: ignore[method-assign]

    with pytest.raises(PostgresSinkWriteError):
        await sink.write({"slug": "a", "display_name": "A"})

    assert sink._buffer == []  # type: ignore[attr-defined]
    assert len(dlq.records) == 1
    assert dlq.records[0].pipeline_id == "orders-postgres"
    assert dlq.records[0].stage == "postgres_sink_flush"
    assert dlq.records[0].record == {"slug": "a", "display_name": "A"}
    assert dlq.records[0].details["postgres"]["reason"] == "unit_test"


@pytest.mark.asyncio
async def test_partial_align_to_target_flush_rolls_back_all_rows_on_late_failure() -> None:
    dlq = _CollectDLQSink()
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=3,
        write_safety_policy=PostgresWriteSafetyPolicy.ALIGN_TO_TARGET,
        poison_record_sink=dlq,
        poison_record_pipeline_id="orders-postgres",
        retry_policy=RetryPolicy[Any](max_attempts=1),
    )
    sink._buffer = [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b"},
        {"slug": "c", "display_name": "C"},
    ]

    async def _fake_load_target_columns():
        return [
            sink_module._TargetColumn("slug", nullable=False, has_default=False, writable=True),
            sink_module._TargetColumn(
                "display_name",
                nullable=True,
                has_default=False,
                writable=True,
            ),
        ]

    class FakeCursor:
        def __init__(self) -> None:
            self.execute_calls = 0

        async def execute(self, sql: str, params: list[Any]) -> None:
            del sql, params
            self.execute_calls += 1
            if self.execute_calls == 2:
                raise RuntimeError("second batch failed")

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
    sink._load_target_columns = _fake_load_target_columns  # type: ignore[method-assign]

    with pytest.raises(PostgresSinkWriteError, match="second batch failed") as exc_info:
        await sink.flush()

    assert conn.commit_calls == 0
    assert conn.rollback_calls == 1
    assert sink._buffer == [  # type: ignore[attr-defined]
        {"slug": "a", "display_name": "A"},
        {"slug": "b"},
        {"slug": "c", "display_name": "C"},
    ]
    await sink._route_failed_buffer_to_dlq(exc_info.value)  # type: ignore[attr-defined]
    assert [record.record for record in dlq.records] == [  # type: ignore[list-item]
        {"slug": "a", "display_name": "A"},
        {"slug": "b"},
        {"slug": "c", "display_name": "C"},
    ]
    assert sink._buffer == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_close_routes_final_flush_failure_to_dlq() -> None:
    dlq = _CollectDLQSink()
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        poison_record_sink=dlq,
        poison_record_pipeline_id="orders-postgres",
        retry_policy=RetryPolicy[Any](max_attempts=1),
    )
    sink._buffer = [{"slug": "late", "display_name": "Late"}]  # type: ignore[attr-defined]

    async def _failing_flush() -> None:
        raise sink._wrap_write_error(  # type: ignore[attr-defined]
            RuntimeError("final flush failed"),
            rows=list(sink._buffer),  # type: ignore[attr-defined]
            columns=["slug", "display_name"],
        )

    sink.flush = _failing_flush  # type: ignore[method-assign]

    await sink.close()

    assert [record.record for record in dlq.records] == [{"slug": "late", "display_name": "Late"}]
    assert sink._buffer == []  # type: ignore[attr-defined]
    assert dlq.close_calls == 1


@pytest.mark.asyncio
async def test_close_raises_final_flush_failure_without_dlq() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        retry_policy=RetryPolicy[Any](max_attempts=1),
    )
    sink._buffer = [{"slug": "late", "display_name": "Late"}]  # type: ignore[attr-defined]

    async def _failing_flush() -> None:
        raise sink._wrap_write_error(  # type: ignore[attr-defined]
            RuntimeError("final flush failed"),
            rows=list(sink._buffer),  # type: ignore[attr-defined]
            columns=["slug", "display_name"],
        )

    sink.flush = _failing_flush  # type: ignore[method-assign]

    with pytest.raises(PostgresSinkWriteError, match="final flush failed"):
        await sink.close()

    assert sink._buffer == [{"slug": "late", "display_name": "Late"}]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_write_pool_prefers_psycopg_pool_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    class FakeAsyncConnectionPool:
        instances: ClassVar[list[FakeAsyncConnectionPool]] = []

        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.conn = FakeConn()
            self.open_calls = 0
            self.getconn_calls: list[float | None] = []
            self.putconn_calls: list[FakeConn] = []
            self.close_calls = 0
            self.instances.append(self)

        async def open(self) -> None:
            self.open_calls += 1

        async def getconn(self, *, timeout: float | None = None) -> FakeConn:
            self.getconn_calls.append(timeout)
            return self.conn

        async def putconn(self, conn: FakeConn) -> None:
            self.putconn_calls.append(conn)

        async def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setitem(
        sys.modules,
        "psycopg_pool",
        SimpleNamespace(AsyncConnectionPool=FakeAsyncConnectionPool),
    )
    sink = PostgresSink(
        dsn=None,
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        pool_size=3,
        pool_acquire_timeout_s=7.0,
        pool_max_lifetime_s=11.0,
        pool_max_idle_s=13.0,
        connection=PostgresConnectionConfig(
            dsn="postgresql://db.internal/agora",
            auth=PostgresAuthConfig(username="agora", password="secret"),
            tls=PostgresTLSConfig(sslmode="verify-full", root_cert_file="/ca.pem"),
        ),
    )

    conn, pooled = await sink._acquire_write_conn()  # type: ignore[attr-defined]
    await sink._release_write_conn(conn, pooled=pooled)  # type: ignore[attr-defined]
    await sink.close()

    pool = FakeAsyncConnectionPool.instances[0]
    assert pooled is True
    assert pool.kwargs == {
        "conninfo": "postgresql://db.internal/agora",
        "kwargs": {
            "user": "agora",
            "password": "secret",
            "sslmode": "verify-full",
            "sslrootcert": "/ca.pem",
            "autocommit": False,
        },
        "min_size": 0,
        "max_size": 3,
        "timeout": 7.0,
        "max_lifetime": 11.0,
        "max_idle": 13.0,
        "open": False,
    }
    assert pool.open_calls == 1
    assert pool.getconn_calls == [7.0]
    assert pool.putconn_calls == [conn]
    assert pool.close_calls == 1


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
    sink._external_write_pool_unavailable = True  # type: ignore[attr-defined]

    class FakeCursor:
        async def execute(self, sql: str, params: list[Any] | None = None) -> None:
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
    snapshot = sink.metrics_snapshot()
    histograms = {
        (histogram.operation, histogram.outcome): histogram
        for histogram in snapshot.latency_histograms
    }
    assert histograms[("flush", "success")].count == 2
    assert histograms[("pool_acquire", "success")].count == 2
    rendered = sink.render_prometheus_metrics(namespace="agora_pg")
    assert "agora_pg_sink_latency_seconds_bucket" in rendered
    assert 'operation="flush",outcome="success"' in rendered
    assert 'operation="pool_acquire",outcome="success"' in rendered


@pytest.mark.asyncio
async def test_write_pool_acquire_times_out_when_pool_is_exhausted() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        pool_size=2,
        pool_acquire_timeout_s=0.01,
    )
    sink._external_write_pool_unavailable = True  # type: ignore[attr-defined]
    sink._write_pool = asyncio.LifoQueue(maxsize=2)  # type: ignore[attr-defined]
    sink._write_pool_open_connections = 2  # type: ignore[attr-defined]

    with pytest.raises(TimeoutError, match="Timed out waiting"):
        await sink._acquire_write_conn()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_write_pool_discards_unhealthy_reused_connection() -> None:
    sink = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
        pool_size=2,
    )
    sink._external_write_pool_unavailable = True  # type: ignore[attr-defined]

    class FakeCursor:
        def __init__(self, *, healthy: bool) -> None:
            self._healthy = healthy

        async def execute(self, sql: str, params: list[Any] | None = None) -> None:
            del sql, params
            if not self._healthy:
                raise ConnectionError("connection lost")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeConn:
        def __init__(self, name: str, *, healthy: bool) -> None:
            self.name = name
            self._healthy = healthy
            self.closed = False

        def cursor(self) -> FakeCursor:
            return FakeCursor(healthy=self._healthy)

        async def close(self) -> None:
            self.closed = True

    bad = FakeConn("bad", healthy=False)
    good = FakeConn("good", healthy=True)
    sink._write_pool = asyncio.LifoQueue(maxsize=2)  # type: ignore[attr-defined]
    sink._write_pool.put_nowait(bad)  # type: ignore[union-attr]
    sink._write_pool_open_connections = 1  # type: ignore[attr-defined]

    async def _fake_create_connection() -> FakeConn:
        return good

    sink._create_connection = _fake_create_connection  # type: ignore[method-assign]

    conn, pooled = await sink._acquire_write_conn()  # type: ignore[attr-defined]

    assert conn is good
    assert pooled is True
    assert bad.closed is True
    assert sink._write_pool_open_connections == 1  # type: ignore[attr-defined]


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


@pytest.mark.parametrize(
    ("sqlstate", "expected_classification", "expected_reason"),
    [
        ("23502", PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION, "not_null_violation"),
        ("23505", PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION, "unique_violation"),
        (
            "23503",
            PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION,
            "foreign_key_violation",
        ),
        ("23514", PostgresPoisonRecordClassification.CONSTRAINT_VIOLATION, "check_violation"),
        (
            "22P02",
            PostgresPoisonRecordClassification.TYPE_MISMATCH,
            "invalid_text_representation",
        ),
        ("42804", PostgresPoisonRecordClassification.TYPE_MISMATCH, "datatype_mismatch"),
    ],
)
def test_wrap_write_error_classifies_sqlstate_matrix(
    sqlstate: str,
    expected_classification: PostgresPoisonRecordClassification,
    expected_reason: str,
) -> None:
    sink = _make_sink()

    class _FakePsycopgError(RuntimeError):
        def __init__(self, message: str, state: str) -> None:
            super().__init__(message)
            self.sqlstate = state

    error = sink._wrap_write_error(  # type: ignore[attr-defined]
        _FakePsycopgError("boom", sqlstate),
        rows=[{"slug": "a", "display_name": "A"}],
        columns=["slug", "display_name"],
    )

    assert error.poison_info.classification is expected_classification
    assert error.poison_info.reason == expected_reason
    assert error.dlq_details["postgres"]["details"]["sqlstate"] == sqlstate

    metrics = sink.metrics_snapshot()
    assert metrics.poison_record_count == 1
