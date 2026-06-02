from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from agora.core.dlq import DLQRecord

from agora_plugins.postgres.dlq import PostgresDLQSink, PostgresDLQSource


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[object]]] = []
        self.rows: list[dict[str, object]] = []
        self._fetched = False

    async def execute(self, sql: str, params: object | None = None) -> None:
        self.calls.append((sql, params))
        self._fetched = False

    async def executemany(self, sql: str, params_seq: list[object]) -> None:
        self.executemany_calls.append((sql, params_seq))
        self._fetched = False

    async def fetchmany(self, size: int = 1) -> list[dict[str, object]]:
        if self._fetched:
            return []
        self._fetched = True
        return list(self.rows)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.commit_calls = 0
        self.rollback_calls = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    async def commit(self) -> None:
        self.commit_calls += 1

    async def rollback(self) -> None:
        self.rollback_calls += 1

    async def close(self) -> None:
        self.closed = True


def _install_fake_psycopg(monkeypatch: pytest.MonkeyPatch, connection: _FakeConnection) -> None:
    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(AsyncConnection=_AsyncConnection))
    monkeypatch.setitem(sys.modules, "psycopg.rows", SimpleNamespace(dict_row=object()))


def _make_record(**overrides) -> DLQRecord:
    defaults = {
        "pipeline_id": "orders",
        "run_id": "run-1",
        "stage": "sink_write",
        "error_type": "RuntimeError",
        "error_message": "sink exploded",
        "record": {"id": 1},
        "source": "orders_source",
        "checkpoint": {"offset": 10},
        "middleware": None,
        "sink": "postgres",
        "created_at": datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
        "attempt": 0,
        "max_attempts": 5,
    }
    defaults.update(overrides)
    return DLQRecord(**defaults)


@pytest.mark.asyncio
async def test_postgres_dlq_sink_inserts_serialized_record(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeConnection()
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")

    await sink.open()
    await sink.write(_make_record())

    assert connection.commit_calls == 2
    insert_sql, insert_params = connection.cursor_obj.executemany_calls[-1]
    assert 'INSERT INTO "agora_dlq"' in insert_sql
    assert insert_params is not None
    assert '"id": 1' in insert_params[0][5]


@pytest.mark.asyncio
async def test_postgres_dlq_sink_replay_updates_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeConnection()
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")
    record = _make_record(attempt=1)

    await sink.open()
    updated = await sink.replay(record)

    assert updated.attempt == 2
    sql, params = connection.cursor_obj.calls[-1]
    assert 'UPDATE "agora_dlq" SET attempt = %s' in sql
    assert params[0] == 2


@pytest.mark.asyncio
async def test_postgres_dlq_sink_acknowledge_deletes_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")
    record = _make_record()

    await sink.open()
    await sink.acknowledge(record)

    sql, params = connection.cursor_obj.calls[-1]
    assert 'DELETE FROM "agora_dlq"' in sql
    assert params == (
        record.pipeline_id,
        record.run_id,
        record.stage,
        record.created_at,
    )


@pytest.mark.asyncio
async def test_postgres_dlq_source_reads_records_with_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    connection.cursor_obj.rows = [
        {
            "pipeline_id": "orders",
            "run_id": "run-1",
            "stage": "sink_write",
            "error_type": "RuntimeError",
            "error_message": "sink exploded",
            "record": '{"id": 1}',
            "source": "orders_source",
            "checkpoint": '{"offset": 10}',
            "middleware": None,
            "sink": "postgres",
            "created_at": datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
            "attempt": 1,
            "max_attempts": 5,
        }
    ]
    _install_fake_psycopg(monkeypatch, connection)

    source = PostgresDLQSource(
        dsn="postgresql://example.invalid/db",
        table="agora_dlq",
        pipeline_id="orders",
        stage="sink_write",
        limit=10,
    )
    await source.open()
    records = [record async for record in source.stream()]

    assert len(records) == 1
    assert records[0].record == {"id": 1}
    assert records[0].checkpoint == {"offset": 10}
    select_sql, select_params = connection.cursor_obj.calls[-1]
    assert 'FROM "agora_dlq"' in select_sql
    assert select_params == ["orders", "sink_write", 10]


@pytest.mark.asyncio
async def test_postgres_dlq_acknowledge_prefers_storage_id_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")
    record = _make_record()
    object.__setattr__(record, "_storage_id", 42)

    await sink.open()
    await sink.acknowledge(record)

    sql, params = connection.cursor_obj.calls[-1]
    assert 'DELETE FROM "agora_dlq" WHERE id = %s' in sql
    assert params == (42,)
