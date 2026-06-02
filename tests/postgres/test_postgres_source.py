from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest
from agora import (
    BatchMapMiddleware,
    Checkpoint,
    DeliveryConfig,
    Pipeline,
    SourceRecordError,
    SourceRecordFailurePolicy,
)

from agora_plugins.postgres.sources import PostgresSource


class _FakeCursor:
    def __init__(self, rows: list[dict], batch_size: int = 2) -> None:
        self._rows = rows
        self._batch_size = batch_size
        self._index = 0
        self.executed_query: str | None = None
        self.executed_params: dict | None = None

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, query: str, params: dict | None = None) -> None:
        self.executed_query = query
        self.executed_params = params

    async def fetchmany(self, size: int):
        del size
        if self._index >= len(self._rows):
            return []
        batch = self._rows[self._index : self._index + self._batch_size]
        self._index += self._batch_size
        return batch


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    async def __aenter__(self) -> _FakeConnection:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[int] = []

    async def open(self) -> None:
        return None

    async def write(self, record: int) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_postgres_source_uses_checkpoint_cursor_for_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [
            {"id": 3, "name": "charlie"},
            {"id": 4, "name": "delta"},
        ]
    )
    connection = _FakeConnection(cursor)

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id, name FROM events WHERE id > %(last_id)s ORDER BY id",
        params={"tenant": "demo"},
        row_mapper=lambda row: row,
        batch_size=2,
        checkpoint_field="id",
        checkpoint_param="last_id",
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="postgres",
            value={"cursor": 2},
        )
    )

    records = [record async for record in source.stream()]

    assert records == [
        {"id": 3, "name": "charlie"},
        {"id": 4, "name": "delta"},
    ]
    assert cursor.executed_params == {"tenant": "demo", "last_id": 2}
    assert source.current_checkpoint() == {"row_number": 2, "cursor": 4}


@pytest.mark.asyncio
async def test_postgres_source_without_checkpoint_config_keeps_row_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}], batch_size=1)
    connection = _FakeConnection(cursor)

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
    )

    records = [record async for record in source.stream()]

    assert records == [1, 2]
    assert source.current_checkpoint() == {"row_number": 2}


@pytest.mark.asyncio
async def test_postgres_source_supports_batch_middleware_on_linear_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}, {"id": 3}], batch_size=2)
    connection = _FakeConnection(cursor)

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
        batch_size=2,
    )
    sink = _CollectSink()

    summary = await (
        Pipeline(source)
        .pipe(BatchMapMiddleware(lambda record: record * 10))
        .build(sink, config=DeliveryConfig(batch_size=2))  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [10, 20, 30]
    assert summary.records_consumed == 3
    assert summary.records_written == 3


@pytest.mark.asyncio
async def test_postgres_source_supports_composite_checkpoint_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [
            {"created_at": "2024-01-01T00:00:00", "id": 3, "name": "charlie"},
            {"created_at": "2024-01-01T00:00:00", "id": 4, "name": "delta"},
        ]
    )
    connection = _FakeConnection(cursor)

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query=(
            "SELECT created_at, id, name FROM events "
            "WHERE created_at > %(last_created_at)s "
            "OR (created_at = %(last_created_at)s AND id > %(last_id)s) "
            "ORDER BY created_at, id"
        ),
        params={"tenant": "demo", "last_created_at": "2023-12-31T00:00:00", "last_id": 0},
        row_mapper=lambda row: row,
        batch_size=2,
        checkpoint_fields=["created_at", "id"],
        checkpoint_params={"created_at": "last_created_at", "id": "last_id"},
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="postgres",
            value={"cursor": {"created_at": "2024-01-01T00:00:00", "id": 2}},
        )
    )

    records = [record async for record in source.stream()]

    assert records == [
        {"created_at": "2024-01-01T00:00:00", "id": 3, "name": "charlie"},
        {"created_at": "2024-01-01T00:00:00", "id": 4, "name": "delta"},
    ]
    assert cursor.executed_params == {
        "tenant": "demo",
        "last_created_at": "2024-01-01T00:00:00",
        "last_id": 2,
    }
    assert source.current_checkpoint() == {
        "row_number": 2,
        "cursor": {"created_at": "2024-01-01T00:00:00", "id": 4},
    }


@pytest.mark.asyncio
async def test_postgres_source_checkpoint_tracks_consumed_rows_when_mapper_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}, {"id": 3}], batch_size=3)
    connection = _FakeConnection(cursor)

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"] if row["id"] != 2 else None,
    )

    records = [record async for record in source.stream()]

    assert records == [1, 3]
    assert source.current_checkpoint() == {"row_number": 3}
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 0,
        "record_drop_count": 1,
    }


@pytest.mark.asyncio
async def test_postgres_source_row_mapper_errors_fail_closed_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}, {"id": 3}], batch_size=3)
    connection = _FakeConnection(cursor)

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"] if row["id"] != 2 else int("bad"),
    )

    with pytest.raises(SourceRecordError) as exc_info:
        _ = [record async for record in source.stream()]

    assert isinstance(exc_info.value.original, ValueError)
    assert exc_info.value.record == {"id": 2}
    assert source.current_checkpoint() == {"row_number": 2}
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 0,
    }


@pytest.mark.asyncio
async def test_postgres_source_can_log_and_continue_record_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}, {"id": 3}], batch_size=3)
    connection = _FakeConnection(cursor)

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"] if row["id"] != 2 else int("bad"),
        on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
    )

    records = [record async for record in source.stream()]

    assert records == [1, 3]
    assert source.current_checkpoint() == {"row_number": 3}
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }


def test_postgres_source_rejects_incomplete_checkpoint_config() -> None:
    with pytest.raises(ValueError, match="provided together"):
        PostgresSource(
            dsn="postgresql://example/test",
            query="SELECT id FROM events",
            row_mapper=lambda row: row,
            checkpoint_field="id",
        )

    with pytest.raises(ValueError, match="Missing"):
        PostgresSource(
            dsn="postgresql://example/test",
            query="SELECT created_at, id FROM events",
            row_mapper=lambda row: row,
            checkpoint_fields=["created_at", "id"],
            checkpoint_params={"created_at": "last_created_at"},
        )
