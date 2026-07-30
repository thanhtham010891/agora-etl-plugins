from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest
from agora import (
    BatchMapMiddleware,
    Checkpoint,
    DeliveryConfig,
    Pipeline,
    SourceRecordFailurePolicy,
)
from agora.core.checkpoint import SourceIdentityMismatchError
from agora.core.source import SourceRecordError

from agora_plugins.postgres import (
    PostgresAuthConfig,
    PostgresConnectionConfig,
    PostgresReplicaStalenessError,
    PostgresTLSConfig,
)
from agora_plugins.postgres.sources import PostgresSource


class _FakeCursor:
    def __init__(
        self,
        rows: list[dict],
        batch_size: int = 2,
        fetchone_rows: list[dict[str, object]] | None = None,
    ) -> None:
        self._rows = rows
        self._batch_size = batch_size
        self._index = 0
        self._fetchone_rows = list(fetchone_rows or [])
        self.executed_query: str | None = None
        self.executed_params: dict | None = None
        self.calls: list[tuple[str, object | None]] = []

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, query: str, params: dict | None = None) -> None:
        self.executed_query = query
        self.executed_params = params
        self.calls.append((query, params))

    async def fetchone(self):
        if not self._fetchone_rows:
            return None
        return self._fetchone_rows.pop(0)

    async def fetchmany(self, size: int):
        del size
        if self._index >= len(self._rows):
            return []
        batch = self._rows[self._index : self._index + self._batch_size]
        self._index += self._batch_size
        return batch


class _FailAfterFirstBatchCursor(_FakeCursor):
    def __init__(self, rows: list[dict], error: Exception) -> None:
        super().__init__(rows, batch_size=1)
        self._error = error
        self._fetch_calls = 0

    async def fetchmany(self, size: int):
        self._fetch_calls += 1
        if self._fetch_calls >= 2:
            raise self._error
        return await super().fetchmany(size)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self._cursor_calls = 0
        self._stream_cursor_assigned = False
        self._stream_cursor_call_index = 2
        probe_rows = list(cursor._fetchone_rows)
        cursor._fetchone_rows = []
        self._probe_fetchone_rows = probe_rows
        self._last_probe_row = (
            probe_rows[-1] if probe_rows else {"is_standby": False, "replay_lag_s": 0.0}
        )
        self.cursor_invocations: list[dict[str, object]] = []
        self.opened_cursors: list[_FakeCursor] = []
        self.closed = False

    async def __aenter__(self) -> _FakeConnection:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self, **kwargs: object) -> _FakeCursor:
        self.cursor_invocations.append(dict(kwargs))
        cursor_call = self._cursor_calls
        self._cursor_calls += 1
        if not self._stream_cursor_assigned and cursor_call == self._stream_cursor_call_index:
            self._stream_cursor_assigned = True
            self.opened_cursors.append(self._cursor)
            return self._cursor
        probe_row = (
            self._probe_fetchone_rows.pop(0) if self._probe_fetchone_rows else self._last_probe_row
        )
        aux_cursor = _FakeCursor([], fetchone_rows=[probe_row])
        self.opened_cursors.append(aux_cursor)
        return aux_cursor

    async def close(self) -> None:
        self.closed = True


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
            source_identity=source.checkpoint_source_identity(),
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
async def test_postgres_source_rejects_checkpoint_from_different_cursor_contract() -> None:
    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events WHERE id > %(last_id)s ORDER BY id",
        row_mapper=lambda row: row,
        checkpoint_field="id",
        checkpoint_param="last_id",
    )
    other_source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM orders WHERE id > %(last_id)s ORDER BY id",
        row_mapper=lambda row: row,
        checkpoint_field="id",
        checkpoint_param="last_id",
    )

    with pytest.raises(SourceIdentityMismatchError, match="saved source identity differs"):
        await source.prepare_resume(
            Checkpoint(
                pipeline_id="events",
                run_id="run-1",
                source="postgres",
                value={"cursor": 2},
                source_identity=other_source.checkpoint_source_identity(),
            )
        )

    reset_source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events WHERE id > %(last_id)s ORDER BY id",
        row_mapper=lambda row: row,
        checkpoint_field="id",
        checkpoint_param="last_id",
        source_identity_mismatch_policy="reset",
    )
    await reset_source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="postgres",
            value={"cursor": 2},
            source_identity=other_source.checkpoint_source_identity(),
        )
    )
    assert reset_source._params == {}  # type: ignore[attr-defined]

    allowed_source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events WHERE id > %(last_id)s ORDER BY id",
        row_mapper=lambda row: row,
        checkpoint_field="id",
        checkpoint_param="last_id",
        source_identity_mismatch_policy="allow",
    )
    await allowed_source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="postgres",
            value={"cursor": 2},
            source_identity=other_source.checkpoint_source_identity(),
        )
    )
    assert allowed_source._params == {"last_id": 2}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_postgres_source_log_and_continue_does_not_advance_cursor_on_mapper_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "bad"},
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

    def _map_row(row: dict[str, object]) -> dict[str, object]:
        if row["id"] == 2:
            raise ValueError("bad row")
        return row

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id, name FROM events ORDER BY id",
        row_mapper=_map_row,
        checkpoint_field="id",
        checkpoint_param="last_id",
        on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
    )

    records = [record async for record in source.stream()]

    assert records == [{"id": 1, "name": "alpha"}]
    assert source.current_checkpoint() == {"row_number": 2, "cursor": 1}
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }


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
            source_identity=source.checkpoint_source_identity(),
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
async def test_postgres_source_row_mapper_can_receive_row_context(
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

    seen_contexts: list[dict] = []
    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events WHERE tenant = %(tenant)s ORDER BY id",
        params={"tenant": "demo"},
        row_mapper=lambda row, context: (
            seen_contexts.append(context) or {"id": row["id"], "ctx": context}
        ),
        checkpoint_field="id",
        checkpoint_param="last_id",
    )

    records = [record async for record in source.stream()]

    assert records == [
        {
            "id": 1,
            "ctx": {
                "row_number": 1,
                "checkpoint_cursor": 1,
                "query": "SELECT id FROM events WHERE tenant = %(tenant)s ORDER BY id",
                "params": {"tenant": "demo"},
                "read_routing": "dsn",
                "connected_server_role": "primary",
                "replica_replay_lag_s": 0.0,
            },
        },
        {
            "id": 2,
            "ctx": {
                "row_number": 2,
                "checkpoint_cursor": 2,
                "query": "SELECT id FROM events WHERE tenant = %(tenant)s ORDER BY id",
                "params": {"tenant": "demo"},
                "read_routing": "dsn",
                "connected_server_role": "primary",
                "replica_replay_lag_s": 0.0,
            },
        },
    ]
    assert seen_contexts == [records[0]["ctx"], records[1]["ctx"]]


@pytest.mark.asyncio
async def test_postgres_source_supports_async_row_mapper_with_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}], batch_size=2)
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

    async def row_mapper(row: dict, context: dict) -> str:
        return f"{context['row_number']}:{row['id']}"

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=row_mapper,
    )

    records = [record async for record in source.stream()]

    assert records == ["1:1", "2:2"]


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


@pytest.mark.asyncio
async def test_postgres_source_retries_transient_connect_failures_before_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}], batch_size=1)
    connection = _FakeConnection(cursor)
    attempts = {"count": 0}

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ConnectionError("temporary network glitch")
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
    snapshot = source.metrics_snapshot()

    assert records == [1, 2]
    assert attempts["count"] == 2
    assert snapshot.stream_run_count == 1
    assert snapshot.query_execution_count == 2
    assert snapshot.retry_count == 1
    assert snapshot.last_stream_succeeded is True


@pytest.mark.asyncio
async def test_postgres_source_does_not_retry_after_progress_has_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FailAfterFirstBatchCursor(
        [{"id": 1}, {"id": 2}],
        ConnectionError("connection dropped after first row"),
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
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
        checkpoint_field="id",
        checkpoint_param="last_id",
    )

    stream = source.stream()
    first = await anext(stream)
    with pytest.raises(ConnectionError, match="connection dropped"):
        await anext(stream)

    snapshot = source.metrics_snapshot()

    assert first == 1
    assert source.current_checkpoint() == {"row_number": 1, "cursor": 1}
    assert snapshot.query_execution_count == 1
    assert snapshot.retry_count == 0
    assert snapshot.last_stream_succeeded is False


@pytest.mark.asyncio
async def test_postgres_source_applies_read_safety_controls_before_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}], batch_size=1)
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
        statement_timeout_ms=5000,
        transaction_read_only=True,
        transaction_isolation_level="repeatable_read",
    )

    records = [record async for record in source.stream()]

    assert records == [1]
    assert connection.cursor_invocations[:3] == [{}, {}, {}]
    assert "pg_is_in_recovery()" in str(connection.opened_cursors[0].calls[0][0])
    assert connection.opened_cursors[1].calls == [
        ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", None),
        ("SELECT set_config('statement_timeout', %s, false)", ("5000",)),
    ]
    assert cursor.calls == [("SELECT id FROM events ORDER BY id", None)]


@pytest.mark.asyncio
async def test_postgres_source_uses_server_side_cursor_fetch_strategy(
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
        fetch_strategy="server_side",
        server_side_cursor_name="agora_cursor",
        server_side_cursor_withhold=True,
    )

    records = [record async for record in source.stream()]

    assert records == [1, 2]
    assert connection.cursor_invocations[:3] == [{}, {}, {"name": "agora_cursor", "withhold": True}]


@pytest.mark.asyncio
async def test_postgres_source_uses_connection_config_for_secure_connect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    cursor = _FakeCursor([{"id": 1}], batch_size=1)
    connection = _FakeConnection(cursor)
    connect_calls: list[dict[str, object]] = []
    password_file = tmp_path / "postgres-password.txt"
    password_file.write_text("super-secret\n", encoding="utf-8")
    monkeypatch.setenv("PG_SSL_ROOT_CERT", "/etc/certs/postgres-ca.pem")
    monkeypatch.setenv("PG_SSL_CERT", "/etc/certs/client.crt")
    monkeypatch.setenv("PG_SSL_KEY", "/etc/certs/client.key")

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args
            connect_calls.append(dict(kwargs))
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn=None,
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
        connection=PostgresConnectionConfig(
            dsn="postgresql://example/test",
            auth=PostgresAuthConfig(
                username="agora",
                password_file=str(password_file),
            ),
            tls=PostgresTLSConfig(
                sslmode="verify-full",
                root_cert_env="PG_SSL_ROOT_CERT",
                cert_env="PG_SSL_CERT",
                key_env="PG_SSL_KEY",
            ),
            connect_timeout_s=7,
            application_name="agora-source",
        ),
    )

    records = [record async for record in source.stream()]

    assert records == [1]
    assert connect_calls == [
        {
            "conninfo": "postgresql://example/test",
            "user": "agora",
            "password": "super-secret",
            "sslmode": "verify-full",
            "sslrootcert": "/etc/certs/postgres-ca.pem",
            "sslcert": "/etc/certs/client.crt",
            "sslkey": "/etc/certs/client.key",
            "connect_timeout": 7,
            "application_name": "agora-source",
            "row_factory": fake_rows.dict_row,
        }
    ]


@pytest.mark.asyncio
async def test_postgres_source_routes_reads_to_standby_and_tracks_replay_lag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [{"id": 1}],
        batch_size=1,
        fetchone_rows=[{"is_standby": True, "replay_lag_s": 0.75}],
    )
    connection = _FakeConnection(cursor)
    connect_calls: list[dict[str, object]] = []

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args
            connect_calls.append(dict(kwargs))
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row, context: {
            "id": row["id"],
            "role": context["connected_server_role"],
            "lag": context["replica_replay_lag_s"],
        },
        read_routing="standby",
        max_replica_replay_lag_s=2.0,
    )

    records = [record async for record in source.stream()]
    snapshot = source.metrics_snapshot()

    assert records == [{"id": 1, "role": "standby", "lag": 0.75}]
    assert connect_calls[0]["target_session_attrs"] == "standby"
    assert snapshot.read_routing == "standby"
    assert snapshot.connected_server_role == "standby"
    assert snapshot.last_replica_replay_lag_s == 0.75
    assert snapshot.max_replica_replay_lag_s == 2.0
    assert snapshot.staleness_guard_block_count == 0
    assert snapshot.staleness_guard_primary_fallback_count == 0


@pytest.mark.asyncio
async def test_postgres_source_health_snapshot_probes_replica_route_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [],
        fetchone_rows=[{"is_standby": True, "replay_lag_s": 0.25}],
    )
    connection = _FakeConnection(cursor)
    connect_calls: list[dict[str, object]] = []

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args
            connect_calls.append(dict(kwargs))
            return connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
        read_routing="standby",
        max_replica_replay_lag_s=1.0,
    )

    health = await source.health_snapshot(force_refresh=True)
    snapshot = source.metrics_snapshot()

    assert connect_calls[0]["target_session_attrs"] == "standby"
    assert health.ready is True
    assert health.connection_ready is True
    assert health.routing_ready is True
    assert health.staleness_guard_ready is True
    assert health.target_session_attrs == "standby"
    assert health.connected_server_role == "standby"
    assert health.last_replica_replay_lag_s == 0.25
    assert snapshot.ready is True
    assert snapshot.connection_ready is True
    assert snapshot.routing_ready is True
    assert snapshot.staleness_guard_ready is True
    assert snapshot.target_session_attrs == "standby"


@pytest.mark.asyncio
async def test_postgres_source_falls_back_to_primary_when_preferred_standby_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    standby_cursor = _FakeCursor(
        [],
        fetchone_rows=[{"is_standby": True, "replay_lag_s": 9.5}],
    )
    primary_cursor = _FakeCursor(
        [{"id": 1}],
        batch_size=1,
        fetchone_rows=[{"is_standby": False, "replay_lag_s": 0.0}],
    )
    standby_connection = _FakeConnection(standby_cursor)
    primary_connection = _FakeConnection(primary_cursor)
    connect_calls: list[dict[str, object]] = []

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args
            connect_calls.append(dict(kwargs))
            if kwargs.get("target_session_attrs") == "primary":
                return primary_connection
            return standby_connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row, context: {
            "id": row["id"],
            "role": context["connected_server_role"],
        },
        read_routing="prefer_standby",
        max_replica_replay_lag_s=3.0,
        on_replica_stale="route_primary",
    )

    records = [record async for record in source.stream()]
    snapshot = source.metrics_snapshot()

    assert records == [{"id": 1, "role": "primary"}]
    assert [call["target_session_attrs"] for call in connect_calls] == [
        "prefer-standby",
        "primary",
    ]
    assert standby_connection.closed is True
    assert snapshot.connected_server_role == "primary"
    assert snapshot.last_replica_replay_lag_s == 0.0
    assert snapshot.staleness_guard_block_count == 0
    assert snapshot.staleness_guard_primary_fallback_count == 1


@pytest.mark.asyncio
async def test_postgres_source_route_primary_rejects_non_primary_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    standby_cursor = _FakeCursor([])
    fallback_cursor = _FakeCursor([])
    standby_connection = _FakeConnection(standby_cursor)
    fallback_connection = _FakeConnection(fallback_cursor)
    connect_calls: list[dict[str, object]] = []

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args
            connect_calls.append(dict(kwargs))
            if kwargs.get("target_session_attrs") == "primary":
                return fallback_connection
            return standby_connection

    fake_psycopg = SimpleNamespace(AsyncConnection=_AsyncConnection)
    fake_rows = SimpleNamespace(dict_row=object())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", fake_rows)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
        read_routing="prefer_standby",
        max_replica_replay_lag_s=3.0,
        on_replica_stale="route_primary",
    )

    async def _inspect_server_role(conn: object) -> tuple[str, float | None]:
        if conn is standby_connection:
            return "standby", 9.5
        return "standby", 0.0

    monkeypatch.setattr(source, "_inspect_server_role", _inspect_server_role)

    with pytest.raises(RuntimeError, match="must connect to a primary server"):
        [record async for record in source.stream()]

    snapshot = source.metrics_snapshot()

    assert [call["target_session_attrs"] for call in connect_calls] == [
        "prefer-standby",
        "primary",
    ]
    assert standby_connection.closed is True
    assert fallback_connection.closed is True
    assert snapshot.connected_server_role == "standby"
    assert snapshot.last_replica_replay_lag_s == 0.0
    assert snapshot.last_health_error is not None
    assert "did not resolve to a primary" in snapshot.last_health_error
    assert snapshot.staleness_guard_block_count == 1
    assert snapshot.staleness_guard_primary_fallback_count == 0


@pytest.mark.asyncio
async def test_postgres_source_health_snapshot_reports_unready_when_standby_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [],
        fetchone_rows=[{"is_standby": True, "replay_lag_s": 12.0}],
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
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
        read_routing="standby",
        max_replica_replay_lag_s=4.0,
    )

    health = await source.health_snapshot(force_refresh=True)
    snapshot = source.metrics_snapshot()

    assert connection.closed is True
    assert health.ready is False
    assert health.connection_ready is True
    assert health.routing_ready is True
    assert health.staleness_guard_ready is False
    assert health.connected_server_role == "standby"
    assert health.last_replica_replay_lag_s == 12.0
    assert health.last_error is not None
    assert "replay lag exceeded" in health.last_error
    assert snapshot.ready is False
    assert snapshot.connection_ready is True
    assert snapshot.routing_ready is True
    assert snapshot.staleness_guard_ready is False
    assert snapshot.last_health_error is not None


@pytest.mark.asyncio
async def test_postgres_source_fail_closes_when_standby_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [],
        fetchone_rows=[{"is_standby": True, "replay_lag_s": 12.0}],
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
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row["id"],
        read_routing="standby",
        max_replica_replay_lag_s=4.0,
    )

    with pytest.raises(PostgresReplicaStalenessError, match="replay lag exceeded"):
        [record async for record in source.stream()]

    snapshot = source.metrics_snapshot()

    assert connection.closed is True
    assert snapshot.connected_server_role == "standby"
    assert snapshot.last_replica_replay_lag_s == 12.0
    assert snapshot.staleness_guard_block_count == 1
    assert snapshot.staleness_guard_primary_fallback_count == 0


@pytest.mark.asyncio
async def test_postgres_source_blocks_when_active_standby_turns_stale_mid_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"id": 1}, {"id": 2}], batch_size=1)
    connection = _FakeConnection(cursor)
    connection._stream_cursor_call_index = 1

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
        batch_size=1,
        read_routing="standby",
        max_replica_replay_lag_s=2.0,
    )

    role_checks = [
        ("standby", 0.5),
        ("standby", 0.5),
        ("standby", 9.0),
    ]

    async def _inspect_server_role(conn: object) -> tuple[str, float | None]:
        del conn
        return role_checks.pop(0)

    monkeypatch.setattr(source, "_inspect_server_role", _inspect_server_role)

    stream = source.stream()

    assert await anext(stream) == 1
    with pytest.raises(PostgresReplicaStalenessError, match="replay lag exceeded"):
        await anext(stream)

    snapshot = source.metrics_snapshot()

    assert connection.closed is False
    assert source.current_checkpoint() == {"row_number": 1}
    assert snapshot.connected_server_role == "standby"
    assert snapshot.last_replica_replay_lag_s == 9.0
    assert snapshot.last_health_error is not None
    assert "replay lag exceeded" in snapshot.last_health_error
    assert snapshot.staleness_guard_block_count == 1
    assert snapshot.staleness_guard_primary_fallback_count == 0


def test_postgres_source_rejects_incomplete_checkpoint_config() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        PostgresSource(
            dsn="postgresql://example/test",
            query="SELECT id FROM events",
            row_mapper=lambda row: row,
            batch_size=0,
        )

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
    with pytest.raises(ValueError, match="statement_timeout_ms"):
        PostgresSource(
            dsn="postgresql://example/test",
            query="SELECT id FROM events",
            row_mapper=lambda row: row,
            statement_timeout_ms=0,
        )
    with pytest.raises(ValueError, match="read_routing"):
        PostgresSource(
            dsn="postgresql://example/test",
            query="SELECT id FROM events",
            row_mapper=lambda row: row,
            read_routing="replica",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="max_replica_replay_lag_s"):
        PostgresSource(
            dsn="postgresql://example/test",
            query="SELECT id FROM events",
            row_mapper=lambda row: row,
            max_replica_replay_lag_s=-1,
        )
