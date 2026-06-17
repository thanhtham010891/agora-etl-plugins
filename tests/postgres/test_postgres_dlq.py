from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from agora.core.dlq import DLQRecord

from agora_plugins.dlq_policy import DLQPayloadPolicy
from agora_plugins.postgres import (
    PostgresAuthConfig,
    PostgresConnectionConfig,
    PostgresTLSConfig,
)
from agora_plugins.postgres.dlq import (
    PostgresDLQSink,
    PostgresDLQSource,
    _record_to_row,
    _row_to_record,
)


class _ReverseCipher:
    def encrypt(self, payload: bytes) -> bytes:
        return payload[::-1]

    def decrypt(self, payload: bytes) -> bytes:
        return payload[::-1]


class _FailingCipher:
    def encrypt(self, payload: bytes) -> bytes:
        del payload
        raise RuntimeError("kms unavailable")


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[object]]] = []
        self.executemany_errors: list[Exception] = []
        self.insert_execute_errors: list[Exception] = []
        self.fetchone_rows: list[dict[str, object] | tuple[object, ...] | None] = []
        self.rows: list[dict[str, object]] = []
        self._fetched = False

    async def execute(self, sql: str, params: object | None = None) -> None:
        self.calls.append((sql, params))
        self._fetched = False
        if "INSERT INTO" in sql and self.insert_execute_errors:
            raise self.insert_execute_errors.pop(0)

    async def executemany(self, sql: str, params_seq: list[object]) -> None:
        self.executemany_calls.append((sql, params_seq))
        self._fetched = False
        if self.executemany_errors:
            raise self.executemany_errors.pop(0)

    async def fetchmany(self, size: int = 1) -> list[dict[str, object]]:
        if self._fetched:
            return []
        self._fetched = True
        return list(self.rows)

    async def fetchone(self) -> dict[str, object] | tuple[object, ...] | None:
        if self.fetchone_rows:
            return self.fetchone_rows.pop(0)
        return {"id": 101}

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

    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(
            AsyncConnection=_AsyncConnection,
            OperationalError=ConnectionError,
            InterfaceError=RuntimeError,
        ),
    )
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
        "details": {"postgres": {"classification": "schema_drift"}},
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
    record = _make_record()

    await sink.open()
    await sink.write(record)

    assert connection.commit_calls == 2
    insert_sql, insert_params = connection.cursor_obj.calls[-1]
    assert 'INSERT INTO "agora_dlq"' in insert_sql
    assert '"dedupe_key"' in insert_sql
    assert "ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL" in insert_sql
    assert "RETURNING id" in insert_sql
    assert insert_params is not None
    assert isinstance(insert_params[0], str)
    assert len(insert_params[0]) == 64
    rendered_params = "\n".join(str(param) for param in insert_params)
    assert '"id": 1' in rendered_params
    assert '"schema_drift"' in rendered_params
    assert record._storage_id == 101
    metrics = sink.metrics_snapshot()
    assert metrics.inserted_record_count == 1
    assert metrics.upserted_record_count == 1
    assert metrics.updated_record_count == 0


@pytest.mark.asyncio
async def test_postgres_dlq_sink_metrics_split_insert_from_dedupe_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    connection.cursor_obj.fetchone_rows = [
        {"id": 101, "inserted": True},
        {"id": 101, "inserted": False},
    ]
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")
    record = _make_record()

    await sink.open()
    await sink.write(record)
    await sink.write(record)

    metrics = sink.metrics_snapshot()
    assert metrics.inserted_record_count == 1
    assert metrics.upserted_record_count == 2
    assert metrics.updated_record_count == 1


@pytest.mark.asyncio
async def test_postgres_dlq_sink_redacts_payload_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeConnection()
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(
        dsn="postgresql://example.invalid/db",
        table="agora_dlq",
        payload_policy=DLQPayloadPolicy.redacted(redact_fields=("ssn",)),
    )
    record = _make_record(
        record={"id": 1, "password": "plain-secret"},
        original_record={"token": "raw-token"},
        processed_record={"ssn": "111-22-3333"},
        details={"client_secret": "client-secret"},
    )

    await sink.open()
    await sink.write(record)

    _, insert_params = connection.cursor_obj.calls[-1]
    assert insert_params is not None
    rendered_params = "\n".join(str(param) for param in insert_params)
    assert "plain-secret" not in rendered_params
    assert "raw-token" not in rendered_params
    assert "111-22-3333" not in rendered_params
    assert "client-secret" not in rendered_params
    assert rendered_params.count("[REDACTED]") >= 4


def test_postgres_dlq_encrypted_payload_requires_policy_for_read() -> None:
    policy = DLQPayloadPolicy.encrypted(
        encryptor=_ReverseCipher(),
        encryption_algorithm="reverse-test",
        encryption_key_id="test-key",
    )
    record = _make_record(
        record={"id": 1, "password": "plain-secret"},
        original_record={"token": "raw-token"},
        checkpoint={"offset": 10, "api_key": "secret-api-key"},
        details={"client_secret": "client-secret"},
    )

    row = _record_to_row(record, payload_policy=policy)
    rendered_row = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)

    assert "plain-secret" not in rendered_row
    assert "raw-token" not in rendered_row
    assert "secret-api-key" not in rendered_row
    assert "client-secret" not in rendered_row
    stored_envelope = json.loads(row["record"])
    assert stored_envelope["payload_encoding"] == "encrypted"
    assert stored_envelope["payload_algorithm"] == "reverse-test"
    assert stored_envelope["payload_key_id"] == "test-key"
    assert row["original_record"] is None
    assert row["processed_record"] is None
    assert row["checkpoint"] is None
    assert row["details"] is None
    with pytest.raises(ValueError, match="Encrypted Postgres DLQ payload"):
        _row_to_record(row)

    decoded = _row_to_record(row, payload_policy=policy)

    assert decoded.record == record.record
    assert decoded.original_record == record.original_record
    assert decoded.checkpoint == record.checkpoint
    assert decoded.details == record.details


@pytest.mark.asyncio
async def test_postgres_dlq_encryption_failure_does_not_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(
        dsn="postgresql://example.invalid/db",
        table="agora_dlq",
        payload_policy=DLQPayloadPolicy.encrypted(encryptor=_FailingCipher()),
    )

    await sink.open()
    with pytest.raises(RuntimeError, match="kms unavailable"):
        await sink.write(_make_record(record={"id": 1, "password": "plain-secret"}))

    assert not any("INSERT INTO" in sql for sql, _ in connection.cursor_obj.calls)


@pytest.mark.asyncio
async def test_postgres_dlq_sink_reconnects_after_connection_write_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _OperationalError(Exception):
        pass

    first_connection = _FakeConnection()
    first_connection.cursor_obj.insert_execute_errors.append(_OperationalError("connection closed"))
    second_connection = _FakeConnection()
    connections = [first_connection, second_connection]

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connections.pop(0)

    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(
            AsyncConnection=_AsyncConnection,
            OperationalError=_OperationalError,
            InterfaceError=RuntimeError,
        ),
    )
    monkeypatch.setitem(sys.modules, "psycopg.rows", SimpleNamespace(dict_row=object()))

    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")

    await sink.open()
    await sink.write(_make_record())

    assert first_connection.rollback_calls == 1
    assert first_connection.closed is True
    assert any("INSERT INTO" in call[0] for call in second_connection.cursor_obj.calls)
    assert second_connection.commit_calls == 2


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
    assert "ORDER BY id ASC LIMIT 1" in sql
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
    assert "ORDER BY id ASC LIMIT 1" in sql
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
            "details": '{"postgres": {"classification": "schema_drift"}}',
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
    assert records[0].details == {"postgres": {"classification": "schema_drift"}}
    select_sql, select_params = connection.cursor_obj.calls[-1]
    assert 'FROM "agora_dlq"' in select_sql
    assert select_params == ["orders", "sink_write", 10]


@pytest.mark.asyncio
async def test_postgres_dlq_source_reconnects_when_connection_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_connection = _FakeConnection()
    first_connection.closed = True
    second_connection = _FakeConnection()
    second_connection.cursor_obj.rows = [
        {
            "id": 99,
            "pipeline_id": "orders",
            "run_id": "run-1",
            "stage": "sink_write",
            "error_type": "RuntimeError",
            "error_message": "sink exploded",
            "record": '{"id": 1}',
            "original_record": None,
            "processed_record": None,
            "source": "orders_source",
            "checkpoint": '{"offset": 10}',
            "details": "{}",
            "middleware": None,
            "sink": "postgres",
            "created_at": datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
            "attempt": 1,
            "max_attempts": 5,
        }
    ]
    connections = [first_connection, second_connection]

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connections.pop(0)

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(AsyncConnection=_AsyncConnection))
    monkeypatch.setitem(sys.modules, "psycopg.rows", SimpleNamespace(dict_row=object()))

    source = PostgresDLQSource(dsn="postgresql://example.invalid/db", table="agora_dlq")

    await source.open()
    records = [record async for record in source.stream()]

    assert len(records) == 1
    assert records[0]._storage_id == 99
    assert source._conn is second_connection  # type: ignore[attr-defined]


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


@pytest.mark.asyncio
async def test_postgres_dlq_sink_replay_counter_updates_only_after_db_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UpdateError(Exception):
        pass

    class _FailingCursor(_FakeCursor):
        async def execute(self, sql: str, params: object | None = None) -> None:
            await super().execute(sql, params)
            if "UPDATE" in sql:
                raise _UpdateError("update failed")

    class _FailingConnection(_FakeConnection):
        def __init__(self) -> None:
            super().__init__()
            self.cursor_obj = _FailingCursor()

    connection = _FailingConnection()
    _install_fake_psycopg(monkeypatch, connection)
    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")

    await sink.open()
    with pytest.raises(_UpdateError):
        await sink.replay(_make_record())

    metrics = sink.metrics_snapshot()
    assert metrics.replay_count == 0
    assert metrics.replayed_record_count == 0


@pytest.mark.asyncio
async def test_postgres_dlq_source_uses_connection_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    connection = _FakeConnection()
    connect_calls: list[dict[str, object]] = []
    dict_row = object()
    password_file = tmp_path / "postgres-password.txt"
    password_file.write_text("dlq-secret\n", encoding="utf-8")

    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args
            connect_calls.append(dict(kwargs))
            return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(AsyncConnection=_AsyncConnection))
    monkeypatch.setitem(sys.modules, "psycopg.rows", SimpleNamespace(dict_row=dict_row))

    source = PostgresDLQSource(
        dsn=None,
        table="agora_dlq",
        connection=PostgresConnectionConfig(
            dsn="postgresql://example.invalid/db",
            auth=PostgresAuthConfig(password_file=str(password_file)),
            tls=PostgresTLSConfig(sslmode="verify-ca", root_cert_file="/etc/certs/pg-ca.pem"),
            connect_timeout_s=3,
            application_name="agora-dlq-source",
        ),
    )

    await source.open()

    assert connect_calls == [
        {
            "conninfo": "postgresql://example.invalid/db",
            "password": "dlq-secret",
            "sslmode": "verify-ca",
            "sslrootcert": "/etc/certs/pg-ca.pem",
            "connect_timeout": 3,
            "application_name": "agora-dlq-source",
            "row_factory": dict_row,
        }
    ]
