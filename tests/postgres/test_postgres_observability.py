from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from agora import Checkpoint
from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.health import ComponentHealthSnapshot
from agora.core.recovery import SourceRecoveryContractSnapshot, SourceRecoveryMode

from agora_plugins.postgres import (
    PostgresDLQSink,
    PostgresDLQSource,
    PostgresDLQSourceMetricsSnapshot,
    PostgresEnterpriseAcceptanceGate,
    PostgresPrometheusExporter,
    PostgresSink,
    PostgresSinkEnterpriseAcceptanceThresholds,
    PostgresSource,
    PostgresSourceEnterpriseAcceptanceThresholds,
    PostgresSourceRecoveryMode,
)


class _FakeCursor:
    def __init__(
        self,
        rows: list[dict[str, object]],
        batch_size: int = 2,
        fetchone_rows: list[dict[str, object]] | None = None,
    ) -> None:
        self._rows = rows
        self._batch_size = batch_size
        self._index = 0
        self._fetchone_rows = list(fetchone_rows or [])
        self.executed_query: str | None = None
        self.executed_params: dict[str, object] | None = None
        self.calls: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[object]]] = []

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, query: str, params: object | None = None) -> None:
        self.executed_query = query
        if isinstance(params, dict):
            self.executed_params = params
        self.calls.append((query, params))

    async def executemany(self, sql: str, params_seq: list[object]) -> None:
        self.executemany_calls.append((sql, params_seq))

    async def fetchone(self) -> dict[str, object] | None:
        if not self._fetchone_rows:
            return None
        return self._fetchone_rows.pop(0)

    async def fetchmany(self, size: int = 1) -> list[dict[str, object]]:
        del size
        if self._index >= len(self._rows):
            return []
        batch = self._rows[self._index : self._index + self._batch_size]
        self._index += self._batch_size
        return batch


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor, *, split_probe_cursors: bool = False) -> None:
        self.cursor_obj = cursor
        self._split_probe_cursors = split_probe_cursors
        self._cursor_calls = 0
        self._stream_cursor_assigned = False
        self._stream_cursor_call_index = 2
        if split_probe_cursors:
            probe_rows = list(cursor._fetchone_rows)
            cursor._fetchone_rows = []
            self._probe_fetchone_rows = probe_rows
            self._last_probe_row = (
                probe_rows[-1] if probe_rows else {"is_standby": False, "replay_lag_s": 0.0}
            )
        else:
            self._probe_fetchone_rows = []
            self._last_probe_row = {"is_standby": False, "replay_lag_s": 0.0}
        self.commit_calls = 0
        self.rollback_calls = 0
        self.closed = False
        self.cursor_invocations: list[dict[str, object]] = []

    async def __aenter__(self) -> _FakeConnection:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self, **kwargs: object) -> _FakeCursor:
        self.cursor_invocations.append(dict(kwargs))
        if not self._split_probe_cursors:
            return self.cursor_obj
        cursor_call = self._cursor_calls
        self._cursor_calls += 1
        if not self._stream_cursor_assigned and cursor_call == self._stream_cursor_call_index:
            self._stream_cursor_assigned = True
            return self.cursor_obj
        probe_row = (
            self._probe_fetchone_rows.pop(0) if self._probe_fetchone_rows else self._last_probe_row
        )
        return _FakeCursor([], fetchone_rows=[probe_row])

    async def commit(self) -> None:
        self.commit_calls += 1

    async def rollback(self) -> None:
        self.rollback_calls += 1

    async def close(self) -> None:
        self.closed = True


def _install_fake_psycopg(
    monkeypatch: pytest.MonkeyPatch,
    connection: _FakeConnection,
) -> None:
    class _AsyncConnection:
        @staticmethod
        async def connect(*args, **kwargs):
            del args, kwargs
            return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(AsyncConnection=_AsyncConnection))
    monkeypatch.setitem(sys.modules, "psycopg.rows", SimpleNamespace(dict_row=object()))


def _make_dlq_record(**overrides):
    from agora.core.dlq import DLQRecord

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
async def test_postgres_source_metrics_snapshot_declares_checkpoint_rerun_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [{"id": 2}, {"id": 3}],
        batch_size=1,
        fetchone_rows=[{"is_standby": False, "replay_lag_s": 0.0}],
    )
    connection = _FakeConnection(cursor, split_probe_cursors=True)
    _install_fake_psycopg(monkeypatch, connection)

    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events WHERE id > %(last_id)s ORDER BY id",
        params={"tenant": "demo"},
        row_mapper=lambda row, context: {"id": row["id"], "ctx": context["row_number"]},
        checkpoint_field="id",
        checkpoint_param="last_id",
        batch_size=1,
        statement_timeout_ms=5000,
        transaction_read_only=True,
        transaction_isolation_level="repeatable_read",
        fetch_strategy="server_side",
        server_side_cursor_withhold=True,
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="postgres",
            value={"cursor": 1},
        )
    )

    records = [record async for record in source.stream()]
    snapshot = source.metrics_snapshot()
    rendered = source.render_prometheus_metrics(namespace="agora_pg")

    assert records == [{"id": 2, "ctx": 1}, {"id": 3, "ctx": 2}]
    assert snapshot.rows_seen == 2
    assert snapshot.stream_run_count == 1
    assert snapshot.query_execution_count == 1
    assert snapshot.retry_count == 0
    assert snapshot.read_routing == "dsn"
    assert snapshot.resume_prepare_count == 1
    assert snapshot.resume_checkpoint_apply_count == 1
    assert snapshot.fetch_strategy == "server_side"
    assert snapshot.statement_timeout_ms == 5000
    assert snapshot.transaction_read_only is True
    assert snapshot.transaction_isolation_level == "repeatable_read"
    assert snapshot.server_side_cursor_withhold is True
    assert snapshot.ready is True
    assert snapshot.connection_ready is True
    assert snapshot.routing_ready is True
    assert snapshot.staleness_guard_ready is True
    assert snapshot.target_session_attrs == "dsn"
    assert snapshot.max_replica_replay_lag_s is None
    assert snapshot.on_replica_stale == "fail_closed"
    assert snapshot.connected_server_role == "primary"
    assert snapshot.last_replica_replay_lag_s == 0.0
    assert snapshot.staleness_guard_block_count == 0
    assert snapshot.staleness_guard_primary_fallback_count == 0
    assert snapshot.record_error_count == 0
    assert snapshot.record_drop_count == 0
    assert snapshot.active_stream_count == 0
    assert snapshot.last_stream_succeeded is True
    assert snapshot.recovery_contract.mode is PostgresSourceRecoveryMode.CHECKPOINT_RERUN
    assert snapshot.recovery_contract.supports_checkpoint is True
    assert snapshot.recovery_contract.requires_pipeline_rerun is True
    assert snapshot.recovery_contract.transparent_failover is False
    assert snapshot.recovery_contract.checkpoint_fields == ("id",)
    assert snapshot.recovery_contract.checkpoint_params == {"id": "last_id"}
    assert isinstance(snapshot.recovery_contract, SourceRecoveryContractSnapshot)
    assert snapshot.recovery_contract.mode is SourceRecoveryMode.CHECKPOINT_RERUN
    assert 'recovery_mode="checkpoint_rerun"' in rendered
    assert 'fetch_strategy="server_side"' in rendered
    assert 'read_routing="dsn"' in rendered
    assert 'target_session_attrs="dsn"' in rendered
    assert 'transaction_isolation_level="repeatable_read"' in rendered
    assert 'config="ready"' in rendered
    assert 'config="connection_ready"' in rendered
    assert 'gauge="statement_timeout_ms"' in rendered
    assert 'gauge="last_replica_replay_lag_s"' in rendered
    assert 'event="retry"' not in rendered
    assert 'event="rows_seen"' not in rendered
    assert 'event="resume_checkpoint_apply"' in rendered
    assert 'gauge="rows_seen_current_run"' in rendered
    assert 'gauge="record_error_count_current_run"' in rendered
    assert 'gauge="record_drop_count_current_run"' in rendered
    assert isinstance(await source.health_snapshot(), ComponentHealthSnapshot)


def test_postgres_enterprise_acceptance_gate_flags_source_and_sink_issues() -> None:
    source = PostgresSource(
        dsn="postgresql://example/test",
        query="SELECT id FROM events ORDER BY id",
        row_mapper=lambda row: row,
    )
    source_snapshot = source.metrics_snapshot()
    sink_snapshot = PostgresSink(
        dsn="postgresql://example.invalid/db",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="slug",
    ).metrics_snapshot()
    gate = PostgresEnterpriseAcceptanceGate()

    source_report = gate.evaluate_source(
        source_snapshot,
        PostgresSourceEnterpriseAcceptanceThresholds(
            require_checkpoint_support=True,
            require_ready=True,
            require_connection_ready=True,
            require_routing_ready=True,
            require_staleness_guard_ready=True,
        ),
    )
    sink_report = gate.evaluate_sink(
        sink_snapshot,
        PostgresSinkEnterpriseAcceptanceThresholds(require_connection_ready=True),
    )

    assert source_report.passed is False
    assert any(
        finding.metric == "recovery_contract.supports_checkpoint"
        for finding in source_report.findings
    )
    assert any(finding.metric == "ready" for finding in source_report.findings)
    assert any(finding.metric == "connection_ready" for finding in source_report.findings)
    assert sink_report.passed is False
    assert isinstance(source_report, AcceptanceReport)
    assert all(isinstance(finding, AcceptanceFinding) for finding in source_report.findings)
    assert any(finding.metric == "connection_ready" for finding in sink_report.findings)


@pytest.mark.asyncio
async def test_postgres_dlq_metrics_snapshots_and_prometheus_track_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dlq_rows = [
        {
            "id": 9,
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
            "details": '{"postgres": {"classification": "schema_drift"}}',
            "middleware": None,
            "sink": "postgres",
            "created_at": datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
            "attempt": 1,
            "max_attempts": 5,
        }
    ]
    cursor = _FakeCursor(
        dlq_rows,
        fetchone_rows=[
            {"id": 101, "inserted": True},
            {"id": 101, "inserted": False},
            {"id": 102, "inserted": True},
        ],
    )
    connection = _FakeConnection(cursor)
    _install_fake_psycopg(monkeypatch, connection)

    sink = PostgresDLQSink(dsn="postgresql://example.invalid/db", table="agora_dlq")
    await sink.open()
    record = _make_dlq_record()
    await sink.write(record)
    await sink.write_batch([record, _make_dlq_record(run_id="run-2")])
    replayed = await sink.replay(record)
    await sink.acknowledge(replayed)
    sink_snapshot = sink.metrics_snapshot()

    source = PostgresDLQSource(
        dsn="postgresql://example.invalid/db",
        table="agora_dlq",
        pipeline_id="orders",
        stage="sink_write",
        limit=10,
    )
    await source.open()
    scanned = [record async for record in source.stream()]
    source_snapshot = source.metrics_snapshot()

    exporter = PostgresPrometheusExporter(namespace="agora_pg")
    rendered_sink = exporter.render_dlq_sink(sink_snapshot)
    rendered_source = exporter.render_dlq_source(source_snapshot)

    assert len(scanned) == 1
    assert sink_snapshot.connection_ready is True
    assert sink_snapshot.table_ready is True
    assert sink_snapshot.write_call_count == 1
    assert sink_snapshot.write_batch_call_count == 1
    assert sink_snapshot.inserted_record_count == 2
    assert sink_snapshot.upserted_record_count == 3
    assert sink_snapshot.updated_record_count == 1
    assert sink_snapshot.replay_count == 1
    assert sink_snapshot.replayed_record_count == 1
    assert sink_snapshot.acknowledge_count == 1
    assert sink_snapshot.acknowledged_record_count == 1
    assert source_snapshot.connection_ready is True
    assert source_snapshot.scan_count == 1
    assert source_snapshot.emitted_record_count == 1
    assert 'event="inserted_record"' in rendered_sink
    assert 'event="upserted_record"' in rendered_sink
    assert 'event="updated_record"' in rendered_sink
    assert 'event="emitted_record"' in rendered_source


def test_postgres_dlq_source_metrics_keep_empty_pipeline_and_stage_labels() -> None:
    snapshot = PostgresDLQSourceMetricsSnapshot(
        table="agora_dlq",
        pipeline_id=None,
        stage=None,
        limit=None,
        connection_ready=True,
        scan_count=1,
        emitted_record_count=2,
    )

    rendered = PostgresPrometheusExporter(namespace="agora_pg").render_dlq_source(snapshot)

    assert (
        'agora_pg_dlq_source_config{table="agora_dlq",pipeline_id="",stage="",'
        'config="connection_ready"} 1'
    ) in rendered
    assert (
        'agora_pg_dlq_source_events_total{table="agora_dlq",pipeline_id="",stage="",'
        'event="emitted_record"} 2'
    ) in rendered
