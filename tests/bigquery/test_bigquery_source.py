from __future__ import annotations

import sys
from threading import Event
from types import ModuleType

import pytest
from agora import Checkpoint
from agora.core.acceptance import AcceptanceReport
from agora.core.checkpoint import SourceIdentityMismatchError
from agora.core.data_plane import DataPlane
from agora.core.health import ComponentHealthSnapshot
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.bigquery import BigQuerySource, BigQuerySourceEnterpriseAcceptanceThresholds


class _TrackedIterator:
    def __init__(
        self, rows: list[dict[str, object]], *, block_after_first: Event | None = None
    ) -> None:
        self._rows = rows
        self._index = 0
        self.next_calls = 0
        self._block_after_first = block_after_first

    def __iter__(self) -> _TrackedIterator:
        return self

    def __next__(self) -> dict[str, object]:
        if self._index >= len(self._rows):
            raise StopIteration
        if self._index >= 1 and self._block_after_first is not None:
            self._block_after_first.wait(timeout=1.0)
        self.next_calls += 1
        row = self._rows[self._index]
        self._index += 1
        return row


class _FakeQueryJob:
    def __init__(self, rows: object, job_id: str = "job-1") -> None:
        self._rows = rows
        self.job_id = job_id

    def result(self, *, page_size: int):
        assert page_size > 0
        return self._rows


class _FakeClient:
    def __init__(self, rows: object) -> None:
        self.rows = rows
        self.query_calls: list[tuple[str, object]] = []
        self.closed = False

    def query(self, query: str, job_config: object) -> _FakeQueryJob:
        self.query_calls.append((query, job_config))
        return _FakeQueryJob(self.rows)

    def close(self) -> None:
        self.closed = True


class _ScalarQueryParameter:
    def __init__(self, name: str, type_: str, value: object) -> None:
        self.name = name
        self.type_ = type_
        self.value = value


class _QueryJobConfig:
    def __init__(self, *, query_parameters: list[object]):
        self.query_parameters = query_parameters


@pytest.fixture
def fake_bigquery_module(monkeypatch: pytest.MonkeyPatch) -> None:
    bigquery_module = ModuleType("google.cloud.bigquery")
    bigquery_module.ScalarQueryParameter = _ScalarQueryParameter
    bigquery_module.QueryJobConfig = _QueryJobConfig
    cloud_module = ModuleType("google.cloud")
    cloud_module.bigquery = bigquery_module
    google_module = ModuleType("google")
    google_module.cloud = cloud_module
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)


@pytest.mark.asyncio
async def test_bigquery_source_builds_checkpointable_table_query(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient(
        [
            {"id": 3, "payload": "c"},
            {"id": 4, "payload": "d"},
        ]
    )
    source = BigQuerySource(
        table="analytics.events",
        row_mapper=lambda row: row,
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        order_by=["id"],
        query_parameters={"tenant": "demo"},
        client=client,
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="bigquery",
            value={"cursor": 2},
            source_identity=source.checkpoint_source_identity(),
        )
    )

    records = [record async for record in source.stream()]

    assert records == [
        {"id": 3, "payload": "c"},
        {"id": 4, "payload": "d"},
    ]
    query_text, job_config = client.query_calls[0]
    assert "FROM `analytics.events`" in query_text
    assert "WHERE id > @checkpoint_cursor" in query_text
    assert query_text.endswith("ORDER BY id")
    assert {param.name: param.value for param in job_config.query_parameters} == {
        "tenant": "demo",
        "checkpoint_cursor": 2,
    }
    assert source.current_checkpoint() == {"row_number": 2, "cursor": 4}
    assert source.recovery_contract().mode.value == "checkpoint_rerun"
    assert source.data_plane_spec().emitted_plane == DataPlane.PYTHON_ROWS


@pytest.mark.asyncio
async def test_bigquery_table_source_rejects_checkpoint_from_different_identity(
    fake_bigquery_module: None,
) -> None:
    original = BigQuerySource(
        table="analytics.orders",
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        client=_FakeClient([]),
    )
    resumed = BigQuerySource(
        table="analytics.payments",
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        client=_FakeClient([]),
    )

    with pytest.raises(SourceIdentityMismatchError, match="saved source identity differs"):
        await resumed.prepare_resume(
            Checkpoint(
                pipeline_id="events",
                run_id="run-1",
                source="bigquery",
                value={"cursor": 2},
                source_identity=original.checkpoint_source_identity(),
            )
        )


@pytest.mark.asyncio
async def test_bigquery_table_source_reset_policy_discards_mismatched_checkpoint(
    fake_bigquery_module: None,
) -> None:
    original = BigQuerySource(
        table="analytics.orders",
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        client=_FakeClient([]),
    )
    client = _FakeClient([{"id": 1}, {"id": 2}])
    resumed = BigQuerySource(
        table="analytics.payments",
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        client=client,
        source_identity_mismatch_policy="reset",
    )

    await resumed.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="bigquery",
            value={"cursor": 99},
            source_identity=original.checkpoint_source_identity(),
        )
    )

    assert [record async for record in resumed.stream()] == [{"id": 1}, {"id": 2}]
    assert "WHERE id > @checkpoint_cursor" not in client.query_calls[0][0]


@pytest.mark.asyncio
async def test_bigquery_source_query_mode_is_full_rerun_and_ignores_resume(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient([{"slug": "alpha"}])
    source = BigQuerySource(
        query="SELECT slug FROM analytics.events ORDER BY slug",
        row_mapper=lambda row: row["slug"],
        client=client,
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="bigquery",
            value={"cursor": "ignored"},
        )
    )

    records = [record async for record in source.stream()]

    assert records == ["alpha"]
    assert client.query_calls[0][0] == "SELECT slug FROM analytics.events ORDER BY slug"
    assert source.recovery_contract().mode.value == "full_rerun"
    assert source.current_checkpoint() == {"row_number": 1}


@pytest.mark.asyncio
async def test_bigquery_source_streams_rows_in_batches_instead_of_materializing_full_result(
    fake_bigquery_module: None,
) -> None:
    unblock = Event()
    rows = _TrackedIterator(
        [{"id": 1}, {"id": 2}, {"id": 3}],
        block_after_first=unblock,
    )
    client = _FakeClient(rows)
    source = BigQuerySource(
        query="SELECT id FROM analytics.events ORDER BY id",
        row_mapper=lambda row: row["id"],
        batch_size=1,
        client=client,
    )

    stream = source.stream()
    first = await anext(stream)

    assert first == 1
    assert rows.next_calls == 1

    unblock.set()
    await stream.aclose()


def test_bigquery_source_rejects_checkpoint_without_ordering() -> None:
    with pytest.raises(ValueError, match="order_by must start with checkpoint_column"):
        BigQuerySource(
            table="analytics.events",
            checkpoint_column="id",
            checkpoint_column_is_unique=True,
            order_by=["tenant_id"],
        )


@pytest.mark.asyncio
async def test_bigquery_source_log_and_continue_keeps_progress_and_drop_metrics(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient([{"id": 1}, {"id": 2}])

    def _map_row(row: dict[str, object]) -> dict[str, object]:
        if row["id"] == 2:
            raise ValueError("bad row")
        return row

    source = BigQuerySource(
        table="analytics.events",
        row_mapper=_map_row,
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
        client=client,
    )

    records = [record async for record in source.stream()]

    assert records == [{"id": 1}]
    assert source.current_checkpoint() == {"row_number": 2, "cursor": 1}
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }


@pytest.mark.asyncio
async def test_bigquery_source_health_and_acceptance_report_track_successful_stream(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient([{"id": 1}, {"id": 2}])
    source = BigQuerySource(
        table="analytics.events",
        row_mapper=lambda row: row,
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        order_by=["id"],
        client=client,
    )

    records = [record async for record in source.stream()]
    health = source.health_snapshot()
    report = source.acceptance_report(
        BigQuerySourceEnterpriseAcceptanceThresholds(require_checkpoint_support=True)
    )

    assert records == [{"id": 1}, {"id": 2}]
    assert isinstance(health, ComponentHealthSnapshot)
    assert health.ready is True
    assert health.query_executed is True
    assert health.last_stream_succeeded is True
    assert isinstance(report, AcceptanceReport)
    assert report.passed is True


def test_bigquery_source_acceptance_report_flags_unrun_query_mode() -> None:
    source = BigQuerySource(
        query="SELECT slug FROM analytics.events ORDER BY slug",
        row_mapper=lambda row: row["slug"],
    )

    report = source.acceptance_report(BigQuerySourceEnterpriseAcceptanceThresholds())

    assert report.passed is False
    assert any(finding.metric == "connection_ready" for finding in report.findings)
    assert any(finding.metric == "query_execution_count" for finding in report.findings)
    assert any(finding.metric == "last_stream_succeeded" for finding in report.findings)


def test_bigquery_source_requires_explicit_safe_checkpoint_strategy() -> None:
    with pytest.raises(ValueError, match="checkpoint_column_is_unique"):
        BigQuerySource(table="analytics.events", checkpoint_column="event_time")


@pytest.mark.asyncio
async def test_bigquery_source_composite_checkpoint_resumes_rows_with_duplicate_cursor(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient(
        [
            {"event_time": 42, "id": 2, "payload": "second"},
            {"event_time": 43, "id": 1, "payload": "next"},
        ]
    )
    source = BigQuerySource(
        table="analytics.events",
        row_mapper=lambda row: row,
        checkpoint_column="event_time",
        checkpoint_tiebreaker_column="id",
        order_by=["event_time", "id"],
        client=client,
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="bigquery",
            value={"cursor": {"cursor": 42, "tiebreaker_cursor": 1}},
            source_identity=source.checkpoint_source_identity(),
        )
    )

    records = [record async for record in source.stream()]

    assert records == [
        {"event_time": 42, "id": 2, "payload": "second"},
        {"event_time": 43, "id": 1, "payload": "next"},
    ]
    query_text, job_config = client.query_calls[0]
    assert "event_time > @checkpoint_cursor" in query_text
    assert "event_time = @checkpoint_cursor AND id > @checkpoint_tiebreaker_cursor" in query_text
    assert query_text.endswith("ORDER BY event_time, id")
    assert {param.name: param.value for param in job_config.query_parameters} == {
        "checkpoint_cursor": 42,
        "checkpoint_tiebreaker_cursor": 1,
    }
    assert source.current_checkpoint() == {
        "row_number": 2,
        "cursor": {"cursor": 43, "tiebreaker_cursor": 1},
    }
