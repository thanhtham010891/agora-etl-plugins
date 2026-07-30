from __future__ import annotations

import sys
from types import ModuleType

import pytest
from agora import DeliveryConfig, Pipeline
from agora.core.acceptance import AcceptanceReport
from agora.core.checkpoint import InMemoryCheckpointStore
from agora.core.data_plane import DataPlane
from agora.core.delivery import DeliveryPolicy, DeliveryPolicyMismatchError
from agora.core.health import ComponentHealthSnapshot
from agora.sources.file import CsvSource

from agora_plugins.bigquery import BigQuerySink, BigQuerySinkEnterpriseAcceptanceThresholds
from agora_plugins.bigquery.sinks.bigquery import BigQuerySinkWriteError


class _FakeLoadJob:
    def __init__(
        self,
        *,
        job_id: str = "load-1",
        errors: list[dict[str, object]] | None = None,
        result_error: Exception | None = None,
    ) -> None:
        self.job_id = job_id
        self.errors = errors
        self.result_error = result_error

    def result(self) -> None:
        if self.result_error is not None:
            raise self.result_error


class _FakeClient:
    def __init__(
        self,
        *,
        errors: list[dict[str, object]] | None = None,
        result_error: Exception | None = None,
    ) -> None:
        self.errors = errors
        self.result_error = result_error
        self.calls: list[tuple[list[dict[str, object]], str, object]] = []

    def load_table_from_json(
        self, rows: list[dict[str, object]], table: str, job_config: object
    ) -> _FakeLoadJob:
        self.calls.append((rows, table, job_config))
        return _FakeLoadJob(errors=self.errors, result_error=self.result_error)


class _LoadJobConfig:
    def __init__(
        self,
        *,
        write_disposition: str,
        create_disposition: str,
        autodetect: bool,
    ) -> None:
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.autodetect = autodetect


@pytest.fixture
def fake_bigquery_module(monkeypatch: pytest.MonkeyPatch) -> None:
    bigquery_module = ModuleType("google.cloud.bigquery")
    bigquery_module.LoadJobConfig = _LoadJobConfig
    bigquery_module.WriteDisposition = ModuleType("WriteDisposition")
    bigquery_module.WriteDisposition.WRITE_APPEND = "WRITE_APPEND"
    bigquery_module.WriteDisposition.WRITE_TRUNCATE = "WRITE_TRUNCATE"
    bigquery_module.CreateDisposition = ModuleType("CreateDisposition")
    bigquery_module.CreateDisposition.CREATE_IF_NEEDED = "CREATE_IF_NEEDED"
    bigquery_module.CreateDisposition.CREATE_NEVER = "CREATE_NEVER"
    cloud_module = ModuleType("google.cloud")
    cloud_module.bigquery = bigquery_module
    google_module = ModuleType("google")
    google_module.cloud = cloud_module
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)


@pytest.mark.asyncio
async def test_bigquery_sink_uses_truncate_only_for_first_flush(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient()
    sink = BigQuerySink(
        table="analytics.events",
        row_mapper=lambda record: record,
        batch_size=2,
        write_disposition="truncate",
        client=client,
    )

    await sink.write({"id": 1})
    await sink.write({"id": 2})
    await sink.write({"id": 3})
    await sink.flush()

    assert len(client.calls) == 2
    first_rows, first_table, first_job_config = client.calls[0]
    second_rows, second_table, second_job_config = client.calls[1]
    assert first_table == second_table == "analytics.events"
    assert first_rows == [{"id": 1}, {"id": 2}]
    assert second_rows == [{"id": 3}]
    assert first_job_config.write_disposition == "WRITE_TRUNCATE"
    assert second_job_config.write_disposition == "WRITE_APPEND"
    assert first_job_config.autodetect is True
    assert second_job_config.autodetect is True
    assert sink.metrics_snapshot().loaded_row_count == 3
    assert sink.data_plane_spec().accepted_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
    )


@pytest.mark.asyncio
async def test_bigquery_sink_raises_structured_error_when_load_job_reports_errors(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient(errors=[{"message": "bad row"}])
    sink = BigQuerySink(
        table="analytics.events",
        row_mapper=lambda record: record,
        client=client,
    )

    await sink.write({"id": 1})

    with pytest.raises(BigQuerySinkWriteError, match="row load errors"):
        await sink.flush()


@pytest.mark.asyncio
async def test_bigquery_sink_wraps_result_exceptions_with_job_metadata(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient(
        errors=[{"message": "bad row"}],
        result_error=RuntimeError("job failed"),
    )
    sink = BigQuerySink(
        table="analytics.events",
        row_mapper=lambda record: record,
        client=client,
    )

    await sink.write({"id": 1})

    with pytest.raises(BigQuerySinkWriteError, match="job failed") as excinfo:
        await sink.flush()

    assert excinfo.value.job_id == "load-1"
    assert excinfo.value.errors == [{"message": "bad row"}]


def test_bigquery_sink_rejects_invalid_write_disposition() -> None:
    with pytest.raises(ValueError, match="write_disposition"):
        BigQuerySink(table="analytics.events", write_disposition="merge")


@pytest.mark.asyncio
async def test_file_to_bigquery_profile_blocks_replay_safe_policy_before_sink_open(
    tmp_path,
) -> None:
    path = tmp_path / "records.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    pipeline = Pipeline(CsvSource(path=path, row_mapper=lambda row: row)).build(
        BigQuerySink(table="analytics.events"),
        config=DeliveryConfig(
            checkpoint=InMemoryCheckpointStore(),
            delivery_policy=DeliveryPolicy(
                require_replay_safe=True,
                require_idempotent_sinks=True,
            ),
        ),
    )

    with pytest.raises(DeliveryPolicyMismatchError) as exc_info:
        await pipeline.run()

    assert [mismatch.code for mismatch in exc_info.value.mismatches] == [
        "sink_not_replay_safe",
        "sink_not_idempotent",
    ]


@pytest.mark.asyncio
async def test_bigquery_sink_disables_autodetect_when_create_never(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient()
    sink = BigQuerySink(
        table="analytics.events",
        row_mapper=lambda record: record,
        create_disposition="create_never",
        client=client,
    )

    await sink.write({"id": 1})
    await sink.flush()

    job_config = client.calls[0][2]
    assert job_config.autodetect is False


@pytest.mark.asyncio
async def test_bigquery_sink_health_and_acceptance_report_track_successful_flush(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient()
    sink = BigQuerySink(
        table="analytics.events",
        row_mapper=lambda record: record,
        client=client,
    )

    await sink.write({"id": 1})
    await sink.flush()

    health = sink.health_snapshot()
    report = sink.acceptance_report(BigQuerySinkEnterpriseAcceptanceThresholds())

    assert isinstance(health, ComponentHealthSnapshot)
    assert health.ready is True
    assert health.flush_count == 1
    assert health.last_flush_succeeded is True
    assert isinstance(report, AcceptanceReport)
    assert report.passed is True


@pytest.mark.asyncio
async def test_bigquery_sink_acceptance_report_flags_failed_flush(
    fake_bigquery_module: None,
) -> None:
    client = _FakeClient(result_error=RuntimeError("job failed"))
    sink = BigQuerySink(
        table="analytics.events",
        row_mapper=lambda record: record,
        client=client,
    )

    await sink.write({"id": 1})
    with pytest.raises(BigQuerySinkWriteError):
        await sink.flush()

    report = sink.acceptance_report(BigQuerySinkEnterpriseAcceptanceThresholds())

    assert report.passed is False
    assert any(finding.metric == "last_flush_succeeded" for finding in report.findings)
    assert any(finding.metric == "flush_error_count" for finding in report.findings)
