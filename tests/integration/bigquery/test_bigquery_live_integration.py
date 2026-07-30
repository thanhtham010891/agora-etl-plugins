from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from agora import Checkpoint
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.bigquery import (
    BigQueryConnectionConfig,
    BigQuerySink,
    BigQuerySinkEnterpriseAcceptanceThresholds,
    BigQuerySinkWriteError,
    BigQuerySource,
    BigQuerySourceEnterpriseAcceptanceThresholds,
)
from agora_plugins.bigquery.config import build_bigquery_client


def _require_integration_enabled() -> None:
    if os.getenv("AGORA_RUN_INTEGRATION") != "1":
        pytest.skip("Set AGORA_RUN_INTEGRATION=1 to run integration tests.")


def _require_env_var(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        pytest.skip(f"Set {name}=... to run live BigQuery integration tests.")
    return value


def _optional_env_var(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def _google_bigquery_module() -> Any:
    return pytest.importorskip("google.cloud.bigquery")


def _bigquery_connection(
    dataset_env: str = "AGORA_TEST_BIGQUERY_DATASET",
) -> tuple[BigQueryConnectionConfig, str]:
    project = _require_env_var("AGORA_TEST_BIGQUERY_PROJECT")
    dataset = _require_env_var(dataset_env)
    location = os.getenv("AGORA_TEST_BIGQUERY_LOCATION", "US")
    credentials_path = os.getenv("AGORA_TEST_BIGQUERY_CREDENTIALS_PATH") or os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS"
    )
    if credentials_path:
        credentials_file = Path(credentials_path)
        if not credentials_file.exists():
            pytest.skip(f"Live BigQuery credentials file does not exist: {credentials_file}")
        return (
            BigQueryConnectionConfig(
                project=project,
                location=location,
                credentials_path=str(credentials_file),
            ),
            dataset,
        )
    return BigQueryConnectionConfig(project=project, location=location), dataset


def _bigquery_soak_cycles() -> int:
    raw_value = os.getenv("AGORA_TEST_BIGQUERY_SOAK_CYCLES", "2")
    try:
        return max(int(raw_value), 1)
    except ValueError:
        pytest.fail("AGORA_TEST_BIGQUERY_SOAK_CYCLES must be an integer >= 1.")


async def _close_bigquery_client(holder: Any) -> None:
    client = getattr(holder, "_client", None)
    close = getattr(client, "close", None)
    if callable(close):
        result = close()
        if asyncio.iscoroutine(result):
            await result


async def _delete_table(connection: BigQueryConnectionConfig, table_id: str) -> None:
    client = build_bigquery_client(connection)
    try:
        await asyncio.wait_for(
            asyncio.to_thread(
                client.delete_table,
                table_id,
                not_found_ok=True,
                retry=None,
                timeout=10.0,
            ),
            timeout=15.0,
        )
    finally:
        client.close()


async def _create_table(
    connection: BigQueryConnectionConfig,
    table_id: str,
    schema: list[Any],
) -> None:
    bigquery = _google_bigquery_module()
    client = build_bigquery_client(connection)
    try:
        table = bigquery.Table(table_id, schema=schema)
        await asyncio.wait_for(
            asyncio.to_thread(client.create_table, table, retry=None, timeout=10.0),
            timeout=15.0,
        )
    finally:
        client.close()
    await _wait_for_table(connection, table_id)


def _table_id(connection: BigQueryConnectionConfig, dataset: str) -> str:
    return f"{connection.project}.{dataset}.agora_it_{uuid.uuid4().hex[:12]}"


def _table_schema(*, include_optional_note: bool = False) -> list[Any]:
    bigquery = _google_bigquery_module()
    fields = [
        bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
    ]
    if include_optional_note:
        fields.append(bigquery.SchemaField("optional_note", "STRING", mode="NULLABLE"))
    return fields


def _typed_query_schema() -> list[Any]:
    bigquery = _google_bigquery_module()
    return [
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED"),
        bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
    ]


def _projection_schema() -> list[Any]:
    bigquery = _google_bigquery_module()
    return [
        bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("tenant", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    ]


def _permission_error_message(exc: BaseException) -> str:
    return str(exc).lower()


def _assert_permission_denied(exc: BaseException, *, dataset: str) -> None:
    message = _permission_error_message(exc)
    assert "permission" in message or "access denied" in message or "not found: dataset" in message
    assert dataset.lower() in message


def _assert_access_denied(exc: BaseException, *, resource_hint: str | None = None) -> None:
    message = _permission_error_message(exc)
    assert (
        "permission" in message
        or "access denied" in message
        or "not found" in message
        or "denied" in message
        or "forbidden" in message
    )
    if resource_hint:
        assert resource_hint.lower() in message


async def _wait_for_table(
    connection: BigQueryConnectionConfig,
    table_id: str,
    *,
    timeout_s: float = 15.0,
) -> None:
    client = build_bigquery_client(connection)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    last_exc: Exception | None = None
    try:
        while True:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(client.get_table, table_id, retry=None, timeout=10.0),
                    timeout=15.0,
                )
                return
            except Exception as exc:
                last_exc = exc
                if loop.time() >= deadline:
                    raise last_exc from exc
                await asyncio.sleep(0.5)
    finally:
        client.close()


async def _seed_rows(
    connection: BigQueryConnectionConfig,
    table_id: str,
    rows: list[dict[str, Any]],
    *,
    batch_size: int = 2,
    write_disposition: str = "truncate",
    create_disposition: str = "create_if_needed",
) -> BigQuerySink:
    sink = BigQuerySink(
        table=table_id,
        row_mapper=lambda record: record,
        batch_size=batch_size,
        write_disposition=write_disposition,
        create_disposition=create_disposition,
        connection=connection,
    )
    await sink.write_batch(list(rows))
    await sink.close()
    return sink


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_dataset_permissions_respect_allowed_and_denied_datasets() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    allowed_sink = BigQuerySink(
        table=table_id,
        row_mapper=lambda record: record,
        batch_size=1,
        write_disposition="append",
        connection=connection,
    )
    allowed_source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)
    denied_dataset = _optional_env_var("AGORA_TEST_BIGQUERY_DENIED_DATASET")
    denied_sink: BigQuerySink[dict[str, Any]] | None = None

    try:
        await allowed_sink.write({"id": 1, "payload": "allowed"})
        await allowed_sink.close()
        assert allowed_sink.metrics_snapshot().loaded_row_count == 1

        allowed_rows = [row async for row in allowed_source.stream()]
        assert allowed_rows == [{"id": 1, "payload": "allowed"}]

        if denied_dataset:
            denied_table_id = _table_id(connection, denied_dataset)
            denied_sink = BigQuerySink(
                table=denied_table_id,
                row_mapper=lambda record: record,
                batch_size=1,
                write_disposition="append",
                connection=connection,
            )
            with pytest.raises(Exception) as excinfo:
                await denied_sink.write({"id": 1, "payload": "blocked"})
            _assert_permission_denied(excinfo.value, dataset=denied_dataset)
    finally:
        if denied_sink is not None:
            await _close_bigquery_client(denied_sink)
        await allowed_source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_existing_table_matrix_supports_create_never_and_missing_table_failure() -> (
    None
):
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    table_source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)
    missing_table_id = _table_id(connection, dataset)
    missing_table_sink: BigQuerySink[dict[str, Any]] | None = None

    try:
        await _create_table(connection, table_id, _table_schema(include_optional_note=True))

        append_seed = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=2,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )
        truncate_seed = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=2,
            write_disposition="truncate",
            create_disposition="create_never",
            connection=connection,
        )

        await append_seed.write_batch(
            [
                {"id": 1, "payload": "alpha", "optional_note": None},
                {"id": 2, "payload": "beta", "optional_note": "seeded"},
            ]
        )
        await append_seed.close()
        append_metrics = append_seed.metrics_snapshot()
        assert append_metrics.loaded_row_count == 2
        assert append_metrics.last_job_id

        await truncate_seed.write_batch(
            [
                {"id": 3, "payload": "gamma", "optional_note": None},
                {"id": 4, "payload": "delta", "optional_note": "replacement"},
            ]
        )
        await truncate_seed.close()
        truncate_metrics = truncate_seed.metrics_snapshot()
        assert truncate_metrics.flush_count == 1
        assert truncate_metrics.loaded_row_count == 2

        all_rows = [row async for row in table_source.stream()]
        assert all_rows == [
            {"id": 3, "payload": "gamma", "optional_note": None},
            {"id": 4, "payload": "delta", "optional_note": "replacement"},
        ]

        missing_table_sink = BigQuerySink(
            table=missing_table_id,
            row_mapper=lambda record: record,
            batch_size=1,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )
        with pytest.raises(Exception, match=r"(?i)(not found|404|does not exist)"):
            await missing_table_sink.write({"id": 99, "payload": "missing"})
    finally:
        if missing_table_sink is not None:
            await _close_bigquery_client(missing_table_sink)
        await table_source.close()
        await _delete_table(connection, table_id)
        await _delete_table(connection, missing_table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_table_source_checkpoint_resume_across_multiple_cycles() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    table_source = BigQuerySource(
        table=table_id,
        checkpoint_column="id",
        checkpoint_column_is_unique=True,
        order_by=["id"],
        connection=connection,
    )

    try:
        await _seed_rows(
            connection,
            table_id,
            [
                {"id": 1, "payload": "alpha"},
                {"id": 2, "payload": "beta"},
                {"id": 3, "payload": "gamma"},
            ],
        )

        all_rows = [row async for row in table_source.stream()]
        assert all_rows == [
            {"id": 1, "payload": "alpha"},
            {"id": 2, "payload": "beta"},
            {"id": 3, "payload": "gamma"},
        ]
        assert table_source.current_checkpoint() == {"row_number": 3, "cursor": 3}
        assert table_source.metrics_snapshot().last_job_id
        assert table_source.recovery_contract().supports_checkpoint is True

        await table_source.prepare_resume(
            Checkpoint(
                pipeline_id="bigquery-local",
                run_id="resume-1",
                source="bigquery",
                value={"cursor": 1},
                source_identity=table_source.checkpoint_source_identity(),
            )
        )
        resumed_rows = [row async for row in table_source.stream()]
        assert resumed_rows == [
            {"id": 2, "payload": "beta"},
            {"id": 3, "payload": "gamma"},
        ]
        assert table_source.current_checkpoint() == {"row_number": 2, "cursor": 3}

        await table_source.prepare_resume(
            Checkpoint(
                pipeline_id="bigquery-local",
                run_id="resume-2",
                source="bigquery",
                value={"cursor": 2},
                source_identity=table_source.checkpoint_source_identity(),
            )
        )
        final_rows = [row async for row in table_source.stream()]
        assert final_rows == [{"id": 3, "payload": "gamma"}]
        assert table_source.current_checkpoint() == {"row_number": 1, "cursor": 3}
        assert table_source.metrics_snapshot().query_execution_count == 3
    finally:
        await table_source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_query_source_reruns_full_query_after_resume_prepare() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    query_source = BigQuerySource(
        query=(f"SELECT id, payload FROM `{table_id}` WHERE id >= @minimum_id ORDER BY id"),
        query_parameters={"minimum_id": 2},
        connection=connection,
    )

    try:
        await _seed_rows(
            connection,
            table_id,
            [
                {"id": 1, "payload": "alpha"},
                {"id": 2, "payload": "beta"},
                {"id": 3, "payload": "gamma"},
            ],
        )

        first_rows = [row async for row in query_source.stream()]
        assert first_rows == [
            {"id": 2, "payload": "beta"},
            {"id": 3, "payload": "gamma"},
        ]
        assert query_source.current_checkpoint() == {"row_number": 2}
        assert query_source.recovery_contract().supports_checkpoint is False

        await query_source.prepare_resume(
            Checkpoint(
                pipeline_id="bigquery-local",
                run_id="query-rerun-1",
                source="bigquery",
                value={"cursor": 999},
            )
        )
        rerun_rows = [row async for row in query_source.stream()]
        assert rerun_rows == first_rows
        assert query_source.current_checkpoint() == {"row_number": 2}
        assert query_source.metrics_snapshot().query_execution_count == 2
    finally:
        await query_source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_query_source_multi_page_batching_reports_enterprise_ready_state() -> (
    None
):
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(
        query=f"SELECT id, payload FROM `{table_id}` ORDER BY id",
        row_mapper=lambda row: row["id"],
        batch_size=7,
        connection=connection,
    )

    try:
        await _seed_rows(
            connection,
            table_id,
            [{"id": idx, "payload": f"row-{idx}"} for idx in range(1, 124)],
            batch_size=25,
        )

        rows = [row async for row in source.stream()]
        metrics = source.metrics_snapshot()
        health = source.health_snapshot()
        report = source.acceptance_report(BigQuerySourceEnterpriseAcceptanceThresholds())

        assert rows == list(range(1, 124))
        assert metrics.query_execution_count == 1
        assert metrics.rows_seen == 123
        assert metrics.emitted_record_count == 123
        assert metrics.last_job_id
        assert health.ready is True
        assert health.last_stream_succeeded is True
        assert report.passed is True
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_query_source_supports_date_timestamp_and_numeric_parameters() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    query_source = BigQuerySource(
        query=(
            f"SELECT payload FROM `{table_id}` "
            "WHERE event_date >= @start_date "
            "AND event_ts >= @start_ts "
            "AND amount >= @minimum_amount "
            "ORDER BY payload"
        ),
        query_parameters={
            "start_date": date(2026, 1, 2),
            "start_ts": datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
            "minimum_amount": Decimal("10.50"),
        },
        row_mapper=lambda row: row["payload"],
        connection=connection,
    )

    try:
        await _create_table(connection, table_id, _typed_query_schema())
        await _seed_rows(
            connection,
            table_id,
            [
                {
                    "event_date": "2026-01-01",
                    "event_ts": "2026-01-01T00:00:00+00:00",
                    "amount": "9.50",
                    "payload": "below-threshold",
                },
                {
                    "event_date": "2026-01-02",
                    "event_ts": "2026-01-02T01:00:00+00:00",
                    "amount": "10.50",
                    "payload": "match-a",
                },
                {
                    "event_date": "2026-01-03",
                    "event_ts": "2026-01-03T06:30:00+00:00",
                    "amount": "18.75",
                    "payload": "match-b",
                },
            ],
            create_disposition="create_never",
        )

        rows = [row async for row in query_source.stream()]
        assert rows == ["match-a", "match-b"]
        metrics = query_source.metrics_snapshot()
        assert metrics.query_execution_count == 1
        assert metrics.last_job_id
    finally:
        await query_source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_query_source_empty_result_still_records_execution() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    query_source = BigQuerySource(
        query=f"SELECT id, payload FROM `{table_id}` WHERE id > @minimum_id ORDER BY id",
        query_parameters={"minimum_id": 999},
        connection=connection,
    )

    try:
        await _seed_rows(
            connection,
            table_id,
            [
                {"id": 1, "payload": "alpha"},
                {"id": 2, "payload": "beta"},
            ],
        )

        rows = [row async for row in query_source.stream()]
        assert rows == []
        assert query_source.current_checkpoint() is None
        metrics = query_source.metrics_snapshot()
        assert metrics.query_execution_count == 1
        assert metrics.last_job_id
        assert metrics.rows_seen == 0
    finally:
        await query_source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_table_source_projects_columns_and_honors_multi_column_ordering() -> (
    None
):
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(
        table=table_id,
        columns=["tenant", "payload"],
        order_by=["tenant", "payload"],
        connection=connection,
    )

    try:
        await _create_table(connection, table_id, _projection_schema())
        await _seed_rows(
            connection,
            table_id,
            [
                {
                    "id": 2,
                    "tenant": "tenant-b",
                    "payload": "bravo",
                    "updated_at": "2026-01-03T00:00:00+00:00",
                },
                {
                    "id": 1,
                    "tenant": "tenant-a",
                    "payload": "charlie",
                    "updated_at": "2026-01-02T00:00:00+00:00",
                },
                {
                    "id": 3,
                    "tenant": "tenant-a",
                    "payload": "alpha",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                },
            ],
            create_disposition="create_never",
        )

        rows = [row async for row in source.stream()]
        assert rows == [
            {"tenant": "tenant-a", "payload": "alpha"},
            {"tenant": "tenant-a", "payload": "charlie"},
            {"tenant": "tenant-b", "payload": "bravo"},
        ]
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_query_mode_denied_dataset_is_not_readable() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    denied_dataset = _optional_env_var("AGORA_TEST_BIGQUERY_DENIED_DATASET")
    if not denied_dataset:
        return

    connection, _ = _bigquery_connection()
    denied_table_id = _table_id(connection, denied_dataset)
    source = BigQuerySource(
        query=f"SELECT id, payload FROM `{denied_table_id}` ORDER BY id",
        connection=connection,
    )

    try:
        with pytest.raises(Exception) as excinfo:
            _ = [row async for row in source.stream()]
        _assert_permission_denied(excinfo.value, dataset=denied_dataset)
    finally:
        await source.close()


def _mapper_with_drop_and_error(row: dict[str, Any]) -> dict[str, Any] | None:
    if row["id"] == 2:
        raise ValueError("bad row")
    if row["id"] == 3:
        return None
    return row


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_source_log_and_continue_tracks_error_and_drop_metrics() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(
        table=table_id,
        order_by=["id"],
        row_mapper=_mapper_with_drop_and_error,
        on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
        connection=connection,
    )

    try:
        await _seed_rows(
            connection,
            table_id,
            [
                {"id": 1, "payload": "alpha"},
                {"id": 2, "payload": "beta"},
                {"id": 3, "payload": "gamma"},
                {"id": 4, "payload": "delta"},
            ],
        )

        rows = [row async for row in source.stream()]
        assert rows == [
            {"id": 1, "payload": "alpha"},
            {"id": 4, "payload": "delta"},
        ]
        metrics = source.metrics_snapshot()
        assert metrics.record_error_count == 1
        assert metrics.record_drop_count == 2
        assert metrics.emitted_record_count == 2
        assert source.runtime_metrics().record_error_count == 1
        assert source.runtime_metrics().record_drop_count == 2
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_recovery_after_failed_flush_preserves_prior_rows() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    table_source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)
    failed_sink: BigQuerySink[dict[str, Any]] | None = None

    try:
        await _create_table(connection, table_id, _table_schema())

        await _seed_rows(
            connection,
            table_id,
            [{"id": 1, "payload": "alpha"}],
            create_disposition="create_never",
        )

        failed_sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=1,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )
        with pytest.raises(BigQuerySinkWriteError) as excinfo:
            await failed_sink.write({"id": {"nested": "bad"}, "payload": "broken"})
        assert excinfo.value.job_id
        assert "json" in str(excinfo.value).lower()
        failed_report = failed_sink.acceptance_report(BigQuerySinkEnterpriseAcceptanceThresholds())
        assert failed_report.passed is False
        assert any(finding.metric == "last_flush_succeeded" for finding in failed_report.findings)
        assert any(finding.metric == "flush_error_count" for finding in failed_report.findings)

        recovery_sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=1,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )
        await recovery_sink.write({"id": 2, "payload": "recovered"})
        await recovery_sink.close()
        recovery_metrics = recovery_sink.metrics_snapshot()
        assert recovery_metrics.loaded_row_count == 1
        recovery_report = recovery_sink.acceptance_report(
            BigQuerySinkEnterpriseAcceptanceThresholds()
        )
        assert recovery_report.passed is True

        rows = [row async for row in table_source.stream()]
        assert rows == [
            {"id": 1, "payload": "alpha"},
            {"id": 2, "payload": "recovered"},
        ]
    finally:
        if failed_sink is not None:
            await _close_bigquery_client(failed_sink)
        await table_source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_rejects_schema_drift_against_existing_table() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)

    try:
        await _create_table(connection, table_id, _table_schema())
        drift_sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=1,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )

        with pytest.raises(BigQuerySinkWriteError) as excinfo:
            await drift_sink.write(
                {"id": 1, "payload": "alpha", "unexpected_field": "schema-drift"}
            )
        assert excinfo.value.job_id
        assert "field" in str(excinfo.value).lower()

        rows = [row async for row in source.stream()]
        assert rows == []
        report = drift_sink.acceptance_report(BigQuerySinkEnterpriseAcceptanceThresholds())
        assert report.passed is False
        assert any(finding.metric == "last_flush_succeeded" for finding in report.findings)
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_rejects_missing_required_field_and_keeps_table_empty() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)

    try:
        await _create_table(connection, table_id, _table_schema())
        sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=1,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )

        with pytest.raises(BigQuerySinkWriteError) as excinfo:
            await sink.write({"id": 1})
        assert excinfo.value.job_id
        assert "payload" in str(excinfo.value).lower()

        rows = [row async for row in source.stream()]
        assert rows == []
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_data_shape_matrix_handles_nullable_fields_and_multi_flush_batches() -> (
    None
):
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)

    try:
        await _create_table(connection, table_id, _table_schema(include_optional_note=True))
        sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=3,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )

        await sink.write_batch(
            [
                {"id": 1, "payload": "alpha", "optional_note": None},
                {"id": 2, "payload": "beta", "optional_note": "note-2"},
                {"id": 3, "payload": "gamma", "optional_note": None},
                {"id": 4, "payload": "delta", "optional_note": "note-4"},
                {"id": 5, "payload": "epsilon", "optional_note": None},
            ]
        )
        await sink.close()
        metrics = sink.metrics_snapshot()
        assert metrics.flush_count == 1
        assert metrics.loaded_row_count == 5
        assert metrics.last_job_id

        rows = [row async for row in source.stream()]
        assert rows == [
            {"id": 1, "payload": "alpha", "optional_note": None},
            {"id": 2, "payload": "beta", "optional_note": "note-2"},
            {"id": 3, "payload": "gamma", "optional_note": None},
            {"id": 4, "payload": "delta", "optional_note": "note-4"},
            {"id": 5, "payload": "epsilon", "optional_note": None},
        ]
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_truncate_multi_flush_replaces_stale_rows_once_then_appends() -> (
    None
):
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)

    try:
        await _create_table(connection, table_id, _table_schema())
        await _seed_rows(
            connection,
            table_id,
            [
                {"id": 0, "payload": "stale-a"},
                {"id": 99, "payload": "stale-b"},
            ],
            create_disposition="create_never",
        )

        sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=2,
            write_disposition="truncate",
            create_disposition="create_never",
            connection=connection,
        )

        for record in [
            {"id": 1, "payload": "alpha"},
            {"id": 2, "payload": "beta"},
            {"id": 3, "payload": "gamma"},
            {"id": 4, "payload": "delta"},
            {"id": 5, "payload": "epsilon"},
        ]:
            await sink.write(record)
        await sink.close()

        metrics = sink.metrics_snapshot()
        assert metrics.flush_count == 3
        assert metrics.loaded_row_count == 5
        assert metrics.last_job_id

        rows = [row async for row in source.stream()]
        assert rows == [
            {"id": 1, "payload": "alpha"},
            {"id": 2, "payload": "beta"},
            {"id": 3, "payload": "gamma"},
            {"id": 4, "payload": "delta"},
            {"id": 5, "payload": "epsilon"},
        ]
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_readonly_table_allows_reads_but_rejects_writes() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    readonly_table = _optional_env_var("AGORA_TEST_BIGQUERY_READONLY_TABLE")
    if not readonly_table:
        return

    connection, _ = _bigquery_connection()
    source = BigQuerySource(table=readonly_table, order_by=["id"], connection=connection)
    sink = BigQuerySink(
        table=readonly_table,
        row_mapper=lambda record: record,
        batch_size=1,
        write_disposition="append",
        create_disposition="create_never",
        connection=connection,
    )

    try:
        rows = [row async for row in source.stream()]
        assert rows
        assert (
            source.acceptance_report(BigQuerySourceEnterpriseAcceptanceThresholds()).passed is True
        )

        with pytest.raises(Exception) as excinfo:
            await sink.write({"id": 999999, "payload": "write-should-fail"})
        _assert_access_denied(excinfo.value, resource_hint=readonly_table.split(".")[-1])
        sink_report = sink.acceptance_report(BigQuerySinkEnterpriseAcceptanceThresholds())
        assert sink_report.passed is False
        assert any(finding.metric == "flush_error_count" for finding in sink_report.findings)
    finally:
        await source.close()
        await _close_bigquery_client(sink)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_streaming_append_keeps_duplicate_business_keys_across_flushes() -> (
    None
):
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id", "payload"], connection=connection)

    try:
        await _create_table(connection, table_id, _table_schema())
        sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=2,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )

        await sink.write({"id": 1, "payload": "first-copy"})
        await sink.write({"id": 1, "payload": "second-copy"})
        await sink.write({"id": 2, "payload": "unique"})
        await sink.close()

        metrics = sink.metrics_snapshot()
        assert metrics.flush_count == 2
        assert metrics.loaded_row_count == 3
        assert metrics.last_job_id

        rows = [row async for row in source.stream()]
        assert rows == [
            {"id": 1, "payload": "first-copy"},
            {"id": 1, "payload": "second-copy"},
            {"id": 2, "payload": "unique"},
        ]
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_sink_handles_large_payload_batches_across_multiple_flushes() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)
    payload_bytes = 16 * 1024
    row_count = 60

    try:
        await _create_table(connection, table_id, _table_schema())
        sink = BigQuerySink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=25,
            write_disposition="append",
            create_disposition="create_never",
            connection=connection,
        )

        for idx in range(row_count):
            await sink.write({"id": idx + 1, "payload": f"row-{idx}-" + ("x" * payload_bytes)})
        await sink.close()

        metrics = sink.metrics_snapshot()
        assert metrics.flush_count == 3
        assert metrics.loaded_row_count == row_count
        assert metrics.submitted_row_count == row_count
        assert metrics.last_job_id

        rows = [row async for row in source.stream()]
        assert len(rows) == row_count
        assert rows[0]["id"] == 1
        assert rows[-1]["id"] == row_count
        assert rows[0]["payload"].startswith("row-0-")
        assert len(rows[0]["payload"]) >= payload_bytes
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_table_source_surfaces_missing_table_not_found() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    missing_table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=missing_table_id, order_by=["id"], connection=connection)

    try:
        with pytest.raises(Exception, match=r"(?i)(not found|404|does not exist)"):
            _ = [row async for row in source.stream()]
    finally:
        await source.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_live_soak_cycles_round_trip_create_read_and_cleanup() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    cycles = _bigquery_soak_cycles()
    sink_job_ids: set[str] = set()
    source_job_ids: set[str] = set()

    for cycle in range(cycles):
        table_id = _table_id(connection, dataset)
        source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)
        try:
            sink = await _seed_rows(
                connection,
                table_id,
                [
                    {"id": cycle * 10 + 1, "payload": f"alpha-{cycle}"},
                    {"id": cycle * 10 + 2, "payload": f"beta-{cycle}"},
                    {"id": cycle * 10 + 3, "payload": f"gamma-{cycle}"},
                ],
                batch_size=2,
            )
            sink_metrics = sink.metrics_snapshot()
            assert sink_metrics.flush_count == 1
            assert sink_metrics.loaded_row_count == 3
            assert sink_metrics.last_job_id
            sink_job_ids.add(sink_metrics.last_job_id)

            rows = [row async for row in source.stream()]
            assert rows == [
                {"id": cycle * 10 + 1, "payload": f"alpha-{cycle}"},
                {"id": cycle * 10 + 2, "payload": f"beta-{cycle}"},
                {"id": cycle * 10 + 3, "payload": f"gamma-{cycle}"},
            ]
            source_metrics = source.metrics_snapshot()
            assert source_metrics.last_job_id
            source_job_ids.add(source_metrics.last_job_id)
        finally:
            await source.close()
            await _delete_table(connection, table_id)

    assert len(sink_job_ids) == cycles
    assert len(source_job_ids) == cycles
