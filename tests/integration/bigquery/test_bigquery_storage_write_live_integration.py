from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

import pytest

from agora_plugins.bigquery import (
    BigQuerySource,
    BigQueryStorageWriteSink,
    BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds,
)

from .test_bigquery_live_integration import (
    _assert_permission_denied,
    _bigquery_connection,
    _close_bigquery_client,
    _create_table,
    _delete_table,
    _google_bigquery_module,
    _optional_env_var,
    _require_integration_enabled,
    _table_id,
)


def _storage_write_typed_schema() -> list[Any]:
    bigquery = _google_bigquery_module()
    return [
        bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("event_dt", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("event_time", "TIME", mode="REQUIRED"),
        bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED"),
        bigquery.SchemaField("large_amount", "BIGNUMERIC", mode="REQUIRED"),
        bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
    ]


def _storage_write_breadth_schema() -> list[Any]:
    bigquery = _google_bigquery_module()
    return [
        bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("payload_json", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("location", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("attempts", "INT64", mode="REPEATED"),
        bigquery.SchemaField("payload_versions", "JSON", mode="REPEATED"),
    ]


def _storage_write_simple_schema() -> list[Any]:
    bigquery = _google_bigquery_module()
    return [
        bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
    ]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_storage_write_live_phase2_typed_rows_round_trip() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)
    sink: BigQueryStorageWriteSink[dict[str, Any]] | None = None

    try:
        await _create_table(connection, table_id, _storage_write_typed_schema())
        sink = BigQueryStorageWriteSink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=2,
            connection=connection,
        )

        await sink.write_batch(
            [
                {
                    "id": 1,
                    "event_date": date(2026, 1, 2),
                    "event_ts": datetime(2026, 1, 2, 1, 2, 3, 456789, tzinfo=UTC),
                    "event_dt": datetime(2026, 1, 2, 4, 5, 6, 123456),
                    "event_time": time(7, 8, 9, 654321),
                    "amount": Decimal("10.50"),
                    "large_amount": Decimal("123456789.123456789123456789"),
                    "payload": "alpha",
                },
                {
                    "id": 2,
                    "event_date": "2026-01-03",
                    "event_ts": "2026-01-03T10:11:12.130000+00:00",
                    "event_dt": "2026-01-03 14:15:16.170000",
                    "event_time": "18:19:20.210000",
                    "amount": "20.75",
                    "large_amount": "987654321.987654321987654321",
                    "payload": "beta",
                },
            ]
        )
        metrics = sink.metrics_snapshot()
        report = sink.acceptance_report(BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds())

        rows = [row async for row in source.stream()]

        assert [row["id"] for row in rows] == [1, 2]
        assert rows[0]["event_date"] == date(2026, 1, 2)
        assert rows[0]["event_ts"].isoformat() == "2026-01-02T01:02:03.456789+00:00"
        assert rows[0]["event_dt"].isoformat(sep=" ") == "2026-01-02 04:05:06.123456"
        assert rows[0]["event_time"].isoformat() == "07:08:09.654321"
        assert rows[0]["amount"] == Decimal("10.50")
        assert rows[0]["large_amount"] == Decimal("123456789.123456789123456789")
        assert rows[1]["event_date"] == date(2026, 1, 3)
        assert rows[1]["event_ts"].isoformat() == "2026-01-03T10:11:12.130000+00:00"
        assert rows[1]["event_dt"].isoformat(sep=" ") == "2026-01-03 14:15:16.170000"
        assert rows[1]["event_time"].isoformat() == "18:19:20.210000"
        assert rows[1]["amount"] == Decimal("20.75")
        assert rows[1]["large_amount"] == Decimal("987654321.987654321987654321")
        assert metrics.appended_row_count == 2
        assert metrics.flush_count == 1
        assert report.passed is True
    finally:
        if sink is not None:
            await sink.close()
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_storage_write_live_round_trips_json_geography_and_repeated_scalars() -> (
    None
):
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(
        query=(
            "SELECT "
            "id, "
            "TO_JSON_STRING(payload_json) AS payload_json, "
            "ST_ASGEOJSON(location) AS location_geojson, "
            "TO_JSON_STRING(tags) AS tags_json, "
            "TO_JSON_STRING(attempts) AS attempts_json, "
            "TO_JSON_STRING(payload_versions) AS payload_versions_json "
            f"FROM `{table_id}` "
            "ORDER BY id"
        ),
        connection=connection,
    )
    sink: BigQueryStorageWriteSink[dict[str, Any]] | None = None

    try:
        await _create_table(connection, table_id, _storage_write_breadth_schema())
        sink = BigQueryStorageWriteSink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=2,
            connection=connection,
        )

        await sink.write_batch(
            [
                {
                    "id": 1,
                    "payload_json": {"tenant": "acme", "active": True, "score": 9},
                    "location": "POINT(106.70098 10.77689)",
                    "tags": ["ga", "local"],
                    "attempts": [1, 2],
                    "payload_versions": [
                        {"status": "open"},
                        ["a", "b"],
                    ],
                },
                {
                    "id": 2,
                    "payload_json": '{"tenant":"beta","active":false}',
                    "location": {"type": "Point", "coordinates": [106.7015, 10.7775]},
                    "tags": ["geojson"],
                    "attempts": [3],
                    "payload_versions": [
                        {"status": "queued"},
                    ],
                },
            ]
        )

        rows = [row async for row in source.stream()]
        metrics = sink.metrics_snapshot()
        report = sink.acceptance_report(BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds())

        assert [row["id"] for row in rows] == [1, 2]
        assert json.loads(rows[0]["payload_json"]) == {"tenant": "acme", "active": True, "score": 9}
        assert json.loads(rows[0]["location_geojson"]) == {
            "type": "Point",
            "coordinates": [106.70098, 10.77689],
        }
        assert json.loads(rows[0]["tags_json"]) == ["ga", "local"]
        assert json.loads(rows[0]["attempts_json"]) == [1, 2]
        assert json.loads(rows[0]["payload_versions_json"]) == [
            {"status": "open"},
            ["a", "b"],
        ]
        assert json.loads(rows[1]["payload_json"]) == {"tenant": "beta", "active": False}
        assert json.loads(rows[1]["location_geojson"]) == {
            "type": "Point",
            "coordinates": [106.7015, 10.7775],
        }
        assert json.loads(rows[1]["tags_json"]) == ["geojson"]
        assert json.loads(rows[1]["attempts_json"]) == [3]
        assert json.loads(rows[1]["payload_versions_json"]) == [{"status": "queued"}]
        assert metrics.appended_row_count == 2
        assert metrics.flush_count == 1
        assert report.passed is True
    finally:
        if sink is not None:
            await sink.close()
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_storage_write_live_chunks_large_flushes_under_request_guard() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    connection, dataset = _bigquery_connection()
    table_id = _table_id(connection, dataset)
    source = BigQuerySource(table=table_id, order_by=["id"], connection=connection)

    try:
        schema = _storage_write_simple_schema()
        await _create_table(connection, table_id, schema)
        sink = BigQueryStorageWriteSink(
            table=table_id,
            row_mapper=lambda record: record,
            batch_size=10,
            max_request_bytes=256,
            table_schema=schema,
            connection=connection,
        )

        await sink.open()
        single_row_size = len(sink._serializer.serialize_row({"id": 1, "payload": "x" * 96}))
        sink._max_request_bytes = single_row_size + 1
        await sink.write_batch(
            [
                {"id": 1, "payload": "x" * 96},
                {"id": 2, "payload": "y" * 96},
                {"id": 3, "payload": "z" * 96},
                {"id": 4, "payload": "w" * 96},
            ]
        )
        await sink.close()

        rows = [row async for row in source.stream()]
        metrics = sink.metrics_snapshot()

        assert [row["id"] for row in rows] == [1, 2, 3, 4]
        assert metrics.appended_row_count == 4
        assert metrics.flush_count >= 2
        assert metrics.append_error_count == 0
    finally:
        await source.close()
        await _delete_table(connection, table_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bigquery_storage_write_live_denied_dataset_is_not_writable() -> None:
    _require_integration_enabled()
    _google_bigquery_module()
    denied_dataset = _optional_env_var("AGORA_TEST_BIGQUERY_DENIED_DATASET")
    if not denied_dataset:
        return

    connection, _ = _bigquery_connection()
    table_id = _table_id(connection, denied_dataset)
    sink = BigQueryStorageWriteSink(
        table=table_id,
        row_mapper=lambda record: record,
        batch_size=1,
        table_schema=_storage_write_simple_schema(),
        connection=connection,
    )

    try:
        with pytest.raises(Exception) as excinfo:
            await sink.write({"id": 1, "payload": "blocked"})
        _assert_permission_denied(excinfo.value, dataset=denied_dataset)
        metrics = sink.metrics_snapshot()
        report = sink.acceptance_report(BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds())
        assert report.passed is False
        assert metrics.append_error_count == 1
        assert any(
            finding.metric in {"connection_ready", "append_error_count", "flush_count"}
            for finding in report.findings
        )
    finally:
        await _close_bigquery_client(sink)
