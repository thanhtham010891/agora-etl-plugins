from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from decimal import Decimal

import pytest
from agora.core.acceptance import AcceptanceReport
from agora.core.data_plane import DataPlane
from agora.core.health import ComponentHealthSnapshot
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from agora_plugins.bigquery import (
    BigQueryStorageWriteSink,
    BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds,
)
from agora_plugins.bigquery.sinks.storage_write import BigQueryStorageWriteSinkError


@dataclass(frozen=True, slots=True)
class _SchemaField:
    name: str
    field_type: str
    mode: str = "NULLABLE"
    fields: tuple[_SchemaField, ...] = field(default_factory=tuple)


class _FakeStream:
    def __init__(
        self, *, fail_with: Exception | None = None, fail_on_call: int | None = None
    ) -> None:
        self.fail_with = fail_with
        self.fail_on_call = fail_on_call
        self.calls: list[list[bytes]] = []
        self.closed = False

    def append_serialized_rows(
        self, serialized_rows: list[bytes], *, timeout: float | None
    ) -> int | None:
        del timeout
        if self.fail_on_call is not None and len(self.calls) + 1 == self.fail_on_call:
            assert self.fail_with is not None
            raise self.fail_with
        if self.fail_with is not None and self.fail_on_call is None:
            raise self.fail_with
        self.calls.append(list(serialized_rows))
        return len(self.calls) - 1

    def close(self) -> None:
        self.closed = True


class _FailingTableClient:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.closed = False

    def get_table(self, table: str) -> None:
        del table
        raise self.exc

    def close(self) -> None:
        self.closed = True


@dataclass
class _BuiltStream:
    stream_name: str
    descriptor_proto: descriptor_pb2.DescriptorProto
    stream: _FakeStream


def _build_stream_factory(stream: _FakeStream, built: list[_BuiltStream]):
    def factory(
        write_client: object, stream_name: str, descriptor_proto: descriptor_pb2.DescriptorProto
    ) -> _FakeStream:
        del write_client
        built.append(
            _BuiltStream(
                stream_name=stream_name,
                descriptor_proto=descriptor_pb2.DescriptorProto.FromString(
                    descriptor_proto.SerializeToString()
                ),
                stream=stream,
            )
        )
        return stream

    return factory


def _decode_rows(
    descriptor_proto: descriptor_pb2.DescriptorProto, payloads: list[bytes]
) -> list[dict[str, object]]:
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "test_bigquery_storage_write.proto"
    file_proto.package = "agora_plugins.bigquery.storagewrite"
    file_proto.syntax = "proto2"
    file_proto.message_type.add().CopyFrom(descriptor_proto)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    descriptor = pool.FindMessageTypeByName(f"{file_proto.package}.{descriptor_proto.name}")
    get_message_class = getattr(message_factory, "GetMessageClass", None)
    if callable(get_message_class):
        message_cls = get_message_class(descriptor)
    else:
        message_cls = message_factory.MessageFactory(pool).GetPrototype(descriptor)
    rows: list[dict[str, object]] = []
    for payload in payloads:
        message = message_cls()
        message.ParseFromString(payload)
        rows.append(_decode_message(message))
    return rows


def _decode_message(message: object) -> dict[str, object]:
    decoded: dict[str, object] = {}
    for descriptor, value in message.ListFields():
        if descriptor.is_repeated:
            decoded[descriptor.name] = [_decode_value(item) for item in value]
            continue
        decoded[descriptor.name] = _decode_value(value)
    return decoded


def _decode_value(value: object) -> object:
    if hasattr(value, "ListFields"):
        return _decode_message(value)
    return value


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_appends_rows_through_default_stream() -> None:
    stream = _FakeStream()
    built: list[_BuiltStream] = []
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(
            _SchemaField("id", "INT64", "REQUIRED"),
            _SchemaField("name", "STRING"),
        ),
        row_mapper=lambda record: record,
        batch_size=2,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, built),
    )

    await sink.write({"id": 1, "name": "alpha"})
    await sink.write({"id": 2, "name": "beta"})

    assert len(stream.calls) == 1
    assert built[0].stream_name == "projects/proj/datasets/analytics/tables/events/streams/_default"
    decoded = _decode_rows(built[0].descriptor_proto, stream.calls[0])
    assert decoded[0]["id"] == 1
    assert decoded[0]["name"] == "alpha"
    assert decoded[1]["id"] == 2
    assert decoded[1]["name"] == "beta"
    assert sink.metrics_snapshot().appended_row_count == 2
    assert sink.metrics_snapshot().last_append_offset == 0
    assert sink.data_plane_spec().accepted_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
    )


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_rejects_oversized_flush_before_append() -> None:
    stream = _FakeStream()
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(_SchemaField("payload", "STRING"),),
        row_mapper=lambda record: record,
        batch_size=10,
        max_request_bytes=1,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, []),
    )

    await sink.write({"payload": "too-large"})

    with pytest.raises(BigQueryStorageWriteSinkError, match="single row"):
        await sink.flush()

    assert stream.calls == []
    report = sink.acceptance_report(BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds())
    assert report.passed is False
    assert any(finding.metric == "last_append_succeeded" for finding in report.findings)
    assert any(finding.metric == "append_error_count" for finding in report.findings)


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_health_and_acceptance_report_track_successful_append() -> (
    None
):
    stream = _FakeStream()
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(_SchemaField("id", "INT64"),),
        row_mapper=lambda record: record,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, []),
    )

    await sink.write({"id": 1})
    await sink.flush()

    health = sink.health_snapshot()
    report = sink.acceptance_report(BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds())

    assert isinstance(health, ComponentHealthSnapshot)
    assert health.ready is True
    assert health.last_append_succeeded is True
    assert isinstance(report, AcceptanceReport)
    assert report.passed is True


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_counts_preflight_open_failures_as_append_errors() -> (
    None
):
    sink = BigQueryStorageWriteSink(
        table="proj.analytics.events",
        table_schema=(_SchemaField("id", "INT64", "REQUIRED"),),
        row_mapper=lambda record: record,
        write_client=object(),
        client=_FailingTableClient(PermissionError("dataset access denied")),
        validate_table_access=True,
    )

    await sink.write({"id": 1})

    with pytest.raises(PermissionError, match="dataset access denied"):
        await sink.flush()

    metrics = sink.metrics_snapshot()
    assert metrics.append_error_count == 1
    assert metrics.last_append_succeeded is False
    assert metrics.last_error == "dataset access denied"


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_phase2_serializes_typed_scalars() -> None:
    stream = _FakeStream()
    built: list[_BuiltStream] = []
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(
            _SchemaField("event_date", "DATE", "REQUIRED"),
            _SchemaField("event_ts", "TIMESTAMP", "REQUIRED"),
            _SchemaField("event_dt", "DATETIME", "REQUIRED"),
            _SchemaField("event_time", "TIME", "REQUIRED"),
            _SchemaField("amount", "NUMERIC", "REQUIRED"),
            _SchemaField("large_amount", "BIGNUMERIC", "REQUIRED"),
        ),
        row_mapper=lambda record: record,
        batch_size=1,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, built),
    )

    event_ts = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
    await sink.write(
        {
            "event_date": date(2026, 1, 2),
            "event_ts": event_ts,
            "event_dt": datetime(2026, 1, 2, 3, 4, 5, 654321),
            "event_time": time(6, 7, 8, 900000),
            "amount": Decimal("10.50"),
            "large_amount": Decimal("123456789.123456789123456789"),
        }
    )

    assert len(stream.calls) == 1
    decoded = _decode_rows(built[0].descriptor_proto, stream.calls[0])
    assert decoded[0]["event_date"] == (date(2026, 1, 2) - date(1970, 1, 1)).days
    assert decoded[0]["event_ts"] == int(event_ts.timestamp() * 1_000_000)
    assert decoded[0]["event_dt"] == "2026-01-02 03:04:05.654321"
    assert decoded[0]["event_time"] == "06:07:08.900000"
    assert decoded[0]["amount"] == "10.50"
    assert decoded[0]["large_amount"] == "123456789.123456789123456789"


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_phase2_serializes_json_geography_and_repeated_scalars() -> (
    None
):
    stream = _FakeStream()
    built: list[_BuiltStream] = []
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(
            _SchemaField("id", "INT64", "REQUIRED"),
            _SchemaField("payload_json", "JSON", "REQUIRED"),
            _SchemaField("location", "GEOGRAPHY", "REQUIRED"),
            _SchemaField("tags", "STRING", "REPEATED"),
            _SchemaField("attempts", "INT64", "REPEATED"),
            _SchemaField("payload_versions", "JSON", "REPEATED"),
        ),
        row_mapper=lambda record: record,
        batch_size=1,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, built),
    )

    await sink.write(
        {
            "id": 7,
            "payload_json": {"tenant": "acme", "active": True, "score": Decimal("9.50")},
            "location": "POINT(106.70098 10.77689)",
            "tags": ["ga", "storage-write"],
            "attempts": [1, 2, 3],
            "payload_versions": [
                {"status": "open"},
                ["a", "b"],
            ],
        }
    )

    assert len(stream.calls) == 1
    decoded = _decode_rows(built[0].descriptor_proto, stream.calls[0])
    assert decoded[0]["id"] == 7
    assert json.loads(decoded[0]["payload_json"]) == {
        "tenant": "acme",
        "active": True,
        "score": "9.50",
    }
    assert decoded[0]["location"] == "POINT(106.70098 10.77689)"
    assert decoded[0]["tags"] == ["ga", "storage-write"]
    assert decoded[0]["attempts"] == [1, 2, 3]
    assert decoded[0]["payload_versions"] == [
        '{"status":"open"}',
        '["a","b"]',
    ]


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_rejects_invalid_json_strings() -> None:
    stream = _FakeStream()
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(_SchemaField("payload_json", "JSON", "REQUIRED"),),
        row_mapper=lambda record: record,
        batch_size=1,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, []),
    )

    with pytest.raises(TypeError, match="valid JSON string"):
        await sink.write({"payload_json": '{"broken": }'})

    assert stream.calls == []


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_chunks_large_flushes_within_request_guard() -> None:
    stream = _FakeStream()
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(
            _SchemaField("id", "INT64", "REQUIRED"),
            _SchemaField("payload", "STRING", "REQUIRED"),
        ),
        row_mapper=lambda record: record,
        batch_size=10,
        max_request_bytes=256,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, []),
    )

    await sink.open()
    single_row_size = len(sink._serializer.serialize_row({"id": 1, "payload": "x" * 64}))
    sink._max_request_bytes = single_row_size + 1
    await sink.write_batch(
        [
            {"id": 1, "payload": "x" * 64},
            {"id": 2, "payload": "y" * 64},
            {"id": 3, "payload": "z" * 64},
        ]
    )
    await sink.flush()

    assert len(stream.calls) == 3
    assert sink.metrics_snapshot().flush_count == 3
    assert sink.metrics_snapshot().appended_row_count == 3
    assert sink.metrics_snapshot().buffered_row_count == 0


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_preserves_unsent_rows_after_partial_chunk_failure() -> (
    None
):
    stream = _FakeStream(
        fail_with=BigQueryStorageWriteSinkError(
            "append failed",
            stream_name="projects/proj/datasets/analytics/tables/events/streams/_default",
        ),
        fail_on_call=2,
    )
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(
            _SchemaField("id", "INT64", "REQUIRED"),
            _SchemaField("payload", "STRING", "REQUIRED"),
        ),
        row_mapper=lambda record: record,
        batch_size=10,
        max_request_bytes=256,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, []),
    )

    await sink.open()
    single_row_size = len(sink._serializer.serialize_row({"id": 1, "payload": "x" * 64}))
    sink._max_request_bytes = single_row_size + 1
    await sink.write_batch(
        [
            {"id": 1, "payload": "x" * 64},
            {"id": 2, "payload": "y" * 64},
            {"id": 3, "payload": "z" * 64},
        ]
    )

    with pytest.raises(BigQueryStorageWriteSinkError, match="append failed"):
        await sink.flush()

    metrics = sink.metrics_snapshot()
    assert len(stream.calls) == 1
    assert metrics.flush_count == 1
    assert metrics.appended_row_count == 1
    assert metrics.append_error_count == 1
    assert metrics.buffered_row_count == 2


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_acceptance_report_flags_failed_append() -> None:
    stream = _FakeStream(
        fail_with=BigQueryStorageWriteSinkError(
            "append failed",
            stream_name="projects/proj/datasets/analytics/tables/events/streams/_default",
        )
    )
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(_SchemaField("id", "INT64"),),
        row_mapper=lambda record: record,
        write_client=object(),
        stream_factory=_build_stream_factory(stream, []),
    )

    await sink.write({"id": 1})
    with pytest.raises(BigQueryStorageWriteSinkError, match="append failed"):
        await sink.flush()

    report = sink.acceptance_report(BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds())

    assert report.passed is False
    assert any(finding.metric == "last_append_succeeded" for finding in report.findings)
    assert any(finding.metric == "append_error_count" for finding in report.findings)


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_rejects_unsupported_schema_types() -> None:
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(_SchemaField("active_window", "INTERVAL"),),
        row_mapper=lambda record: record,
        write_client=object(),
        stream_factory=_build_stream_factory(_FakeStream(), []),
    )

    with pytest.raises(ValueError, match="phase 2 supports"):
        await sink.open()


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_rejects_record_schema_with_ga_boundary_error() -> None:
    sink = BigQueryStorageWriteSink(
        table="analytics.events",
        project="proj",
        table_schema=(
            _SchemaField(
                "metadata",
                "RECORD",
                fields=(_SchemaField("source", "STRING"),),
            ),
        ),
        row_mapper=lambda record: record,
        write_client=object(),
        stream_factory=_build_stream_factory(_FakeStream(), []),
    )

    with pytest.raises(ValueError, match="does not yet support RECORD fields"):
        await sink.open()
