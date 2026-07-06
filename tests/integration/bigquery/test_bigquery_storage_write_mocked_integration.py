from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from agora import DeliveryConfig, Pipeline
from agora.core.source import IterableSource

from agora_plugins.bigquery import BigQueryStorageWriteSink


@dataclass(frozen=True, slots=True)
class _SchemaField:
    name: str
    field_type: str
    mode: str = "NULLABLE"
    fields: tuple[_SchemaField, ...] = field(default_factory=tuple)


class _FakeStream:
    def __init__(self) -> None:
        self.calls: list[list[bytes]] = []

    def append_serialized_rows(
        self, serialized_rows: list[bytes], *, timeout: float | None
    ) -> int | None:
        del timeout
        self.calls.append(list(serialized_rows))
        return len(self.calls) - 1

    def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_bigquery_storage_write_sink_pipeline_mocked_round_trip() -> None:
    stream = _FakeStream()
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
        stream_factory=lambda write_client, stream_name, descriptor_proto: stream,
    )

    summary = await (
        Pipeline(IterableSource([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]))
        .build(sink, config=DeliveryConfig(batch_size=2))
        .run(max_records=2)
    )

    assert summary.records_written == 2
    assert sink.metrics_snapshot().appended_row_count == 2
    assert len(stream.calls) == 1
