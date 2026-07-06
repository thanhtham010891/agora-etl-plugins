from __future__ import annotations

import json

import pytest
from agora.core.data_plane import DataPlane

from agora_plugins.s3 import S3Sink


class _FakeClient:
    def __init__(self) -> None:
        self.objects: list[tuple[str, bytes]] = []

    def put_object(self, **kwargs: bytes | str) -> None:
        assert kwargs["Bucket"] == "demo-bucket"
        if kwargs.get("IfNoneMatch") == "*" and any(
            key == kwargs["Key"] for key, _ in self.objects
        ):
            raise RuntimeError("precondition failed")
        self.objects.append((str(kwargs["Key"]), kwargs["Body"]))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_s3_sink_partitions_and_rolls_files_by_record_limit() -> None:
    client = _FakeClient()
    sink = S3Sink(
        bucket="demo-bucket",
        prefix="exports",
        format="jsonl",
        row_mapper=lambda record: record,
        partition_path_fn=lambda record: f"dt={record['dt']}",
        max_records_per_file=2,
        flush_every=1,
        run_id="run-1",
        client=client,
    )

    await sink.write({"id": 1, "dt": "2026-07-06"})
    await sink.write({"id": 2, "dt": "2026-07-06"})
    await sink.write({"id": 3, "dt": "2026-07-07"})
    await sink.close()

    assert [key for key, _ in client.objects] == [
        "exports/run_id=run-1/dt=2026-07-06/part-00000.jsonl",
        "exports/run_id=run-1/dt=2026-07-07/part-00001.jsonl",
    ]
    first_payload = client.objects[0][1].decode("utf-8").strip().splitlines()
    assert [json.loads(line) for line in first_payload] == [
        {"id": 1, "dt": "2026-07-06"},
        {"id": 2, "dt": "2026-07-06"},
    ]
    assert sink.metrics_snapshot().uploaded_record_count == 3
    assert sink.data_plane_spec().accepted_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
    )


@pytest.mark.asyncio
async def test_s3_sink_requires_mapping_rows_for_csv_and_parquet() -> None:
    client = _FakeClient()
    sink = S3Sink(
        bucket="demo-bucket",
        format="csv",
        row_mapper=lambda record: "bad",
        client=client,
    )

    with pytest.raises(TypeError, match="dict\\[str, Any\\]"):
        await sink.write({"id": 1})
        await sink.close()


@pytest.mark.asyncio
async def test_s3_sink_restart_uses_new_run_identity_and_never_overwrites() -> None:
    client = _FakeClient()
    first = S3Sink(
        bucket="demo-bucket",
        prefix="exports",
        format="jsonl",
        run_id="run-1",
        client=client,
    )
    second = S3Sink(
        bucket="demo-bucket",
        prefix="exports",
        format="jsonl",
        run_id="run-2",
        client=client,
    )

    await first.write({"id": 1})
    await first.close()
    await second.write({"id": 2})
    await second.close()

    assert [key for key, _ in client.objects] == [
        "exports/run_id=run-1/part-00000.jsonl",
        "exports/run_id=run-2/part-00000.jsonl",
    ]


@pytest.mark.asyncio
async def test_s3_sink_fails_closed_when_a_run_key_already_exists() -> None:
    client = _FakeClient()
    first = S3Sink(bucket="demo-bucket", format="jsonl", run_id="run-1", client=client)
    retry = S3Sink(bucket="demo-bucket", format="jsonl", run_id="run-1", client=client)

    await first.write({"id": 1})
    await first.close()
    await retry.write({"id": 2})
    with pytest.raises(RuntimeError, match="precondition failed"):
        await retry.close()
