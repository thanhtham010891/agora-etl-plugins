from __future__ import annotations

import io

import pytest
from agora import Checkpoint, DeliveryConfig, Pipeline
from agora.core.checkpoint import InMemoryCheckpointStore, SourceIdentityMismatchError
from agora.core.data_plane import DataPlane
from agora.core.delivery import DeliveryPolicy, DeliveryPolicyMismatchError
from agora.sources.file import CsvSource

from agora_plugins.s3 import S3Sink, S3Source


class _FakePaginator:
    def __init__(self, pages: list[dict[str, object]]) -> None:
        self._pages = pages

    def paginate(self, **kwargs: object):
        assert "Bucket" in kwargs
        return list(self._pages)


class _FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._stream = io.BytesIO(payload)
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return self._stream.read(size)


class _FakeClient:
    def __init__(self, *, pages: list[dict[str, object]], objects: dict[str, bytes]) -> None:
        self._pages = pages
        self._objects = objects

    def get_paginator(self, name: str) -> _FakePaginator:
        assert name == "list_objects_v2"
        return _FakePaginator(self._pages)

    def get_object(self, **kwargs: str) -> dict[str, object]:
        assert kwargs["Bucket"] == "demo-bucket"
        key = str(kwargs["Key"])
        return {"Body": _FakeBody(self._objects[key])}


@pytest.mark.asyncio
async def test_s3_source_resumes_from_next_object_boundary() -> None:
    client = _FakeClient(
        pages=[
            {"Contents": [{"Key": "prefix/0001.jsonl"}, {"Key": "prefix/0002.jsonl"}]},
        ],
        objects={
            "prefix/0001.jsonl": b'{"id": 1}\n{"id": 2}\n',
            "prefix/0002.jsonl": b'{"id": 3}\n',
        },
    )
    source = S3Source(
        bucket="demo-bucket",
        prefix="prefix/",
        format="jsonl",
        client=client,
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="s3",
            value={"object_key": "prefix/0001.jsonl"},
            source_identity=source.checkpoint_source_identity(),
        )
    )

    records = [record async for record in source.stream()]

    assert records == [{"id": 3}]
    assert source.current_checkpoint() == {"object_key": "prefix/0002.jsonl"}
    assert source.recovery_contract().mode.value == "checkpoint_rerun"
    assert source.data_plane_spec().emitted_plane == DataPlane.PYTHON_ROWS


@pytest.mark.asyncio
async def test_s3_source_rejects_checkpoint_from_different_dataset_identity() -> None:
    original = S3Source(bucket="demo-bucket", prefix="orders/", format="jsonl", client=object())
    resumed = S3Source(bucket="demo-bucket", prefix="payments/", format="jsonl", client=object())

    with pytest.raises(SourceIdentityMismatchError, match="saved source identity differs"):
        await resumed.prepare_resume(
            Checkpoint(
                pipeline_id="events",
                run_id="run-1",
                source="s3",
                value={"object_key": "orders/0001.jsonl"},
                source_identity=original.checkpoint_source_identity(),
            )
        )


@pytest.mark.asyncio
async def test_s3_source_reset_policy_discards_mismatched_checkpoint() -> None:
    client = _FakeClient(
        pages=[{"Contents": [{"Key": "payments/0001.jsonl"}, {"Key": "payments/0002.jsonl"}]}],
        objects={
            "payments/0001.jsonl": b'{"id": 1}\n',
            "payments/0002.jsonl": b'{"id": 2}\n',
        },
    )
    original = S3Source(bucket="demo-bucket", prefix="orders/", format="jsonl", client=object())
    resumed = S3Source(
        bucket="demo-bucket",
        prefix="payments/",
        format="jsonl",
        client=client,
        source_identity_mismatch_policy="reset",
    )

    await resumed.prepare_resume(
        Checkpoint(
            pipeline_id="events",
            run_id="run-1",
            source="s3",
            value={"object_key": "orders/0001.jsonl"},
            source_identity=original.checkpoint_source_identity(),
        )
    )

    assert [record async for record in resumed.stream()] == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_file_to_s3_profile_blocks_replay_safe_policy_before_sink_open(tmp_path) -> None:
    path = tmp_path / "records.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    sink = S3Sink(bucket="analytics", format="jsonl", client=object())
    pipeline = Pipeline(CsvSource(path=path, row_mapper=lambda row: row)).build(
        sink,
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
async def test_s3_source_tracks_completed_object_only_after_success() -> None:
    client = _FakeClient(
        pages=[{"Contents": [{"Key": "prefix/data.csv"}]}],
        objects={"prefix/data.csv": b"id\n1\n2\n"},
    )
    source = S3Source(
        bucket="demo-bucket",
        prefix="prefix/",
        format="csv",
        row_mapper=lambda row: {"id": int(row["id"] or "0")},
        client=client,
    )

    records = [record async for record in source.stream()]

    assert records == [{"id": 1}, {"id": 2}]
    snapshot = source.metrics_snapshot()
    assert snapshot.completed_object_count == 1
    assert snapshot.last_completed_key == "prefix/data.csv"


@pytest.mark.asyncio
async def test_s3_source_downloads_objects_in_bounded_chunks() -> None:
    payload = b'{"id": 1}\n' * 300_000
    body = _FakeBody(payload)

    class ChunkTrackingClient(_FakeClient):
        def get_object(self, **kwargs: str) -> dict[str, object]:
            del kwargs
            return {"Body": body}

    client = ChunkTrackingClient(
        pages=[{"Contents": [{"Key": "prefix/large.jsonl"}]}],
        objects={},
    )
    source = S3Source(bucket="demo-bucket", prefix="prefix/", format="jsonl", client=client)

    records = [record async for record in source.stream()]

    assert len(records) == 300_000
    assert body.read_sizes
    assert set(body.read_sizes) == {1024 * 1024}
