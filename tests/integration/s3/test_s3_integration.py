from __future__ import annotations

import os
import socket
import time
import uuid

import pytest
from agora import Checkpoint
from botocore.exceptions import ClientError

from agora_plugins.s3 import S3Sink, S3Source


def _require_integration_enabled() -> None:
    if os.getenv("AGORA_RUN_INTEGRATION") != "1":
        pytest.skip("Set AGORA_RUN_INTEGRATION=1 to run integration tests.")


def _wait_for_tcp_endpoint(host: str, port: int, *, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.25)
    pytest.skip(f"Service {host}:{port} is not reachable.")


def _s3_settings() -> dict[str, str]:
    endpoint_url = os.getenv("AGORA_TEST_S3_ENDPOINT_URL", "http://127.0.0.1:19000")
    access_key_id = os.getenv("AGORA_TEST_S3_ACCESS_KEY_ID", "minioadmin")
    secret_access_key = os.getenv("AGORA_TEST_S3_SECRET_ACCESS_KEY", "minioadmin")
    bucket = os.getenv("AGORA_TEST_S3_BUCKET", "agora-s3-test")
    region = os.getenv("AGORA_TEST_S3_REGION", "us-east-1")
    host_port = endpoint_url.removeprefix("http://").removeprefix("https://")
    host, port = host_port.split(":", 1)
    _wait_for_tcp_endpoint(host, int(port))
    return {
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key,
        "bucket": bucket,
        "region_name": region,
    }


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.parametrize("format", ["jsonl", "csv", "parquet"])
async def test_s3_round_trip_dataset_formats(format: str) -> None:
    _require_integration_enabled()
    if format == "parquet":
        pytest.importorskip("pyarrow")
    settings = _s3_settings()
    prefix = f"integration/{format}/{uuid.uuid4().hex}"
    records = [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "beta"},
        {"id": 3, "name": "gamma"},
    ]
    sink = S3Sink(
        bucket=settings["bucket"],
        prefix=prefix,
        format=format,  # type: ignore[arg-type]
        row_mapper=lambda record: record,
        flush_every=1,
        max_records_per_file=10,
        endpoint_url=settings["endpoint_url"],
        aws_access_key_id=settings["aws_access_key_id"],
        aws_secret_access_key=settings["aws_secret_access_key"],
        region_name=settings["region_name"],
        addressing_style="path",
    )
    for record in records:
        await sink.write(record)
    await sink.close()

    if format == "csv":
        source = S3Source(
            bucket=settings["bucket"],
            prefix=prefix,
            format="csv",
            row_mapper=lambda row: {"id": int(row["id"] or "0"), "name": row["name"]},
            endpoint_url=settings["endpoint_url"],
            aws_access_key_id=settings["aws_access_key_id"],
            aws_secret_access_key=settings["aws_secret_access_key"],
            region_name=settings["region_name"],
            addressing_style="path",
        )
    else:
        source = S3Source(
            bucket=settings["bucket"],
            prefix=prefix,
            format=format,  # type: ignore[arg-type]
            endpoint_url=settings["endpoint_url"],
            aws_access_key_id=settings["aws_access_key_id"],
            aws_secret_access_key=settings["aws_secret_access_key"],
            region_name=settings["region_name"],
            addressing_style="path",
        )

    observed = [record async for record in source.stream()]
    assert observed == records


@pytest.mark.asyncio
@pytest.mark.integration
async def test_s3_resume_skips_completed_object_boundary() -> None:
    _require_integration_enabled()
    settings = _s3_settings()
    prefix = f"integration/resume/{uuid.uuid4().hex}"
    records = [
        {"id": 1, "partition": "a"},
        {"id": 2, "partition": "a"},
        {"id": 3, "partition": "a"},
    ]
    sink = S3Sink(
        bucket=settings["bucket"],
        prefix=prefix,
        format="jsonl",
        row_mapper=lambda record: record,
        partition_path_fn=lambda record: f"p={record['partition']}",
        flush_every=1,
        max_records_per_file=2,
        run_id="run-1",
        endpoint_url=settings["endpoint_url"],
        aws_access_key_id=settings["aws_access_key_id"],
        aws_secret_access_key=settings["aws_secret_access_key"],
        region_name=settings["region_name"],
        addressing_style="path",
    )
    for record in records:
        await sink.write(record)
    await sink.close()

    source = S3Source(
        bucket=settings["bucket"],
        prefix=prefix,
        format="jsonl",
        endpoint_url=settings["endpoint_url"],
        aws_access_key_id=settings["aws_access_key_id"],
        aws_secret_access_key=settings["aws_secret_access_key"],
        region_name=settings["region_name"],
        addressing_style="path",
    )
    await source.prepare_resume(
        Checkpoint(
            pipeline_id="s3-resume",
            run_id="run-1",
            source="s3",
            value={"object_key": f"{prefix}/run_id=run-1/p=a/part-00000.jsonl"},
        )
    )

    observed = [record async for record in source.stream()]
    assert observed == [{"id": 3, "partition": "a"}]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_s3_sink_rejects_restart_collision_for_same_run_id() -> None:
    _require_integration_enabled()
    settings = _s3_settings()
    prefix = f"integration/collision/{uuid.uuid4().hex}"
    sink_kwargs = {
        "bucket": settings["bucket"],
        "prefix": prefix,
        "format": "jsonl",
        "run_id": "replay-run",
        "endpoint_url": settings["endpoint_url"],
        "aws_access_key_id": settings["aws_access_key_id"],
        "aws_secret_access_key": settings["aws_secret_access_key"],
        "region_name": settings["region_name"],
        "addressing_style": "path",
    }
    first = S3Sink(**sink_kwargs)
    await first.write({"id": 1})
    await first.close()

    restarted = S3Sink(**sink_kwargs)
    await restarted.write({"id": 2})
    with pytest.raises(ClientError) as caught:
        await restarted.close()

    assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
