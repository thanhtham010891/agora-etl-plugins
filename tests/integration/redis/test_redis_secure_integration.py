from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from agora import Pipeline
from agora.core.dlq import DLQRecord

from agora_plugins.redis import RedisDLQSink, RedisDLQSource, RedisSink, RedisStreamSource

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 15.0


class _CollectValueSink:
    sink_name = "collect_value"

    def __init__(self) -> None:
        self.records: list[object] = []

    async def open(self) -> None:
        return None

    async def write(self, record: object) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


def _build_secure_redis_url(
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    ca_file: str | None,
) -> str:
    query = ["ssl_check_hostname=false"]
    if ca_file is not None:
        query.append(f"ssl_ca_certs={ca_file}")
    return f"rediss://{username}:{password}@{host}:{port}/0?" + "&".join(query)


def _make_dlq_record(unique_suffix: str) -> DLQRecord:
    return DLQRecord(
        pipeline_id=f"secure-{unique_suffix}",
        run_id="run-1",
        stage="sink_write",
        error_type="ValueError",
        error_message="boom",
        record={"id": 1},
        source="redis",
        checkpoint={"offset": 1},
        middleware=None,
        sink="redis",
        created_at=datetime(2026, 6, 22, 12, 0, tzinfo=UTC),
        attempt=0,
        max_attempts=5,
    )


def _assert_redis_auth_rejected(message: str) -> None:
    normalized = message.lower()
    assert any(
        marker in normalized
        for marker in (
            "invalid username-password pair",
            "authentication required",
            "wrongpass",
            "user is disabled",
        )
    ), message


def _assert_redis_tls_rejected(message: str) -> None:
    normalized = message.lower()
    assert any(
        marker in normalized
        for marker in (
            "certificate verify failed",
            "self-signed certificate",
            "ssl",
            "tls",
            "unknown ca",
        )
    ), message


@pytest.mark.asyncio
async def test_redis_sink_dlq_and_stream_source_work_over_tls_acl(
    redis_secure_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    key = f"agora:redis:secure:{unique_suffix}:key"
    stream_name = f"agora:redis:secure:{unique_suffix}:stream"
    group = f"group-{unique_suffix}"
    key_prefix = f"agora:redis:secure:{unique_suffix}:dlq"
    client = redis.Redis.from_url(redis_secure_url, decode_responses=True)
    sink = RedisSink(
        url=redis_secure_url,
        key_fn=lambda record: record["key"],
        serializer=lambda record: record["value"],
    )
    dlq_sink = RedisDLQSink(url=redis_secure_url, key_prefix=key_prefix)
    dlq_source = RedisDLQSource(url=redis_secure_url, key_prefix=key_prefix, limit=10)
    stream_source = RedisStreamSource(
        url=redis_secure_url,
        stream=stream_name,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: int(fields["value"]),
        block_ms=250,
        batch_size=1,
    )
    collect_sink = _CollectValueSink()

    try:
        await sink.open()
        await sink.write({"key": key, "value": "secure"})
        assert client.get(key) == "secure"

        record = _make_dlq_record(unique_suffix)
        await dlq_sink.open()
        await dlq_sink.write(record)
        await dlq_source.open()
        records = [item async for item in dlq_source.stream()]
        assert len(records) == 1
        assert records[0].pipeline_id == record.pipeline_id

        client.xadd(stream_name, {"value": "1"})
        client.xadd(stream_name, {"value": "2"})
        summary = await asyncio.wait_for(
            Pipeline(stream_source).build(collect_sink).run(max_records=2),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        metrics = stream_source.metrics_snapshot()
    finally:
        await sink.close()
        await dlq_sink.close()
        await dlq_source.close()
        await stream_source.close()
        keys = list(client.scan_iter(match=f"agora:redis:secure:{unique_suffix}*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert summary.records_consumed == 2
    assert collect_sink.records == [1, 2]
    assert metrics.acked_message_count == 2
    assert metrics.record_error_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("component", ["sink", "source"])
@pytest.mark.parametrize(
    ("mode", "expectation"),
    [
        ("wrong_password", "auth"),
        ("wrong_username", "auth"),
        ("missing_ca", "tls"),
        ("rogue_ca", "tls"),
    ],
)
async def test_redis_tls_acl_auth_failures_are_rejected_clearly(
    component: str,
    mode: str,
    expectation: str,
    redis_secure_assets: dict[str, str],
    unique_suffix: str,
) -> None:
    url = _build_secure_redis_url(
        host=redis_secure_assets["host"],
        port=int(redis_secure_assets["port"]),
        username=(
            "definitely-wrong-user" if mode == "wrong_username" else redis_secure_assets["username"]
        ),
        password=(
            "definitely-wrong-password"
            if mode == "wrong_password"
            else redis_secure_assets["password"]
        ),
        ca_file=(
            None
            if mode == "missing_ca"
            else (
                redis_secure_assets["rogue_ca_file"]
                if mode == "rogue_ca"
                else redis_secure_assets["ca_file"]
            )
        ),
    )

    if component == "sink":
        sink = RedisSink(
            url=url,
            key_fn=lambda record: record["key"],
            serializer=lambda record: record["value"],
        )
        with pytest.raises(Exception) as excinfo:
            await sink.open()
            await sink.write({"key": f"agora:redis:secure:{unique_suffix}:bad", "value": "boom"})
        await sink.close()
    else:
        source = RedisStreamSource(
            url=url,
            stream=f"agora:redis:secure:{unique_suffix}:stream",
            group=f"group-{unique_suffix}",
            consumer="consumer-a",
            deserializer=lambda fields: int(fields["value"]),
            block_ms=250,
            batch_size=1,
        )
        with pytest.raises(Exception) as excinfo:
            await source.open()
        await source.close()

    message = str(excinfo.value)
    if expectation == "auth":
        _assert_redis_auth_rejected(message)
    else:
        _assert_redis_tls_rejected(message)
