from __future__ import annotations

import asyncio

import pytest
from agora import IterableSource, Pipeline
from agora.core.types import DedupStoreFailurePolicy
from agora.middlewares.dedup import DedupMiddleware

from agora_plugins.redis import RedisStore

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 15.0


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[str] = []

    async def open(self) -> None:
        return None

    async def write(self, record: str) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CollectDLQSink:
    sink_name = "collect_dlq"

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


@pytest.mark.asyncio
async def test_redis_dedup_store_shares_state_across_pipeline_instances(
    redis_url: str,
    unique_suffix: str,
) -> None:
    prefix = f"agora:dedup:it:{unique_suffix}:"
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(redis_url, decode_responses=True)

    try:
        first_sink = _CollectSink()
        first_summary = await asyncio.wait_for(
            (
                Pipeline(IterableSource(["a", "b"]))
                .pipe(
                    DedupMiddleware(
                        key=lambda record: record,
                        store=RedisStore(url=redis_url, key_prefix=prefix),
                    )
                )
                .build(first_sink)  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        second_sink = _CollectSink()
        second_summary = await asyncio.wait_for(
            (
                Pipeline(IterableSource(["b", "c"]))
                .pipe(
                    DedupMiddleware(
                        key=lambda record: record,
                        store=RedisStore(url=redis_url, key_prefix=prefix),
                    )
                )
                .build(second_sink)  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        keys = list(client.scan_iter(match=f"{prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert first_sink.records == ["a", "b"]
    assert first_summary.records_written == 2
    assert second_sink.records == ["c"]
    assert second_summary.records_dropped == 1


@pytest.mark.asyncio
async def test_redis_dedup_fail_open_passes_record_when_backend_is_unreachable() -> None:
    sink = _CollectSink()
    summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(["a"]))
            .pipe(
                DedupMiddleware(
                    key=lambda record: record,
                    store=RedisStore(url="redis://127.0.0.1:1"),
                    store_failure_policy=DedupStoreFailurePolicy.FAIL_OPEN,
                )
            )
            .build(sink)  # type: ignore[arg-type]
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert sink.records == ["a"]
    assert summary.records_written == 1


@pytest.mark.asyncio
async def test_redis_dedup_fail_closed_routes_record_to_dlq_when_backend_is_unreachable() -> None:
    sink = _CollectSink()
    dlq = _CollectDLQSink()
    summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(["a"]))
            .pipe(
                DedupMiddleware(
                    key=lambda record: record,
                    store=RedisStore(url="redis://127.0.0.1:1"),
                )
            )
            .build(sink, dlq=dlq)  # type: ignore[arg-type]
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert sink.records == []
    assert summary.records_dropped == 1
    assert len(dlq.records) == 1
