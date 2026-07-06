from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import UTC, datetime

import pytest
from agora import DeliveryConfig, InMemoryCheckpointStore, IterableSource, Pipeline
from agora.core.checkpoint import Checkpoint, SQLiteCheckpointStore
from agora.core.dlq import DLQRecord
from agora.core.types import DedupStoreFailurePolicy, SourceRecordFailurePolicy
from agora.middlewares.dedup import DedupMiddleware

from agora_plugins.dlq_policy import DLQPayloadPolicy
from agora_plugins.redis import (
    RedisBackend,
    RedisDLQSink,
    RedisDLQSource,
    RedisSink,
    RedisStore,
    RedisStreamSource,
)
from tests.integration._process_death import (
    assert_process_died_after_checkpoint,
    read_jsonl,
    run_process_death_child,
)

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 15.0


class _ReverseCipher:
    def encrypt(self, payload: bytes) -> bytes:
        return payload[::-1]

    def decrypt(self, payload: bytes) -> bytes:
        return payload[::-1]


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


class _FailAfterPipelineExecute:
    def __init__(self, pipeline, wrapper: _FailAfterEvalRedisClient) -> None:
        self._pipeline = pipeline
        self._wrapper = wrapper

    def eval(self, *args, **kwargs):
        return self._pipeline.eval(*args, **kwargs)

    async def execute(self):
        result = await self._pipeline.execute()
        if self._wrapper.failures_remaining:
            self._wrapper.failures_remaining -= 1
            raise self._wrapper.connection_error_cls("response lost after pipeline execute")
        return result

    async def __aenter__(self):
        await self._pipeline.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._pipeline.__aexit__(exc_type, exc, tb)


class _FailAfterEvalRedisClient:
    def __init__(self, client, connection_error_cls) -> None:
        self._client = client
        self.connection_error_cls = connection_error_cls
        self.failures_remaining = 1

    async def eval(self, *args, **kwargs):
        result = await self._client.eval(*args, **kwargs)
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise self.connection_error_cls("response lost after eval")
        return result

    def pipeline(self, *, transaction: bool):
        return _FailAfterPipelineExecute(
            self._client.pipeline(transaction=transaction),
            self,
        )

    async def aclose(self) -> None:
        await self._client.aclose()


def _make_dlq_record(unique_suffix: str, *, record_id: int, **overrides) -> DLQRecord:
    defaults = {
        "pipeline_id": f"orders-{unique_suffix}",
        "run_id": "run-1",
        "stage": "sink_write",
        "error_type": "RuntimeError",
        "error_message": "boom",
        "record": {"id": record_id},
        "source": "orders_source",
        "checkpoint": {"offset": record_id},
        "middleware": None,
        "sink": "redis",
        "created_at": datetime(2026, 6, 21, 12, record_id, tzinfo=UTC),
        "attempt": 0,
        "max_attempts": 5,
    }
    defaults.update(overrides)
    return DLQRecord(**defaults)


def _xadd_stream_messages(
    client,
    stream: str,
    values: list[int],
) -> None:
    for value in values:
        client.xadd(stream, {"value": str(value)})


def _xadd_raw_stream_messages(
    client,
    stream: str,
    values: list[str],
) -> None:
    for value in values:
        client.xadd(stream, {"value": value})


def _group_pending_count(client, stream: str, group: str) -> int:
    groups = client.xinfo_groups(stream)
    for item in groups:
        if item.get("name") == group:
            return int(item.get("pending", 0))
    return 0


async def _wait_for_condition(
    predicate, *, timeout_s: float = 5.0, interval_s: float = 0.05
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval_s)
    raise AssertionError("Condition did not become true before timeout.")


async def _consume_stream_phase(
    *,
    redis_url: str,
    stream: str,
    group: str,
    consumer: str,
    sink: _CollectValueSink,
    checkpoint_store: InMemoryCheckpointStore | None = None,
    max_records: int = 1,
    reclaim_idle_ms: int | None = None,
    reclaim_batch_size: int | None = None,
    deserializer=None,
    on_deserialize_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
):
    source = RedisStreamSource(
        url=redis_url,
        stream=stream,
        group=group,
        consumer=consumer,
        deserializer=deserializer or (lambda fields: int(fields["value"])),
        block_ms=250,
        batch_size=1,
        reclaim_idle_ms=reclaim_idle_ms,
        reclaim_batch_size=reclaim_batch_size,
        on_deserialize_error=on_deserialize_error,
    )
    pipeline = Pipeline(source)
    if checkpoint_store is None:
        bound = pipeline.build(sink)
    else:
        bound = pipeline.build(sink, config=DeliveryConfig(checkpoint=checkpoint_store))
    summary = await asyncio.wait_for(
        bound.run(max_records=max_records),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    return source, summary


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
async def test_redis_backend_set_if_absent_is_atomic_under_concurrent_writers(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    prefix = f"agora:state:cas:{unique_suffix}:"
    key = "winner"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    backend = RedisBackend(url=redis_url, prefix=prefix)

    async def _attempt(index: int) -> tuple[int, bool]:
        accepted = await asyncio.to_thread(
            backend.set_if_absent,
            key,
            {"writer": index},
            expires_at=time.time() + 60,
        )
        return index, accepted

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(_attempt(index) for index in range(64))),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        stored = backend.get(key)
    finally:
        keys = list(client.scan_iter(match=f"{prefix}*"))
        if keys:
            client.delete(*keys)
        backend.close()
        client.close()

    winners = [index for index, accepted in results if accepted]
    assert len(winners) == 1
    assert stored is not None
    assert stored.value == {"writer": winners[0]}
    assert stored.expires_at is not None


@pytest.mark.asyncio
async def test_redis_backend_compare_and_set_is_atomic_under_concurrent_writers(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    prefix = f"agora:state:cas-update:{unique_suffix}:"
    key = "winner"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    backend = RedisBackend(url=redis_url, prefix=prefix)
    backend.set(key, {"writer": None}, expires_at=time.time() + 60)
    expected = backend.get(key)
    assert expected is not None

    async def _attempt(index: int) -> tuple[int, bool]:
        accepted = await asyncio.to_thread(
            backend.compare_and_set,
            key,
            expected,
            {"writer": index},
            expires_at=time.time() + 60,
        )
        return index, accepted

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(_attempt(index) for index in range(64))),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        stored = backend.get(key)
    finally:
        keys = list(client.scan_iter(match=f"{prefix}*"))
        if keys:
            client.delete(*keys)
        backend.close()
        client.close()

    winners = [index for index, accepted in results if accepted]
    assert len(winners) == 1
    assert stored is not None
    assert stored.value == {"writer": winners[0]}
    assert stored.expires_at is not None


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
            .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert sink.records == []
    assert summary.records_dropped == 0
    assert summary.records_errored == 1
    assert len(dlq.records) == 1


@pytest.mark.asyncio
async def test_redis_sink_reconnects_after_broker_restart(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
) -> None:
    redis = pytest.importorskip("redis")
    key_prefix = f"agora:redis:sink-reconnect:{unique_suffix}"
    sink = RedisSink(
        url=redis_url,
        key_fn=lambda record: record["key"],
        serializer=lambda record: record["value"],
    )
    client = redis.Redis.from_url(redis_url, decode_responses=True)

    try:
        await sink.open()
        await sink.write({"key": f"{key_prefix}:before", "value": "alpha"})

        await asyncio.to_thread(redis_service_control)

        await sink.write({"key": f"{key_prefix}:after", "value": "bravo"})
        metrics = sink.metrics_snapshot()
        report = sink.acceptance_report()
        rendered = sink.render_prometheus_metrics(namespace="agora_test_redis")

        assert client.get(f"{key_prefix}:after") == "bravo"
    finally:
        await sink.close()
        keys = list(client.scan_iter(match=f"{key_prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert metrics.connection_ready is True
    assert metrics.write_call_count == 2
    assert metrics.written_record_count == 2
    assert report.passed is True
    assert (
        'agora_test_redis_sink_state{target="127.0.0.1:16379/0",mode="set",state="connection_ready"} 1'
        in rendered
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("lpush", ["5", "4", "3"]),
        ("rpush", ["3", "4", "5"]),
    ],
)
async def test_redis_sink_list_maxlen_trims_against_real_redis(
    redis_url: str,
    unique_suffix: str,
    mode: str,
    expected: list[str],
) -> None:
    redis = pytest.importorskip("redis")
    key = f"agora:redis:list-maxlen:{mode}:{unique_suffix}"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    sink = RedisSink(
        url=redis_url,
        key_fn=lambda _record: key,
        serializer=lambda record: str(record),
        mode=mode,
        maxlen=3,
    )

    try:
        client.delete(key)
        await sink.open()

        await asyncio.wait_for(
            sink.write_batch([1, 2, 3, 4, 5]),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        metrics = sink.metrics_snapshot()
        assert client.llen(key) == 3
        assert client.lrange(key, 0, -1) == expected
        assert metrics.maxlen == 3
        assert metrics.written_record_count == 5
        assert metrics.pipeline_execute_count == 1
    finally:
        await sink.close()
        client.delete(key)
        client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("lpush", ["2", "1"]),
        ("rpush", ["1", "2"]),
    ],
)
async def test_redis_sink_list_retry_does_not_duplicate_against_real_redis(
    redis_url: str,
    unique_suffix: str,
    mode: str,
    expected: list[str],
) -> None:
    redis = pytest.importorskip("redis")
    key = f"agora:redis:list-idempotent:{mode}:{unique_suffix}"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    sink = RedisSink(
        url=redis_url,
        key_fn=lambda _record: key,
        serializer=lambda record: str(record),
        mode=mode,
        maxlen=5,
    )

    try:
        client.delete(key)
        await sink.open()
        sink._client = _FailAfterEvalRedisClient(  # type: ignore[attr-defined]
            sink._client,  # type: ignore[attr-defined]
            redis.exceptions.ConnectionError,
        )

        await asyncio.wait_for(
            sink.write_batch([1, 2]),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        metrics = sink.metrics_snapshot()
        assert client.lrange(key, 0, -1) == expected
        assert metrics.written_record_count == 2
        assert metrics.pipeline_execute_count == 1
    finally:
        await sink.close()
        client.delete(key)
        client.close()


@pytest.mark.asyncio
async def test_redis_dlq_round_trip_replay_and_acknowledge_against_real_redis(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    key_prefix = f"agora:redis:dlq:{unique_suffix}"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    sink = RedisDLQSink(url=redis_url, key_prefix=key_prefix)
    source = RedisDLQSource(url=redis_url, key_prefix=key_prefix, limit=10)

    record_one = _make_dlq_record(unique_suffix, record_id=1)
    record_two = _make_dlq_record(unique_suffix, record_id=2)

    try:
        await sink.open()
        await source.open()

        await sink.write_batch([record_one, record_two])
        pipeline_index_key = f"{key_prefix}:__index__:pipeline:{record_one.pipeline_id}"
        stage_index_key = f"{key_prefix}:__index__:stage:{record_one.stage}"
        pipeline_stage_index_key = (
            f"{key_prefix}:__index__:pipeline_stage:{record_one.pipeline_id}:{record_one.stage}"
        )
        initial_secondary_lengths = {
            "pipeline": client.llen(pipeline_index_key),
            "stage": client.llen(stage_index_key),
            "pipeline_stage": client.llen(pipeline_stage_index_key),
        }
        initial_records = [record async for record in source.stream()]

        replayed = await sink.replay(initial_records[0])
        await sink.acknowledge(initial_records[1])
        remaining_secondary_lengths = {
            "pipeline": client.llen(pipeline_index_key),
            "stage": client.llen(stage_index_key),
            "pipeline_stage": client.llen(pipeline_stage_index_key),
        }

        follow_up_source = RedisDLQSource(url=redis_url, key_prefix=key_prefix, limit=10)
        await follow_up_source.open()
        try:
            remaining_records = [record async for record in follow_up_source.stream()]
            source_metrics = follow_up_source.metrics_snapshot()
            source_report = follow_up_source.acceptance_report()
            source_rendered = follow_up_source.render_prometheus_metrics(
                namespace="agora_test_redis"
            )
        finally:
            await follow_up_source.close()

        sink_metrics = sink.metrics_snapshot()
        sink_report = sink.acceptance_report()
        sink_rendered = sink.render_prometheus_metrics(namespace="agora_test_redis")
    finally:
        await source.close()
        await sink.close()
        keys = list(client.scan_iter(match=f"{key_prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert [record.record for record in initial_records] == [{"id": 1}, {"id": 2}]
    assert initial_secondary_lengths == {"pipeline": 2, "stage": 2, "pipeline_stage": 2}
    assert replayed.attempt == 1
    assert len(remaining_records) == 1
    assert remaining_records[0].record == {"id": 1}
    assert remaining_records[0].attempt == 1
    assert remaining_secondary_lengths == {"pipeline": 1, "stage": 1, "pipeline_stage": 1}
    assert sink_metrics.inserted_record_count == 2
    assert sink_metrics.upserted_record_count == 2
    assert sink_metrics.updated_record_count == 0
    assert sink_metrics.replay_count == 1
    assert sink_metrics.acknowledge_count == 1
    assert sink_report.passed is True
    assert source_metrics.scan_count == 1
    assert source_metrics.emitted_record_count == 1
    assert source_report.passed is True
    assert (
        'agora_test_redis_dlq_sink_state{key_prefix="'
        f'{key_prefix}",state="connection_ready"}} 1'.replace("}}", "}")
        in sink_rendered
    )
    assert (
        'agora_test_redis_dlq_source_state{key_prefix="'
        f'{key_prefix}",pipeline_id="",stage="",state="connection_ready"}} 1'.replace("}}", "}")
        in source_rendered
    )


@pytest.mark.asyncio
async def test_redis_dlq_payload_policy_redacts_and_encrypts_against_real_redis(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    redacted_prefix = f"agora:redis:dlq-redacted:{unique_suffix}"
    encrypted_prefix = f"agora:redis:dlq-encrypted:{unique_suffix}"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    encrypted_policy = DLQPayloadPolicy.encrypted(
        encryptor=_ReverseCipher(),
        encryption_algorithm="reverse-test",
        encryption_key_id="integration-test",
    )
    redacted_sink = RedisDLQSink(
        url=redis_url,
        key_prefix=redacted_prefix,
        payload_policy=DLQPayloadPolicy.redacted(redact_fields=("ssn",)),
    )
    encrypted_sink = RedisDLQSink(
        url=redis_url,
        key_prefix=encrypted_prefix,
        payload_policy=encrypted_policy,
    )
    encrypted_source = RedisDLQSource(
        url=redis_url,
        key_prefix=encrypted_prefix,
        payload_policy=encrypted_policy,
    )

    redacted_record = _make_dlq_record(
        unique_suffix,
        record_id=3,
        record={"id": 3, "password": "plain-secret"},
        original_record={"token": "raw-token"},
        processed_record={"ssn": "111-22-3333"},
        checkpoint={"offset": 3, "api_key": "secret-api-key"},
        details={"client_secret": "client-secret"},
    )
    encrypted_record = _make_dlq_record(
        unique_suffix,
        record_id=4,
        record={"id": 4, "password": "plain-secret"},
        original_record={"token": "raw-token"},
        processed_record={"ssn": "111-22-3333"},
        checkpoint={"offset": 4, "api_key": "secret-api-key"},
        details={"client_secret": "client-secret"},
    )

    try:
        await redacted_sink.open()
        await encrypted_sink.open()
        await encrypted_source.open()
        await redacted_sink.write(redacted_record)
        await encrypted_sink.write(encrypted_record)

        redacted_key = client.lrange(f"{redacted_prefix}:__index__", 0, -1)[0]
        encrypted_key = client.lrange(f"{encrypted_prefix}:__index__", 0, -1)[0]
        redacted_hash = client.hgetall(redacted_key)
        encrypted_hash = client.hgetall(encrypted_key)
        encrypted_records = [record async for record in encrypted_source.stream()]
    finally:
        await redacted_sink.close()
        await encrypted_sink.close()
        await encrypted_source.close()
        keys = list(client.scan_iter(match=f"{redacted_prefix}*"))
        keys.extend(client.scan_iter(match=f"{encrypted_prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()

    redacted_storage = "\n".join(redacted_hash.values())
    encrypted_storage = "\n".join(encrypted_hash.values())
    for secret in ("plain-secret", "raw-token", "111-22-3333", "secret-api-key", "client-secret"):
        assert secret not in redacted_storage
        assert secret not in encrypted_storage
    assert "[REDACTED]" in redacted_storage
    assert '"payload_encoding": "encrypted"' in encrypted_hash["record"]
    assert encrypted_hash["original_record"] == ""
    assert encrypted_hash["processed_record"] == ""
    assert encrypted_hash["checkpoint"] == ""
    assert encrypted_hash["details"] == ""
    assert len(encrypted_records) == 1
    assert encrypted_records[0].record == encrypted_record.record
    assert encrypted_records[0].original_record == encrypted_record.original_record
    assert encrypted_records[0].checkpoint == encrypted_record.checkpoint
    assert encrypted_records[0].details == encrypted_record.details


@pytest.mark.asyncio
async def test_redis_stream_source_checkpoint_resume_survives_broker_restart(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:stream:{unique_suffix}"
    group = f"group-{unique_suffix}"
    consumer = f"consumer-{unique_suffix}"
    checkpoint_store = InMemoryCheckpointStore()
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    first_sink = _CollectValueSink()
    second_sink = _CollectValueSink()

    try:
        _xadd_stream_messages(client, stream, [1, 2, 3, 4])

        first_summary = await asyncio.wait_for(
            (
                Pipeline(
                    RedisStreamSource(
                        url=redis_url,
                        stream=stream,
                        group=group,
                        consumer=consumer,
                        deserializer=lambda fields: int(fields["value"]),
                        block_ms=250,
                        batch_size=1,
                    )
                )
                .build(first_sink, config=DeliveryConfig(checkpoint=checkpoint_store))
                .run(max_records=1)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        await asyncio.to_thread(redis_service_control)

        second_source = RedisStreamSource(
            url=redis_url,
            stream=stream,
            group=group,
            consumer=consumer,
            deserializer=lambda fields: int(fields["value"]),
            block_ms=250,
            batch_size=1,
        )
        second_summary = await asyncio.wait_for(
            (
                Pipeline(second_source)
                .build(second_sink, config=DeliveryConfig(checkpoint=checkpoint_store))
                .run(max_records=3)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        metrics = second_source.metrics_snapshot()
        rendered = second_source.render_prometheus_metrics(namespace="agora_test_redis")
    finally:
        keys = list(client.scan_iter(match=f"{stream}*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert first_sink.records == [1]
    assert first_summary.records_consumed == 1
    assert first_summary.last_checkpoint is not None
    assert isinstance(first_summary.last_checkpoint.value["message_id"], str)
    assert second_sink.records == [2, 3, 4]
    assert second_summary.records_consumed == 3
    assert second_summary.last_checkpoint is not None
    assert (
        second_summary.last_checkpoint.value["message_id"]
        != first_summary.last_checkpoint.value["message_id"]
    )
    assert metrics.acked_message_count == 3
    assert metrics.record_error_count == 0
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="'
        + consumer
        + '",event="acked_message"} 3'
    ) in rendered


@pytest.mark.asyncio
async def test_redis_stream_checkpoint_resume_survives_process_death_after_checkpoint_before_xack(
    redis_url: str,
    unique_suffix: str,
    tmp_path,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:process-death:{unique_suffix}"
    group = f"group-process-death-{unique_suffix}"
    consumer = f"consumer-process-death-{unique_suffix}"
    pipeline_id = f"agora-redis-process-death-{unique_suffix}"
    checkpoint_path = tmp_path / "redis-checkpoint.db"
    output_path = tmp_path / "redis-child-output.jsonl"
    config_path = tmp_path / "redis-child-config.json"
    client = redis.Redis.from_url(redis_url, decode_responses=True)

    try:
        _xadd_stream_messages(client, stream, [1, 2])
        config_path.write_text(
            json.dumps(
                {
                    "mode": "redis",
                    "redis_url": redis_url,
                    "stream": stream,
                    "group": group,
                    "consumer": consumer,
                    "pipeline_id": pipeline_id,
                    "checkpoint_path": str(checkpoint_path),
                    "output_path": str(output_path),
                }
            ),
            encoding="utf-8",
        )

        child = await asyncio.to_thread(
            run_process_death_child,
            config_path,
            timeout_s=_INTEGRATION_TIMEOUT_S,
        )
        assert_process_died_after_checkpoint(child)

        store = SQLiteCheckpointStore(checkpoint_path)
        try:
            checkpoint = await store.load(pipeline_id)
            pending_after_child = _group_pending_count(client, stream, group)
            sink = _CollectValueSink()
            summary = await asyncio.wait_for(
                (
                    Pipeline(
                        RedisStreamSource(
                            url=redis_url,
                            stream=stream,
                            group=group,
                            consumer=consumer,
                            deserializer=lambda fields: int(fields["value"]),
                            block_ms=250,
                            batch_size=1,
                        ),
                        id=pipeline_id,
                    )
                    .build(sink, config=DeliveryConfig(checkpoint=store))
                    .run(max_records=1)
                ),
                timeout=_INTEGRATION_TIMEOUT_S,
            )
            pending_after_resume = _group_pending_count(client, stream, group)
        finally:
            await store.close()
    finally:
        keys = list(client.scan_iter(match=f"{stream}*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert read_jsonl(output_path) == [1]
    assert checkpoint is not None
    assert checkpoint.value["message_id"] is not None
    assert pending_after_child == 1
    assert sink.records == [2]
    assert summary.records_consumed == 1
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value["message_id"] != checkpoint.value["message_id"]
    assert pending_after_resume == 1


@pytest.mark.asyncio
async def test_redis_stream_resume_rejects_multi_consumer_group_against_real_redis(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:resume-guard:{unique_suffix}"
    group = f"group-{unique_suffix}"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    source = RedisStreamSource(
        url=redis_url,
        stream=stream,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: int(fields["value"]),
        block_ms=50,
        batch_size=1,
    )

    try:
        client.delete(stream)
        message_ids = [client.xadd(stream, {"value": str(value)}) for value in [1, 2, 3]]
        client.xgroup_create(stream, group, id="0")
        client.xreadgroup(group, "consumer-a", {stream: ">"}, count=1)
        client.xreadgroup(group, "consumer-b", {stream: ">"}, count=1)
        before_group = client.xinfo_groups(stream)[0]

        await source.open()
        await source.prepare_resume(
            Checkpoint(
                pipeline_id="pipe",
                run_id="run",
                source="redis_stream",
                value={"message_id": message_ids[0]},
            )
        )

        with pytest.raises(RuntimeError, match="single-consumer"):
            async for _record in source.stream():
                pass

        after_group = client.xinfo_groups(stream)[0]
        assert after_group["last-delivered-id"] == before_group["last-delivered-id"]
    finally:
        await source.close()
        client.delete(stream)
        client.close()


@pytest.mark.asyncio
async def test_redis_stream_source_reclaims_pending_messages_after_broker_restart(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:reclaim:{unique_suffix}"
    group = f"group-{unique_suffix}"
    producer = redis.Redis.from_url(redis_url, decode_responses=True)
    sink = _CollectValueSink()

    first_source = RedisStreamSource(
        url=redis_url,
        stream=stream,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: int(fields["value"]),
        block_ms=250,
        batch_size=1,
    )

    try:
        _xadd_stream_messages(producer, stream, [1])
        await first_source.open()
        first_stream = first_source.stream()
        first_record = await asyncio.wait_for(anext(first_stream), timeout=_INTEGRATION_TIMEOUT_S)
        assert first_record == 1
        await first_stream.aclose()
        await first_source.close()

        await asyncio.sleep(0.05)
        await asyncio.to_thread(redis_service_control)

        _xadd_stream_messages(producer, stream, [2])

        reclaim_source = RedisStreamSource(
            url=redis_url,
            stream=stream,
            group=group,
            consumer="consumer-b",
            deserializer=lambda fields: int(fields["value"]),
            block_ms=250,
            batch_size=1,
            reclaim_idle_ms=1,
            reclaim_batch_size=10,
        )
        summary = await asyncio.wait_for(
            Pipeline(reclaim_source).build(sink).run(max_records=2),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        metrics = reclaim_source.metrics_snapshot()
        rendered = reclaim_source.render_prometheus_metrics(namespace="agora_test_redis")
    finally:
        keys = list(producer.scan_iter(match=f"{stream}*"))
        if keys:
            producer.delete(*keys)
        producer.close()

    assert sink.records == [1, 2]
    assert summary.records_consumed == 2
    assert metrics.reclaimed_message_count >= 1
    assert metrics.acked_message_count == 2
    assert metrics.last_reclaim_at is not None
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="consumer-b",event="reclaimed_message"} '
    ) in rendered


@pytest.mark.asyncio
async def test_redis_stream_source_checkpoint_resume_survives_multi_cycle_broker_flap(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
    redis_broker_flap_cycles: int,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:stream-multi:{unique_suffix}"
    group = f"group-{unique_suffix}"
    consumer = f"consumer-{unique_suffix}"
    checkpoint_store = InMemoryCheckpointStore()
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    total_records = redis_broker_flap_cycles + 1
    phase_summaries = []
    phase_metrics = []
    checkpoint_ids: list[str] = []
    sink = _CollectValueSink()

    try:
        _xadd_stream_messages(client, stream, list(range(1, total_records + 1)))
        for cycle in range(total_records):
            if cycle > 0:
                await asyncio.to_thread(redis_service_control)
            source, summary = await _consume_stream_phase(
                redis_url=redis_url,
                stream=stream,
                group=group,
                consumer=consumer,
                sink=sink,
                checkpoint_store=checkpoint_store,
                max_records=1,
            )
            phase_summaries.append(summary)
            phase_metrics.append(source.metrics_snapshot())
            assert summary.last_checkpoint is not None
            checkpoint_ids.append(str(summary.last_checkpoint.value["message_id"]))
    finally:
        keys = list(client.scan_iter(match=f"{stream}*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert sink.records == list(range(1, total_records + 1))
    assert len(phase_summaries) == total_records
    assert all(summary.records_consumed == 1 for summary in phase_summaries)
    assert len(set(checkpoint_ids)) == total_records
    assert all(metrics.acked_message_count == 1 for metrics in phase_metrics)
    assert all(metrics.record_error_count == 0 for metrics in phase_metrics)


@pytest.mark.asyncio
async def test_redis_stream_source_reclaims_pending_messages_after_multi_cycle_broker_flap(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
    redis_broker_flap_cycles: int,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:reclaim-multi:{unique_suffix}"
    group = f"group-{unique_suffix}"
    producer = redis.Redis.from_url(redis_url, decode_responses=True)
    expected_pending = list(range(1, redis_broker_flap_cycles + 1))
    sink = _CollectValueSink()

    try:
        for value in expected_pending:
            _xadd_stream_messages(producer, stream, [value])
            first_source = RedisStreamSource(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="consumer-a",
                deserializer=lambda fields: int(fields["value"]),
                block_ms=250,
                batch_size=1,
            )
            await first_source.open()
            first_stream = first_source.stream()
            first_record = await asyncio.wait_for(
                anext(first_stream), timeout=_INTEGRATION_TIMEOUT_S
            )
            assert first_record == value
            await first_stream.aclose()
            await first_source.close()
            await asyncio.sleep(0.05)
            await asyncio.to_thread(redis_service_control)

        live_tail_value = redis_broker_flap_cycles + 100
        _xadd_stream_messages(producer, stream, [live_tail_value])

        reclaim_source, summary = await _consume_stream_phase(
            redis_url=redis_url,
            stream=stream,
            group=group,
            consumer="consumer-b",
            sink=sink,
            max_records=redis_broker_flap_cycles + 1,
            reclaim_idle_ms=1,
            reclaim_batch_size=max(redis_broker_flap_cycles, 1),
        )
        metrics = reclaim_source.metrics_snapshot()
        rendered = reclaim_source.render_prometheus_metrics(namespace="agora_test_redis")
    finally:
        keys = list(producer.scan_iter(match=f"{stream}*"))
        if keys:
            producer.delete(*keys)
        producer.close()

    assert sink.records == [*expected_pending, live_tail_value]
    assert summary.records_consumed == redis_broker_flap_cycles + 1
    assert metrics.reclaimed_message_count >= redis_broker_flap_cycles
    assert metrics.acked_message_count == redis_broker_flap_cycles + 1
    assert metrics.last_reclaim_at is not None
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="consumer-b",event="reclaimed_message"} '
        f"{metrics.reclaimed_message_count}"
    ) in rendered


@pytest.mark.asyncio
async def test_redis_stream_source_reclaim_fairness_interleaves_live_tail_against_real_redis(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:reclaim-fair:{unique_suffix}"
    group = f"group-{unique_suffix}"
    producer = redis.Redis.from_url(redis_url, decode_responses=True)
    sink = _CollectValueSink()

    first_source = RedisStreamSource(
        url=redis_url,
        stream=stream,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: int(fields["value"]),
        block_ms=250,
        batch_size=1,
    )

    try:
        _xadd_stream_messages(producer, stream, [1, 2])
        await first_source.open()
        first_stream = first_source.stream()
        assert await asyncio.wait_for(anext(first_stream), timeout=_INTEGRATION_TIMEOUT_S) == 1
        assert await asyncio.wait_for(anext(first_stream), timeout=_INTEGRATION_TIMEOUT_S) == 2
        await first_stream.aclose()
        await first_source.close()

        await asyncio.sleep(0.05)
        _xadd_stream_messages(producer, stream, [3])

        fairness_source = RedisStreamSource(
            url=redis_url,
            stream=stream,
            group=group,
            consumer="consumer-b",
            deserializer=lambda fields: int(fields["value"]),
            block_ms=250,
            batch_size=1,
            reclaim_idle_ms=1,
            reclaim_batch_size=1,
            max_consecutive_reclaim_batches=1,
        )
        summary = await asyncio.wait_for(
            Pipeline(fairness_source).build(sink).run(max_records=3),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        metrics = fairness_source.metrics_snapshot()
        rendered = fairness_source.render_prometheus_metrics(namespace="agora_test_redis")
    finally:
        with contextlib.suppress(Exception):
            await first_source.close()
        keys = list(producer.scan_iter(match=f"{stream}*"))
        if keys:
            producer.delete(*keys)
        producer.close()

    assert sink.records == [1, 3, 2]
    assert summary.records_consumed == 3
    assert metrics.reclaimed_message_count == 2
    assert metrics.reclaim_fairness_yield_count == 1
    assert metrics.max_consecutive_reclaim_batches == 1
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="consumer-b",event="reclaim_fairness_yield"} 1'
    ) in rendered


@pytest.mark.asyncio
async def test_redis_stream_source_log_and_continue_resume_survives_broker_restart(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:stream-log-continue:{unique_suffix}"
    group = f"group-{unique_suffix}"
    consumer = f"consumer-{unique_suffix}"
    checkpoint_store = InMemoryCheckpointStore()
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    first_sink = _CollectValueSink()
    second_sink = _CollectValueSink()

    try:
        _xadd_raw_stream_messages(client, stream, ["1", "bad", "2"])

        first_source, first_summary = await _consume_stream_phase(
            redis_url=redis_url,
            stream=stream,
            group=group,
            consumer=consumer,
            sink=first_sink,
            checkpoint_store=checkpoint_store,
            max_records=1,
        )
        await asyncio.to_thread(redis_service_control)

        second_source, second_summary = await _consume_stream_phase(
            redis_url=redis_url,
            stream=stream,
            group=group,
            consumer=consumer,
            sink=second_sink,
            checkpoint_store=checkpoint_store,
            max_records=1,
            on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
        )
        metrics = second_source.metrics_snapshot()
        rendered = second_source.render_prometheus_metrics(namespace="agora_test_redis")
        pending_count = _group_pending_count(client, stream, group)
    finally:
        keys = list(client.scan_iter(match=f"{stream}*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert first_sink.records == [1]
    assert first_summary.records_consumed == 1
    assert first_source.metrics_snapshot().acked_message_count == 1
    assert second_sink.records == [2]
    assert second_summary.records_consumed == 1
    assert metrics.record_error_count == 1
    assert metrics.record_drop_count == 1
    assert metrics.acked_message_count == 2
    assert pending_count == 0
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="'
        + consumer
        + '",event="record_drop"} 1'
    ) in rendered


@pytest.mark.asyncio
async def test_redis_stream_source_log_and_continue_reclaim_clears_pending_poison_after_restart(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:reclaim-log-continue:{unique_suffix}"
    group = f"group-{unique_suffix}"
    producer = redis.Redis.from_url(redis_url, decode_responses=True)
    sink = _CollectValueSink()

    first_source = RedisStreamSource(
        url=redis_url,
        stream=stream,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: fields["value"],
        block_ms=250,
        batch_size=1,
    )

    try:
        _xadd_raw_stream_messages(producer, stream, ["bad"])
        await first_source.open()
        first_stream = first_source.stream()
        first_record = await asyncio.wait_for(anext(first_stream), timeout=_INTEGRATION_TIMEOUT_S)
        assert first_record == "bad"
        await first_stream.aclose()
        await first_source.close()
        assert _group_pending_count(producer, stream, group) == 1

        await asyncio.sleep(0.05)
        await asyncio.to_thread(redis_service_control)

        _xadd_raw_stream_messages(producer, stream, ["2"])
        reclaim_source, summary = await _consume_stream_phase(
            redis_url=redis_url,
            stream=stream,
            group=group,
            consumer="consumer-b",
            sink=sink,
            max_records=1,
            reclaim_idle_ms=1,
            reclaim_batch_size=10,
            on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
        )
        metrics = reclaim_source.metrics_snapshot()
        rendered = reclaim_source.render_prometheus_metrics(namespace="agora_test_redis")
        pending_count = _group_pending_count(producer, stream, group)
    finally:
        keys = list(producer.scan_iter(match=f"{stream}*"))
        if keys:
            producer.delete(*keys)
        producer.close()

    assert sink.records == [2]
    assert summary.records_consumed == 1
    assert metrics.reclaimed_message_count >= 1
    assert metrics.record_error_count == 1
    assert metrics.record_drop_count == 1
    assert metrics.acked_message_count == 2
    assert pending_count == 0
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="consumer-b",event="record_drop"} 1'
    ) in rendered


@pytest.mark.asyncio
async def test_redis_stream_source_log_and_continue_acknowledges_reclaimed_poison_once(
    redis_url: str,
    unique_suffix: str,
    redis_service_control,
) -> None:
    redis = pytest.importorskip("redis")
    stream = f"agora:redis:reclaim-loop:{unique_suffix}"
    group = f"group-{unique_suffix}"
    producer = redis.Redis.from_url(redis_url, decode_responses=True)

    first_source = RedisStreamSource(
        url=redis_url,
        stream=stream,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: fields["value"],
        block_ms=250,
        batch_size=1,
    )

    try:
        _xadd_raw_stream_messages(producer, stream, ["bad"])
        await first_source.open()
        first_stream = first_source.stream()
        first_record = await asyncio.wait_for(anext(first_stream), timeout=_INTEGRATION_TIMEOUT_S)
        assert first_record == "bad"
        await first_stream.aclose()
        await first_source.close()
        assert _group_pending_count(producer, stream, group) == 1

        await asyncio.sleep(0.05)
        await asyncio.to_thread(redis_service_control)

        loop_source = RedisStreamSource(
            url=redis_url,
            stream=stream,
            group=group,
            consumer="consumer-b",
            deserializer=lambda fields: int(fields["value"]),
            block_ms=250,
            batch_size=1,
            ack_on_success=False,
            reclaim_idle_ms=1,
            reclaim_batch_size=10,
            on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
        )
        await loop_source.open()
        loop_stream = loop_source.stream()
        next_record_task = asyncio.create_task(anext(loop_stream))
        try:
            await _wait_for_condition(
                lambda: loop_source.metrics_snapshot().acked_message_count >= 1,
                timeout_s=5.0,
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(next_record_task), timeout=0.5)
        finally:
            next_record_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await next_record_task
            await loop_stream.aclose()
            await loop_source.close()

        metrics = loop_source.metrics_snapshot()
        report = loop_source.acceptance_report()
        rendered = loop_source.render_prometheus_metrics(namespace="agora_test_redis")
        pending_count = _group_pending_count(producer, stream, group)
    finally:
        keys = list(producer.scan_iter(match=f"{stream}*"))
        if keys:
            producer.delete(*keys)
        producer.close()

    assert metrics.reclaimed_message_count >= 1
    assert metrics.record_error_count >= 1
    assert metrics.record_drop_count >= 1
    assert metrics.acked_message_count == 1
    assert metrics.poison_loop_risk.detected is False
    assert metrics.poison_loop_risk.loop_count == 0
    assert metrics.poison_loop_risk.distinct_message_count == 0
    assert metrics.poison_loop_risk.last_message_id is None
    assert not any(finding.metric == "poison_loop_count" for finding in report.findings)
    assert pending_count == 0
    assert (
        "agora_test_redis_source_state"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="consumer-b",state="poison_loop_risk"} 0'
    ) in rendered
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="consumer-b",event="reclaimed_message"} '
        f"{metrics.reclaimed_message_count}"
    ) in rendered
    assert (
        "agora_test_redis_source_events_total"
        '{stream="'
        + stream
        + '",group="'
        + group
        + '",consumer="consumer-b",event="poison_loop"} 0'
    ) in rendered
