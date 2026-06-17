from __future__ import annotations

import asyncio
import contextlib

import pytest
from agora import Pipeline

from agora_plugins.redis import RedisStreamSource

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 20.0


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


async def _ack_delivery(source: RedisStreamSource[object]) -> None:
    callback = source.delivery_success_callback()
    if callback is not None:
        await callback()


def _xadd_stream_messages(client, stream: str, values: list[int]) -> None:
    for value in values:
        client.xadd(stream, {"value": str(value)})


def _delete_matching_keys(url: str, pattern: str) -> None:
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(url, decode_responses=True)
    try:
        keys = list(client.scan_iter(match=pattern))
        if keys:
            client.delete(*keys)
    finally:
        client.close()


def _group_pending_count(client, stream: str, group: str) -> int:
    try:
        groups = client.xinfo_groups(stream)
    except Exception:
        return 0
    for item in groups:
        if item.get("name") == group:
            return int(item.get("pending", 0))
    return 0


def _ensure_stream_group(client, stream: str, group: str) -> None:
    try:
        client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _wait_for_replication_barrier(client, marker_key: str) -> None:
    client.set(marker_key, "ready")
    await _wait_for_condition(
        lambda: int(client.execute_command("WAIT", 1, 1000)) >= 1,
        timeout_s=5.0,
    )


async def _replicate_barrier_for_url(url: str, marker_key: str) -> None:
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(url, decode_responses=True)
    try:
        await _wait_for_replication_barrier(client, marker_key)
    finally:
        client.close()


async def _wait_for_condition(
    predicate, *, timeout_s: float = 5.0, interval_s: float = 0.05
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval_s)
    raise AssertionError("Condition did not become true before timeout.")


@pytest.mark.asyncio
async def test_redis_stream_source_reconnects_and_tails_after_sentinel_crash_failover(
    redis_sentinel_url: str,
    redis_sentinel_control,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    stream_name = f"agora:redis:sentinel-tail:{unique_suffix}"
    group = f"group-{unique_suffix}"
    producer = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
    source = RedisStreamSource(
        url=redis_sentinel_url,
        stream=stream_name,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: int(fields["value"]),
        block_ms=250,
        batch_size=1,
    )
    failed_node: str | None = None

    try:
        await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready)
        _ensure_stream_group(producer, stream_name, group)
        await _wait_for_replication_barrier(producer, f"{stream_name}:group-sync")
        _xadd_stream_messages(producer, stream_name, [1])
        await source.open()
        stream = source.stream()

        first_record = await asyncio.wait_for(anext(stream), timeout=_INTEGRATION_TIMEOUT_S)
        assert first_record == 1
        await _ack_delivery(source)

        next_record_task = asyncio.create_task(anext(stream))
        failed_node = await asyncio.to_thread(redis_sentinel_control.crash_failover)
        await asyncio.to_thread(redis_sentinel_control.wait_for_proxy_writable)

        producer_after_failover = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
        try:
            _xadd_stream_messages(producer_after_failover, stream_name, [2, 3])
        finally:
            producer_after_failover.close()

        second_record = await asyncio.wait_for(next_record_task, timeout=_INTEGRATION_TIMEOUT_S)
        await _ack_delivery(source)
        third_record = await asyncio.wait_for(anext(stream), timeout=_INTEGRATION_TIMEOUT_S)
        await _ack_delivery(source)

        metrics = source.metrics_snapshot()
    finally:
        if failed_node is not None:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(redis_sentinel_control.start_node, failed_node)
        with contextlib.suppress(Exception):
            await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready, timeout_s=10.0)
        with contextlib.suppress(Exception):
            await source.close()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_delete_matching_keys, redis_sentinel_url, f"{stream_name}*")
        producer.close()

    assert second_record == 2
    assert third_record == 3
    assert metrics.reconnect_count >= 1
    assert metrics.acked_message_count == 3
    assert metrics.record_error_count == 0
    assert metrics.last_reconnect_at is not None


@pytest.mark.asyncio
async def test_redis_stream_source_reclaims_pending_messages_after_sentinel_graceful_failover(
    redis_sentinel_url: str,
    redis_sentinel_control,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    stream_name = f"agora:redis:sentinel-reclaim:{unique_suffix}"
    group = f"group-{unique_suffix}"
    producer = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
    first_source = RedisStreamSource(
        url=redis_sentinel_url,
        stream=stream_name,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: int(fields["value"]),
        block_ms=250,
        batch_size=1,
    )
    sink = _CollectValueSink()

    try:
        await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready)
        _xadd_stream_messages(producer, stream_name, [1])
        await first_source.open()
        first_stream = first_source.stream()
        first_record = await asyncio.wait_for(anext(first_stream), timeout=_INTEGRATION_TIMEOUT_S)
        assert first_record == 1
        await first_stream.aclose()
        await first_source.close()

        await _wait_for_condition(
            lambda: _group_pending_count(producer, stream_name, group) == 1,
            timeout_s=5.0,
        )
        await _wait_for_replication_barrier(producer, f"{stream_name}:sync")

        await asyncio.to_thread(redis_sentinel_control.graceful_failover)
        await asyncio.to_thread(redis_sentinel_control.wait_for_proxy_writable)
        producer_after_failover = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
        try:
            _xadd_stream_messages(producer_after_failover, stream_name, [2])
        finally:
            producer_after_failover.close()

        reclaim_source = RedisStreamSource(
            url=redis_sentinel_url,
            stream=stream_name,
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
    finally:
        with contextlib.suppress(Exception):
            await first_source.close()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready, timeout_s=10.0)
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_delete_matching_keys, redis_sentinel_url, f"{stream_name}*")
        producer.close()

    assert sink.records == [1, 2]
    assert summary.records_consumed == 2
    assert metrics.reclaimed_message_count >= 1
    assert metrics.acked_message_count == 2
    assert metrics.record_error_count == 0


@pytest.mark.asyncio
async def test_redis_stream_source_survives_multi_cycle_sentinel_crash_failover_live_tail(
    redis_sentinel_url: str,
    redis_sentinel_control,
    redis_sentinel_failover_cycles: int,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    stream_name = f"agora:redis:sentinel-live-tail-multi:{unique_suffix}"
    group = f"group-{unique_suffix}"
    expected_records = list(range(1, redis_sentinel_failover_cycles + 2))
    producer = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
    source = RedisStreamSource(
        url=redis_sentinel_url,
        stream=stream_name,
        group=group,
        consumer="consumer-a",
        deserializer=lambda fields: int(fields["value"]),
        block_ms=250,
        batch_size=1,
    )

    try:
        await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready)
        _ensure_stream_group(producer, stream_name, group)
        await _wait_for_replication_barrier(producer, f"{stream_name}:group-sync")
        _xadd_stream_messages(producer, stream_name, [expected_records[0]])
        await source.open()
        stream = source.stream()

        first_record = await asyncio.wait_for(anext(stream), timeout=_INTEGRATION_TIMEOUT_S)
        assert first_record == expected_records[0]
        await _ack_delivery(source)

        observed_records = [first_record]
        for expected_record in expected_records[1:]:
            await _replicate_barrier_for_url(
                redis_sentinel_url,
                f"{stream_name}:tail-sync:{expected_record}",
            )
            next_record_task = asyncio.create_task(anext(stream))
            failed_node = await asyncio.to_thread(redis_sentinel_control.crash_failover)
            producer_after_failover = redis.Redis.from_url(
                redis_sentinel_url, decode_responses=True
            )
            try:
                _xadd_stream_messages(producer_after_failover, stream_name, [expected_record])
            finally:
                producer_after_failover.close()
            observed_record = await asyncio.wait_for(
                next_record_task,
                timeout=_INTEGRATION_TIMEOUT_S,
            )
            observed_records.append(observed_record)
            await _ack_delivery(source)
            await asyncio.to_thread(redis_sentinel_control.start_node, failed_node)
            await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready)

        metrics = source.metrics_snapshot()
    finally:
        with contextlib.suppress(Exception):
            await source.close()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_delete_matching_keys, redis_sentinel_url, f"{stream_name}*")
        producer.close()

    assert observed_records == expected_records
    assert metrics.reconnect_count >= redis_sentinel_failover_cycles
    assert metrics.acked_message_count == len(expected_records)
    assert metrics.record_error_count == 0
    assert metrics.last_reconnect_at is not None


@pytest.mark.asyncio
async def test_redis_stream_source_reclaims_pending_messages_after_multi_cycle_sentinel_graceful_failover(
    redis_sentinel_url: str,
    redis_sentinel_control,
    redis_sentinel_failover_cycles: int,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    stream_name = f"agora:redis:sentinel-reclaim-multi:{unique_suffix}"
    group = f"group-{unique_suffix}"
    expected_pending = list(range(1, redis_sentinel_failover_cycles + 1))
    sink = _CollectValueSink()
    reclaim_source: RedisStreamSource[int] | None = None

    try:
        await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready)
        for expected_record in expected_pending:
            phase_producer = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
            try:
                _xadd_stream_messages(phase_producer, stream_name, [expected_record])
                first_source = RedisStreamSource(
                    url=redis_sentinel_url,
                    stream=stream_name,
                    group=group,
                    consumer="consumer-a",
                    deserializer=lambda fields: int(fields["value"]),
                    block_ms=250,
                    batch_size=1,
                )
                try:
                    await first_source.open()
                    first_stream = first_source.stream()
                    first_record = await asyncio.wait_for(
                        anext(first_stream),
                        timeout=_INTEGRATION_TIMEOUT_S,
                    )
                    assert first_record == expected_record
                    await first_stream.aclose()
                finally:
                    with contextlib.suppress(Exception):
                        await first_source.close()

                await _wait_for_condition(
                    lambda current_producer=phase_producer, expected_pending_count=expected_record: (
                        _group_pending_count(current_producer, stream_name, group)
                        >= expected_pending_count
                    ),
                    timeout_s=5.0,
                )
                await _wait_for_replication_barrier(
                    phase_producer,
                    f"{stream_name}:sync:{expected_record}",
                )
                await asyncio.to_thread(redis_sentinel_control.graceful_failover)
                await asyncio.to_thread(redis_sentinel_control.ensure_topology_ready)
            finally:
                phase_producer.close()

        live_tail_value = redis_sentinel_failover_cycles + 100
        producer_after_failover = redis.Redis.from_url(redis_sentinel_url, decode_responses=True)
        try:
            _xadd_stream_messages(producer_after_failover, stream_name, [live_tail_value])
        finally:
            producer_after_failover.close()

        reclaim_source = RedisStreamSource(
            url=redis_sentinel_url,
            stream=stream_name,
            group=group,
            consumer="consumer-b",
            deserializer=lambda fields: int(fields["value"]),
            block_ms=250,
            batch_size=1,
            reclaim_idle_ms=1,
            reclaim_batch_size=max(redis_sentinel_failover_cycles, 1),
        )
        summary = await asyncio.wait_for(
            Pipeline(reclaim_source)
            .build(sink)
            .run(max_records=redis_sentinel_failover_cycles + 1),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        metrics = reclaim_source.metrics_snapshot()
    finally:
        if reclaim_source is not None:
            with contextlib.suppress(Exception):
                await reclaim_source.close()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_delete_matching_keys, redis_sentinel_url, f"{stream_name}*")

    assert sink.records == [*expected_pending, live_tail_value]
    assert summary.records_consumed == redis_sentinel_failover_cycles + 1
    assert metrics.reclaimed_message_count >= redis_sentinel_failover_cycles
    assert metrics.acked_message_count == redis_sentinel_failover_cycles + 1
    assert metrics.record_error_count == 0
    assert metrics.last_reclaim_at is not None
