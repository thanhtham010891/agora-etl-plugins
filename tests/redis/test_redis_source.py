from __future__ import annotations

import asyncio
from typing import Any

import pytest
from agora import DeliveryConfig, Pipeline, SourceRecordFailurePolicy
from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.checkpoint import Checkpoint, SourceIdentityMismatchError
from agora.core.health import ComponentHealthSnapshot
from agora.core.retry import RetryPolicy
from agora.core.source import SourceRecordError

from agora_plugins.redis import RedisSourceEnterpriseAcceptanceThresholds, RedisStreamSource


def _redis_message_id_score(message_id: str) -> tuple[int, int]:
    major, _, minor = message_id.partition("-")
    return int(major), int(minor or 0)


class _FakeRedisClient:
    def __init__(
        self,
        entries: list[tuple[str, list[tuple[str, dict[str, str]]]]],
        reclaimed_batches: list[list[tuple[str, dict[str, str]]]] | None = None,
        consumers: list[dict[str, object]] | None = None,
    ) -> None:
        self._entries = list(entries)
        self._reclaimed_batches = list(reclaimed_batches or [])
        self._consumers = consumers if consumers is not None else [{"name": "c"}]
        self.xack_calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.xreadgroup_calls: list[dict[str, str]] = []
        self.xgroup_setid_calls: list[tuple[str, str, str]] = []
        self.xinfo_consumers_calls: list[tuple[str, str]] = []
        self.xautoclaim_calls: list[tuple[str, str, str, int, str, int | None]] = []
        self.aclose_calls = 0

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int,
        block: int,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        self.xreadgroup_calls.append(dict(streams))
        del group, consumer, count, block
        if not self._entries:
            raise asyncio.CancelledError
        entry = self._entries.pop(0)
        if not entry[1]:
            return []
        return [entry]

    async def xack(self, stream: str, group: str, *msg_ids: str) -> None:
        self.xack_calls.append((stream, group, msg_ids))

    async def xgroup_setid(self, stream: str, group: str, message_id: str) -> None:
        self.xgroup_setid_calls.append((stream, group, message_id))

    async def xinfo_consumers(self, stream: str, group: str) -> list[dict[str, object]]:
        self.xinfo_consumers_calls.append((stream, group))
        return list(self._consumers)

    async def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str,
        *,
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, str]]], list[str]]:
        self.xautoclaim_calls.append((stream, group, consumer, min_idle_time, start_id, count))
        if not self._reclaimed_batches:
            return ("0-0", [], [])
        return ("0-0", self._reclaimed_batches.pop(0), [])

    async def aclose(self) -> None:
        self.aclose_calls += 1


class _SeekAwareRedisClient(_FakeRedisClient):
    def __init__(self, messages: list[tuple[str, dict[str, str]]]) -> None:
        super().__init__(entries=[])
        self._messages = list(messages)
        self._cursor = "0-0"
        self._read_once = False

    async def xgroup_setid(self, stream: str, group: str, message_id: str) -> None:
        await super().xgroup_setid(stream, group, message_id)
        self._cursor = message_id
        self._read_once = False

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int,
        block: int,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        self.xreadgroup_calls.append(dict(streams))
        del group, consumer, block
        if self._read_once:
            raise asyncio.CancelledError
        self._read_once = True
        cursor_score = _redis_message_id_score(self._cursor)
        messages = [
            (message_id, fields)
            for message_id, fields in self._messages
            if _redis_message_id_score(message_id) > cursor_score
        ][:count]
        if not messages:
            return []
        return [("events", messages)]


class _AlwaysFailXackClient(_FakeRedisClient):
    async def xack(self, stream: str, group: str, *msg_ids: str) -> None:
        await super().xack(stream, group, *msg_ids)
        raise RuntimeError("ack failed")


async def _ack_delivery(source: RedisStreamSource[object]) -> None:
    callback = source.delivery_success_callback()
    if callback is not None:
        await callback()


def _acked_ids(client: _FakeRedisClient) -> list[str]:
    ids: list[str] = []
    for _stream, _group, msg_ids in client.xack_calls:
        ids.extend(msg_ids)
    return ids


async def _noop_async() -> None:
    return None


class _CountSink:
    sink_name = "count"

    def __init__(self) -> None:
        self.count = 0

    async def open(self) -> None:
        return None

    async def write(self, record: object) -> None:
        del record
        self.count += 1

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CollectSink:
    sink_name = "collect"

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
    sink_name = "dlq"

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


class _CrashAfterSaveCheckpointStore:
    def __init__(self) -> None:
        self.fail_next_save = True
        self._checkpoint: Checkpoint | None = None
        self.saved_checkpoints: list[Checkpoint] = []

    async def load(self, key: str) -> Checkpoint | None:
        del key
        return self._checkpoint

    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        del key
        self._checkpoint = checkpoint
        self.saved_checkpoints.append(checkpoint)
        if self.fail_next_save:
            self.fail_next_save = False
            raise RuntimeError("crash after checkpoint save")

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_redis_stream_source_fails_closed_on_bad_messages_by_default() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("1-0", {"value": "1"}),
                    ("2-0", {"value": "bad"}),
                ],
            )
        ]
    )
    source._client = client  # type: ignore[attr-defined]

    records: list[int] = []
    with pytest.raises(SourceRecordError) as exc_info:
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [1]
    assert _acked_ids(client) == ["1-0"]
    assert isinstance(exc_info.value.original, ValueError)
    assert exc_info.value.record == {"message_id": "2-0", "fields": {"value": "bad"}}
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 0,
    }


@pytest.mark.asyncio
async def test_redis_stream_source_exposes_active_delivery_identity() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
    )
    source._client = _FakeRedisClient([("events", [("1-0", {"value": "1"})])])  # type: ignore[attr-defined]

    with pytest.raises(asyncio.CancelledError):
        async for _record in source.stream():
            context = source.delivery_context()
            assert context is not None
            assert context.delivery_id == "events:1-0"
            assert context.to_dict()["group"] == "g"
            await _ack_delivery(source)

    assert source.delivery_context() is None
    assert source.delivery_success_callback() is None


@pytest.mark.asyncio
async def test_redis_stream_source_can_log_and_continue_on_bad_messages() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
        on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("1-0", {"value": "1"}),
                    ("2-0", {"value": "bad"}),
                    ("3-0", {"value": "3"}),
                ],
            )
        ]
    )
    source._client = client  # type: ignore[attr-defined]

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [1, 3]
    assert _acked_ids(client) == ["1-0", "2-0", "3-0"]
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }


@pytest.mark.asyncio
async def test_redis_stream_source_batches_success_acks() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
        ack_batch_size=3,
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("1-0", {"value": "1"}),
                    ("2-0", {"value": "2"}),
                    ("3-0", {"value": "3"}),
                ],
            )
        ]
    )
    source._client = client  # type: ignore[attr-defined]

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [1, 2, 3]
    assert _acked_ids(client) == ["1-0", "2-0", "3-0"]
    assert client.xack_calls == [("events", "g", ("1-0", "2-0", "3-0"))]
    metrics = source.metrics_snapshot()
    assert metrics.ack_flush_count == 1
    assert metrics.acked_message_count == 3
    assert metrics.emitted_record_count == 3
    assert metrics.pending_ack_count == 0
    assert metrics.last_ack_at is not None


@pytest.mark.asyncio
async def test_redis_stream_source_restores_pending_acks_when_retry_ack_fails() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: fields,
    )
    client = _AlwaysFailXackClient([])
    source._client = client  # type: ignore[attr-defined]
    source._pending_ack_ids = ["1-0"]  # type: ignore[attr-defined]

    async def _recover(exc: Exception, *, context: str) -> bool:
        del exc, context
        return True

    source._recover_from_connection_error = _recover  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="ack failed"):
        await source._flush_pending_acks()  # type: ignore[attr-defined]

    assert client.xack_calls == [
        ("events", "g", ("1-0",)),
        ("events", "g", ("1-0",)),
    ]
    assert source.metrics_snapshot().pending_ack_count == 1


@pytest.mark.asyncio
async def test_redis_stream_source_resume_replays_pending_then_tails_new_messages() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("3-0", {"value": "3"}),
                ],
            ),
            (
                "events",
                [],
            ),
            (
                "events",
                [
                    ("4-0", {"value": "4"}),
                ],
            ),
        ]
    )
    source._client = client  # type: ignore[attr-defined]

    await source.prepare_resume(
        Checkpoint(
            pipeline_id="pipe",
            run_id="run",
            source="redis_stream",
            value={"message_id": "2-0"},
            source_identity=source.checkpoint_source_identity(),
        )
    )

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [3, 4]
    assert client.xinfo_consumers_calls == [("events", "g")]
    assert client.xgroup_setid_calls == [("events", "g", "2-0")]
    assert client.xreadgroup_calls[:3] == [{"events": ">"}, {"events": ">"}, {"events": ">"}]
    assert _acked_ids(client) == ["3-0", "4-0"]
    assert source.current_checkpoint() == {
        "stream": "events",
        "group": "g",
        "consumer": "c",
        "message_id": "4-0",
    }


@pytest.mark.asyncio
async def test_redis_stream_source_rejects_checkpoint_from_different_input() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
    )
    other_source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="orders",
        group="g",
        consumer="c",
    )
    rotated_credential_source = RedisStreamSource(
        url="redis://:new-secret@localhost:6379",
        stream="events",
        group="g",
        consumer="c",
    )
    prior_credential_source = RedisStreamSource(
        url="redis://:old-secret@localhost:6379",
        stream="events",
        group="g",
        consumer="c",
    )

    assert (
        rotated_credential_source.checkpoint_source_identity()
        == prior_credential_source.checkpoint_source_identity()
    )

    with pytest.raises(SourceIdentityMismatchError, match="saved source identity differs"):
        await source.prepare_resume(
            Checkpoint(
                pipeline_id="pipe",
                run_id="run",
                source="redis_stream",
                value={"message_id": "2-0"},
                source_identity=other_source.checkpoint_source_identity(),
            )
        )


@pytest.mark.asyncio
async def test_redis_stream_source_resume_requires_xinfo_consumers_support() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    client = _FakeRedisClient([])
    client.xinfo_consumers = None  # type: ignore[method-assign]
    source._client = client  # type: ignore[attr-defined]

    await source.prepare_resume(
        Checkpoint(
            pipeline_id="pipe",
            run_id="run",
            source="redis_stream",
            value={"message_id": "2-0"},
            source_identity=source.checkpoint_source_identity(),
        )
    )

    with pytest.raises(TypeError, match="xinfo_consumers"):
        async for _record in source.stream():
            pass


@pytest.mark.asyncio
async def test_redis_stream_source_resume_requires_xgroup_setid_support() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    client = _FakeRedisClient([])
    client.xgroup_setid = None  # type: ignore[method-assign]
    source._client = client  # type: ignore[attr-defined]

    await source.prepare_resume(
        Checkpoint(
            pipeline_id="pipe",
            run_id="run",
            source="redis_stream",
            value={"message_id": "2-0"},
            source_identity=source.checkpoint_source_identity(),
        )
    )

    with pytest.raises(TypeError, match="xgroup_setid"):
        async for _record in source.stream():
            pass


@pytest.mark.asyncio
async def test_redis_stream_source_resume_rejects_multi_consumer_group() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    client = _FakeRedisClient(
        [],
        consumers=[{"name": "c"}, {"name": "c2"}],
    )
    source._client = client  # type: ignore[attr-defined]

    await source.prepare_resume(
        Checkpoint(
            pipeline_id="pipe",
            run_id="run",
            source="redis_stream",
            value={"message_id": "2-0"},
            source_identity=source.checkpoint_source_identity(),
        )
    )

    with pytest.raises(RuntimeError, match="single-consumer"):
        async for _record in source.stream():
            pass

    assert client.xinfo_consumers_calls == [("events", "g")]
    assert client.xgroup_setid_calls == []


@pytest.mark.asyncio
async def test_redis_stream_source_acknowledges_dropped_bad_messages_when_ack_disabled() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
        ack_on_success=False,
        on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("1-0", {"value": "bad"}),
                    ("2-0", {"value": "2"}),
                ],
            )
        ]
    )
    source._client = client  # type: ignore[attr-defined]

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [2]
    assert _acked_ids(client) == ["1-0"]
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }


@pytest.mark.asyncio
async def test_redis_stream_source_reclaims_pending_messages_before_tailing_new_ones() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
        reclaim_idle_ms=60_000,
        reclaim_batch_size=2,
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("4-0", {"value": "4"}),
                ],
            )
        ],
        reclaimed_batches=[
            [
                ("2-0", {"value": "2"}),
                ("3-0", {"value": "3"}),
            ]
        ],
    )
    source._client = client  # type: ignore[attr-defined]

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [2, 3, 4]
    assert client.xautoclaim_calls[0] == ("events", "g", "c", 60_000, "0-0", 2)
    assert client.xreadgroup_calls[0] == {"events": ">"}
    assert _acked_ids(client) == ["2-0", "3-0", "4-0"]
    metrics = source.metrics_snapshot()
    assert metrics.reclaimed_message_count == 2
    assert metrics.last_reclaim_at is not None


@pytest.mark.asyncio
async def test_redis_stream_source_fairness_yields_to_live_tail_after_reclaim_streak() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
        reclaim_idle_ms=60_000,
        reclaim_batch_size=1,
        max_consecutive_reclaim_batches=1,
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("3-0", {"value": "3"}),
                ],
            )
        ],
        reclaimed_batches=[
            [("1-0", {"value": "1"})],
            [("2-0", {"value": "2"})],
        ],
    )
    source._client = client  # type: ignore[attr-defined]

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [1, 3, 2]
    assert len(client.xautoclaim_calls) == 2
    assert client.xreadgroup_calls == [{"events": ">"}, {"events": ">"}]
    assert _acked_ids(client) == ["1-0", "3-0", "2-0"]
    metrics = source.metrics_snapshot()
    assert metrics.max_consecutive_reclaim_batches == 1
    assert metrics.reclaimed_message_count == 2
    assert metrics.reclaim_fairness_yield_count == 2
    assert metrics.consecutive_reclaim_batch_count == 0


@pytest.mark.asyncio
async def test_redis_stream_source_reconnects_after_retryable_read_error() -> None:
    redis = pytest.importorskip("redis")

    class _FlakyRedisClient(_FakeRedisClient):
        def __init__(self) -> None:
            super().__init__(entries=[])
            self.failed_once = False

        async def xreadgroup(
            self,
            group: str,
            consumer: str,
            streams: dict[str, str],
            *,
            count: int,
            block: int,
        ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
            del group, consumer, streams, count, block
            if not self.failed_once:
                self.failed_once = True
                raise redis.exceptions.ConnectionError("connection dropped during failover")
            raise asyncio.CancelledError

    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    first_client = _FlakyRedisClient()
    second_client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("1-0", {"value": "1"}),
                ],
            )
        ]
    )
    source._client = first_client  # type: ignore[attr-defined]

    async def _build_client() -> _FakeRedisClient:
        return second_client

    async def _ensure_group(_client: object) -> None:
        source._group_ready = True  # type: ignore[attr-defined]

    source._build_client = _build_client  # type: ignore[method-assign]
    source._ensure_group = _ensure_group  # type: ignore[method-assign]

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    metrics = source.metrics_snapshot()
    assert records == [1]
    assert _acked_ids(second_client) == ["1-0"]
    assert metrics.reconnect_count == 1
    assert metrics.last_reconnect_at is not None


@pytest.mark.asyncio
async def test_redis_stream_source_reconnect_uses_configured_retry_policy() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        reconnect_retry_policy=RetryPolicy[Any](
            max_attempts=1,
            retry_exceptions=(RuntimeError,),
        ),
    )
    source._client = _FakeRedisClient([])  # type: ignore[attr-defined]

    async def _build_client() -> _FakeRedisClient:
        raise RuntimeError("still down")

    source._build_client = _build_client  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="still down"):
        await source._reconnect_client()  # type: ignore[attr-defined]

    assert source.metrics_snapshot().reconnect_count == 0


@pytest.mark.asyncio
async def test_redis_stream_source_acknowledges_reclaimed_poison_with_log_and_continue() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
        ack_on_success=False,
        reclaim_idle_ms=60_000,
        reclaim_batch_size=1,
        on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
    )
    client = _FakeRedisClient(
        [],
        reclaimed_batches=[
            [("1-0", {"value": "bad"})],
        ],
    )
    source._client = client  # type: ignore[attr-defined]

    with pytest.raises(asyncio.CancelledError):
        async for _record in source.stream():
            pytest.fail("No record should be emitted for reclaimed poison messages.")

    metrics = source.metrics_snapshot()
    report = source.acceptance_report()
    rendered = source.render_prometheus_metrics(namespace="agora_test_redis")

    assert metrics.reclaimed_message_count == 1
    assert metrics.record_error_count == 1
    assert metrics.record_drop_count == 1
    assert metrics.acked_message_count == 1
    assert metrics.poison_loop_risk.detected is False
    assert metrics.poison_loop_risk.loop_count == 0
    assert metrics.poison_loop_risk.distinct_message_count == 0
    assert metrics.poison_loop_risk.last_message_id is None
    assert metrics.poison_loop_risk.last_detected_at is None
    assert isinstance(report, AcceptanceReport)
    assert all(isinstance(finding, AcceptanceFinding) for finding in report.findings)
    assert not any(finding.metric == "poison_loop_count" for finding in report.findings)
    assert _acked_ids(client) == ["1-0"]
    assert (
        "agora_test_redis_source_state"
        '{stream="events",group="g",consumer="c",state="poison_loop_risk"} 0'
    ) in rendered
    assert (
        "agora_test_redis_source_events_total"
        '{stream="events",group="g",consumer="c",event="poison_loop"} 0'
    ) in rendered


@pytest.mark.asyncio
async def test_redis_stream_source_acknowledges_reclaimed_poison_after_dlq_route() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
        reclaim_idle_ms=60_000,
        reclaim_batch_size=1,
    )
    client = _FakeRedisClient([], reclaimed_batches=[[("1-0", {"value": "bad"})]])
    dlq = _CollectDLQSink()
    sink = _CollectSink()
    source._client = client  # type: ignore[attr-defined]
    source.open = _noop_async  # type: ignore[method-assign]
    source.close = _noop_async  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="invalid literal for int"):
        await Pipeline(source).build(sink, config=DeliveryConfig(dlq=dlq)).run(max_records=1)  # type: ignore[arg-type]

    assert _acked_ids(client) == ["1-0"]
    assert len(dlq.records) == 1


@pytest.mark.asyncio
async def test_redis_stream_source_acknowledges_final_record_when_pipeline_stops_at_max_records() -> (
    None
):
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    client = _FakeRedisClient(
        [
            (
                "events",
                [
                    ("1-0", {"value": "1"}),
                    ("2-0", {"value": "2"}),
                ],
            )
        ]
    )
    source._client = client  # type: ignore[attr-defined]

    async def _noop_open() -> None:
        return None

    source.open = _noop_open  # type: ignore[method-assign]
    sink = _CountSink()

    summary = await Pipeline(source).build(sink).run(max_records=1)  # type: ignore[arg-type]

    assert summary.records_consumed == 1
    assert sink.count == 1
    assert _acked_ids(client) == ["1-0"]


@pytest.mark.asyncio
async def test_redis_stream_checkpoint_resume_survives_crash_after_checkpoint_before_xack() -> None:
    store = _CrashAfterSaveCheckpointStore()

    async def _noop_open() -> None:
        return None

    first_source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    first_client = _SeekAwareRedisClient(
        [
            ("1-0", {"value": "1"}),
            ("2-0", {"value": "2"}),
        ]
    )
    first_source._client = first_client  # type: ignore[attr-defined]
    first_source._group_ready = True  # type: ignore[attr-defined]
    first_source.open = _noop_open  # type: ignore[method-assign]
    first_sink = _CollectSink()

    with pytest.raises(RuntimeError, match="crash after checkpoint save"):
        await (
            Pipeline(first_source, id="redis-crash-window")
            .build(first_sink, config=DeliveryConfig(checkpoint=store))
            .run(max_records=1)
        )

    saved = await store.load("redis-crash-window")
    assert saved is not None
    assert saved.value == {
        "stream": "events",
        "group": "g",
        "consumer": "c",
        "message_id": "1-0",
    }
    assert first_sink.records == [1]
    assert _acked_ids(first_client) == []

    second_source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        deserializer=lambda fields: int(fields["value"]),
    )
    second_client = _SeekAwareRedisClient(
        [
            ("1-0", {"value": "1"}),
            ("2-0", {"value": "2"}),
        ]
    )
    second_source._client = second_client  # type: ignore[attr-defined]
    second_source._group_ready = True  # type: ignore[attr-defined]
    second_source.open = _noop_open  # type: ignore[method-assign]
    second_sink = _CollectSink()

    summary = await (
        Pipeline(second_source, id="redis-crash-window")
        .build(second_sink, config=DeliveryConfig(checkpoint=store))
        .run(max_records=1)
    )

    assert summary.records_consumed == 1
    assert second_sink.records == [2]
    assert second_client.xgroup_setid_calls == [("events", "g", "1-0")]
    assert _acked_ids(second_client) == ["2-0"]


def test_redis_stream_source_health_acceptance_and_prometheus_surface() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        ack_batch_size=5,
        reclaim_idle_ms=30_000,
    )
    source._client = object()  # type: ignore[attr-defined]
    source._group_ready = True  # type: ignore[attr-defined]
    source._pending_ack_ids = ["1-0", "2-0"]  # type: ignore[attr-defined]
    source._poison_loop_count = 2  # type: ignore[attr-defined]
    source._poison_loop_message_ids = {"2-0"}  # type: ignore[attr-defined]
    source._last_poison_loop_message_id = "2-0"  # type: ignore[attr-defined]
    source._max_consecutive_reclaim_batches = 3  # type: ignore[attr-defined]
    source._consecutive_reclaim_batch_count = 2  # type: ignore[attr-defined]
    source._reclaim_fairness_yield_count = 4  # type: ignore[attr-defined]
    source._last_error = "boom"  # type: ignore[attr-defined]

    health = source.health_snapshot()
    metrics = source.metrics_snapshot()
    report = source.acceptance_report(
        RedisSourceEnterpriseAcceptanceThresholds(
            max_pending_ack_count=2,
            max_poison_loop_count=2,
        )
    )
    rendered = source.render_prometheus_metrics(namespace="agora_test_redis")

    assert health.ready is True
    assert isinstance(health, ComponentHealthSnapshot)
    assert health.connection_ready is True
    assert health.group_ready is True
    assert health.ack_enabled is True
    assert health.reclaim_enabled is True
    assert health.last_error == "boom"
    assert metrics.pending_ack_count == 2
    assert metrics.max_consecutive_reclaim_batches == 3
    assert metrics.consecutive_reclaim_batch_count == 2
    assert metrics.reclaim_fairness_yield_count == 4
    assert metrics.poison_loop_risk.detected is True
    assert metrics.poison_loop_risk.loop_count == 2
    assert report.passed is True
    assert (
        'agora_test_redis_source_state{stream="events",group="g",consumer="c",state="ready"} 1'
        in rendered
    )
    assert (
        'agora_test_redis_source_state{stream="events",group="g",consumer="c",state="poison_loop_risk"} 1'
        in rendered
    )
    assert (
        'agora_test_redis_source_gauge{stream="events",group="g",consumer="c",gauge="pending_ack_count"} 2'
        in rendered
    )
    assert (
        'agora_test_redis_source_gauge{stream="events",group="g",consumer="c",gauge="consecutive_reclaim_batch_count"} 2'
        in rendered
    )
    assert (
        'agora_test_redis_source_gauge{stream="events",group="g",consumer="c",gauge="max_consecutive_reclaim_batches"} 3'
        in rendered
    )
    assert (
        'agora_test_redis_source_events_total{stream="events",group="g",consumer="c",event="reclaim_fairness_yield"} 4'
        in rendered
    )
    assert "# TYPE agora_test_redis_source_age_ms gauge" not in rendered


def test_redis_stream_source_acceptance_rejects_ack_before_delivery() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="events",
        group="g",
        consumer="c",
        ack_on_success=False,
    )
    source._client = object()  # type: ignore[attr-defined]
    source._group_ready = True  # type: ignore[attr-defined]

    report = source.acceptance_report()
    relaxed_report = source.acceptance_report(
        RedisSourceEnterpriseAcceptanceThresholds(require_ack_on_success=False)
    )

    assert report.passed is False
    assert {finding.metric for finding in report.findings} == {"ack_on_success"}
    assert relaxed_report.passed is True
