from __future__ import annotations

import asyncio

import pytest
from agora import Pipeline, SourceRecordError, SourceRecordFailurePolicy
from agora.core.checkpoint import Checkpoint

from agora_plugins.redis import RedisStreamSource


class _FakeRedisClient:
    def __init__(
        self,
        entries: list[tuple[str, list[tuple[str, dict[str, str]]]]],
        reclaimed_batches: list[list[tuple[str, dict[str, str]]]] | None = None,
    ) -> None:
        self._entries = list(entries)
        self._reclaimed_batches = list(reclaimed_batches or [])
        self.xack_calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.xreadgroup_calls: list[dict[str, str]] = []
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


async def _ack_delivery(source: RedisStreamSource[object]) -> None:
    callback = source.delivery_success_callback()
    if callback is not None:
        await callback()


def _acked_ids(client: _FakeRedisClient) -> list[str]:
    ids: list[str] = []
    for _stream, _group, msg_ids in client.xack_calls:
        ids.extend(msg_ids)
    return ids


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
        )
    )

    records: list[int] = []
    with pytest.raises(asyncio.CancelledError):
        async for record in source.stream():
            records.append(record)
            await _ack_delivery(source)

    assert records == [3, 4]
    assert client.xreadgroup_calls[:3] == [
        {"events": "2-0"},
        {"events": "3-0"},
        {"events": ">"},
    ]
    assert _acked_ids(client) == ["3-0", "4-0"]
    assert source.current_checkpoint() == {
        "stream": "events",
        "group": "g",
        "consumer": "c",
        "message_id": "4-0",
    }


@pytest.mark.asyncio
async def test_redis_stream_source_does_not_ack_bad_messages_when_ack_disabled() -> None:
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
    assert _acked_ids(client) == []
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
