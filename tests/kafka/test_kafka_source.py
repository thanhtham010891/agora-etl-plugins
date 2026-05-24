from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from agora import SourceRecordError, SourceRecordFailurePolicy
from agora.core.checkpoint import Checkpoint

from agora_plugins.kafka import KafkaSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _FakeMessage:
    def __init__(
        self, value: bytes, *, topic: str = "t", partition: int = 0, offset: int = 0
    ) -> None:
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset


class _FakeConsumer:
    def __init__(self, values: list[bytes]) -> None:
        self._messages = [_FakeMessage(v, offset=i) for i, v in enumerate(values)]
        self.commit_calls = 0
        self.commit_offsets: list[dict[object, int] | None] = []
        self.stop_calls = 0
        self.seek_calls: list[tuple[object, int]] = []

    def __aiter__(self) -> AsyncIterator[_FakeMessage]:
        async def _gen():
            for message in self._messages:
                yield message

        return _gen()

    async def commit(self, offsets: dict[object, int] | None = None) -> None:
        self.commit_calls += 1
        self.commit_offsets.append(offsets)

    async def stop(self) -> None:
        self.stop_calls += 1

    def seek(self, partition: object, offset: int) -> None:
        self.seek_calls.append((partition, offset))

    def assignment(self) -> set[tuple[str, int]]:
        return {(message.topic, message.partition) for message in self._messages}


class _FakeBatchConsumer:
    def __init__(self, batches: list[list[bytes]]) -> None:
        self._batches = [
            [
                _FakeMessage(value, topic="events", offset=offset)
                for offset, value in enumerate(batch, start=batch_index * 100)
            ]
            for batch_index, batch in enumerate(batches)
        ]
        self.commit_calls = 0
        self.commit_offsets: list[dict[object, int] | None] = []
        self.stop_calls = 0
        self.seek_calls: list[tuple[object, int]] = []

    async def getmany(
        self,
        *,
        timeout_ms: int,
        max_records: int,
    ) -> dict[tuple[str, int], list[_FakeMessage]]:
        del timeout_ms
        if not self._batches:
            raise StopAsyncIteration
        batch = self._batches.pop(0)
        return {("events", 0): batch[:max_records]}

    async def commit(self, offsets: dict[object, int] | None = None) -> None:
        self.commit_calls += 1
        self.commit_offsets.append(offsets)

    async def stop(self) -> None:
        self.stop_calls += 1

    def seek(self, partition: object, offset: int) -> None:
        self.seek_calls.append((partition, offset))

    def assignment(self) -> set[tuple[str, int]]:
        return {(message.topic, message.partition) for batch in self._batches for message in batch}


@dataclass(frozen=True)
class _FakeTopicPartition:
    topic: str
    partition: int


@pytest.mark.asyncio
async def test_manual_commit_batches_processed_records() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=2,
    )
    source._consumer = _FakeConsumer([b"a", b"b", b"c"])  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["a", "b", "c"]
    assert source._consumer.commit_calls == 2  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_close_flushes_pending_manual_commit() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=10,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]
    assert records == ["a"]
    assert consumer.commit_calls == 1

    source._pending_commit_count = 1  # type: ignore[attr-defined]
    await source.close()

    assert consumer.commit_calls == 2
    assert consumer.stop_calls == 1


@pytest.mark.asyncio
async def test_bad_messages_fail_closed_by_default() -> None:
    calls: list[bytes] = []

    def deserializer(value: bytes) -> str:
        calls.append(value)
        if value == b"bad":
            raise ValueError("bad payload")
        return value.decode()

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=2,
    )
    consumer = _FakeConsumer([b"good", b"bad", b"ok"])
    source._consumer = consumer  # type: ignore[attr-defined]

    with pytest.raises(SourceRecordError, match="bad payload") as exc_info:
        _ = [record async for record in source.stream()]

    assert calls == [b"good", b"bad"]
    assert consumer.commit_calls == 1
    assert isinstance(exc_info.value.original, ValueError)
    assert exc_info.value.record == b"bad"
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 0,
    }


@pytest.mark.asyncio
async def test_bad_messages_can_log_and_continue_when_opted_in() -> None:
    calls: list[bytes] = []

    def deserializer(value: bytes) -> str:
        calls.append(value)
        if value == b"bad":
            raise ValueError("bad payload")
        return value.decode()

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=2,
        on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
    )
    consumer = _FakeConsumer([b"good", b"bad", b"ok"])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["good", "ok"]
    assert calls == [b"good", b"bad", b"ok"]
    assert consumer.commit_calls == 2
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }


@pytest.mark.asyncio
async def test_getmany_batches_messages_when_supported() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=2,
        poll_timeout_ms=10,
        max_poll_records=10,
    )
    consumer = _FakeBatchConsumer([[b"a", b"b"], [b"c"]])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["a", "b", "c"]
    assert consumer.commit_calls == 2


@pytest.mark.asyncio
async def test_prepare_resume_seeks_consumer_to_next_offset() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=2,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]

    await source.prepare_resume(
        Checkpoint(
            pipeline_id="pipe",
            run_id="run",
            source="kafka",
            value={"topic": "t", "partition": 0, "offset": 7},
        )
    )

    records = [record async for record in source.stream()]

    assert records == ["a"]
    assert consumer.seek_calls == [(_FakeTopicPartition("t", 0), 8)]


@pytest.mark.asyncio
async def test_current_checkpoint_tracks_offsets_for_multiple_partitions() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=10,
    )
    consumer = _FakeConsumer([])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="events", partition=0, offset=5),
        _FakeMessage(b"b", topic="events", partition=1, offset=8),
        _FakeMessage(b"c", topic="events", partition=0, offset=6),
    ]
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["a", "b", "c"]
    assert source.current_checkpoint() == {
        "topic": "events",
        "partition": 0,
        "offset": 6,
        "offsets": [
            {"topic": "events", "partition": 0, "offset": 6},
            {"topic": "events", "partition": 1, "offset": 8},
        ],
    }
    assert consumer.commit_offsets[-1] == {
        ("events", 0): 7,
        ("events", 1): 9,
    }


@pytest.mark.asyncio
async def test_prepare_resume_seeks_all_partitions_from_offset_map() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=2,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]

    await source.prepare_resume(
        Checkpoint(
            pipeline_id="pipe",
            run_id="run",
            source="kafka",
            value={
                "offsets": [
                    {"topic": "t", "partition": 0, "offset": 7},
                    {"topic": "t", "partition": 1, "offset": 3},
                ]
            },
        )
    )
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="t", partition=0, offset=8),
    ]
    consumer.assignment = lambda: {("t", 0), ("t", 1)}  # type: ignore[method-assign]

    records = [record async for record in source.stream()]

    assert records == ["a"]
    assert consumer.seek_calls == [
        (_FakeTopicPartition("t", 0), 8),
        (_FakeTopicPartition("t", 1), 4),
    ]


@pytest.mark.asyncio
async def test_open_passes_fetch_tuning_to_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs_seen: dict[str, Any] = {}

    class _FakeAIOKafkaConsumer:
        def __init__(self, *topics: str, **kwargs: Any) -> None:
            kwargs_seen["topics"] = topics
            kwargs_seen.update(kwargs)

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    monkeypatch.setitem(
        sys.modules,
        "aiokafka",
        SimpleNamespace(AIOKafkaConsumer=_FakeAIOKafkaConsumer),
    )

    source = KafkaSource(
        topics=["events"],
        bootstrap_servers="broker:9092",
        group_id="agora-test",
        fetch_min_bytes=1024,
        fetch_max_wait_ms=250,
        max_partition_fetch_bytes=2_097_152,
        max_poll_records=321,
    )

    await source.open()

    assert kwargs_seen["topics"] == ("events",)
    assert kwargs_seen["bootstrap_servers"] == "broker:9092"
    assert kwargs_seen["group_id"] == "agora-test"
    assert kwargs_seen["max_poll_records"] == 321
    assert kwargs_seen["fetch_min_bytes"] == 1024
    assert kwargs_seen["fetch_max_wait_ms"] == 250
    assert kwargs_seen["max_partition_fetch_bytes"] == 2_097_152


@pytest.mark.asyncio
async def test_kafka_source_supports_async_deserializer_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle: dict[str, int] = {"open": 0, "close": 0}

    class AsyncDeserializer:
        async def open(self) -> None:
            lifecycle["open"] += 1

        async def close(self) -> None:
            lifecycle["close"] += 1

        async def __call__(self, value: bytes) -> str:
            return value.decode("utf-8").upper()

    class _FakeAIOKafkaConsumer:
        def __init__(self, *topics: str, **kwargs: Any) -> None:
            del topics, kwargs
            self._messages = [_FakeMessage(b"hello", topic="events", partition=0, offset=0)]

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def __aiter__(self) -> AsyncIterator[_FakeMessage]:
            async def _gen():
                for message in self._messages:
                    yield message

            return _gen()

        async def commit(self, offsets: dict[object, int] | None = None) -> None:
            del offsets
            return

        def assignment(self) -> set[tuple[str, int]]:
            return {("events", 0)}

    monkeypatch.setitem(
        sys.modules,
        "aiokafka",
        SimpleNamespace(
            AIOKafkaConsumer=_FakeAIOKafkaConsumer,
            TopicPartition=_FakeTopicPartition,
        ),
    )

    source = KafkaSource(
        topics=["events"],
        deserializer=AsyncDeserializer(),
        enable_auto_commit=False,
    )

    await source.open()
    records = [record async for record in source.stream()]
    await source.close()

    assert records == ["HELLO"]
    assert lifecycle == {"open": 1, "close": 1}
