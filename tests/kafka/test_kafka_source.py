from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from agora import (
    DeliveryConfig,
    Pipeline,
    SourceRecordFailurePolicy,
)
from agora.core.checkpoint import Checkpoint, InMemoryCheckpointStore
from agora.core.failures import PoisonRecordClassification
from agora.core.source import SourceRecordError

from agora_plugins.kafka import (
    KafkaOpenTelemetryTracing,
    KafkaPoisonRecordClassification,
    KafkaPoisonRecordPolicy,
    KafkaSASLConfig,
    KafkaSecurityConfig,
    KafkaSource,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from agora.core.dlq import DLQRecord


class _FakeMessage:
    def __init__(
        self,
        value: bytes,
        *,
        topic: str = "t",
        partition: int = 0,
        offset: int = 0,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        timestamp: int | None = None,
        timestamp_type: int | None = None,
    ) -> None:
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.headers = headers or []
        self.timestamp = timestamp
        self.timestamp_type = timestamp_type


class _OAuthTokenProvider:
    async def token(self) -> str:
        return "access-token"


class _FakeConsumer:
    def __init__(self, values: list[bytes]) -> None:
        self._messages = [_FakeMessage(v, offset=i) for i, v in enumerate(values)]
        self.commit_calls = 0
        self.commit_offsets: list[dict[object, int] | None] = []
        self.stop_calls = 0
        self.seek_calls: list[tuple[object, int]] = []
        self.seek_to_beginning_calls: list[tuple[object, ...]] = []
        self.seek_to_end_calls: list[tuple[object, ...]] = []
        self.pause_calls: list[tuple[object, ...]] = []
        self.resume_calls: list[tuple[object, ...]] = []
        self.position_calls = 0
        self.end_offsets_calls = 0
        self.committed_calls = 0
        self.position_map: dict[tuple[str, int], int] = {
            (message.topic, message.partition): message.offset + 1 for message in self._messages
        }
        self.end_offset_map: dict[tuple[str, int], int] = dict(self.position_map)
        self.committed_map: dict[tuple[str, int], int] = dict(self.position_map)

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

    def seek_to_beginning(self, *partitions: object) -> None:
        self.seek_to_beginning_calls.append(partitions)

    def seek_to_end(self, *partitions: object) -> None:
        self.seek_to_end_calls.append(partitions)

    def assignment(self) -> set[tuple[str, int]]:
        return {(message.topic, message.partition) for message in self._messages}

    def pause(self, *partitions: object) -> None:
        self.pause_calls.append(partitions)

    def resume(self, *partitions: object) -> None:
        self.resume_calls.append(partitions)

    async def position(self, partition: object) -> int | None:
        self.position_calls += 1
        topic = getattr(partition, "topic", None)
        part = getattr(partition, "partition", None)
        if topic is None and isinstance(partition, tuple) and len(partition) >= 2:
            topic = partition[0]
            part = partition[1]
        return self.position_map.get((str(topic), int(part)))

    async def end_offsets(self, partitions: list[object]) -> dict[object, int]:
        self.end_offsets_calls += 1
        values: dict[object, int] = {}
        for partition in partitions:
            topic = getattr(partition, "topic", None)
            part = getattr(partition, "partition", None)
            if topic is None and isinstance(partition, tuple) and len(partition) >= 2:
                topic = partition[0]
                part = partition[1]
            values[partition] = self.end_offset_map[(str(topic), int(part))]
        return values

    async def committed(self, partition: object) -> int | None:
        self.committed_calls += 1
        topic = getattr(partition, "topic", None)
        part = getattr(partition, "partition", None)
        if topic is None and isinstance(partition, tuple) and len(partition) >= 2:
            topic = partition[0]
            part = partition[1]
        return self.committed_map.get((str(topic), int(part)))


class _SeekAwareFakeConsumer(_FakeConsumer):
    def __init__(self, values: list[bytes]) -> None:
        super().__init__(values)
        self._seek_offsets: dict[tuple[str, int], int] = {}

    def __aiter__(self) -> AsyncIterator[_FakeMessage]:
        async def _gen():
            for message in self._messages:
                start_offset = self._seek_offsets.get((message.topic, message.partition), 0)
                if message.offset >= start_offset:
                    yield message

        return _gen()

    def seek(self, partition: object, offset: int) -> None:
        super().seek(partition, offset)
        topic = getattr(partition, "topic", None)
        part = getattr(partition, "partition", None)
        if topic is None and isinstance(partition, tuple) and len(partition) >= 2:
            topic = partition[0]
            part = partition[1]
        self._seek_offsets[(str(topic), int(part))] = int(offset)


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
        self.seek_to_beginning_calls: list[tuple[object, ...]] = []
        self.seek_to_end_calls: list[tuple[object, ...]] = []
        self.pause_calls: list[tuple[object, ...]] = []
        self.resume_calls: list[tuple[object, ...]] = []
        self.position_calls = 0
        self.end_offsets_calls = 0
        self.committed_calls = 0
        self.position_map: dict[tuple[str, int], int] = {}
        self.end_offset_map: dict[tuple[str, int], int] = {}
        self.committed_map: dict[tuple[str, int], int] = {}

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

    def seek_to_beginning(self, *partitions: object) -> None:
        self.seek_to_beginning_calls.append(partitions)

    def seek_to_end(self, *partitions: object) -> None:
        self.seek_to_end_calls.append(partitions)

    def assignment(self) -> set[tuple[str, int]]:
        return {(message.topic, message.partition) for batch in self._batches for message in batch}

    def pause(self, *partitions: object) -> None:
        self.pause_calls.append(partitions)

    def resume(self, *partitions: object) -> None:
        self.resume_calls.append(partitions)

    async def position(self, partition: object) -> int | None:
        self.position_calls += 1
        topic = getattr(partition, "topic", None)
        part = getattr(partition, "partition", None)
        if topic is None and isinstance(partition, tuple) and len(partition) >= 2:
            topic = partition[0]
            part = partition[1]
        return self.position_map.get((str(topic), int(part)))

    async def end_offsets(self, partitions: list[object]) -> dict[object, int]:
        self.end_offsets_calls += 1
        values: dict[object, int] = {}
        for partition in partitions:
            topic = getattr(partition, "topic", None)
            part = getattr(partition, "partition", None)
            if topic is None and isinstance(partition, tuple) and len(partition) >= 2:
                topic = partition[0]
                part = partition[1]
            values[partition] = self.end_offset_map[(str(topic), int(part))]
        return values

    async def committed(self, partition: object) -> int | None:
        self.committed_calls += 1
        topic = getattr(partition, "topic", None)
        part = getattr(partition, "partition", None)
        if topic is None and isinstance(partition, tuple) and len(partition) >= 2:
            topic = partition[0]
            part = partition[1]
        return self.committed_map.get((str(topic), int(part)))


@dataclass(frozen=True)
class _FakeTopicPartition:
    topic: str
    partition: int


class _FakeSpan:
    def __init__(self, tracer: _FakeTracer, name: str, kwargs: dict[str, Any]) -> None:
        self._tracer = tracer
        self._name = name
        self._kwargs = kwargs

    def __enter__(self) -> _FakeSpan:
        self._tracer.entered.append((self._name, self._kwargs))
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tracer.exited.append(self._name)


class _FakeTracer:
    def __init__(self) -> None:
        self.entered: list[tuple[str, dict[str, Any]]] = []
        self.exited: list[str] = []

    def start_as_current_span(self, name: str, **kwargs: Any) -> _FakeSpan:
        return _FakeSpan(self, name, kwargs)


class _FakePropagator:
    def __init__(self) -> None:
        self.extracted: list[dict[str, str]] = []

    def extract(self, carrier: dict[str, str]) -> str:
        self.extracted.append(dict(carrier))
        return "extracted-context"


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def open(self) -> None:
        return None

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CrashAfterSaveCheckpointStore(InMemoryCheckpointStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_next_save = True
        self.saved_checkpoints: list[Checkpoint] = []

    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        await super().save(key, checkpoint)
        self.saved_checkpoints.append(checkpoint)
        if self.fail_next_save:
            self.fail_next_save = False
            raise RuntimeError("crash after checkpoint save")


class _CollectDLQSink:
    sink_name = "dlq"

    def __init__(self) -> None:
        self.records: list[DLQRecord] = []
        self.open_calls = 0
        self.close_calls = 0

    async def open(self) -> None:
        self.open_calls += 1

    async def write(self, record: DLQRecord) -> None:
        self.records.append(record)

    async def write_batch(self, records: list[DLQRecord]) -> None:
        self.records.extend(records)

    async def close(self) -> None:
        self.close_calls += 1


class _FailingDLQSink:
    sink_name = "dlq"

    async def open(self) -> None:
        return None

    async def write(self, record: DLQRecord) -> None:
        del record
        raise RuntimeError("dlq unavailable")

    async def write_batch(self, records: list[DLQRecord]) -> None:
        del records
        raise RuntimeError("dlq unavailable")

    async def close(self) -> None:
        return None


def test_kafka_source_requires_topics_or_pattern() -> None:
    with pytest.raises(
        ValueError,
        match="requires `topics`, `topic_pattern`, or `assignments`",
    ):
        KafkaSource()


def test_kafka_source_rejects_topics_and_pattern_together() -> None:
    with pytest.raises(ValueError, match="either `topics` or `topic_pattern`"):
        KafkaSource(topics=["events"], topic_pattern=r"events\..*")


def test_kafka_source_rejects_assignments_with_topics_or_pattern() -> None:
    with pytest.raises(ValueError, match="accepts `assignments` only"):
        KafkaSource(topics=["events"], assignments=[("events", 0)])


def test_kafka_source_warns_on_plaintext_non_local_bootstrap() -> None:
    with pytest.warns(UserWarning, match="bootstrap_servers='broker.prod.example.com:9092'"):
        KafkaSource(
            topics=["events"],
            bootstrap_servers="broker.prod.example.com:9092",
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"commit_every": 0}, "commit_every"),
        ({"poll_timeout_ms": -1}, "poll_timeout_ms"),
        ({"max_idle_polls": 0}, "max_idle_polls"),
        ({"max_poll_records": 0}, "max_poll_records"),
        ({"fetch_min_bytes": 0}, "fetch_min_bytes"),
        ({"fetch_max_wait_ms": -1}, "fetch_max_wait_ms"),
        ({"max_partition_fetch_bytes": 0}, "max_partition_fetch_bytes"),
        ({"health_snapshot_cache_ms": -1}, "health_snapshot_cache_ms"),
    ],
)
def test_kafka_source_rejects_invalid_tuning_values(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        KafkaSource(topics=["events"], **kwargs)


@pytest.mark.parametrize(
    ("extra_config", "exception_type", "message"),
    [
        ({"request_timeout_ms": 0}, ValueError, "request_timeout_ms"),
        ({"session_timeout_ms": False}, TypeError, "session_timeout_ms"),
        ({"retry_backoff_ms": -1}, ValueError, "retry_backoff_ms"),
    ],
)
def test_kafka_source_rejects_invalid_extra_consumer_tuning(
    extra_config: dict[str, object],
    exception_type: type[Exception],
    message: str,
) -> None:
    with pytest.raises(exception_type, match=message):
        KafkaSource(topics=["events"], extra_config=extra_config)


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
async def test_kafka_source_tracing_extracts_traceparent_and_spans_deserialize() -> None:
    tracer = _FakeTracer()
    propagator = _FakePropagator()
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=10,
        tracing=KafkaOpenTelemetryTracing(
            enabled=True,
            tracer=tracer,
            propagator=propagator,
            consumer_span_kind="consumer",
            client_span_kind="client",
        ),
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages[0].headers = [("traceparent", b"00-incoming-trace")]
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["a"]
    assert propagator.extracted == [{"traceparent": "00-incoming-trace"}]
    assert tracer.entered[0][0] == "kafka.consume"
    assert tracer.entered[0][1]["kind"] == "consumer"
    assert tracer.entered[0][1]["context"] == "extracted-context"
    assert tracer.entered[0][1]["attributes"]["messaging.kafka.offset"] == 0
    assert tracer.entered[-1][0] == "kafka.commit"


@pytest.mark.asyncio
async def test_delivery_context_tracks_active_record_coordinates() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=10,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]

    stream = source.stream()
    record = await anext(stream)

    assert record == "a"
    assert source.delivery_context() is not None
    assert source.delivery_context().to_dict() == {
        "topic": "t",
        "partition": 0,
        "offset": 0,
        "consumer_group": "agora-consumer",
        "bootstrap_servers": "localhost:9092",
        "subscription_mode": "topics",
        "batch_size": 1,
        "batch_index": 0,
        "key": None,
        "headers": [],
        "timestamp": None,
        "timestamp_type": None,
        "delivery_id": "t:0:0",
    }

    await stream.aclose()

    assert source.delivery_context() is None


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

    source._cursor_state.pending_commit_count = 1  # type: ignore[attr-defined]
    await source.close()

    assert consumer.commit_calls == 2
    assert consumer.stop_calls == 1


@pytest.mark.asyncio
async def test_kafka_checkpoint_resume_survives_crash_after_checkpoint_before_commit() -> None:
    store = _CrashAfterSaveCheckpointStore()
    first_source = KafkaSource(
        topics=["events"],
        deserializer=lambda value: value.decode(),
        enable_auto_commit=False,
        commit_every=1,
    )
    first_consumer = _SeekAwareFakeConsumer([b"first", b"second"])
    first_source._consumer = first_consumer  # type: ignore[attr-defined]

    async def _noop() -> None:
        return None

    first_source.open = _noop  # type: ignore[method-assign]
    first_source.close = _noop  # type: ignore[method-assign]
    first_sink = _CollectSink()

    with pytest.raises(RuntimeError, match="crash after checkpoint save"):
        await (
            Pipeline(first_source, id="kafka-crash-window")
            .build(first_sink, config=DeliveryConfig(checkpoint=store))
            .run(max_records=1)
        )

    saved = await store.load("kafka-crash-window")
    assert saved is not None
    assert saved.value == {
        "topic": "t",
        "partition": 0,
        "offset": 0,
        "offsets": [{"topic": "t", "partition": 0, "offset": 0}],
    }
    assert first_sink.records == ["first"]
    assert first_consumer.commit_calls == 0

    second_source = KafkaSource(
        topics=["events"],
        deserializer=lambda value: value.decode(),
        enable_auto_commit=False,
        commit_every=1,
    )
    second_consumer = _SeekAwareFakeConsumer([b"first", b"second"])
    second_source._consumer = second_consumer  # type: ignore[attr-defined]
    second_source.open = _noop  # type: ignore[method-assign]
    second_source.close = _noop  # type: ignore[method-assign]
    second_sink = _CollectSink()

    summary = await (
        Pipeline(second_source, id="kafka-crash-window")
        .build(second_sink, config=DeliveryConfig(checkpoint=store))
        .run(max_records=1)
    )

    assert summary.records_consumed == 1
    assert second_sink.records == ["second"]
    assert second_consumer.seek_calls == [(("t", 0), 1)]
    assert second_consumer.commit_calls == 1
    assert second_consumer.commit_offsets == [{("t", 0): 2}]


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
    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 0,
        "batch_deserialize_error_count": 0,
        "manual_assign_partition_count": 0,
        "paused_partition_count": 0,
        "poison_record_dlq_write_count": 0,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 0,
        "poison_record_fail_closed_count": 1,
        "poison_record_deserialization_count": 1,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_deserializer_can_receive_kafka_message_metadata() -> None:
    seen_metadata: list[dict[str, Any]] = []

    def deserializer(value: bytes, metadata: dict[str, Any]) -> dict[str, Any]:
        seen_metadata.append(metadata)
        return {"value": value.decode(), "metadata": metadata}

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=2,
    )
    consumer = _FakeConsumer([])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(
            b"hello",
            topic="events",
            partition=2,
            offset=41,
            key=b"tenant-1",
            headers=[("event_type", b"order.created")],
            timestamp=1_717_000_000_000,
            timestamp_type=1,
        )
    ]
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == [
        {
            "value": "hello",
            "metadata": {
                "topic": "events",
                "partition": 2,
                "offset": 41,
                "key": b"tenant-1",
                "headers": [("event_type", b"order.created")],
                "timestamp": 1_717_000_000_000,
                "timestamp_type": 1,
                "consumer_group": "agora-consumer",
                "bootstrap_servers": "localhost:9092",
                "subscription_mode": "topics",
                "batch_size": 1,
                "batch_index": 0,
            },
        }
    ]
    assert seen_metadata == [records[0]["metadata"]]


@pytest.mark.asyncio
async def test_async_deserializer_can_receive_kafka_message_metadata() -> None:
    async def deserializer(value: bytes, metadata: dict[str, Any]) -> str:
        return f"{metadata['topic']}:{metadata['partition']}:{value.decode()}"

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=2,
    )
    source._consumer = _FakeConsumer([b"hello"])  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["t:0:hello"]


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
    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 0,
        "batch_deserialize_error_count": 0,
        "manual_assign_partition_count": 0,
        "paused_partition_count": 0,
        "poison_record_dlq_write_count": 0,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 1,
        "poison_record_fail_closed_count": 0,
        "poison_record_deserialization_count": 1,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_bad_messages_can_dlq_and_continue_when_opted_in() -> None:
    dlq = _CollectDLQSink()

    def deserializer(value: bytes) -> str:
        if value == b"bad":
            raise ValueError("bad payload")
        return value.decode()

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=2,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=dlq,
        poison_record_pipeline_id="orders-kafka-source",
        poison_record_max_attempts=5,
    )
    consumer = _FakeConsumer([b"good", b"bad", b"ok"])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["good", "ok"]
    assert consumer.commit_calls == 2
    assert len(dlq.records) == 1
    poison = dlq.records[0]
    assert poison.pipeline_id == "orders-kafka-source"
    assert poison.stage == "kafka_deserialize"
    assert poison.error_type == "ValueError"
    assert poison.error_message == "bad payload"
    assert poison.max_attempts == 5
    assert poison.record == {
        "topic": "t",
        "partition": 0,
        "offset": 1,
        "key": None,
        "value": {"encoding": "utf-8", "data": "bad"},
        "headers": [],
        "timestamp": None,
        "timestamp_type": None,
        "metadata": {
            "topic": "t",
            "partition": 0,
            "offset": 1,
            "key": None,
            "headers": [],
            "timestamp": None,
            "timestamp_type": None,
            "consumer_group": "agora-consumer",
            "bootstrap_servers": "localhost:9092",
            "subscription_mode": "topics",
            "batch_size": 1,
            "batch_index": 0,
        },
        "poison": {
            "classification": "deserialization",
            "policy": "dlq_and_continue",
        },
    }
    assert KafkaPoisonRecordClassification is PoisonRecordClassification
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }
    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 0,
        "batch_deserialize_error_count": 0,
        "manual_assign_partition_count": 0,
        "paused_partition_count": 0,
        "poison_record_dlq_write_count": 1,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 0,
        "poison_record_fail_closed_count": 0,
        "poison_record_deserialization_count": 1,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_poison_dlq_write_failure_continues_and_commits_when_policy_allows_progress() -> None:
    def deserializer(value: bytes) -> str:
        if value == b"bad":
            raise ValueError("bad payload")
        return value.decode()

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=1,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=_FailingDLQSink(),
    )
    consumer = _FakeConsumer([b"good", b"bad", b"ok"])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["good", "ok"]
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 1,
        "record_drop_count": 1,
    }
    assert consumer.commit_calls == 3
    metrics = source.operational_metrics().to_dict()
    assert metrics["poison_record_dlq_write_count"] == 0
    assert metrics["poison_record_dlq_write_failure_count"] == 1
    assert metrics["poison_record_deserialization_count"] == 1


@pytest.mark.asyncio
async def test_poison_dlq_write_failure_raises_when_policy_is_fail_closed() -> None:
    def deserializer(value: bytes) -> str:
        del value
        raise ValueError("bad payload")

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=1,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_FAIL_CLOSED,
        poison_record_sink=_FailingDLQSink(),
    )
    consumer = _FakeConsumer([b"bad"])
    source._consumer = consumer  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="dlq unavailable"):
        _ = [record async for record in source.stream()]

    assert source.operational_metrics().to_dict()["poison_record_dlq_write_failure_count"] == 1


@pytest.mark.asyncio
async def test_poison_dlq_binary_payloads_are_base64_encoded() -> None:
    dlq = _CollectDLQSink()

    def deserializer(value: bytes) -> str:
        raise ValueError("bad payload")

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=1,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=dlq,
    )
    consumer = _FakeConsumer([b"\x00\x00\x00\x00\x06\x06\x00"])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == []
    assert len(dlq.records) == 1
    assert dlq.records[0].record["value"] == {  # type: ignore[index]
        "encoding": "base64",
        "data": "AAAAAAYGAA==",
    }
    assert dlq.records[0].record["poison"] == {  # type: ignore[index]
        "classification": "deserialization",
        "policy": "dlq_and_continue",
    }


@pytest.mark.asyncio
async def test_poison_dlq_classifies_json_schema_validation_errors() -> None:
    pytest.importorskip("jsonschema")
    from jsonschema.exceptions import ValidationError

    dlq = _CollectDLQSink()

    def deserializer(value: bytes) -> str:
        raise ValidationError(f"{value.decode('utf-8')!r} is not of type 'integer'")

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=1,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=dlq,
    )
    source._consumer = _FakeConsumer([b"511"])  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == []
    assert dlq.records[0].record["poison"] == {  # type: ignore[index]
        "classification": "schema_validation",
        "policy": "dlq_and_continue",
    }
    assert source.operational_metrics().to_dict()["poison_record_schema_validation_count"] == 1


@pytest.mark.asyncio
async def test_poison_dlq_classifies_protobuf_registry_binding_mismatch_errors() -> None:
    dlq = _CollectDLQSink()

    def deserializer(value: bytes) -> str:
        raise ValueError(
            "Protobuf schema-registry binding mismatch: payload indexes (1,) resolve to "
            "'agora.integration.CustomerCreated', but local message_type is "
            "'agora.integration.OrderCreated'."
        )

    source = KafkaSource(
        topics=["events"],
        deserializer=deserializer,
        enable_auto_commit=False,
        commit_every=1,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=dlq,
    )
    source._consumer = _FakeConsumer([b"proto"])  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == []
    assert dlq.records[0].record["poison"] == {  # type: ignore[index]
        "classification": "schema_registry_binding_mismatch",
        "policy": "dlq_and_continue",
    }
    assert (
        source.operational_metrics().to_dict()[
            "poison_record_schema_registry_binding_mismatch_count"
        ]
        == 1
    )


def test_dlq_poison_policy_requires_sink() -> None:
    with pytest.raises(ValueError, match="poison_record_sink"):
        KafkaSource(
            topics=["events"],
            deserializer=lambda b: b.decode(),
            poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        )


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
async def test_getmany_can_exit_after_configured_idle_polls() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=2,
        poll_timeout_ms=10,
        max_poll_records=10,
        max_idle_polls=2,
    )
    consumer = _FakeBatchConsumer([[b"a", b"b"], [], []])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["a", "b"]
    assert consumer.commit_calls == 1


@pytest.mark.asyncio
async def test_batch_deserializer_receives_richer_consume_context() -> None:
    seen_contexts: list[dict[str, Any]] = []

    def batch_deserializer(values: list[bytes], context: dict[str, Any]) -> list[str]:
        seen_contexts.append(context)
        return [value.decode("utf-8").upper() for value in values]

    source = KafkaSource(
        topics=["events"],
        batch_deserializer=batch_deserializer,
        enable_auto_commit=False,
        commit_every=10,
        poll_timeout_ms=10,
        max_poll_records=10,
    )
    consumer = _FakeBatchConsumer([[b"a", b"b"]])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == ["A", "B"]
    assert seen_contexts == [
        {
            "topics": ["events"],
            "topic_pattern": None,
            "assignments": [{"topic": "events", "partition": 0}],
            "consumer_group": "agora-consumer",
            "bootstrap_servers": "localhost:9092",
            "subscription_mode": "topics",
            "batch_size": 2,
            "messages": [
                {
                    "topic": "events",
                    "partition": 0,
                    "offset": 0,
                    "key": None,
                    "headers": [],
                    "timestamp": None,
                    "timestamp_type": None,
                    "consumer_group": "agora-consumer",
                    "bootstrap_servers": "localhost:9092",
                    "subscription_mode": "topics",
                    "batch_size": 2,
                    "batch_index": 0,
                },
                {
                    "topic": "events",
                    "partition": 0,
                    "offset": 1,
                    "key": None,
                    "headers": [],
                    "timestamp": None,
                    "timestamp_type": None,
                    "consumer_group": "agora-consumer",
                    "bootstrap_servers": "localhost:9092",
                    "subscription_mode": "topics",
                    "batch_size": 2,
                    "batch_index": 1,
                },
            ],
        }
    ]
    assert consumer.commit_calls == 1


@pytest.mark.asyncio
async def test_operational_metrics_track_batch_errors_and_manual_assignments() -> None:
    def batch_deserializer(values: list[bytes]) -> list[str]:
        raise ValueError(f"bad batch: {values!r}")

    source = KafkaSource(
        assignments=[("events", 0), ("events", 1)],
        batch_deserializer=batch_deserializer,
        enable_auto_commit=False,
        commit_every=10,
        poll_timeout_ms=10,
        max_poll_records=10,
        on_deserialize_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
    )
    consumer = _FakeBatchConsumer([[b"a", b"b"]])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == []
    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 0,
        "batch_deserialize_error_count": 1,
        "manual_assign_partition_count": 2,
        "paused_partition_count": 0,
        "poison_record_dlq_write_count": 0,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 2,
        "poison_record_fail_closed_count": 0,
        "poison_record_deserialization_count": 2,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_batch_deserializer_errors_can_dlq_each_message_and_continue() -> None:
    dlq = _CollectDLQSink()

    def batch_deserializer(values: list[bytes]) -> list[str]:
        raise ValueError(f"bad batch: {values!r}")

    source = KafkaSource(
        assignments=[("events", 0), ("events", 1)],
        batch_deserializer=batch_deserializer,
        enable_auto_commit=False,
        commit_every=10,
        poll_timeout_ms=10,
        max_poll_records=10,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=dlq,
        poison_record_pipeline_id="orders-batch-kafka-source",
    )
    consumer = _FakeBatchConsumer([[b"a", b"b"]])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == []
    assert [record.stage for record in dlq.records] == [
        "kafka_batch_deserialize",
        "kafka_batch_deserialize",
    ]
    assert [record.record["poison"] for record in dlq.records] == [  # type: ignore[index]
        {
            "classification": "deserialization",
            "policy": "dlq_and_continue",
        },
        {
            "classification": "deserialization",
            "policy": "dlq_and_continue",
        },
    ]
    assert [record.record["value"] for record in dlq.records] == [  # type: ignore[index]
        {"encoding": "utf-8", "data": "a"},
        {"encoding": "utf-8", "data": "b"},
    ]
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 2,
        "record_drop_count": 2,
    }
    assert source.current_checkpoint() == {
        "topic": "events",
        "partition": 0,
        "offset": 1,
        "offsets": [
            {"topic": "events", "partition": 0, "offset": 1},
        ],
    }
    assert consumer.commit_offsets[-1] == {
        ("events", 0): 2,
    }
    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 0,
        "batch_deserialize_error_count": 1,
        "manual_assign_partition_count": 2,
        "paused_partition_count": 0,
        "poison_record_dlq_write_count": 2,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 0,
        "poison_record_fail_closed_count": 0,
        "poison_record_deserialization_count": 2,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_batch_deserializer_count_mismatch_dlqs_and_commits_offsets() -> None:
    dlq = _CollectDLQSink()

    def batch_deserializer(values: list[bytes]) -> list[str]:
        return [values[0].decode("utf-8")]

    source = KafkaSource(
        topics=["events"],
        batch_deserializer=batch_deserializer,
        enable_auto_commit=False,
        commit_every=1,
        poll_timeout_ms=10,
        max_poll_records=10,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=dlq,
    )
    consumer = _FakeBatchConsumer([[b"a", b"b"]])
    source._consumer = consumer  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert records == []
    assert [record.stage for record in dlq.records] == [
        "kafka_batch_deserialize_count_mismatch",
        "kafka_batch_deserialize_count_mismatch",
    ]
    assert consumer.commit_offsets[-1] == {
        ("events", 0): 2,
    }
    assert source.runtime_metrics().to_dict() == {
        "record_error_count": 2,
        "record_drop_count": 2,
    }
    assert source.current_checkpoint() == {
        "topic": "events",
        "partition": 0,
        "offset": 1,
        "offsets": [
            {"topic": "events", "partition": 0, "offset": 1},
        ],
    }


@pytest.mark.asyncio
async def test_revoked_partitions_drop_checkpoint_history_after_commit() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=100,
    )
    consumer = _FakeConsumer([])
    source._consumer = consumer  # type: ignore[attr-defined]
    source._active_assignment = {("t", 0), ("t", 1)}  # type: ignore[attr-defined]
    source._cursor_state.processed_offsets = {("t", 0): 5, ("t", 1): 8}  # type: ignore[attr-defined]
    source._cursor_state.committable_offsets = {("t", 0): 5, ("t", 1): 8}  # type: ignore[attr-defined]
    source._cursor_state.last_seen = ("t", 1, 8)  # type: ignore[attr-defined]
    source._cursor_state.pending_commit_count = 2  # type: ignore[attr-defined]

    await source._handle_partitions_revoked({("t", 1)})

    assert consumer.commit_offsets[-1] == {
        ("t", 0): 6,
        ("t", 1): 9,
    }
    assert source.current_checkpoint() == {
        "topic": "t",
        "partition": 0,
        "offset": 5,
        "offsets": [
            {"topic": "t", "partition": 0, "offset": 5},
        ],
    }
    assert source._cursor_state.committable_offsets == {("t", 0): 5}  # type: ignore[attr-defined]
    assert source._active_assignment == {("t", 0)}  # type: ignore[attr-defined]


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
async def test_manual_assignments_and_start_offsets_seek_exact_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    class FakeConsumer:
        def __init__(self, *topics: str, **kwargs: Any) -> None:
            seen["topics"] = topics
            seen["kwargs"] = kwargs
            self.seek_calls: list[tuple[object, int]] = []
            self.assigned: list[object] = []

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def assign(self, partitions: list[object]) -> None:
            self.assigned = list(partitions)

        def assignment(self) -> set[object]:
            return set(self.assigned)

        def seek(self, partition: object, offset: int) -> None:
            self.seek_calls.append((partition, offset))

        async def getmany(
            self,
            *,
            timeout_ms: int,
            max_records: int,
        ) -> dict[tuple[str, int], list[_FakeMessage]]:
            del timeout_ms, max_records
            raise StopAsyncIteration

    monkeypatch.setitem(
        sys.modules,
        "aiokafka",
        SimpleNamespace(
            AIOKafkaConsumer=FakeConsumer,
            TopicPartition=_FakeTopicPartition,
        ),
    )

    source = KafkaSource(
        assignments=[("events", 0), ("events", 1)],
        start_offsets={("events", 0): 12, ("events", 1): 34},
    )

    await source.open()
    await source._bootstrap_consumer_state()  # type: ignore[attr-defined]

    consumer = source._consumer  # type: ignore[attr-defined]
    assert seen["topics"] == ()
    assert consumer.assigned == [
        _FakeTopicPartition("events", 0),
        _FakeTopicPartition("events", 1),
    ]
    assert consumer.seek_calls == [
        (_FakeTopicPartition("events", 0), 12),
        (_FakeTopicPartition("events", 1), 34),
    ]


@pytest.mark.asyncio
async def test_kafka_source_open_closes_deserializer_if_consumer_start_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle: list[str] = []

    class _Deserializer:
        def open(self) -> None:
            lifecycle.append("open")

        def close(self) -> None:
            lifecycle.append("close")

        def __call__(self, value: bytes) -> str:
            return value.decode()

    class FakeConsumer:
        def __init__(self, *topics: str, **kwargs: Any) -> None:
            del topics, kwargs

        async def start(self) -> None:
            raise RuntimeError("boom")

    fake_aiokafka = SimpleNamespace(
        AIOKafkaConsumer=FakeConsumer,
        TopicPartition=_FakeTopicPartition,
    )
    monkeypatch.setitem(sys.modules, "aiokafka", fake_aiokafka)

    source = KafkaSource(
        topics=["events"],
        deserializer=_Deserializer(),
    )

    with pytest.raises(RuntimeError, match="boom"):
        await source.open()

    assert lifecycle == ["open", "close"]
    assert source._consumer is None  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_source_close_stops_consumer_even_if_commit_fails() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=10,
    )

    class FakeConsumer:
        def __init__(self) -> None:
            self.stop_calls = 0

        async def commit(self, offsets: dict[object, int] | None = None) -> None:
            del offsets
            raise RuntimeError("commit failed")

        async def stop(self) -> None:
            self.stop_calls += 1

    consumer = FakeConsumer()
    source._consumer = consumer  # type: ignore[attr-defined]
    source._cursor_state.pending_commit_count = 1  # type: ignore[attr-defined]
    source._cursor_state.processed_offsets = {("events", 0): 7}  # type: ignore[attr-defined]

    await source.close()

    assert consumer.stop_calls == 1
    assert source._consumer is None  # type: ignore[attr-defined]


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
async def test_commit_now_flushes_tracked_offsets_immediately() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=100,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]
    source._cursor_state.processed_offsets = {("t", 0): 7, ("t", 1): 2}  # type: ignore[attr-defined]
    source._cursor_state.committable_offsets = {("t", 0): 7, ("t", 1): 2}  # type: ignore[attr-defined]
    source._cursor_state.pending_commit_count = 2  # type: ignore[attr-defined]

    await source.commit_now()

    assert consumer.commit_offsets == [
        {
            _FakeTopicPartition("t", 0): 8,
            _FakeTopicPartition("t", 1): 3,
        }
    ]
    assert source._cursor_state.pending_commit_count == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_seek_to_offsets_repositions_consumer_and_discards_pending_tracking() -> None:
    source = KafkaSource(
        assignments=[("t", 0), ("t", 1)],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=100,
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="t", partition=0, offset=0),
        _FakeMessage(b"b", topic="t", partition=1, offset=1),
    ]
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]
    source._cursor_state.processed_offsets = {("t", 0): 7}  # type: ignore[attr-defined]
    source._cursor_state.pending_commit_count = 1  # type: ignore[attr-defined]
    source._cursor_state.last_seen = ("t", 0, 7)  # type: ignore[attr-defined]

    await source.seek_to_offsets({("t", 0): 15, ("t", 1): 23})

    assert consumer.seek_calls == [
        (_FakeTopicPartition("t", 0), 15),
        (_FakeTopicPartition("t", 1), 23),
    ]
    assert source._cursor_state.start_offsets == {("t", 0): 15, ("t", 1): 23}  # type: ignore[attr-defined]
    assert source._cursor_state.processed_offsets == {}  # type: ignore[attr-defined]
    assert source._cursor_state.pending_commit_count == 0  # type: ignore[attr-defined]
    assert source.current_checkpoint() is None


@pytest.mark.asyncio
async def test_seek_helpers_drive_beginning_and_end_controls_for_target_partitions() -> None:
    source = KafkaSource(
        assignments=[("t", 0), ("t", 1)],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=100,
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="t", partition=0, offset=0),
        _FakeMessage(b"b", topic="t", partition=1, offset=1),
    ]
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]

    await source.seek_to_beginning([("t", 1)])
    await source.seek_to_end()

    assert consumer.seek_to_beginning_calls == [(_FakeTopicPartition("t", 1),)]
    assert consumer.seek_to_end_calls == [
        (_FakeTopicPartition("t", 0), _FakeTopicPartition("t", 1))
    ]


@pytest.mark.asyncio
async def test_seek_helpers_reject_unassigned_partitions() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=100,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="assigned partitions"):
        await source.seek_to_beginning([("missing", 0)])


@pytest.mark.asyncio
async def test_pause_and_resume_reject_unassigned_partitions() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=100,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="assigned partitions"):
        source.pause([("missing", 0)])

    with pytest.raises(ValueError, match="assigned partitions"):
        source.resume([("missing", 0)])

    assert consumer.pause_calls == []
    assert consumer.resume_calls == []
    assert source._paused_partitions == set()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_pipeline_checkpoint_does_not_advance_past_emitted_record_when_batch_prefetched() -> (
    None
):
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=100,
        poll_timeout_ms=10,
        max_poll_records=10,
    )
    consumer = _FakeBatchConsumer([[b"a", b"b", b"c"]])
    source._consumer = consumer  # type: ignore[attr-defined]

    async def _noop() -> None:
        return None

    source.open = _noop  # type: ignore[method-assign]
    store = InMemoryCheckpointStore()
    sink = _CollectSink()

    summary = await (
        Pipeline(source)
        .build(sink, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
        .run(max_records=1)
    )

    stored = await store.load("kafka")

    assert sink.records == ["a"]
    assert summary.records_consumed == 1
    assert summary.records_written == 1
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value["offset"] == 0
    assert stored is not None
    assert stored.value["offset"] == 0
    assert consumer.commit_offsets[-1] == {("events", 0): 1}


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
async def test_open_subscribes_to_topic_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    class _FakeAIOKafkaConsumer:
        def __init__(self, *topics: str, **kwargs: Any) -> None:
            seen["topics"] = topics
            seen.update(kwargs)

        def subscribe(
            self,
            *,
            topics: list[str] | None = None,
            pattern: str | None = None,
            listener: object | None = None,
        ) -> None:
            seen["subscribe_topics"] = topics
            seen["subscribe_pattern"] = pattern
            seen["listener"] = listener

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
        topic_pattern=r"orders\..*",
        bootstrap_servers="broker:9092",
        group_id="agora-pattern",
    )

    await source.open()

    assert seen["topics"] == ()
    assert seen["bootstrap_servers"] == "broker:9092"
    assert seen["group_id"] == "agora-pattern"
    assert seen["subscribe_topics"] is None
    assert seen["subscribe_pattern"] == r"orders\..*"
    assert seen["listener"] is not None


@pytest.mark.asyncio
async def test_open_subscribes_to_topics_without_rebalance_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    class _FakeAIOKafkaConsumer:
        def __init__(self, *topics: str, **kwargs: Any) -> None:
            seen["topics"] = topics
            seen.update(kwargs)

        def subscribe(
            self,
            *,
            topics: list[str] | None = None,
            pattern: str | None = None,
            listener: object | None = None,
        ) -> None:
            seen["subscribe_topics"] = topics
            seen["subscribe_pattern"] = pattern
            seen["listener"] = listener

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
        group_id="agora-topics",
    )

    await source.open()

    assert seen["topics"] == ("events",)
    assert seen["subscribe_topics"] == ["events"]
    assert seen["subscribe_pattern"] is None
    assert seen["listener"] is not None


@pytest.mark.asyncio
async def test_rebalance_listener_commits_before_revocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}
    delegate_calls: list[tuple[str, list[tuple[str, int]]]] = []

    class Delegate:
        async def on_partitions_revoked(self, partitions: object) -> None:
            delegate_calls.append(("revoked", list(partitions)))

        async def on_partitions_assigned(self, partitions: object) -> None:
            delegate_calls.append(("assigned", list(partitions)))

    class _FakeAIOKafkaConsumer:
        def __init__(self, *topics: str, **kwargs: Any) -> None:
            seen["topics"] = topics
            seen.update(kwargs)
            self.commit_offsets: list[dict[object, int] | None] = []

        def subscribe(
            self,
            *,
            topics: list[str] | None = None,
            pattern: str | None = None,
            listener: object | None = None,
        ) -> None:
            seen["subscribe_topics"] = topics
            seen["subscribe_pattern"] = pattern
            seen["listener"] = listener

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def commit(self, offsets: dict[object, int] | None = None) -> None:
            self.commit_offsets.append(offsets)

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
        rebalance_listener=Delegate(),
        enable_auto_commit=False,
        commit_every=10,
    )

    await source.open()
    source._cursor_state.processed_offsets = {("events", 0): 7}  # type: ignore[attr-defined]
    source._cursor_state.committable_offsets = {("events", 0): 7}  # type: ignore[attr-defined]
    source._cursor_state.pending_commit_count = 1  # type: ignore[attr-defined]

    listener = seen["listener"]
    assert listener is not None

    await listener.on_partitions_assigned([_FakeTopicPartition("events", 0)])
    await listener.on_partitions_revoked([_FakeTopicPartition("events", 0)])

    consumer = source._consumer  # type: ignore[attr-defined]
    assert consumer.commit_offsets == [{_FakeTopicPartition("events", 0): 8}]
    assert delegate_calls == [
        ("assigned", [_FakeTopicPartition("events", 0)]),
        ("revoked", [_FakeTopicPartition("events", 0)]),
    ]
    assert source._cursor_state.processed_offsets == {}  # type: ignore[attr-defined]
    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 1,
        "batch_deserialize_error_count": 0,
        "manual_assign_partition_count": 0,
        "paused_partition_count": 0,
        "poison_record_dlq_write_count": 0,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 0,
        "poison_record_fail_closed_count": 0,
        "poison_record_deserialization_count": 0,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_pause_and_resume_control_assignment_backpressure() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=2,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]

    source.pause()
    source.resume()
    source.pause([("t", 0)])
    source.resume([("t", 0)])

    assert consumer.pause_calls == [
        (_FakeTopicPartition("t", 0),),
        (_FakeTopicPartition("t", 0),),
    ]
    assert consumer.resume_calls == [
        (_FakeTopicPartition("t", 0),),
        (_FakeTopicPartition("t", 0),),
    ]
    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 0,
        "batch_deserialize_error_count": 0,
        "manual_assign_partition_count": 0,
        "paused_partition_count": 0,
        "poison_record_dlq_write_count": 0,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 0,
        "poison_record_fail_closed_count": 0,
        "poison_record_deserialization_count": 0,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_operational_metrics_report_paused_partition_count() -> None:
    source = KafkaSource(
        assignments=[("events", 0), ("events", 1)],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=2,
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="events", partition=0, offset=0),
        _FakeMessage(b"b", topic="events", partition=1, offset=1),
    ]
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]

    source.pause()

    assert source.operational_metrics().to_dict() == {
        "rebalance_count": 0,
        "batch_deserialize_error_count": 0,
        "manual_assign_partition_count": 2,
        "paused_partition_count": 2,
        "poison_record_dlq_write_count": 0,
        "poison_record_dlq_write_failure_count": 0,
        "poison_record_log_only_count": 0,
        "poison_record_fail_closed_count": 0,
        "poison_record_deserialization_count": 0,
        "poison_record_schema_evolution_count": 0,
        "poison_record_schema_validation_count": 0,
        "poison_record_schema_registry_binding_mismatch_count": 0,
        "poison_record_unknown_count": 0,
    }


@pytest.mark.asyncio
async def test_resume_subset_after_pause_all_keeps_subset_resumed_on_rebalance() -> None:
    source = KafkaSource(
        assignments=[("events", 0), ("events", 1)],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="events", partition=0, offset=0),
        _FakeMessage(b"b", topic="events", partition=1, offset=0),
    ]
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]
    source._active_assignment = {("events", 0), ("events", 1)}  # type: ignore[attr-defined]

    source.pause()
    source.resume([("events", 0)])
    source._apply_pause_state([_FakeTopicPartition("events", 0), _FakeTopicPartition("events", 1)])  # type: ignore[attr-defined]

    assert source._pause_all_requested is False  # type: ignore[attr-defined]
    assert source._paused_partitions == {("events", 1)}  # type: ignore[attr-defined]
    assert consumer.resume_calls[-1] == (_FakeTopicPartition("events", 0),)
    assert consumer.pause_calls[-1] == (_FakeTopicPartition("events", 1),)


@pytest.mark.asyncio
async def test_health_snapshot_reports_assignment_lag_and_readiness() -> None:
    source = KafkaSource(
        assignments=[("events", 0), ("events", 1)],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=5,
        max_idle_polls=3,
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="events", partition=0, offset=0),
        _FakeMessage(b"b", topic="events", partition=1, offset=0),
    ]
    consumer.position_map = {("events", 0): 2, ("events", 1): 5}
    consumer.committed_map = {("events", 0): 1, ("events", 1): 5}
    consumer.end_offset_map = {("events", 0): 6, ("events", 1): 5}
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]
    source._active_assignment = {("events", 0), ("events", 1)}  # type: ignore[attr-defined]
    source._paused_partitions = {("events", 1)}  # type: ignore[attr-defined]
    source._cursor_state.pending_commit_count = 2  # type: ignore[attr-defined]
    source._idle_poll_count = 1  # type: ignore[attr-defined]
    source._rebalance_count = 3  # type: ignore[attr-defined]
    source._cursor_state.processed_offsets = {("events", 0): 1, ("events", 1): 4}  # type: ignore[attr-defined]
    source._cursor_state.committable_offsets = {("events", 0): 0, ("events", 1): 4}  # type: ignore[attr-defined]

    snapshot = await source.health_snapshot()

    assert snapshot.ready is True
    assert snapshot.stalled is False
    assert snapshot.assignment_count == 2
    assert snapshot.paused_partition_count == 1
    assert snapshot.pending_commit_count == 2
    assert snapshot.rebalance_count == 3
    assert snapshot.total_lag == 4
    assert snapshot.lagging_partition_count == 1
    assert snapshot.max_lag == 4
    assert snapshot.total_commit_lag == 5
    assert snapshot.max_commit_lag == 5
    assert [partition.to_dict() for partition in snapshot.partitions] == [
        {
            "topic": "events",
            "partition": 0,
            "current_offset": 2,
            "committed_offset": 1,
            "processed_offset": 1,
            "committable_offset": 0,
            "end_offset": 6,
            "lag": 4,
            "commit_lag": 5,
            "delivery_gap": 1,
            "commit_gap": 0,
            "paused": False,
        },
        {
            "topic": "events",
            "partition": 1,
            "current_offset": 5,
            "committed_offset": 5,
            "processed_offset": 4,
            "committable_offset": 4,
            "end_offset": 5,
            "lag": 0,
            "commit_lag": 0,
            "delivery_gap": 0,
            "commit_gap": 0,
            "paused": True,
        },
    ]


@pytest.mark.asyncio
async def test_health_snapshot_clears_stale_assignment_when_consumer_reports_none() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=5,
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages = []  # type: ignore[attr-defined]
    source._consumer = consumer  # type: ignore[attr-defined]
    source._active_assignment = {("events", 0)}  # type: ignore[attr-defined]

    snapshot = await source.health_snapshot(force_refresh=True)

    assert snapshot.ready is False
    assert snapshot.assignment_count == 0
    assert snapshot.partitions == ()
    assert source._active_assignment == set()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_health_snapshot_marks_source_stalled_after_idle_threshold() -> None:
    source = KafkaSource(
        assignments=[("events", 0)],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=1,
        max_idle_polls=2,
    )
    consumer = _FakeConsumer([b"a"])
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]
    source._active_assignment = {("events", 0)}  # type: ignore[attr-defined]
    source._idle_poll_count = 2  # type: ignore[attr-defined]

    snapshot = await source.health_snapshot()

    assert snapshot.ready is True
    assert snapshot.stalled is True


@pytest.mark.asyncio
async def test_health_snapshot_uses_short_lived_cache_for_partition_reads() -> None:
    source = KafkaSource(
        assignments=[("events", 0), ("events", 1)],
        deserializer=lambda b: b.decode(),
        enable_auto_commit=False,
        commit_every=1,
        health_snapshot_cache_ms=5_000,
    )
    consumer = _FakeConsumer([b"a"])
    consumer._messages = [  # type: ignore[attr-defined]
        _FakeMessage(b"a", topic="events", partition=0, offset=0),
        _FakeMessage(b"b", topic="events", partition=1, offset=0),
    ]
    consumer.position_map = {("events", 0): 2, ("events", 1): 4}
    consumer.committed_map = {("events", 0): 1, ("events", 1): 3}
    consumer.end_offset_map = {("events", 0): 7, ("events", 1): 8}
    source._consumer = consumer  # type: ignore[attr-defined]
    source._topic_partition_cls = _FakeTopicPartition  # type: ignore[attr-defined]
    source._active_assignment = {("events", 0), ("events", 1)}  # type: ignore[attr-defined]

    first = await source.health_snapshot()
    second = await source.health_snapshot()

    assert first is second
    assert consumer.position_calls == 2
    assert consumer.committed_calls == 2
    assert consumer.end_offsets_calls == 1


def test_kafka_source_includes_first_class_security_kwargs() -> None:
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        security_protocol="SASL_SSL",
        security=KafkaSecurityConfig(
            security_protocol="SASL_SSL",
            sasl=KafkaSASLConfig(
                mechanism="SCRAM-SHA-256",
                username="svc",
                password="secret",
            ),
            tls={},
        ),
    )

    kwargs = source._security_kwargs()  # type: ignore[attr-defined]

    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "SCRAM-SHA-256"
    assert kwargs["sasl_plain_username"] == "svc"
    assert kwargs["sasl_plain_password"] == "secret"
    assert "ssl_context" in kwargs


def test_kafka_source_includes_oauthbearer_security_kwargs() -> None:
    provider = _OAuthTokenProvider()
    source = KafkaSource(
        topics=["events"],
        deserializer=lambda b: b.decode(),
        security_protocol="SASL_SSL",
        security=KafkaSecurityConfig(
            security_protocol="SASL_SSL",
            sasl=KafkaSASLConfig(
                mechanism="OAUTHBEARER",
                oauth_token_provider=provider,
            ),
            tls={},
        ),
    )

    kwargs = source._security_kwargs()  # type: ignore[attr-defined]

    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "OAUTHBEARER"
    assert kwargs["sasl_oauth_token_provider"] is provider
    assert "sasl_plain_username" not in kwargs
    assert "sasl_plain_password" not in kwargs
    assert "ssl_context" in kwargs


def test_kafka_source_rejects_conflicting_security_protocol() -> None:
    with pytest.raises(ValueError, match="must match"):
        KafkaSource(
            topics=["events"],
            deserializer=lambda b: b.decode(),
            security_protocol="PLAINTEXT",
            security=KafkaSecurityConfig(
                security_protocol="SSL",
                tls={"cafile": "/tmp/ca.pem"},
            ),
        )


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
