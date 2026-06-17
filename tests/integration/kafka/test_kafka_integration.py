from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime

import pytest
from agora import DeliveryConfig, InMemoryCheckpointStore, Pipeline
from agora.core.checkpoint import Checkpoint, SQLiteCheckpointStore
from agora.core.dlq import DLQRecord
from agora.core.source import IterableSource

from agora_plugins.kafka import (
    DLQPayloadPolicy,
    KafkaDLQSink,
    KafkaDLQSource,
    KafkaPoisonRecordPolicy,
    KafkaSink,
    KafkaSource,
    KafkaSourceRuntime,
)
from tests.integration._process_death import (
    assert_process_died_after_checkpoint,
    read_jsonl,
    run_process_death_child,
)

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 30.0


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    async def open(self) -> None:
        return None

    async def write(self, record: dict[str, object]) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _PendingAckFailureSink:
    sink_name = "pending_ack_failure"

    def __init__(self) -> None:
        self.records: list[object] = []
        self.pending_ack_wait_count = 0

    async def open(self) -> None:
        return None

    async def write(self, record: object) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def wait_for_pending_acks(self) -> None:
        self.pending_ack_wait_count += 1
        raise RuntimeError("pending ack rejected")

    async def close(self) -> None:
        return None


class _FailingDLQSink:
    sink_name = "failing_dlq"

    async def open(self) -> None:
        return None

    async def write(self, record: object) -> None:
        del record
        raise RuntimeError("dlq unavailable")

    async def close(self) -> None:
        return None


class _FailingOffsetsKafkaSink(KafkaSink[dict[str, object]]):
    async def send_offsets_to_transaction(self, offsets: object, group_id: str) -> None:
        del offsets, group_id
        raise RuntimeError("offset transaction rejected")


class _ReverseCipher:
    def encrypt(self, plaintext: bytes) -> bytes:
        return plaintext[::-1]

    def decrypt(self, ciphertext: bytes) -> bytes:
        return ciphertext[::-1]


class _RebalanceListener:
    def __init__(self) -> None:
        self.events: list[tuple[str, list[tuple[str, int]]]] = []

    async def on_partitions_revoked(self, partitions: object) -> None:
        self.events.append(
            (
                "revoked",
                [
                    (str(getattr(item, "topic", item[0])), int(getattr(item, "partition", item[1])))
                    for item in partitions
                ],
            )
        )

    async def on_partitions_assigned(self, partitions: object) -> None:
        self.events.append(
            (
                "assigned",
                [
                    (str(getattr(item, "topic", item[0])), int(getattr(item, "partition", item[1])))
                    for item in partitions
                ],
            )
        )


async def _wait_for_rebalance_events(
    *listeners: _RebalanceListener,
    timeout_s: float = 10.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if all(listener.events for listener in listeners):
            return
        await asyncio.sleep(0.1)
    pytest.fail("Timed out waiting for Kafka rebalance listener events.")


async def _ensure_topic_exists(
    bootstrap_servers: str,
    topic: str,
    *,
    num_partitions: int = 1,
) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        try:
            await admin.create_topics(
                [
                    NewTopic(
                        name=topic,
                        num_partitions=num_partitions,
                        replication_factor=1,
                    )
                ]
            )
        except TopicAlreadyExistsError:
            return
    finally:
        await admin.close()


def _partitioned_source_records(
    *,
    partitions: int,
    records_per_partition: int,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for partition in range(partitions):
        for seq in range(records_per_partition):
            records.append(
                {
                    "partition": partition,
                    "payload": {
                        "partition": partition,
                        "seq": seq,
                    },
                }
            )
    return records


def _partition_offset_pairs(records: list[dict[str, object]]) -> list[tuple[int, int]]:
    return sorted(
        (
            int(record["metadata"]["partition"]),
            int(record["metadata"]["offset"]),
        )
        for record in records
    )


async def _collect_from_source(source: KafkaSource[dict[str, object]]) -> list[dict[str, object]]:
    return [record async for record in source.stream()]


async def _collect_kafka_topic(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    max_records: int,
    read_committed: bool = False,
) -> list[dict[str, object]]:
    collected = _CollectSink()
    extra_config = {"isolation_level": "read_committed"} if read_committed else None
    await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    deserializer=lambda value: json.loads(value.decode("utf-8")),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    poll_timeout_ms=250,
                    max_idle_polls=2,
                    extra_config=extra_config,
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=max_records)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    return collected.records


async def _kafka_committed_offset(
    *,
    bootstrap_servers: str,
    group_id: str,
    topic: str,
    partition: int = 0,
) -> int | None:
    from aiokafka import AIOKafkaConsumer, TopicPartition

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        return await consumer.committed(TopicPartition(topic, partition))
    finally:
        await consumer.stop()


def _make_dlq_record(
    *,
    pipeline_id: str,
    run_id: str,
    stage: str,
    event_id: int,
    attempt: int = 0,
) -> DLQRecord:
    return DLQRecord(
        pipeline_id=pipeline_id,
        run_id=run_id,
        stage=stage,
        error_type="ValueError",
        error_message="bad payload",
        record={"id": event_id},
        original_record={"id": event_id, "raw": True},
        processed_record={"id": event_id, "normalized": True},
        source="kafka",
        checkpoint={"offset": event_id},
        middleware="normalize",
        sink="postgres",
        created_at=datetime(2026, 6, 18, 12, event_id, tzinfo=UTC),
        attempt=attempt,
        max_attempts=5,
    )


@pytest.mark.asyncio
async def test_kafka_source_and_sink_round_trip_against_real_broker(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(
                IterableSource(
                    [
                        {"id": 1, "name": "alpha"},
                        {"id": 2, "name": "bravo"},
                        {"id": 3, "name": "charlie"},
                    ]
                )
            )
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record).encode("utf-8"),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    try:
        consumer_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-it-{unique_suffix}",
                        deserializer=lambda value: json.loads(value.decode("utf-8")),
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=2,
                    )
                )
                .build(collected)  # type: ignore[arg-type]
                .run(max_records=3)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    except TimeoutError:
        pytest.fail(
            "Kafka integration test timed out while waiting for produced records. "
            "Check `docker compose ps`, `docker compose logs kafka`, and rerun "
            "with `pytest -vv -s -k kafka -m integration`."
        )

    assert producer_summary.records_written == 3
    assert consumer_summary.records_consumed == 3
    assert collected.records == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "bravo"},
        {"id": 3, "name": "charlie"},
    ]


@pytest.mark.asyncio
async def test_kafka_idempotent_sink_survives_broker_flap_without_duplicates(
    kafka_bootstrap: str,
    unique_suffix: str,
    kafka_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-idempotent-flap-{unique_suffix}"
    records = [{"id": index, "payload": f"record-{index}"} for index in range(20)]
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)

    sink = KafkaSink(
        topic=topic,
        bootstrap_servers=kafka_bootstrap,
        serializer=lambda record: json.dumps(record).encode("utf-8"),
        max_pending_acks=len(records) + 1,
        linger_ms=5_000,
        request_timeout_ms=10_000,
        retry_backoff_ms=100,
    )
    await sink.open()
    try:
        producer = sink._producer  # type: ignore[attr-defined]
        assert producer is not None
        assert getattr(producer, "_txn_manager", None) is not None

        await sink.write_batch(records)
        pending_acks = list(sink._pending_acks)  # type: ignore[attr-defined]
        assert len(pending_acks) == len(records)
        assert any(callable(getattr(ack, "done", None)) and not ack.done() for ack in pending_acks)

        await asyncio.to_thread(kafka_broker_flap_control)
        await asyncio.wait_for(sink.flush(), timeout=60.0)
    finally:
        await sink.close()

    await asyncio.sleep(1.0)
    collected = await _collect_kafka_topic(
        bootstrap_servers=kafka_bootstrap,
        topic=topic,
        group_id=f"agora-it-idempotent-flap-{unique_suffix}",
        max_records=len(records),
    )

    seen_ids = [int(record["id"]) for record in collected]
    assert len(collected) == len(records)
    assert len(set(seen_ids)) == len(records)
    assert sorted(seen_ids) == list(range(len(records)))


@pytest.mark.asyncio
async def test_kafka_transactional_sink_commit_visible_and_abort_hidden_against_real_broker(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-txn-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)

    sink = KafkaSink(
        topic=topic,
        bootstrap_servers=kafka_bootstrap,
        serializer=lambda record: json.dumps(record).encode("utf-8"),
        transactional_id=f"agora-it-txn-{unique_suffix}",
        transaction_per_batch=True,
    )
    await sink.open()
    try:
        await sink.begin_transaction()
        await sink.write({"id": "aborted"})
        await sink.abort_transaction()

        await sink.write_batch([{"id": "committed-1"}, {"id": "committed-2"}])
    finally:
        await sink.close()

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_bootstrap,
                    group_id=f"agora-it-txn-{unique_suffix}",
                    deserializer=lambda value: json.loads(value.decode("utf-8")),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    extra_config={"isolation_level": "read_committed"},
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=2)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 2
    assert collected.records == [{"id": "committed-1"}, {"id": "committed-2"}]


@pytest.mark.asyncio
async def test_kafka_runtime_transactional_offsets_commit_and_abort_against_real_broker(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    input_topic = f"agora-it-eos-in-{unique_suffix}"
    output_topic = f"agora-it-eos-out-{unique_suffix}"
    abort_input_topic = f"agora-it-eos-abort-in-{unique_suffix}"
    abort_output_topic = f"agora-it-eos-abort-out-{unique_suffix}"
    group_id = f"agora-it-eos-{unique_suffix}"
    abort_group_id = f"agora-it-eos-abort-{unique_suffix}"
    for topic in (input_topic, output_topic, abort_input_topic, abort_output_topic):
        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)

    await asyncio.wait_for(
        (
            Pipeline(IterableSource([{"id": 1}, {"id": 2}]))
            .build(
                KafkaSink(
                    topic=input_topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record).encode("utf-8"),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    source = KafkaSource(
        topics=[input_topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        poll_timeout_ms=250,
        max_idle_polls=2,
    )
    output_sink = KafkaSink(
        topic=output_topic,
        bootstrap_servers=kafka_bootstrap,
        serializer=lambda record: json.dumps(record).encode("utf-8"),
        transactional_id=f"agora-it-eos-sink-{unique_suffix}",
    )
    runtime = KafkaSourceRuntime(source)
    try:
        await source.open()
        await output_sink.open()
        consumed = await runtime.drain_to(
            output_sink,
            transform=lambda record: {"id": record["id"], "processed": True},
            max_records=2,
            transactional_offsets=True,
        )
    finally:
        await output_sink.close()
        await source.close()

    assert consumed == [{"id": 1}, {"id": 2}]
    assert await _collect_kafka_topic(
        bootstrap_servers=kafka_bootstrap,
        topic=output_topic,
        group_id=f"{group_id}-output",
        max_records=2,
        read_committed=True,
    ) == [{"id": 1, "processed": True}, {"id": 2, "processed": True}]
    assert (
        await _collect_kafka_topic(
            bootstrap_servers=kafka_bootstrap,
            topic=input_topic,
            group_id=group_id,
            max_records=1,
        )
        == []
    )

    await asyncio.wait_for(
        (
            Pipeline(IterableSource([{"id": "abort-me"}]))
            .build(
                KafkaSink(
                    topic=abort_input_topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record).encode("utf-8"),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    abort_source = KafkaSource(
        topics=[abort_input_topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=abort_group_id,
        deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        poll_timeout_ms=250,
        max_idle_polls=2,
    )
    failing_sink = _FailingOffsetsKafkaSink(
        topic=abort_output_topic,
        bootstrap_servers=kafka_bootstrap,
        serializer=lambda record: json.dumps(record).encode("utf-8"),
        transactional_id=f"agora-it-eos-abort-sink-{unique_suffix}",
    )
    abort_runtime = KafkaSourceRuntime(abort_source)
    abort_stream = None
    try:
        await abort_source.open()
        await failing_sink.open()
        abort_stream = abort_source.stream()
        first_record = await asyncio.wait_for(anext(abort_stream), timeout=_INTEGRATION_TIMEOUT_S)
        with pytest.raises(RuntimeError, match="offset transaction rejected"):
            await abort_runtime.deliver(
                first_record,
                failing_sink,
                transform=lambda record: {"id": record["id"], "processed": True},
                transactional_offsets=True,
            )
    finally:
        if abort_stream is not None:
            with contextlib.suppress(Exception):
                await abort_stream.aclose()
        await failing_sink.close()
        await abort_source.close()

    assert (
        await _collect_kafka_topic(
            bootstrap_servers=kafka_bootstrap,
            topic=abort_output_topic,
            group_id=f"{abort_group_id}-output",
            max_records=1,
            read_committed=True,
        )
        == []
    )
    assert await _collect_kafka_topic(
        bootstrap_servers=kafka_bootstrap,
        topic=abort_input_topic,
        group_id=abort_group_id,
        max_records=1,
    ) == [{"id": "abort-me"}]


@pytest.mark.asyncio
async def test_kafka_runtime_deliver_flush_false_waits_for_pending_acks_before_offset_commit(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-pending-ack-{unique_suffix}"
    group_id = f"agora-it-pending-ack-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    await asyncio.wait_for(
        (
            Pipeline(IterableSource([{"id": 1, "name": "alpha"}]))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record).encode("utf-8"),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    source = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
    )
    runtime = KafkaSourceRuntime(source)
    failing_sink = _PendingAckFailureSink()
    stream = None
    try:
        await source.open()
        stream = source.stream()
        first_record = await asyncio.wait_for(anext(stream), timeout=_INTEGRATION_TIMEOUT_S)

        with pytest.raises(RuntimeError, match="pending ack rejected"):
            await runtime.deliver(
                first_record,
                failing_sink,  # type: ignore[arg-type]
                flush=False,
            )
    finally:
        if stream is not None:
            with contextlib.suppress(Exception):
                await stream.aclose()
        await source.close()

    redelivered = _CollectSink()
    redelivery_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_bootstrap,
                    group_id=group_id,
                    deserializer=lambda value, metadata: {
                        "payload": json.loads(value.decode("utf-8")),
                        "metadata": metadata,
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                )
            )
            .build(redelivered)  # type: ignore[arg-type]
            .run(max_records=1)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert failing_sink.pending_ack_wait_count == 1
    assert failing_sink.records == [first_record]
    assert first_record["metadata"]["offset"] == 0
    assert redelivery_summary.records_consumed == 1
    assert redelivered.records[0]["metadata"]["offset"] == first_record["metadata"]["offset"]
    assert redelivered.records[0]["payload"] == first_record["payload"]


@pytest.mark.asyncio
async def test_kafka_poison_dlq_write_failure_does_not_advance_group_offset(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-poison-dlq-fail-{unique_suffix}"
    group_id = f"agora-it-poison-dlq-fail-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    await asyncio.wait_for(
        (
            Pipeline(IterableSource([b"bad"]))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: record,
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    def _raising_deserializer(value: bytes) -> dict[str, object]:
        del value
        raise ValueError("bad payload")

    source = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=_raising_deserializer,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        poison_record_sink=_FailingDLQSink(),
        poison_record_pipeline_id=f"agora-it-poison-{unique_suffix}",
    )
    runtime = KafkaSourceRuntime(source)

    await source.open()
    try:
        with pytest.raises(RuntimeError, match="dlq unavailable"):
            _ = [record async for record in source.stream()]
        metrics = source.operational_metrics().to_dict()
        prometheus = await runtime.render_prometheus_metrics(namespace="agora_it_kafka")
    finally:
        await source.close()

    redelivered = _CollectSink()
    redelivery_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_bootstrap,
                    group_id=group_id,
                    deserializer=lambda value, metadata: {
                        "payload": value.decode("utf-8"),
                        "metadata": metadata,
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                )
            )
            .build(redelivered)  # type: ignore[arg-type]
            .run(max_records=1)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert metrics["poison_record_dlq_write_count"] == 0
    assert metrics["poison_record_dlq_write_failure_count"] == 1
    assert metrics["poison_record_deserialization_count"] == 1
    assert 'event="poison_dlq_write_failure"} 1' in prometheus
    assert 'event="poison_classification_deserialization"} 1' in prometheus
    assert redelivery_summary.records_consumed == 1
    assert redelivered.records[0]["payload"] == "bad"
    assert redelivered.records[0]["metadata"]["offset"] == 0


@pytest.mark.asyncio
async def test_kafka_round_trip_preserves_headers_and_exposes_metadata(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-meta-{unique_suffix}"
    source_records = [
        {
            "key": "order-1",
            "headers": [("tenant", "acme"), ("event_type", "order.created")],
            "payload": {"id": 1, "name": "alpha"},
        },
        {
            "key": "order-2",
            "headers": [("tenant", "acme"), ("event_type", "order.updated")],
            "payload": {"id": 2, "name": "bravo"},
        },
    ]

    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    key_fn=lambda record: record["key"].encode("utf-8"),
                    headers_fn=lambda record: [
                        (name, value.encode("utf-8")) for name, value in record["headers"]
                    ],
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    try:
        consumer_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-it-meta-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .build(collected)  # type: ignore[arg-type]
                .run(max_records=2)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    except TimeoutError:
        pytest.fail(
            "Kafka metadata integration test timed out while waiting for produced records. "
            "Check `docker compose ps`, `docker compose logs kafka`, and rerun "
            "with `pytest -vv -s -k kafka -m integration`."
        )

    assert producer_summary.records_written == 2
    assert consumer_summary.records_consumed == 2

    assert [record["payload"] for record in collected.records] == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "bravo"},
    ]
    assert [record["metadata"]["topic"] for record in collected.records] == [topic, topic]
    assert [record["metadata"]["partition"] for record in collected.records] == [0, 0]
    assert [record["metadata"]["offset"] for record in collected.records] == [0, 1]
    assert [record["metadata"]["key"] for record in collected.records] == [
        b"order-1",
        b"order-2",
    ]
    assert [record["metadata"]["headers"] for record in collected.records] == [
        [
            ("tenant", b"acme"),
            ("event_type", b"order.created"),
        ],
        [
            ("tenant", b"acme"),
            ("event_type", b"order.updated"),
        ],
    ]
    assert all(isinstance(record["metadata"]["timestamp"], int) for record in collected.records)
    assert all(record["metadata"]["timestamp_type"] is not None for record in collected.records)


@pytest.mark.asyncio
async def test_kafka_supports_dynamic_topic_routing_and_pattern_subscription(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topics = [
        f"agora-it-route-{unique_suffix}.created",
        f"agora-it-route-{unique_suffix}.updated",
    ]
    for topic in topics:
        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)

    producer_summary = await asyncio.wait_for(
        (
            Pipeline(
                IterableSource(
                    [
                        {"topic": topics[0], "payload": {"id": 1, "state": "created"}},
                        {"topic": topics[1], "payload": {"id": 2, "state": "updated"}},
                    ]
                )
            )
            .build(
                KafkaSink(
                    topic=topics[0],
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    topic_fn=lambda record: record["topic"],
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topic_pattern=rf"agora-it-route-{unique_suffix}\..*",
                    bootstrap_servers=kafka_bootstrap,
                    group_id=f"agora-it-route-{unique_suffix}",
                    deserializer=lambda value, metadata: {
                        "payload": json.loads(value.decode("utf-8")),
                        "topic": metadata["topic"],
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=2)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert producer_summary.records_written == 2
    assert consumer_summary.records_consumed == 2
    assert sorted(record["topic"] for record in collected.records) == sorted(topics)
    assert sorted(
        (record["payload"] for record in collected.records),
        key=lambda item: item["id"],
    ) == [
        {"id": 1, "state": "created"},
        {"id": 2, "state": "updated"},
    ]


@pytest.mark.asyncio
async def test_kafka_manual_assignments_and_exact_start_offsets_work_across_multiple_partitions(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-assign-{unique_suffix}"
    source_records = _partitioned_source_records(partitions=2, records_per_partition=4)

    await asyncio.wait_for(
        _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
        timeout=10.0,
    )
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    assignments=[(topic, 0), (topic, 1)],
                    start_offsets={(topic, 0): 1, (topic, 1): 2},
                    bootstrap_servers=kafka_bootstrap,
                    group_id=f"agora-it-assign-{unique_suffix}",
                    deserializer=lambda value, metadata: {
                        "payload": json.loads(value.decode("utf-8")),
                        "metadata": metadata,
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=5)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert producer_summary.records_written == 8
    assert consumer_summary.records_consumed == 5
    assert consumer_summary.records_written == 5
    assert _partition_offset_pairs(collected.records) == [
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 2),
        (1, 3),
    ]
    assert sorted(
        (record["payload"]["partition"], record["payload"]["seq"]) for record in collected.records
    ) == [
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 2),
        (1, 3),
    ]


@pytest.mark.asyncio
async def test_kafka_resume_from_mixed_partition_offset_checkpoint_map(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-resume-mix-{unique_suffix}"
    store = InMemoryCheckpointStore()
    source_records = _partitioned_source_records(partitions=2, records_per_partition=4)

    await asyncio.wait_for(
        _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
        timeout=10.0,
    )
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await store.save(
        "kafka",
        Checkpoint(
            pipeline_id="kafka",
            run_id="seed",
            source="kafka",
            value={
                "offsets": [
                    {"topic": topic, "partition": 0, "offset": 1},
                    {"topic": topic, "partition": 1, "offset": 2},
                ]
            },
        ),
    )

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    assignments=[(topic, 0), (topic, 1)],
                    bootstrap_servers=kafka_bootstrap,
                    group_id=f"agora-it-resume-mix-{unique_suffix}",
                    deserializer=lambda value, metadata: {
                        "payload": json.loads(value.decode("utf-8")),
                        "metadata": metadata,
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                )
            )
            .build(
                collected,  # type: ignore[arg-type]
                config=DeliveryConfig(checkpoint=store),
            )
            .run(max_records=3)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert producer_summary.records_written == 8
    assert consumer_summary.records_consumed == 3
    assert consumer_summary.records_written == 3
    assert _partition_offset_pairs(collected.records) == [
        (0, 2),
        (0, 3),
        (1, 3),
    ]
    assert sorted(
        (record["payload"]["partition"], record["payload"]["seq"]) for record in collected.records
    ) == [
        (0, 2),
        (0, 3),
        (1, 3),
    ]
    assert consumer_summary.last_checkpoint is not None
    assert consumer_summary.last_checkpoint.value["offsets"] == [
        {"topic": topic, "partition": 0, "offset": 3},
        {"topic": topic, "partition": 1, "offset": 3},
    ]


@pytest.mark.asyncio
async def test_kafka_seek_to_offsets_works_against_real_multi_partition_assignments(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-seek-offsets-{unique_suffix}"
    source_records = _partitioned_source_records(partitions=2, records_per_partition=4)

    await asyncio.wait_for(
        _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
        timeout=10.0,
    )
    await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    source = KafkaSource(
        assignments=[(topic, 0), (topic, 1)],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-it-seek-offsets-{unique_suffix}",
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        max_idle_polls=2,
    )

    await source.open()
    try:
        await source.seek_to_offsets({(topic, 0): 2, (topic, 1): 1})
        records = await asyncio.wait_for(
            _collect_from_source(source), timeout=_INTEGRATION_TIMEOUT_S
        )
    finally:
        await source.close()

    assert _partition_offset_pairs(records) == [
        (0, 2),
        (0, 3),
        (1, 1),
        (1, 2),
        (1, 3),
    ]
    assert sorted(
        (record["payload"]["partition"], record["payload"]["seq"]) for record in records
    ) == [
        (0, 2),
        (0, 3),
        (1, 1),
        (1, 2),
        (1, 3),
    ]


@pytest.mark.asyncio
async def test_kafka_seek_helpers_can_be_combined_with_pause_and_resume(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-seek-controls-{unique_suffix}"
    source_records = _partitioned_source_records(partitions=2, records_per_partition=3)

    await asyncio.wait_for(
        _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
        timeout=10.0,
    )
    await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    source = KafkaSource(
        assignments=[(topic, 0), (topic, 1)],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-it-seek-controls-{unique_suffix}",
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        max_idle_polls=2,
    )

    await source.open()
    try:
        await source.seek_to_end()
        assert (
            await asyncio.wait_for(_collect_from_source(source), timeout=_INTEGRATION_TIMEOUT_S)
            == []
        )

        await source.seek_to_beginning()
        source.pause([(topic, 1)])
        partition_zero_records = await asyncio.wait_for(
            _collect_from_source(source),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        source.resume([(topic, 1)])
        await source.seek_to_end([(topic, 0)])
        await source.seek_to_beginning([(topic, 1)])
        partition_one_records = await asyncio.wait_for(
            _collect_from_source(source),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        await source.close()

    assert _partition_offset_pairs(partition_zero_records) == [
        (0, 0),
        (0, 1),
        (0, 2),
    ]
    assert _partition_offset_pairs(partition_one_records) == [
        (1, 0),
        (1, 1),
        (1, 2),
    ]


@pytest.mark.asyncio
async def test_kafka_rebalance_handoff_commits_offsets_for_next_consumer(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-rebalance-{unique_suffix}"
    listener_one = _RebalanceListener()
    listener_two = _RebalanceListener()

    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    await asyncio.wait_for(
        (
            Pipeline(
                IterableSource(
                    [
                        {"partition": 0, "payload": {"id": 1, "name": "alpha"}},
                        {"partition": 0, "payload": {"id": 2, "name": "bravo"}},
                        {"partition": 0, "payload": {"id": 3, "name": "charlie"}},
                    ]
                )
            )
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    group_id = f"agora-it-rebalance-{unique_suffix}"
    source_one = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=100,
        rebalance_listener=listener_one,
        max_idle_polls=2,
    )
    source_two = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        rebalance_listener=listener_two,
        max_idle_polls=2,
    )

    stream_one = None
    source_one_snapshot = None
    source_two_snapshot = None
    source_two_prometheus = ""
    try:
        await source_one.open()
        stream_one = source_one.stream()
        first_record = await asyncio.wait_for(anext(stream_one), timeout=_INTEGRATION_TIMEOUT_S)
        ack_hook = source_one.delivery_success_callback()
        assert ack_hook is not None
        await ack_hook()

        await source_two.open()
        await _wait_for_rebalance_events(listener_one, listener_two)
        source_one_snapshot = await KafkaSourceRuntime(source_one).metrics_snapshot()

        if stream_one is not None:
            await stream_one.aclose()
            stream_one = None
        await source_one.close()
        await asyncio.sleep(2.0)

        handoff_records = await asyncio.wait_for(
            _collect_from_source(source_two),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        source_two_runtime = KafkaSourceRuntime(source_two)
        source_two_snapshot = await source_two_runtime.metrics_snapshot()
        source_two_prometheus = await source_two_runtime.render_prometheus_metrics(
            namespace="agora_it_kafka"
        )
    finally:
        if stream_one is not None:
            with contextlib.suppress(Exception):
                await stream_one.aclose()
        await source_two.close()
        await source_one.close()

    assert first_record["metadata"]["offset"] == 0
    assert [record["metadata"]["offset"] for record in handoff_records] == [1, 2]
    assert [record["payload"]["name"] for record in handoff_records] == ["bravo", "charlie"]
    assert any(event[0] == "revoked" for event in listener_one.events)
    assert any(event[0] == "assigned" for event in listener_two.events)
    assert source_one_snapshot is not None
    assert source_two_snapshot is not None
    assert source_one_snapshot.health.rebalance_count >= 1
    assert source_two_snapshot.health.ready is True
    assert source_two_snapshot.health.rebalance_count >= 1
    assert source_two_snapshot.health.assignment_count == 1
    assert source_two_snapshot.health.pending_commit_count == 0
    assert source_two_snapshot.health.last_commit_age_ms is not None
    assert source_two_snapshot.runtime.record_error_count == 0
    assert source_two_snapshot.runtime.record_drop_count == 0
    assert 'event="rebalance"}' in source_two_prometheus
    assert 'gauge="pending_commit_count"} 0' in source_two_prometheus


@pytest.mark.asyncio
async def test_kafka_rebalance_preserves_per_partition_ordering(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-rebalance-order-{unique_suffix}"
    group_id = f"agora-it-rebalance-order-{unique_suffix}"
    listener_one = _RebalanceListener()
    listener_two = _RebalanceListener()
    partitions = 2
    records_per_partition = 5

    await asyncio.wait_for(
        _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=partitions),
        timeout=10.0,
    )

    initial_records = [
        {
            "partition": partition,
            "key": f"order-key-{partition}",
            "payload": {
                "partition": partition,
                "key": f"order-key-{partition}",
                "seq": 0,
            },
        }
        for partition in range(partitions)
    ]
    await asyncio.wait_for(
        (
            Pipeline(IterableSource(initial_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    key_fn=lambda record: record["key"].encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    source_one = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        poll_timeout_ms=250,
        max_poll_records=1,
        rebalance_listener=listener_one,
        max_idle_polls=4,
    )
    source_two = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        poll_timeout_ms=250,
        rebalance_listener=listener_two,
        max_idle_polls=4,
    )

    stream_one = None
    source_one_open = False
    source_two_open = False
    first_records: list[dict[str, object]] = []
    try:
        await source_one.open()
        source_one_open = True
        stream_one = source_one.stream()
        seen_partitions: set[int] = set()
        while seen_partitions != set(range(partitions)):
            record = await asyncio.wait_for(anext(stream_one), timeout=_INTEGRATION_TIMEOUT_S)
            first_records.append(record)
            seen_partitions.add(int(record["metadata"]["partition"]))
            ack_hook = source_one.delivery_success_callback()
            assert ack_hook is not None
            await ack_hook()

        await source_two.open()
        source_two_open = True
        await _wait_for_rebalance_events(listener_one, listener_two)

        if stream_one is not None:
            await stream_one.aclose()
            stream_one = None
        await source_one.close()
        source_one_open = False
        await asyncio.sleep(2.0)

        remaining_records = [
            {
                "partition": partition,
                "key": f"order-key-{partition}",
                "payload": {
                    "partition": partition,
                    "key": f"order-key-{partition}",
                    "seq": seq,
                },
            }
            for seq in range(1, records_per_partition)
            for partition in range(partitions)
        ]
        await asyncio.wait_for(
            (
                Pipeline(IterableSource(remaining_records))
                .build(
                    KafkaSink(
                        topic=topic,
                        bootstrap_servers=kafka_bootstrap,
                        serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                        key_fn=lambda record: record["key"].encode("utf-8"),
                        partition_fn=lambda record: int(record["partition"]),
                    )
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        handoff_records = await asyncio.wait_for(
            _collect_from_source(source_two),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        if stream_one is not None:
            with contextlib.suppress(Exception):
                await stream_one.aclose()
        if source_two_open:
            await source_two.close()
        if source_one_open:
            await source_one.close()

    combined = first_records + handoff_records
    assert len(combined) == partitions * records_per_partition
    assert len(
        {(record["metadata"]["partition"], record["metadata"]["offset"]) for record in combined}
    ) == len(combined)

    by_partition: dict[int, list[int]] = {partition: [] for partition in range(partitions)}
    by_key: dict[str, list[int]] = {f"order-key-{partition}": [] for partition in range(partitions)}
    for record in combined:
        payload = record["payload"]
        metadata = record["metadata"]
        partition = int(metadata["partition"])
        key = str(payload["key"])
        assert payload["partition"] == partition
        assert metadata["key"] == key.encode("utf-8")
        by_partition[partition].append(int(payload["seq"]))
        by_key[key].append(int(payload["seq"]))

    expected_sequence = list(range(records_per_partition))
    for partition in range(partitions):
        assert by_partition[partition] == expected_sequence
        assert by_key[f"order-key-{partition}"] == expected_sequence
    assert any(event[0] == "revoked" for event in listener_one.events)
    assert any(event[0] == "assigned" for event in listener_two.events)


@pytest.mark.asyncio
async def test_kafka_rebalance_handoff_survives_broker_flap_for_next_consumer(
    kafka_bootstrap: str,
    unique_suffix: str,
    kafka_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-rebalance-flap-{unique_suffix}"
    listener_one = _RebalanceListener()
    listener_two = _RebalanceListener()

    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    await asyncio.wait_for(
        (
            Pipeline(
                IterableSource(
                    [
                        {"partition": 0, "payload": {"id": 1, "name": "alpha"}},
                        {"partition": 0, "payload": {"id": 2, "name": "bravo"}},
                        {"partition": 0, "payload": {"id": 3, "name": "charlie"}},
                        {"partition": 0, "payload": {"id": 4, "name": "delta"}},
                    ]
                )
            )
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    group_id = f"agora-it-rebalance-flap-{unique_suffix}"
    source_one = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=100,
        rebalance_listener=listener_one,
        max_idle_polls=2,
    )
    source_two = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        rebalance_listener=listener_two,
        max_idle_polls=2,
    )

    stream_one = None
    source_one_open = False
    source_two_open = False
    try:
        await source_one.open()
        source_one_open = True
        stream_one = source_one.stream()
        first_record = await asyncio.wait_for(anext(stream_one), timeout=_INTEGRATION_TIMEOUT_S)
        ack_hook = source_one.delivery_success_callback()
        assert ack_hook is not None
        await ack_hook()

        await source_two.open()
        source_two_open = True
        await _wait_for_rebalance_events(listener_one, listener_two)
        await asyncio.to_thread(kafka_broker_flap_control)
        await asyncio.sleep(3.0)

        if stream_one is not None:
            await stream_one.aclose()
            stream_one = None
        await source_one.close()
        source_one_open = False
        await asyncio.sleep(2.0)

        handoff_records = await asyncio.wait_for(
            _collect_from_source(source_two),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        if stream_one is not None:
            with contextlib.suppress(Exception):
                await stream_one.aclose()
        if source_two_open:
            await source_two.close()
        if source_one_open:
            await source_one.close()

    assert first_record["metadata"]["offset"] == 0
    assert [record["metadata"]["offset"] for record in handoff_records] == [1, 2, 3]
    assert [record["payload"]["name"] for record in handoff_records] == [
        "bravo",
        "charlie",
        "delta",
    ]
    assert any(event[0] == "revoked" for event in listener_one.events)
    assert any(event[0] == "assigned" for event in listener_two.events)


@pytest.mark.asyncio
async def test_kafka_dlq_replay_and_acknowledge_preserve_latest_visible_state(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-dlq-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)

    sink = KafkaDLQSink(topic=topic, bootstrap_servers=kafka_bootstrap)
    source = KafkaDLQSource(
        topic=topic,
        bootstrap_servers=kafka_bootstrap,
        pipeline_id="orders",
        stage="middleware",
    )
    record_one = _make_dlq_record(
        pipeline_id="orders",
        run_id="run-1",
        stage="middleware",
        event_id=1,
    )
    record_two = _make_dlq_record(
        pipeline_id="orders",
        run_id="run-2",
        stage="sink_write",
        event_id=2,
    )

    await sink.open()
    try:
        await sink.write(record_one)
        await sink.write(record_two)
        replayed = await sink.replay(record_one)
        await sink.acknowledge(record_two)
    finally:
        await sink.close()

    await asyncio.sleep(1.0)

    await source.open()
    try:
        records = [record async for record in source.stream()]
    finally:
        await source.close()

    assert replayed.attempt == 1
    assert [record.run_id for record in records] == ["run-1"]
    assert records[0].attempt == 1
    assert records[0]._storage_id == record_one._storage_id


@pytest.mark.asyncio
async def test_kafka_dlq_redaction_policy_removes_sensitive_payload_from_topic(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-dlq-redact-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    sink = KafkaDLQSink(
        topic=topic,
        bootstrap_servers=kafka_bootstrap,
        payload_policy=DLQPayloadPolicy.redacted(
            redact_fields=("ssn",),
            redact_headers=("x-private",),
        ),
    )
    record = _make_dlq_record(
        pipeline_id="orders",
        run_id="run-redact",
        stage="middleware",
        event_id=1,
    )
    record.record.update(
        {
            "password": "plain-secret",
            "headers": [
                {"key": "authorization", "value": {"encoding": "utf-8", "data": "Bearer abc"}},
                {"key": "tenant", "value": {"encoding": "utf-8", "data": "acme"}},
                {
                    "key": "x-private",
                    "value": {"encoding": "utf-8", "data": "top-secret-header"},
                },
            ],
        }
    )
    record.original_record["token"] = "raw-token"
    record.processed_record["ssn"] = "111-22-3333"

    await sink.open()
    try:
        await sink.write(record)
    finally:
        await sink.close()

    envelopes = await _collect_kafka_topic(
        bootstrap_servers=kafka_bootstrap,
        topic=topic,
        group_id=f"agora-it-dlq-redact-{unique_suffix}",
        max_records=1,
    )
    payload = envelopes[0]["payload"]
    rendered = json.dumps(envelopes[0], sort_keys=True)

    assert payload["record"]["password"] == "[REDACTED]"
    assert payload["record"]["headers"][0]["value"] == {
        "encoding": "redacted",
        "data": "[REDACTED]",
    }
    assert payload["record"]["headers"][1]["value"] == {"encoding": "utf-8", "data": "acme"}
    assert payload["record"]["headers"][2]["value"] == {
        "encoding": "redacted",
        "data": "[REDACTED]",
    }
    assert payload["original_record"]["token"] == "[REDACTED]"
    assert payload["processed_record"]["ssn"] == "[REDACTED]"
    assert "plain-secret" not in rendered
    assert "Bearer abc" not in rendered
    assert "top-secret-header" not in rendered
    assert "raw-token" not in rendered
    assert "111-22-3333" not in rendered


@pytest.mark.asyncio
async def test_kafka_dlq_encryption_policy_persists_ciphertext_and_replays_with_decryptor(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-dlq-encrypt-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    policy = DLQPayloadPolicy.encrypted(
        encryptor=_ReverseCipher(),
        encryption_algorithm="reverse",
        encryption_key_id="integration-test",
    )
    sink = KafkaDLQSink(
        topic=topic,
        bootstrap_servers=kafka_bootstrap,
        payload_policy=policy,
    )
    source = KafkaDLQSource(
        topic=topic,
        bootstrap_servers=kafka_bootstrap,
        pipeline_id="orders",
        stage="middleware",
        payload_policy=policy,
    )
    record = _make_dlq_record(
        pipeline_id="orders",
        run_id="run-encrypt",
        stage="middleware",
        event_id=1,
    )
    record.record.update({"password": "plain-secret"})
    record.original_record["token"] = "raw-token"

    await sink.open()
    try:
        await sink.write(record)
    finally:
        await sink.close()

    envelopes = await _collect_kafka_topic(
        bootstrap_servers=kafka_bootstrap,
        topic=topic,
        group_id=f"agora-it-dlq-encrypt-raw-{unique_suffix}",
        max_records=1,
    )
    rendered = json.dumps(envelopes[0], sort_keys=True)
    assert envelopes[0]["payload_encoding"] == "encrypted"
    assert envelopes[0]["payload_algorithm"] == "reverse"
    assert envelopes[0]["payload_key_id"] == "integration-test"
    assert "payload" not in envelopes[0]
    assert "plain-secret" not in rendered
    assert "raw-token" not in rendered

    await source.open()
    try:
        records = [record async for record in source.stream()]
    finally:
        await source.close()

    assert [record.run_id for record in records] == ["run-encrypt"]
    assert records[0].record["password"] == "plain-secret"
    assert records[0].original_record["token"] == "raw-token"


@pytest.mark.asyncio
async def test_kafka_checkpoint_resume_survives_multiple_short_restarts(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-restart-{unique_suffix}"
    store = InMemoryCheckpointStore()
    source_records = _partitioned_source_records(partitions=2, records_per_partition=5)

    await asyncio.wait_for(
        _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
        timeout=10.0,
    )
    await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    all_pairs: list[tuple[int, int]] = []
    final_checkpoint = None
    final_run_pairs: list[tuple[int, int]] = []
    for max_records in (4, 3, 3):
        collected = _CollectSink()
        summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        assignments=[(topic, 0), (topic, 1)],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-it-restart-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .build(
                    collected,  # type: ignore[arg-type]
                    config=DeliveryConfig(checkpoint=store),
                )
                .run(max_records=max_records)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        round_pairs = _partition_offset_pairs(collected.records)
        all_pairs.extend(round_pairs)
        final_run_pairs = round_pairs
        final_checkpoint = summary.last_checkpoint

    assert len(all_pairs) == 10
    assert len(set(all_pairs)) == 10
    assert sorted(all_pairs) == [
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (0, 4),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (1, 4),
    ]
    assert final_checkpoint is not None
    expected_final_offsets = [
        {
            "topic": topic,
            "partition": partition,
            "offset": max(
                offset for item_partition, offset in final_run_pairs if item_partition == partition
            ),
        }
        for partition in sorted({partition for partition, _ in final_run_pairs})
    ]
    assert final_checkpoint.value["offsets"] == expected_final_offsets


@pytest.mark.asyncio
async def test_kafka_checkpoint_resume_survives_process_death_after_checkpoint_before_commit(
    kafka_bootstrap: str,
    unique_suffix: str,
    tmp_path,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-process-death-{unique_suffix}"
    group_id = f"agora-it-process-death-{unique_suffix}"
    pipeline_id = f"agora-it-process-death-{unique_suffix}"
    checkpoint_path = tmp_path / "kafka-checkpoint.db"
    output_path = tmp_path / "kafka-child-output.jsonl"
    config_path = tmp_path / "kafka-child-config.json"

    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    await asyncio.wait_for(
        (
            Pipeline(IterableSource([{"id": 1}, {"id": 2}]))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record).encode("utf-8"),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    await asyncio.sleep(1.0)

    config_path.write_text(
        json.dumps(
            {
                "mode": "kafka",
                "bootstrap_servers": kafka_bootstrap,
                "topic": topic,
                "group_id": group_id,
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
        committed_after_child = await _kafka_committed_offset(
            bootstrap_servers=kafka_bootstrap,
            group_id=group_id,
            topic=topic,
        )
        collected = _CollectSink()
        summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=group_id,
                        deserializer=lambda value: json.loads(value.decode("utf-8")),
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                        poll_timeout_ms=250,
                        max_idle_polls=4,
                    ),
                    id=pipeline_id,
                )
                .build(collected, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
                .run(max_records=1)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        await store.close()

    assert read_jsonl(output_path) == [{"id": 1}]
    assert checkpoint is not None
    assert checkpoint.value["offset"] == 0
    assert committed_after_child is None
    assert collected.records == [{"id": 2}]
    assert summary.records_consumed == 1
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value["offset"] == 1


@pytest.mark.asyncio
async def test_kafka_checkpoint_resume_survives_broker_flap(
    kafka_bootstrap: str,
    unique_suffix: str,
    kafka_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-broker-flap-{unique_suffix}"
    store = InMemoryCheckpointStore()
    source_records = _partitioned_source_records(partitions=2, records_per_partition=5)

    await asyncio.wait_for(
        _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
        timeout=10.0,
    )
    await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    first_collected = _CollectSink()
    first_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    assignments=[(topic, 0), (topic, 1)],
                    bootstrap_servers=kafka_bootstrap,
                    group_id=f"agora-it-broker-flap-{unique_suffix}",
                    deserializer=lambda value, metadata: {
                        "payload": json.loads(value.decode("utf-8")),
                        "metadata": metadata,
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                )
            )
            .build(
                first_collected,  # type: ignore[arg-type]
                config=DeliveryConfig(checkpoint=store),
            )
            .run(max_records=4)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.to_thread(kafka_broker_flap_control)
    await asyncio.sleep(3.0)

    second_collected = _CollectSink()
    second_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    assignments=[(topic, 0), (topic, 1)],
                    bootstrap_servers=kafka_bootstrap,
                    group_id=f"agora-it-broker-flap-{unique_suffix}",
                    deserializer=lambda value, metadata: {
                        "payload": json.loads(value.decode("utf-8")),
                        "metadata": metadata,
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                )
            )
            .build(
                second_collected,  # type: ignore[arg-type]
                config=DeliveryConfig(checkpoint=store),
            )
            .run(max_records=6)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    combined_pairs = _partition_offset_pairs(first_collected.records) + _partition_offset_pairs(
        second_collected.records
    )
    assert first_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint is not None
    assert len(combined_pairs) == 10
    assert len(set(combined_pairs)) == 10
    assert sorted(combined_pairs) == [
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (0, 4),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (1, 4),
    ]
