from __future__ import annotations

from dataclasses import dataclass

import pytest
from agora.core.source import SourceRuntimeMetrics

from agora_plugins.kafka import KafkaDeliveryContext
from agora_plugins.kafka.runtime import KafkaSourceRuntime, KafkaTransformSinkRuntime


class _FakeSink:
    sink_name = "fake"

    def __init__(self) -> None:
        self.open_count = 0
        self.close_count = 0
        self.writes: list[object] = []
        self.flush_count = 0

    async def open(self) -> None:
        self.open_count += 1

    async def write(self, record: object) -> None:
        self.writes.append(record)

    async def flush(self) -> None:
        self.flush_count += 1

    async def close(self) -> None:
        self.close_count += 1


class _FakeTransactionalSink(_FakeSink):
    def __init__(self, *, fail_commit: bool = False) -> None:
        super().__init__()
        self.fail_commit = fail_commit
        self.transaction_calls: list[object] = []

    async def begin_transaction(self) -> None:
        self.transaction_calls.append("begin")

    async def write(self, record: object) -> None:
        self.transaction_calls.append(("write", record))
        await super().write(record)

    async def flush(self) -> None:
        self.transaction_calls.append("flush")
        await super().flush()

    async def send_offsets_to_transaction(self, offsets: object, group_id: str) -> None:
        self.transaction_calls.append(("offsets", offsets, group_id))

    async def commit_transaction(self) -> None:
        self.transaction_calls.append("commit")
        if self.fail_commit:
            raise RuntimeError("commit failed")

    async def abort_transaction(self) -> None:
        self.transaction_calls.append("abort")


class _FakePendingAckSink(_FakeSink):
    def __init__(self) -> None:
        super().__init__()
        self.pending_ack_count = 0

    async def write(self, record: object) -> None:
        await super().write(record)
        self.pending_ack_count += 1

    async def wait_for_pending_acks(self) -> None:
        self.pending_ack_count = 0


@dataclass
class _FakeOperationalMetrics:
    rebalance_count: int = 0
    batch_deserialize_error_count: int = 0
    manual_assign_partition_count: int = 0
    paused_partition_count: int = 0
    poison_record_dlq_write_count: int = 0
    poison_record_dlq_write_failure_count: int = 0
    poison_record_log_only_count: int = 0
    poison_record_fail_closed_count: int = 0
    poison_record_deserialization_count: int = 0
    poison_record_schema_evolution_count: int = 0
    poison_record_schema_validation_count: int = 0
    poison_record_schema_registry_binding_mismatch_count: int = 0
    poison_record_unknown_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "rebalance_count": self.rebalance_count,
            "batch_deserialize_error_count": self.batch_deserialize_error_count,
            "manual_assign_partition_count": self.manual_assign_partition_count,
            "paused_partition_count": self.paused_partition_count,
            "poison_record_dlq_write_count": self.poison_record_dlq_write_count,
            "poison_record_dlq_write_failure_count": (self.poison_record_dlq_write_failure_count),
            "poison_record_log_only_count": self.poison_record_log_only_count,
            "poison_record_fail_closed_count": self.poison_record_fail_closed_count,
            "poison_record_deserialization_count": self.poison_record_deserialization_count,
            "poison_record_schema_evolution_count": self.poison_record_schema_evolution_count,
            "poison_record_schema_validation_count": self.poison_record_schema_validation_count,
            "poison_record_schema_registry_binding_mismatch_count": (
                self.poison_record_schema_registry_binding_mismatch_count
            ),
            "poison_record_unknown_count": self.poison_record_unknown_count,
        }


@dataclass
class _FakeHealthSnapshot:
    ready: bool = True
    stalled: bool = False
    consumer_group: str = "orders"
    bootstrap_servers: str = "kafka:9092"
    subscription_mode: str = "manual_assign"
    assignment_count: int = 0
    paused_partition_count: int = 1
    pending_commit_count: int = 0
    rebalance_count: int = 4
    idle_poll_count: int = 0
    record_error_count: int = 2
    record_drop_count: int = 1
    last_poll_age_ms: float | None = None
    last_message_age_ms: float | None = None
    last_commit_age_ms: float | None = None
    last_rebalance_age_ms: float | None = None
    total_lag: int | None = None
    lagging_partition_count: int = 0
    max_lag: int | None = None
    total_commit_lag: int | None = None
    max_commit_lag: int | None = None
    partitions: tuple[object, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "stalled": self.stalled,
            "consumer_group": self.consumer_group,
            "bootstrap_servers": self.bootstrap_servers,
            "subscription_mode": self.subscription_mode,
            "assignment_count": self.assignment_count,
            "paused_partition_count": self.paused_partition_count,
            "pending_commit_count": self.pending_commit_count,
            "rebalance_count": self.rebalance_count,
            "idle_poll_count": self.idle_poll_count,
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
            "last_poll_age_ms": self.last_poll_age_ms,
            "last_message_age_ms": self.last_message_age_ms,
            "last_commit_age_ms": self.last_commit_age_ms,
            "last_rebalance_age_ms": self.last_rebalance_age_ms,
            "total_lag": self.total_lag,
            "lagging_partition_count": self.lagging_partition_count,
            "max_lag": self.max_lag,
            "total_commit_lag": self.total_commit_lag,
            "max_commit_lag": self.max_commit_lag,
            "partitions": list(self.partitions),
        }


class _FakeSource:
    source_name = "fake-kafka"

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = list(records)
        self._record_index = 0
        self._current_ack = None
        self._current_delivery_context = None
        self.open_count = 0
        self.close_count = 0
        self.acked_offsets: list[int] = []
        self.transaction_committed_offsets: list[int] = []
        self.commit_now_calls = 0
        self.seek_offsets_calls: list[dict[tuple[str, int], int]] = []
        self.seek_to_beginning_calls: list[object] = []
        self.seek_to_end_calls: list[object] = []
        self.pause_calls: list[object] = []
        self.resume_calls: list[object] = []

    async def open(self) -> None:
        self.open_count += 1

    async def close(self) -> None:
        self.close_count += 1

    async def commit_now(self) -> None:
        self.commit_now_calls += 1

    async def seek_to_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        self.seek_offsets_calls.append(dict(offsets))

    async def seek_to_beginning(self, partitions: object = None) -> None:
        self.seek_to_beginning_calls.append(partitions)

    async def seek_to_end(self, partitions: object = None) -> None:
        self.seek_to_end_calls.append(partitions)

    def pause(self, partitions: object = None) -> None:
        self.pause_calls.append(partitions)

    def resume(self, partitions: object = None) -> None:
        self.resume_calls.append(partitions)

    def current_checkpoint(self) -> dict[str, object]:
        return {"offset": self._record_index - 1}

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(record_error_count=2, record_drop_count=1)

    def operational_metrics(self) -> _FakeOperationalMetrics:
        return _FakeOperationalMetrics(
            rebalance_count=4,
            batch_deserialize_error_count=3,
            manual_assign_partition_count=2,
            paused_partition_count=1,
            poison_record_dlq_write_count=5,
            poison_record_schema_validation_count=1,
        )

    async def health_snapshot(self) -> _FakeHealthSnapshot:
        return _FakeHealthSnapshot(assignment_count=len(self._records))

    def delivery_success_callback(self):
        return self._current_ack

    def delivery_transaction_offsets_callback(self):
        return self._current_tx_offsets

    def delivery_transaction_committed_callback(self):
        return self._current_tx_committed

    def delivery_context(self):
        return self._current_delivery_context

    async def stream(self):
        while self._record_index < len(self._records):
            record = self._records[self._record_index]
            offset = int(record["offset"])
            acknowledged = False

            async def _ack(*, _offset: int = offset) -> None:
                nonlocal acknowledged
                if acknowledged:
                    return
                acknowledged = True
                self.acked_offsets.append(_offset)

            async def _tx_offsets(
                *,
                _topic: str = str(record.get("topic", "topic")),
                _partition: int = int(record.get("partition", 0)),
                _offset: int = offset,
            ) -> tuple[dict[tuple[str, int], int], str]:
                return {(_topic, _partition): _offset + 1}, "orders"

            async def _tx_committed(*, _offset: int = offset) -> None:
                nonlocal acknowledged
                acknowledged = True
                self.transaction_committed_offsets.append(_offset)

            self._current_ack = _ack
            self._current_tx_offsets = _tx_offsets
            self._current_tx_committed = _tx_committed
            topic = str(record.get("topic", "topic"))
            partition = int(record.get("partition", 0))
            self._current_delivery_context = KafkaDeliveryContext(
                topic=topic,
                partition=partition,
                offset=offset,
                consumer_group="orders",
                bootstrap_servers="kafka:9092",
                subscription_mode="manual_assign",
                batch_size=1,
                batch_index=0,
                headers=tuple(record.get("headers", [])),
            )
            self._record_index += 1
            try:
                yield record
            finally:
                self._current_ack = None
                self._current_tx_offsets = None
                self._current_tx_committed = None
                self._current_delivery_context = None


@pytest.mark.asyncio
async def test_kafka_source_runtime_deliver_transforms_flushes_and_acks() -> None:
    source = _FakeSource([{"offset": 0, "payload": "alpha"}])
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]
    sink = _FakeSink()

    stream = source.stream()
    record = await anext(stream)
    try:
        delivered = await runtime.deliver(
            record,
            sink,  # type: ignore[arg-type]
            transform=lambda item: {"value": str(item["payload"]).upper()},
        )
    finally:
        await stream.aclose()

    assert delivered == {"value": "ALPHA"}
    assert sink.writes == [{"value": "ALPHA"}]
    assert sink.flush_count == 1
    assert source.acked_offsets == [0]


@pytest.mark.asyncio
async def test_kafka_source_runtime_deliver_waits_for_sink_acks_without_flush() -> None:
    source = _FakeSource([{"offset": 0, "payload": "alpha"}])
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]
    sink = _FakePendingAckSink()

    stream = source.stream()
    record = await anext(stream)
    try:
        delivered = await runtime.deliver(
            record,
            sink,  # type: ignore[arg-type]
            transform=lambda item: {"value": str(item["payload"]).upper()},
            flush=False,
        )
    finally:
        await stream.aclose()

    assert delivered == {"value": "ALPHA"}
    assert sink.flush_count == 0
    assert sink.pending_ack_count == 0
    assert source.acked_offsets == [0]


@pytest.mark.asyncio
async def test_kafka_source_runtime_deliver_can_commit_offsets_transactionally() -> None:
    source = _FakeSource([{"topic": "orders", "partition": 2, "offset": 4, "payload": "alpha"}])
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]
    sink = _FakeTransactionalSink()

    stream = source.stream()
    record = await anext(stream)
    try:
        delivered = await runtime.deliver(
            record,
            sink,  # type: ignore[arg-type]
            transform=lambda item: {"value": str(item["payload"]).upper()},
            transactional_offsets=True,
        )
    finally:
        await stream.aclose()

    assert delivered == {"value": "ALPHA"}
    assert sink.transaction_calls == [
        "begin",
        ("write", {"value": "ALPHA"}),
        "flush",
        ("offsets", {("orders", 2): 5}, "orders"),
        "commit",
    ]
    assert source.acked_offsets == []
    assert source.transaction_committed_offsets == [4]


@pytest.mark.asyncio
async def test_kafka_source_runtime_transactional_delivery_aborts_on_failure() -> None:
    source = _FakeSource([{"offset": 1, "payload": "alpha"}])
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]
    sink = _FakeTransactionalSink(fail_commit=True)

    stream = source.stream()
    record = await anext(stream)
    try:
        with pytest.raises(RuntimeError, match="commit failed"):
            await runtime.deliver(
                record,
                sink,  # type: ignore[arg-type]
                transform=lambda item: {"value": str(item["payload"]).upper()},
                transactional_offsets=True,
            )
    finally:
        await stream.aclose()

    assert sink.transaction_calls == [
        "begin",
        ("write", {"value": "ALPHA"}),
        "flush",
        ("offsets", {("topic", 0): 2}, "orders"),
        "commit",
        "abort",
    ]
    assert source.acked_offsets == []
    assert source.transaction_committed_offsets == []


@pytest.mark.asyncio
async def test_kafka_source_runtime_drain_to_respects_max_records() -> None:
    source = _FakeSource(
        [
            {"offset": 0, "payload": "alpha"},
            {"offset": 1, "payload": "bravo"},
            {"offset": 2, "payload": "charlie"},
        ]
    )
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]
    sink = _FakeSink()

    records = await runtime.drain_to(
        sink,  # type: ignore[arg-type]
        transform=lambda item: {"value": item["payload"]},
        max_records=2,
    )

    assert records == [
        {"offset": 0, "payload": "alpha"},
        {"offset": 1, "payload": "bravo"},
    ]
    assert sink.writes == [
        {"value": "alpha"},
        {"value": "bravo"},
    ]
    assert sink.flush_count == 2
    assert source.acked_offsets == [0, 1]


@pytest.mark.asyncio
async def test_kafka_source_runtime_injects_delivery_metadata_and_key() -> None:
    source = _FakeSource(
        [
            {
                "topic": "orders",
                "partition": 2,
                "offset": 7,
                "payload": "alpha",
                "headers": [("tenant", b"acme")],
            }
        ]
    )
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]
    sink = _FakeSink()

    stream = source.stream()
    record = await anext(stream)
    try:
        delivered = await runtime.deliver(
            record,
            sink,  # type: ignore[arg-type]
            transform=lambda item: {"value": str(item["payload"]).upper()},
            delivery_metadata_field="kafka_metadata",
            delivery_key_field="kafka_delivery_key",
        )
    finally:
        await stream.aclose()

    assert delivered == {
        "value": "ALPHA",
        "kafka_metadata": {
            "topic": "orders",
            "partition": 2,
            "offset": 7,
            "consumer_group": "orders",
            "bootstrap_servers": "kafka:9092",
            "subscription_mode": "manual_assign",
            "batch_size": 1,
            "batch_index": 0,
            "key": None,
            "headers": [("tenant", b"acme")],
            "timestamp": None,
            "timestamp_type": None,
            "delivery_id": "orders:2:7",
        },
        "kafka_delivery_key": "orders:2:7",
    }
    assert sink.writes == [delivered]
    assert source.acked_offsets == [7]


@pytest.mark.asyncio
async def test_kafka_source_runtime_rejects_delivery_injection_for_non_mapping_payloads() -> None:
    source = _FakeSource([{"offset": 0, "payload": "alpha"}])
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]
    sink = _FakeSink()

    stream = source.stream()
    record = await anext(stream)
    try:
        with pytest.raises(TypeError, match="mutable mappings"):
            await runtime.deliver(
                record,
                sink,  # type: ignore[arg-type]
                transform=lambda item: str(item["payload"]).upper(),
                delivery_key_field="kafka_delivery_key",
            )
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_kafka_source_runtime_proxies_operator_controls() -> None:
    source = _FakeSource([])
    runtime = KafkaSourceRuntime(source)  # type: ignore[arg-type]

    await runtime.commit_now()
    await runtime.seek_to_offsets({("topic", 0): 3})
    await runtime.seek_to_beginning([("topic", 0)])
    await runtime.seek_to_end([("topic", 1)])
    runtime.pause([("topic", 0)])
    runtime.resume([("topic", 1)])
    snapshot = await runtime.health_snapshot()
    metrics_snapshot = await runtime.metrics_snapshot()
    rendered = await runtime.render_prometheus_metrics(namespace="test_kafka")

    assert source.commit_now_calls == 1
    assert source.seek_offsets_calls == [{("topic", 0): 3}]
    assert source.seek_to_beginning_calls == [[("topic", 0)]]
    assert source.seek_to_end_calls == [[("topic", 1)]]
    assert source.pause_calls == [[("topic", 0)]]
    assert source.resume_calls == [[("topic", 1)]]
    assert snapshot.ready is True
    assert snapshot.assignment_count == 0
    assert metrics_snapshot.runtime.record_error_count == 2
    assert metrics_snapshot.operational.batch_deserialize_error_count == 3
    assert metrics_snapshot.health.consumer_group == "orders"
    assert 'test_kafka_source_events_total{consumer_group="orders"' in rendered


@pytest.mark.asyncio
async def test_kafka_transform_sink_runtime_opens_closes_and_drains() -> None:
    source = _FakeSource(
        [
            {"offset": 0, "payload": "alpha"},
            {"offset": 1, "payload": "bravo"},
        ]
    )
    sink = _FakeSink()
    runtime = KafkaTransformSinkRuntime(
        source,  # type: ignore[arg-type]
        sink,  # type: ignore[arg-type]
        transform=lambda item: {"value": str(item["payload"]).upper()},
    )

    await runtime.open()
    try:
        records = await runtime.drain()
    finally:
        await runtime.close()

    assert records == [
        {"offset": 0, "payload": "alpha"},
        {"offset": 1, "payload": "bravo"},
    ]
    assert sink.writes == [{"value": "ALPHA"}, {"value": "BRAVO"}]
    assert sink.flush_count == 2
    assert source.acked_offsets == [0, 1]
    assert source.open_count == 1
    assert source.close_count == 1
    assert sink.open_count == 1
    assert sink.close_count == 1


@pytest.mark.asyncio
async def test_kafka_transform_sink_runtime_deliver_and_controls_proxy() -> None:
    source = _FakeSource([{"offset": 0, "payload": "alpha"}])
    sink = _FakeSink()
    runtime = KafkaTransformSinkRuntime(
        source,  # type: ignore[arg-type]
        sink,  # type: ignore[arg-type]
        transform=lambda item: {"value": item["payload"]},
        flush_each_record=False,
    )

    stream = source.stream()
    record = await anext(stream)
    try:
        delivered = await runtime.deliver(record)
    finally:
        await stream.aclose()

    await runtime.commit_now()
    await runtime.seek_to_offsets({("topic", 0): 5})
    await runtime.seek_to_beginning([("topic", 0)])
    await runtime.seek_to_end([("topic", 1)])
    runtime.pause([("topic", 0)])
    runtime.resume([("topic", 1)])
    snapshot = await runtime.health_snapshot()
    metrics_snapshot = await runtime.metrics_snapshot()
    rendered = await runtime.render_prometheus_metrics(namespace="test_kafka")

    assert delivered == {"value": "alpha"}
    assert sink.writes == [{"value": "alpha"}]
    assert sink.flush_count == 0
    assert source.acked_offsets == [0]
    assert source.commit_now_calls == 1
    assert source.seek_offsets_calls == [{("topic", 0): 5}]
    assert source.seek_to_beginning_calls == [[("topic", 0)]]
    assert source.seek_to_end_calls == [[("topic", 1)]]
    assert source.pause_calls == [[("topic", 0)]]
    assert source.resume_calls == [[("topic", 1)]]
    assert snapshot.ready is True
    assert snapshot.assignment_count == 1
    assert metrics_snapshot.operational.manual_assign_partition_count == 2
    assert "test_kafka_source_gauge" in rendered


@pytest.mark.asyncio
async def test_kafka_transform_sink_runtime_uses_default_delivery_injection_fields() -> None:
    source = _FakeSource([{"topic": "orders", "partition": 1, "offset": 4, "payload": "alpha"}])
    sink = _FakeSink()
    runtime = KafkaTransformSinkRuntime(
        source,  # type: ignore[arg-type]
        sink,  # type: ignore[arg-type]
        transform=lambda item: {"value": item["payload"]},
        delivery_metadata_field="kafka_metadata",
        delivery_key_field="kafka_delivery_key",
    )

    records = await runtime.drain(max_records=1)

    assert records == [{"topic": "orders", "partition": 1, "offset": 4, "payload": "alpha"}]
    assert sink.writes == [
        {
            "value": "alpha",
            "kafka_metadata": {
                "topic": "orders",
                "partition": 1,
                "offset": 4,
                "consumer_group": "orders",
                "bootstrap_servers": "kafka:9092",
                "subscription_mode": "manual_assign",
                "batch_size": 1,
                "batch_index": 0,
                "key": None,
                "headers": [],
                "timestamp": None,
                "timestamp_type": None,
                "delivery_id": "orders:1:4",
            },
            "kafka_delivery_key": "orders:1:4",
        }
    ]
