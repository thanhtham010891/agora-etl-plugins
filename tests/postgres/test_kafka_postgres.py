from __future__ import annotations

from types import SimpleNamespace

import pytest
from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.health import ComponentHealthSnapshot
from agora.core.source import SourceRuntimeMetrics

from agora_plugins.kafka import (
    KafkaPoisonRecordPolicy,
    KafkaSourceHealthSnapshot,
    KafkaSourceMetricsSnapshot,
    KafkaSourceOperationalMetrics,
)
from agora_plugins.kafka.runtime import KafkaRuntimeReadinessError
from agora_plugins.postgres import (
    KafkaPostgresDeliveryConfig,
    KafkaPostgresEnterpriseAcceptanceGate,
    KafkaPostgresEnterpriseAcceptanceThresholds,
    KafkaPostgresPoisonDLQConfig,
    KafkaPostgresPrometheusExporter,
    KafkaPostgresRuntime,
    KafkaPostgresRuntimeHealthSnapshot,
    KafkaPostgresRuntimeMetricsSnapshot,
    PostgresSinkMetricsSnapshot,
    build_kafka_postgres_runtime,
    build_kafka_postgres_sink,
    build_kafka_postgres_source,
    with_kafka_delivery_fields,
    wrap_kafka_postgres_deserializer,
)


class _FakeSink:
    def __init__(self) -> None:
        self.writes: list[object] = []
        self.flush_count = 0

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def write(self, record: object) -> None:
        self.writes.append(record)

    async def flush(self) -> None:
        self.flush_count += 1


class _FakeSource:
    def __init__(self, record: dict[str, object]) -> None:
        self._record = record
        self.acked_offsets: list[int] = []
        self._ack = None
        self._delivery_context = None

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def stream(self):
        acknowledged = False
        offset = int(self._record["offset"])

        async def _ack() -> None:
            nonlocal acknowledged
            if acknowledged:
                return
            acknowledged = True
            self.acked_offsets.append(offset)

        self._ack = _ack
        delivery_id = f"{self._record['topic']}:{self._record['partition']}:{offset}"
        self._delivery_context = SimpleNamespace(
            delivery_id=delivery_id,
            to_dict=lambda: {
                "topic": self._record["topic"],
                "partition": self._record["partition"],
                "offset": offset,
                "consumer_group": "orders",
                "bootstrap_servers": "kafka:9092",
                "subscription_mode": "manual_assign",
                "batch_size": 1,
                "batch_index": 0,
                "key": None,
                "headers": [],
                "timestamp": None,
                "timestamp_type": None,
                "delivery_id": delivery_id,
            },
        )
        try:
            yield self._record
        finally:
            self._ack = None
            self._delivery_context = None

    def delivery_success_callback(self):
        return self._ack

    def delivery_context(self):
        return self._delivery_context

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(record_error_count=1, record_drop_count=0)

    def operational_metrics(self) -> KafkaSourceOperationalMetrics:
        return KafkaSourceOperationalMetrics(
            rebalance_count=2,
            manual_assign_partition_count=1,
            poison_record_dlq_write_count=3,
        )

    async def health_snapshot(self) -> KafkaSourceHealthSnapshot:
        return KafkaSourceHealthSnapshot(
            ready=True,
            stalled=False,
            consumer_group="orders",
            bootstrap_servers="kafka:9092",
            subscription_mode="manual_assign",
            assignment_count=1,
            paused_partition_count=0,
            pending_commit_count=0,
            rebalance_count=2,
            idle_poll_count=0,
            record_error_count=1,
            record_drop_count=0,
        )


class _FakePoisonDLQSink:
    def __init__(self, *, connection_ready: bool = True, table_ready: bool = True) -> None:
        self._connection_ready = connection_ready
        self._table_ready = table_ready

    def metrics_snapshot(self):
        return SimpleNamespace(
            connection_ready=self._connection_ready,
            table_ready=self._table_ready,
        )


class _FakeDeserializer:
    def __init__(self) -> None:
        self.open_count = 0
        self.close_count = 0

    async def open(self) -> None:
        self.open_count += 1

    async def close(self) -> None:
        self.close_count += 1

    async def __call__(self, value: bytes) -> dict[str, object]:
        return {"decoded": value.decode("utf-8")}


def test_with_kafka_delivery_fields_preserves_injected_fields() -> None:
    mapper = with_kafka_delivery_fields(lambda row: {"event_id": row["event_id"]})

    mapped = mapper(
        {
            "event_id": 1,
            "kafka_delivery_key": "orders:0:1",
            "kafka_metadata": {"topic": "orders", "offset": 1},
        }
    )

    assert mapped == {
        "event_id": 1,
        "kafka_delivery_key": "orders:0:1",
        "kafka_metadata": {"topic": "orders", "offset": 1},
    }


@pytest.mark.asyncio
async def test_wrap_kafka_postgres_deserializer_attaches_metadata_and_lifecycle() -> None:
    inner = _FakeDeserializer()
    deserializer = wrap_kafka_postgres_deserializer(inner)

    await deserializer.open()
    record = await deserializer(
        b"alpha",
        {
            "topic": "orders",
            "partition": 0,
            "offset": 1,
        },
    )
    await deserializer.close()

    assert record == {
        "payload": {"decoded": "alpha"},
        "metadata": {
            "topic": "orders",
            "partition": 0,
            "offset": 1,
        },
    }
    assert inner.open_count == 1
    assert inner.close_count == 1


def test_build_kafka_postgres_sink_defaults_to_delivery_key_conflict() -> None:
    sink = build_kafka_postgres_sink(
        dsn="postgresql://localhost/test",
        table="events",
        row_mapper=lambda row: {"event_id": row["event_id"]},
    )

    assert sink._conflict_keys == ["kafka_delivery_key"]  # type: ignore[attr-defined]
    mapped = sink._row_mapper(  # type: ignore[attr-defined]
        {
            "event_id": 1,
            "kafka_delivery_key": "orders:0:1",
            "kafka_metadata": {"topic": "orders", "offset": 1},
        }
    )
    assert mapped["kafka_delivery_key"] == "orders:0:1"
    assert mapped["kafka_metadata"] == {"topic": "orders", "offset": 1}
    assert sink.delivery_capability().replay_safe is True


def test_build_kafka_postgres_sink_does_not_claim_replay_safety_without_delivery_key() -> None:
    sink = build_kafka_postgres_sink(
        dsn="postgresql://localhost/test",
        table="events",
        row_mapper=lambda row: {"event_id": row["event_id"]},
        conflict_key="event_id",
    )

    assert sink.delivery_capability().replay_safe is False


def test_build_kafka_postgres_sink_respects_custom_delivery_config() -> None:
    sink = build_kafka_postgres_sink(
        dsn="postgresql://localhost/test",
        table="events",
        row_mapper=lambda row: {"event_id": row["event_id"]},
        delivery=KafkaPostgresDeliveryConfig(
            key_field="delivery_id",
            metadata_field=None,
        ),
    )

    assert sink._conflict_keys == ["delivery_id"]  # type: ignore[attr-defined]
    mapped = sink._row_mapper(  # type: ignore[attr-defined]
        {
            "event_id": 1,
            "delivery_id": "orders:0:1",
            "kafka_metadata": {"topic": "orders", "offset": 1},
        }
    )
    assert mapped == {
        "event_id": 1,
        "delivery_id": "orders:0:1",
    }


def test_build_kafka_postgres_runtime_wires_sink_and_delivery_defaults() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 5,
            "payload": "alpha",
        }
    )

    runtime = build_kafka_postgres_runtime(
        source=source,  # type: ignore[arg-type]
        dsn="postgresql://localhost/test",
        table="events",
        transform=lambda item: {"value": item["payload"]},
    )

    assert isinstance(runtime, KafkaPostgresRuntime)
    assert runtime.delivery == KafkaPostgresDeliveryConfig()
    assert runtime.sink._conflict_keys == ["kafka_delivery_key"]  # type: ignore[attr-defined]
    mapped = runtime.sink._row_mapper(  # type: ignore[attr-defined]
        {
            "value": "alpha",
            "kafka_delivery_key": "orders:0:5",
            "kafka_metadata": {"topic": "orders", "offset": 5},
        }
    )
    assert mapped["kafka_delivery_key"] == "orders:0:5"
    assert mapped["kafka_metadata"] == {"topic": "orders", "offset": 5}


@pytest.mark.asyncio
async def test_build_kafka_postgres_source_wraps_payload_and_configures_postgres_poison_dlq() -> (
    None
):
    source = build_kafka_postgres_source(
        topics=["orders"],
        bootstrap_servers="kafka:9092",
        group_id="orders-consumer",
        deserializer=lambda value: {"decoded": value.decode("utf-8")},
        poison_dlq=KafkaPostgresPoisonDLQConfig(
            dsn="postgresql://localhost/test",
            table="orders_dlq",
            policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
            pipeline_id="orders-kafka-source",
            max_attempts=7,
        ),
    )

    record = await source._deserializer(  # type: ignore[attr-defined]
        b"alpha",
        {
            "topic": "orders",
            "partition": 0,
            "offset": 1,
        },
    )

    assert record == {
        "payload": {"decoded": "alpha"},
        "metadata": {
            "topic": "orders",
            "partition": 0,
            "offset": 1,
        },
    }
    assert source._poison_record_policy == KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE  # type: ignore[attr-defined]
    assert source._poison_record_pipeline_id == "orders-kafka-source"  # type: ignore[attr-defined]
    assert source._poison_record_max_attempts == 7  # type: ignore[attr-defined]
    assert source._poison_record_sink is not None  # type: ignore[attr-defined]
    assert source._poison_record_sink._table == "orders_dlq"  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_injects_delivery_fields_by_default() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 3,
            "payload": "alpha",
        }
    )
    sink = _FakeSink()
    runtime = KafkaPostgresRuntime(
        source,  # type: ignore[arg-type]
        sink,  # type: ignore[arg-type]
        transform=lambda item: {"value": item["payload"]},
    )

    records = await runtime.drain(max_records=1)

    assert records == [
        {
            "topic": "orders",
            "partition": 0,
            "offset": 3,
            "payload": "alpha",
        }
    ]
    assert sink.writes == [
        {
            "value": "alpha",
            "kafka_delivery_key": "orders:0:3",
            "kafka_metadata": {
                "topic": "orders",
                "partition": 0,
                "offset": 3,
                "consumer_group": "orders",
                "bootstrap_servers": "kafka:9092",
                "subscription_mode": "manual_assign",
                "batch_size": 1,
                "batch_index": 0,
                "key": None,
                "headers": [],
                "timestamp": None,
                "timestamp_type": None,
                "delivery_id": "orders:0:3",
            },
        }
    ]
    assert source.acked_offsets == [3]


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_observability_snapshot_and_prometheus() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 9,
            "payload": "alpha",
        }
    )
    source._agora_postgres_poison_dlq_config = KafkaPostgresPoisonDLQConfig(  # type: ignore[attr-defined]
        dsn="postgresql://localhost/test",
        table="orders_dlq",
        policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        pipeline_id="orders-kafka-source",
    )
    source._poison_record_sink = _FakePoisonDLQSink()  # type: ignore[attr-defined]
    runtime = build_kafka_postgres_runtime(
        source=source,  # type: ignore[arg-type]
        dsn="postgresql://localhost/test",
        table="events",
        transform=lambda item: {"value": item["payload"]},
        batch_size=2,
    )

    await runtime.sink.write({"value": "alpha", "kafka_delivery_key": "orders:0:9"})  # type: ignore[arg-type]
    health = await runtime.health_snapshot()
    snapshot = await runtime.observability_snapshot()
    rendered = await runtime.render_prometheus_metrics(namespace="agora_kafka_postgres")
    rendered_direct = KafkaPostgresPrometheusExporter(namespace="agora_kafka_postgres").render(
        snapshot
    )

    assert health == snapshot.health
    assert snapshot.health.ready is False
    assert snapshot.health.source_ready is True
    assert snapshot.health.source_stalled is False
    assert snapshot.health.sink_connection_ready is False
    assert snapshot.health.poison_dlq_enabled is True
    assert snapshot.health.poison_dlq_ready is True
    assert snapshot.delivery_key_field == "kafka_delivery_key"
    assert snapshot.delivery_metadata_field == "kafka_metadata"
    assert snapshot.poison_dlq_enabled is True
    assert snapshot.poison_dlq_table == "orders_dlq"
    assert snapshot.poison_dlq_policy == KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE
    assert snapshot.sink.buffered_row_count == 1
    assert snapshot.sink.enqueue_count == 1
    assert snapshot.source.health.consumer_group == "orders"
    assert 'agora_kafka_postgres_runtime_state{consumer_group="orders"' in rendered
    assert 'agora_kafka_postgres_runtime_config{consumer_group="orders"' in rendered
    assert (
        'agora_kafka_postgres_sink_gauge{consumer_group="orders",bootstrap_servers="kafka:9092",'
        'table="events",insert_mode="sql",sink_write_safety_policy="strict",'
        'delivery_key_field="kafka_delivery_key",'
        'delivery_metadata_field="kafka_metadata",poison_dlq_table="orders_dlq",'
        'poison_dlq_policy="dlq_and_continue",poison_dlq_pipeline_id="orders-kafka-source",'
        'gauge="buffered_row_count"} 1'
    ) in rendered
    assert [
        line for line in rendered_direct.splitlines() if not line.startswith("# scrape_time")
    ] == [line for line in rendered.splitlines() if not line.startswith("# scrape_time")]


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_reads_live_poison_sink_from_source() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 9,
            "payload": "alpha",
        }
    )
    source._agora_postgres_poison_dlq_config = KafkaPostgresPoisonDLQConfig(  # type: ignore[attr-defined]
        dsn="postgresql://localhost/test",
        table="orders_dlq",
        policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
    )
    source._poison_record_sink = _FakePoisonDLQSink(connection_ready=True, table_ready=True)  # type: ignore[attr-defined]
    runtime = build_kafka_postgres_runtime(
        source=source,  # type: ignore[arg-type]
        dsn="postgresql://localhost/test",
        table="events",
        transform=lambda item: {"value": item["payload"]},
        batch_size=2,
    )
    source._poison_record_sink = _FakePoisonDLQSink(connection_ready=False, table_ready=True)  # type: ignore[attr-defined]

    snapshot = await runtime.observability_snapshot()

    assert snapshot.health.poison_dlq_ready is False


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_acceptance_report_uses_runtime_health() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 10,
            "payload": "alpha",
        }
    )
    source._agora_postgres_poison_dlq_config = KafkaPostgresPoisonDLQConfig(  # type: ignore[attr-defined]
        dsn="postgresql://localhost/test",
        table="orders_dlq",
        policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
    )
    source._poison_record_sink = _FakePoisonDLQSink()  # type: ignore[attr-defined]
    runtime = build_kafka_postgres_runtime(
        source=source,  # type: ignore[arg-type]
        dsn="postgresql://localhost/test",
        table="events",
        transform=lambda item: {"value": item["payload"]},
        batch_size=2,
    )
    await runtime.sink.write({"value": "alpha", "kafka_delivery_key": "orders:0:10"})  # type: ignore[arg-type]

    snapshot = await runtime.observability_snapshot()
    report = await runtime.acceptance_report(
        KafkaPostgresEnterpriseAcceptanceThresholds(
            require_runtime_ready=True,
            require_source_ready=True,
            require_source_not_stalled=True,
            require_sink_connection_ready=False,
            require_poison_dlq_ready=True,
            max_pending_commit_count=None,
            max_idle_poll_count=None,
            max_total_lag=None,
            max_max_lag=None,
            max_total_commit_lag=None,
            max_max_commit_lag=None,
            max_last_poll_age_ms=None,
            max_last_message_age_ms=None,
            max_last_commit_age_ms=None,
            max_buffered_row_count=None,
            max_sink_retry_count=None,
            max_poison_dlq_write_count=None,
            max_record_error_count=None,
            max_record_drop_count=None,
        )
    )

    assert isinstance(snapshot.health, ComponentHealthSnapshot)
    assert report.passed is False
    assert {finding.metric for finding in report.findings} == {"runtime.ready"}


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_ensure_ready_returns_snapshot_when_healthy() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 12,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_postgres_runtime(
        source=source,  # type: ignore[arg-type]
        dsn="postgresql://localhost/test",
        table="events",
        transform=lambda item: {"value": item["payload"]},
        batch_size=2,
    )
    await runtime.sink.write({"value": "alpha", "kafka_delivery_key": "orders:0:12"})  # type: ignore[arg-type]
    runtime.sink._conn = object()  # type: ignore[attr-defined]

    health, snapshot, report = await runtime.ensure_ready(
        KafkaPostgresEnterpriseAcceptanceThresholds(
            require_runtime_ready=True,
            require_source_ready=True,
            require_source_not_stalled=True,
            require_sink_connection_ready=False,
            require_poison_dlq_ready=False,
            max_pending_commit_count=None,
            max_idle_poll_count=None,
            max_total_lag=None,
            max_max_lag=None,
            max_total_commit_lag=None,
            max_max_commit_lag=None,
            max_last_poll_age_ms=None,
            max_last_message_age_ms=None,
            max_last_commit_age_ms=None,
            max_buffered_row_count=None,
            max_sink_retry_count=None,
            max_poison_dlq_write_count=None,
            max_record_error_count=None,
            max_record_drop_count=None,
        )
    )

    assert health == snapshot.health
    assert report.passed is True


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_ensure_ready_raises_when_gate_fails() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 13,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_postgres_runtime(
        source=source,  # type: ignore[arg-type]
        dsn="postgresql://localhost/test",
        table="events",
        transform=lambda item: {"value": item["payload"]},
        batch_size=2,
    )

    with pytest.raises(KafkaRuntimeReadinessError, match=r"sink\.connection_ready"):
        await runtime.ensure_ready()


@pytest.mark.asyncio
async def test_kafka_postgres_enterprise_acceptance_gate_passes_healthy_runtime() -> None:
    report = KafkaPostgresEnterpriseAcceptanceGate().evaluate(
        KafkaPostgresRuntimeMetricsSnapshot(
            health=KafkaPostgresRuntimeHealthSnapshot(
                ready=True,
                source_ready=True,
                source_stalled=False,
                sink_connection_ready=True,
                sink_write_safety_policy="strict",
                poison_dlq_enabled=False,
                poison_dlq_ready=None,
            ),
            source=KafkaSourceMetricsSnapshot(
                health=KafkaSourceHealthSnapshot(
                    ready=True,
                    stalled=False,
                    consumer_group="orders",
                    bootstrap_servers="kafka:9092",
                    subscription_mode="manual_assign",
                    assignment_count=1,
                    paused_partition_count=0,
                    pending_commit_count=0,
                    rebalance_count=2,
                    idle_poll_count=0,
                    record_error_count=0,
                    record_drop_count=0,
                    total_lag=0,
                    max_lag=0,
                    total_commit_lag=0,
                    max_commit_lag=0,
                ),
                operational=KafkaSourceOperationalMetrics(),
                runtime=SourceRuntimeMetrics(record_error_count=0, record_drop_count=0),
            ),
            sink=PostgresSinkMetricsSnapshot(
                table="events",
                conflict_keys=("kafka_delivery_key",),
                batch_size=2,
                upsert=True,
                insert_mode="sql",
                pool_size=1,
                max_rows_per_statement=None,
                max_parameters_per_statement=32_000,
                write_safety_policy="strict",
                buffered_row_count=0,
                retry_count=0,
                connection_ready=True,
            ),
            delivery_key_field="kafka_delivery_key",
            delivery_metadata_field="kafka_metadata",
        )
    )

    assert report.passed is True
    assert report.findings == ()


def test_kafka_postgres_enterprise_acceptance_gate_reports_threshold_failures() -> None:
    acceptance = KafkaPostgresEnterpriseAcceptanceGate(
        KafkaPostgresEnterpriseAcceptanceThresholds(
            require_runtime_ready=True,
            require_poison_dlq_ready=True,
            max_pending_commit_count=0,
            max_idle_poll_count=0,
            max_total_lag=0,
            max_max_lag=0,
            max_total_commit_lag=0,
            max_max_commit_lag=0,
            max_last_poll_age_ms=1_000.0,
            max_last_message_age_ms=1_000.0,
            max_last_commit_age_ms=1_000.0,
            max_buffered_row_count=0,
            max_sink_retry_count=0,
            max_poison_dlq_write_count=0,
            max_record_error_count=0,
            max_record_drop_count=0,
        )
    )
    report = acceptance.evaluate(
        KafkaPostgresRuntimeMetricsSnapshot(
            health=KafkaPostgresRuntimeHealthSnapshot(
                ready=False,
                source_ready=False,
                source_stalled=True,
                sink_connection_ready=False,
                sink_write_safety_policy="strict",
                poison_dlq_enabled=True,
                poison_dlq_ready=False,
            ),
            source=KafkaSourceMetricsSnapshot(
                health=KafkaSourceHealthSnapshot(
                    ready=False,
                    stalled=True,
                    consumer_group="orders",
                    bootstrap_servers="kafka:9092",
                    subscription_mode="manual_assign",
                    assignment_count=1,
                    paused_partition_count=0,
                    pending_commit_count=4,
                    rebalance_count=2,
                    idle_poll_count=3,
                    record_error_count=2,
                    record_drop_count=1,
                    last_poll_age_ms=7_500.0,
                    last_message_age_ms=6_000.0,
                    last_commit_age_ms=12_500.0,
                    total_lag=9,
                    max_lag=5,
                    total_commit_lag=7,
                    max_commit_lag=4,
                ),
                operational=KafkaSourceOperationalMetrics(poison_record_dlq_write_count=2),
                runtime=SourceRuntimeMetrics(record_error_count=2, record_drop_count=1),
            ),
            sink=PostgresSinkMetricsSnapshot(
                table="events",
                conflict_keys=("kafka_delivery_key",),
                batch_size=2,
                upsert=True,
                insert_mode="sql",
                pool_size=1,
                max_rows_per_statement=None,
                max_parameters_per_statement=32_000,
                write_safety_policy="strict",
                buffered_row_count=2,
                retry_count=1,
                connection_ready=False,
            ),
            delivery_key_field="kafka_delivery_key",
            delivery_metadata_field="kafka_metadata",
        )
    )

    metrics = {finding.metric for finding in report.findings}

    assert report.passed is False
    assert isinstance(report, AcceptanceReport)
    assert all(isinstance(finding, AcceptanceFinding) for finding in report.findings)
    assert isinstance(report.to_dict(), dict)
    assert "runtime.ready" in metrics
    assert "source.ready" in metrics
    assert "source.stalled" in metrics
    assert "sink.connection_ready" in metrics
    assert "poison_dlq.ready" in metrics
    assert "source.pending_commit_count" in metrics
    assert "source.idle_poll_count" in metrics
    assert "source.total_lag" in metrics
    assert "source.max_lag" in metrics
    assert "source.total_commit_lag" in metrics
    assert "source.max_commit_lag" in metrics
    assert "source.last_poll_age_ms" in metrics
    assert "source.last_message_age_ms" in metrics
    assert "source.last_commit_age_ms" in metrics
    assert "sink.buffered_row_count" in metrics
    assert "sink.retry_count" in metrics
    assert "source.poison_record_dlq_write_count" in metrics
    assert "source.record_error_count" in metrics
    assert "source.record_drop_count" in metrics


def test_kafka_postgres_acceptance_gate_rejects_unsafe_replay_recipe() -> None:
    report = KafkaPostgresEnterpriseAcceptanceGate().evaluate(
        KafkaPostgresRuntimeMetricsSnapshot(
            health=KafkaPostgresRuntimeHealthSnapshot(
                ready=True,
                source_ready=True,
                source_stalled=False,
                sink_connection_ready=True,
                sink_write_safety_policy="align_to_target",
                poison_dlq_enabled=False,
                poison_dlq_ready=None,
            ),
            source=KafkaSourceMetricsSnapshot(
                health=KafkaSourceHealthSnapshot(
                    ready=True,
                    stalled=False,
                    consumer_group="orders",
                    bootstrap_servers="kafka:9092",
                    subscription_mode="manual_assign",
                    assignment_count=1,
                    paused_partition_count=0,
                    pending_commit_count=0,
                    rebalance_count=0,
                    idle_poll_count=0,
                    record_error_count=0,
                    record_drop_count=0,
                    total_lag=0,
                    max_lag=0,
                    total_commit_lag=0,
                    max_commit_lag=0,
                ),
                operational=KafkaSourceOperationalMetrics(),
                runtime=SourceRuntimeMetrics(record_error_count=0, record_drop_count=0),
            ),
            sink=PostgresSinkMetricsSnapshot(
                table="events",
                conflict_keys=("event_id",),
                batch_size=2,
                upsert=False,
                insert_mode="sql",
                pool_size=1,
                max_rows_per_statement=None,
                max_parameters_per_statement=32_000,
                write_safety_policy="align_to_target",
                buffered_row_count=0,
                retry_count=0,
                connection_ready=True,
            ),
            delivery_key_field="kafka_delivery_key",
            delivery_metadata_field="kafka_metadata",
        )
    )

    assert report.passed is False
    assert {finding.metric for finding in report.findings} == {
        "sink.delivery_key_conflict",
        "sink.upsert",
        "sink.write_safety_policy",
    }
