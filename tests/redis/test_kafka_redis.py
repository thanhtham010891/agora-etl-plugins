from __future__ import annotations

from types import SimpleNamespace

import pytest
from agora.core.acceptance import AcceptanceFinding, AcceptanceReport
from agora.core.health import ComponentHealthSnapshot
from agora.core.source import SourceRuntimeMetrics

from agora_plugins.kafka import KafkaSourceHealthSnapshot, KafkaSourceOperationalMetrics
from agora_plugins.kafka.runtime import KafkaRuntimeReadinessError
from agora_plugins.redis import (
    KafkaRedisDeliveryConfig,
    KafkaRedisEnterpriseAcceptanceGate,
    KafkaRedisEnterpriseAcceptanceThresholds,
    KafkaRedisPrometheusExporter,
    KafkaRedisRuntime,
    KafkaRedisRuntimeHealthSnapshot,
    KafkaRedisStorageConfig,
    build_kafka_redis_runtime,
    build_kafka_redis_sink,
    build_kafka_redis_source,
    wrap_kafka_redis_deserializer,
)


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    async def set(self, key: str, value: object, **kwargs: object) -> None:
        self.calls.append(("set", (key, value), kwargs))

    async def xadd(self, key: str, value: dict[str, object], **kwargs: object) -> None:
        self.calls.append(("xadd", (key, value), kwargs))

    async def mset(self, mapping: dict[str, object]) -> None:
        self.calls.append(("mset", (mapping,), {}))


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


@pytest.mark.asyncio
async def test_wrap_kafka_redis_deserializer_attaches_metadata_and_lifecycle() -> None:
    inner = _FakeDeserializer()
    deserializer = wrap_kafka_redis_deserializer(inner)

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


@pytest.mark.asyncio
async def test_build_kafka_redis_sink_defaults_to_redis_key_and_json_value() -> None:
    sink = build_kafka_redis_sink(url="redis://localhost:6379/0")
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write(
        {
            "redis_key": "customers:1",
            "value": {"name": "ALPHA"},
            "kafka_delivery_key": "orders:0:1",
        }
    )

    assert client.calls == [
        ("set", ("customers:1", '{"name": "ALPHA"}'), {}),
    ]
    assert sink.metrics_snapshot().to_dict()["written_record_count"] == 1


@pytest.mark.asyncio
async def test_build_kafka_redis_sink_can_preserve_delivery_fields_in_value() -> None:
    sink = build_kafka_redis_sink(
        url="redis://localhost:6379/0",
        delivery=KafkaRedisDeliveryConfig(
            key_field="delivery_id",
            metadata_field="delivery_metadata",
        ),
        storage=KafkaRedisStorageConfig(
            preserve_delivery_fields_in_value=True,
        ),
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write(
        {
            "redis_key": "customers:9",
            "value": {"payload": "alpha"},
            "delivery_id": "orders:0:9",
            "delivery_metadata": {"topic": "orders", "offset": 9},
        }
    )

    assert client.calls == [
        (
            "set",
            (
                "customers:9",
                '{"delivery_id": "orders:0:9", "delivery_metadata": {"offset": 9, "topic": "orders"}, "payload": "alpha"}',
            ),
            {},
        )
    ]


@pytest.mark.asyncio
async def test_build_kafka_redis_sink_supports_custom_storage_fields() -> None:
    sink = build_kafka_redis_sink(
        url="redis://localhost:6379/0",
        storage=KafkaRedisStorageConfig(
            redis_key_field="cache_key",
            value_field="payload",
        ),
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write(
        {
            "cache_key": "customers:10",
            "payload": {"name": "BETA"},
        }
    )

    assert client.calls == [
        ("set", ("customers:10", '{"name": "BETA"}'), {}),
    ]


@pytest.mark.asyncio
async def test_build_kafka_redis_sink_xadd_serializes_nested_field_values() -> None:
    sink = build_kafka_redis_sink(url="redis://localhost:6379/0", mode="xadd")
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write(
        {
            "redis_key": "events",
            "value": {
                "id": 1,
                "customer": {"name": "ALPHA"},
                "tags": ["new", "vip"],
            },
        }
    )

    assert client.calls == [
        (
            "xadd",
            (
                "events",
                {
                    "id": 1,
                    "customer": '{"name": "ALPHA"}',
                    "tags": '["new", "vip"]',
                },
            ),
            {},
        )
    ]


def test_build_kafka_redis_runtime_wires_sink_and_delivery_defaults() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 5,
            "payload": "alpha",
        }
    )

    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "redis_key": f"customers:{item['offset']}",
            "value": item["payload"],
        },
    )

    assert isinstance(runtime, KafkaRedisRuntime)
    assert runtime.delivery == KafkaRedisDeliveryConfig()
    assert runtime.storage == KafkaRedisStorageConfig()
    assert runtime.sink.metrics_snapshot().mode == "set"  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_kafka_redis_runtime_respects_custom_delivery_config() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 1,
            "offset": 7,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "redis_key": "customers:7",
            "value": {"payload": item["payload"]},
        },
        serializer=lambda row: row,
        delivery=KafkaRedisDeliveryConfig(
            key_field="delivery_id",
            metadata_field=None,
        ),
    )

    runtime.sink._client = _FakeClient()  # type: ignore[attr-defined]
    await runtime.drain(max_records=1)

    assert runtime.sink._client.calls == [  # type: ignore[attr-defined]
        (
            "set",
            (
                "customers:7",
                {
                    "redis_key": "customers:7",
                    "value": {"payload": "alpha"},
                    "delivery_id": "orders:1:7",
                },
            ),
            {},
        )
    ]


@pytest.mark.asyncio
async def test_kafka_redis_runtime_can_preserve_delivery_fields_in_value_payload() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 1,
            "offset": 11,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "redis_key": "customers:11",
            "value": {"payload": item["payload"]},
        },
        delivery=KafkaRedisDeliveryConfig(
            key_field="delivery_id",
            metadata_field="delivery_metadata",
        ),
        storage=KafkaRedisStorageConfig(
            preserve_delivery_fields_in_value=True,
        ),
    )

    runtime.sink._client = _FakeClient()  # type: ignore[attr-defined]
    await runtime.drain(max_records=1)
    snapshot = await runtime.observability_snapshot()

    assert runtime.sink._client.calls == [  # type: ignore[attr-defined]
        (
            "set",
            (
                "customers:11",
                '{"delivery_id": "orders:1:11", "delivery_metadata": {"batch_index": 0, "batch_size": 1, "bootstrap_servers": "kafka:9092", "consumer_group": "orders", "delivery_id": "orders:1:11", "headers": [], "key": null, "offset": 11, "partition": 1, "subscription_mode": "manual_assign", "timestamp": null, "timestamp_type": null, "topic": "orders"}, "payload": "alpha"}',
            ),
            {},
        )
    ]
    assert snapshot.redis_key_field == "redis_key"
    assert snapshot.value_field == "value"
    assert snapshot.preserve_delivery_fields_in_value is True


@pytest.mark.asyncio
async def test_kafka_redis_runtime_supports_custom_storage_fields() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 2,
            "offset": 13,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "cache_key": "customers:13",
            "payload": {"payload": item["payload"]},
        },
        storage=KafkaRedisStorageConfig(
            redis_key_field="cache_key",
            value_field="payload",
        ),
    )

    runtime.sink._client = _FakeClient()  # type: ignore[attr-defined]
    await runtime.drain(max_records=1)
    snapshot = await runtime.observability_snapshot()

    assert runtime.sink._client.calls == [  # type: ignore[attr-defined]
        (
            "set",
            ("customers:13", '{"payload": "alpha"}'),
            {},
        )
    ]
    assert snapshot.redis_key_field == "cache_key"
    assert snapshot.value_field == "payload"
    assert snapshot.preserve_delivery_fields_in_value is False


@pytest.mark.asyncio
async def test_build_kafka_redis_source_wraps_payload_with_metadata() -> None:
    source = build_kafka_redis_source(
        topics=["orders"],
        bootstrap_servers="kafka:9092",
        group_id="orders-consumer",
        deserializer=lambda value: {"decoded": value.decode("utf-8")},
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


@pytest.mark.asyncio
async def test_kafka_redis_runtime_observability_snapshot_and_prometheus() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 3,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "redis_key": "customers:3",
            "value": {"payload": item["payload"]},
        },
        serializer=lambda row: row,
    )

    runtime.sink._client = _FakeClient()  # type: ignore[attr-defined]
    records = await runtime.drain(max_records=1)
    health = await runtime.health_snapshot()
    snapshot = await runtime.observability_snapshot()
    rendered = await runtime.render_prometheus_metrics(namespace="agora_kafka_redis")
    rendered_direct = KafkaRedisPrometheusExporter(namespace="agora_kafka_redis").render(snapshot)

    assert records == [
        {
            "topic": "orders",
            "partition": 0,
            "offset": 3,
            "payload": "alpha",
        }
    ]
    assert runtime.sink._client.calls == [  # type: ignore[attr-defined]
        (
            "set",
            (
                "customers:3",
                {
                    "redis_key": "customers:3",
                    "value": {"payload": "alpha"},
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
                },
            ),
            {},
        )
    ]
    assert source.acked_offsets == [3]
    assert health == snapshot.health
    assert snapshot.health.ready is True
    assert snapshot.health.source_ready is True
    assert snapshot.health.source_stalled is False
    assert snapshot.health.sink_connection_ready is True
    assert snapshot.health.sink_mode == "set"
    assert snapshot.delivery_key_field == "kafka_delivery_key"
    assert snapshot.delivery_metadata_field == "kafka_metadata"
    assert snapshot.redis_key_field == "redis_key"
    assert snapshot.value_field == "value"
    assert snapshot.preserve_delivery_fields_in_value is False
    assert snapshot.sink.mode == "set"
    assert snapshot.sink.target == "localhost:6379/0"
    assert snapshot.sink.write_call_count == 1
    assert snapshot.sink.written_record_count == 1
    assert snapshot.source.health.consumer_group == "orders"
    assert 'agora_kafka_redis_runtime_state{consumer_group="orders"' in rendered
    assert 'agora_kafka_redis_runtime_config{consumer_group="orders"' in rendered
    assert (
        'agora_kafka_redis_sink_events_total{consumer_group="orders",bootstrap_servers="kafka:9092",'
        'sink_target="localhost:6379/0",sink_mode="set",redis_key_field="redis_key",'
        'value_field="value",delivery_key_field="kafka_delivery_key",delivery_metadata_field="kafka_metadata",'
        'event="written_record"} 1'
    ) in rendered
    assert (
        'agora_kafka_redis_sink_events_total{consumer_group="orders",bootstrap_servers="kafka:9092",'
        'sink_target="localhost:6379/0",sink_mode="set",redis_key_field="redis_key",'
        'value_field="value",delivery_key_field="kafka_delivery_key",delivery_metadata_field="kafka_metadata",'
        'event="written_record"} 1'
    ) in rendered_direct
    assert (
        'agora_kafka_redis_sink_age_ms{consumer_group="orders",bootstrap_servers="kafka:9092",'
        'sink_target="localhost:6379/0",sink_mode="set",redis_key_field="redis_key",'
        'value_field="value",delivery_key_field="kafka_delivery_key",delivery_metadata_field="kafka_metadata",'
        'activity="write"} '
    ) in rendered_direct


@pytest.mark.asyncio
async def test_kafka_redis_runtime_acceptance_report_uses_runtime_health() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 4,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "redis_key": "customers:4",
            "value": {"payload": item["payload"]},
        },
        serializer=lambda row: row,
    )

    snapshot = await runtime.observability_snapshot()
    report = await runtime.acceptance_report(
        KafkaRedisEnterpriseAcceptanceThresholds(
            require_runtime_ready=True,
            require_source_ready=True,
            require_source_not_stalled=True,
            require_sink_connection_ready=True,
            max_pending_commit_count=None,
            max_idle_poll_count=None,
            max_total_lag=None,
            max_max_lag=None,
            max_total_commit_lag=None,
            max_max_commit_lag=None,
            max_last_poll_age_ms=None,
            max_last_message_age_ms=None,
            max_last_commit_age_ms=None,
            max_record_error_count=None,
            max_record_drop_count=None,
        )
    )

    assert isinstance(snapshot.health, ComponentHealthSnapshot)
    assert report.passed is False
    assert {finding.metric for finding in report.findings} == {
        "runtime.ready",
        "sink.connection_ready",
    }


@pytest.mark.asyncio
async def test_kafka_redis_runtime_ensure_ready_returns_snapshot_when_healthy() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 5,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "redis_key": "customers:5",
            "value": {"payload": item["payload"]},
        },
        serializer=lambda row: row,
    )
    runtime.sink._client = _FakeClient()  # type: ignore[attr-defined]
    await runtime.drain(max_records=1)

    health, snapshot, report = await runtime.ensure_ready(
        KafkaRedisEnterpriseAcceptanceThresholds(
            require_runtime_ready=True,
            require_source_ready=True,
            require_source_not_stalled=True,
            require_sink_connection_ready=True,
            max_pending_commit_count=None,
            max_idle_poll_count=None,
            max_total_lag=None,
            max_max_lag=None,
            max_total_commit_lag=None,
            max_max_commit_lag=None,
            max_last_poll_age_ms=None,
            max_last_message_age_ms=None,
            max_last_commit_age_ms=None,
            max_record_error_count=None,
            max_record_drop_count=None,
        )
    )

    assert health == snapshot.health
    assert report.passed is True


@pytest.mark.asyncio
async def test_kafka_redis_runtime_ensure_ready_raises_when_gate_fails() -> None:
    source = _FakeSource(
        {
            "topic": "orders",
            "partition": 0,
            "offset": 6,
            "payload": "alpha",
        }
    )
    runtime = build_kafka_redis_runtime(
        source=source,  # type: ignore[arg-type]
        url="redis://localhost:6379/0",
        transform=lambda item: {
            "redis_key": "customers:6",
            "value": {"payload": item["payload"]},
        },
        serializer=lambda row: row,
    )

    with pytest.raises(KafkaRuntimeReadinessError, match=r"sink\.connection_ready"):
        await runtime.ensure_ready()


def test_kafka_redis_enterprise_acceptance_gate_passes_healthy_runtime() -> None:
    report = KafkaRedisEnterpriseAcceptanceGate().evaluate(
        snapshot=SimpleNamespace(  # type: ignore[arg-type]
            health=KafkaRedisRuntimeHealthSnapshot(
                ready=True,
                source_ready=True,
                source_stalled=False,
                sink_connection_ready=True,
                sink_mode="set",
            ),
            source=SimpleNamespace(
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
                runtime=SourceRuntimeMetrics(record_error_count=0, record_drop_count=0),
            ),
        ),
    )

    assert report.passed is True
    assert report.findings == ()


def test_kafka_redis_enterprise_acceptance_gate_reports_threshold_failures() -> None:
    report = KafkaRedisEnterpriseAcceptanceGate(
        KafkaRedisEnterpriseAcceptanceThresholds(
            require_runtime_ready=True,
            require_source_ready=True,
            require_source_not_stalled=True,
            require_sink_connection_ready=True,
            max_pending_commit_count=0,
            max_idle_poll_count=0,
            max_total_lag=0,
            max_max_lag=0,
            max_total_commit_lag=0,
            max_max_commit_lag=0,
            max_last_poll_age_ms=1_000.0,
            max_last_message_age_ms=1_000.0,
            max_last_commit_age_ms=1_000.0,
            max_record_error_count=0,
            max_record_drop_count=0,
        )
    ).evaluate(
        snapshot=SimpleNamespace(  # type: ignore[arg-type]
            health=KafkaRedisRuntimeHealthSnapshot(
                ready=False,
                source_ready=False,
                source_stalled=True,
                sink_connection_ready=False,
                sink_mode="set",
            ),
            source=SimpleNamespace(
                health=KafkaSourceHealthSnapshot(
                    ready=False,
                    stalled=True,
                    consumer_group="orders",
                    bootstrap_servers="kafka:9092",
                    subscription_mode="manual_assign",
                    assignment_count=1,
                    paused_partition_count=0,
                    pending_commit_count=4,
                    rebalance_count=0,
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
                runtime=SourceRuntimeMetrics(record_error_count=2, record_drop_count=1),
            ),
        ),
    )

    metrics = {finding.metric for finding in report.findings}
    assert report.passed is False
    assert isinstance(report, AcceptanceReport)
    assert all(isinstance(finding, AcceptanceFinding) for finding in report.findings)
    assert "runtime.ready" in metrics
    assert "source.ready" in metrics
    assert "source.stalled" in metrics
    assert "sink.connection_ready" in metrics
    assert "source.pending_commit_count" in metrics
    assert "source.idle_poll_count" in metrics
    assert "source.total_lag" in metrics
    assert "source.max_lag" in metrics
    assert "source.total_commit_lag" in metrics
    assert "source.max_commit_lag" in metrics
    assert "source.last_poll_age_ms" in metrics
    assert "source.last_message_age_ms" in metrics
    assert "source.last_commit_age_ms" in metrics
    assert "source.record_error_count" in metrics
    assert "source.record_drop_count" in metrics
