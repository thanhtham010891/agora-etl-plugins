from __future__ import annotations

import asyncio
import contextlib
import json
import os
from typing import cast

import pytest
from agora import DeliveryConfig, InMemoryCheckpointStore, IterableSource, MapMiddleware, Pipeline
from agora.core.checkpoint import Checkpoint
from agora.core.retry import RetryPolicy

from agora_plugins.kafka import (
    AvroSchemaRegistryDeserializer,
    AvroSchemaRegistrySerializer,
    KafkaPoisonRecordPolicy,
    KafkaSink,
    KafkaSource,
    KafkaTransformSinkRuntime,
)
from agora_plugins.postgres import (
    KafkaPostgresEnterpriseAcceptanceThresholds,
    KafkaPostgresPoisonDLQConfig,
    KafkaPostgresRuntime,
    PostgresDLQSink,
    PostgresDLQSource,
    PostgresSink,
    build_kafka_postgres_runtime,
    build_kafka_postgres_source,
)
from tests.integration._runtime_readiness import assert_runtime_readiness

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 30.0
_REBALANCE_EVENT_POLL_INTERVAL_S = 0.05
_SECURE_SOAK_PRODUCER_SETTLE_S = 0.05
_SECURE_SOAK_REBALANCE_TIMEOUT_S = 12.0
_SECURE_SOAK_INITIAL_RECORD_TIMEOUT_S = 20.0
_SECURE_SOAK_DRAIN_TIMEOUT_S = 45.0
_SECURE_SOAK_DLQ_READ_TIMEOUT_S = 20.0
_RUNTIME_READINESS_THRESHOLDS = KafkaPostgresEnterpriseAcceptanceThresholds(
    require_runtime_ready=True,
    require_source_ready=True,
    require_source_not_stalled=True,
    require_sink_connection_ready=True,
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


def _cluster_restart_sequence(*, default_cycles: int = 1) -> list[int]:
    raw_cycles = os.getenv("AGORA_TEST_KAFKA_CLUSTER_SOAK_CYCLES")
    if raw_cycles is None or raw_cycles == "":
        cycles = default_cycles
    else:
        try:
            cycles = int(raw_cycles)
        except ValueError:
            cycles = default_cycles
    return [1, 2, 3] * max(cycles, 1)


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
        await asyncio.sleep(_REBALANCE_EVENT_POLL_INTERVAL_S)
    pytest.fail("Timed out waiting for Kafka rebalance listener events.")


async def _wait_for_rebalance_event_counts(
    *requirements: tuple[_RebalanceListener, int],
    timeout_s: float = 10.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if all(len(listener.events) >= minimum for listener, minimum in requirements):
            return
        await asyncio.sleep(_REBALANCE_EVENT_POLL_INTERVAL_S)
    pytest.fail("Timed out waiting for Kafka rebalance listener event counts.")


def _header_value(headers: list[tuple[str, bytes]], name: str) -> str:
    for header_name, header_value in headers:
        if header_name == name:
            return header_value.decode("utf-8")
    raise KeyError(name)


async def _ensure_topic_exists(
    bootstrap_servers: str,
    topic: str,
    *,
    num_partitions: int = 1,
    replication_factor: int = 1,
    security: object | None = None,
) -> None:
    from inspect import Parameter, signature

    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin_kwargs: dict[str, object] = {}
    if security is not None:
        raw_kwargs = security.to_aiokafka_admin_kwargs()
        parameters = signature(AIOKafkaAdminClient.__init__).parameters
        if any(parameter.kind is Parameter.VAR_KEYWORD for parameter in parameters.values()):
            admin_kwargs = raw_kwargs
        else:
            admin_kwargs = {key: value for key, value in raw_kwargs.items() if key in parameters}
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers, **admin_kwargs)
    await admin.start()
    try:
        try:
            await admin.create_topics(
                [
                    NewTopic(
                        name=topic,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                    )
                ]
            )
        except TopicAlreadyExistsError:
            return
    finally:
        await admin.close()


async def _topic_partition_leader_id(
    bootstrap_servers: str,
    topic: str,
    partition: int,
) -> int:
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        topics = await admin.describe_topics([topic])
    finally:
        await admin.close()

    for described_topic in topics:
        if str(described_topic.get("topic")) != topic:
            continue
        for described_partition in described_topic.get("partitions", []):
            if int(described_partition.get("partition")) == partition:
                return int(described_partition["leader"])
    pytest.fail(f"Unable to resolve leader for {topic} partition {partition}.")


async def _wait_for_topic_partition_leader_change(
    bootstrap_servers: str,
    topic: str,
    partition: int,
    *,
    previous_leader_id: int,
    timeout_s: float = 15.0,
) -> int:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        leader_id = await _topic_partition_leader_id(bootstrap_servers, topic, partition)
        if leader_id >= 0 and leader_id != previous_leader_id:
            return leader_id
        await asyncio.sleep(_REBALANCE_EVENT_POLL_INTERVAL_S)
    pytest.fail(
        f"Timed out waiting for leader change on {topic} partition {partition} "
        f"after broker {previous_leader_id} restart."
    )


async def _consumer_group_coordinator_id(
    bootstrap_servers: str,
    group_id: str,
) -> int:
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        return int(await admin.find_coordinator(group_id))
    finally:
        await admin.close()


async def _wait_for_consumer_group_coordinator_change(
    bootstrap_servers: str,
    group_id: str,
    *,
    previous_coordinator_id: int,
    timeout_s: float = 20.0,
) -> int:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        try:
            coordinator_id = await _consumer_group_coordinator_id(bootstrap_servers, group_id)
        except Exception:
            await asyncio.sleep(_REBALANCE_EVENT_POLL_INTERVAL_S)
            continue
        if coordinator_id >= 0 and coordinator_id != previous_coordinator_id:
            return coordinator_id
        await asyncio.sleep(_REBALANCE_EVENT_POLL_INTERVAL_S)
    pytest.fail(
        f"Timed out waiting for consumer-group coordinator change for {group_id} "
        f"after broker {previous_coordinator_id} stop."
    )


def _kafka_postgres_json_row_mapper(row: dict[str, object]) -> dict[str, object]:
    return {
        "event_id": row["event_id"],
        "display_name": row["display_name"],
        "tenant": row["tenant"],
        "event_type": row["event_type"],
        "kafka_topic": row["kafka_topic"],
        "kafka_partition": row["kafka_partition"],
        "kafka_offset": row["kafka_offset"],
        "kafka_metadata": json.dumps(
            row["kafka_metadata"],
            sort_keys=True,
            default=(
                lambda value: (
                    value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)
                )
            ),
        ),
    }


async def _produce_customer_records(
    *,
    kafka_bootstrap: str,
    topic: str,
    source_records: list[dict[str, object]],
) -> None:
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
                    key_fn=lambda record: record["key"].encode("utf-8"),
                    partition_fn=(
                        lambda record: int(record["partition"]) if "partition" in record else None
                    ),
                    headers_fn=lambda record: [
                        (name, value.encode("utf-8")) for name, value in record["headers"]
                    ],
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert producer_summary.records_written == len(source_records)


async def _produce_schema_registry_customer_records(
    *,
    kafka_bootstrap: str,
    security: object,
    topic: str,
    source_records: list[dict[str, object]],
    serializer: object,
) -> None:
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=serializer,
                    key_fn=lambda record: record["key"].encode("utf-8"),
                    partition_fn=lambda record: int(record["partition"]),
                    headers_fn=lambda record: [
                        (name, value.encode("utf-8")) for name, value in record["headers"]
                    ],
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert producer_summary.records_written == len(source_records)


class _SchemaRegistryEnvelopeDeserializer:
    def __init__(self, inner: object) -> None:
        self._inner = inner

    async def open(self) -> None:
        await self._inner.open()

    async def close(self) -> None:
        await self._inner.close()

    async def __call__(
        self,
        value: bytes,
        metadata: dict[str, object],
    ) -> dict[str, object]:
        return {
            "payload": await self._inner(value),
            "metadata": metadata,
        }


class _SchemaRegistryPayloadSerializer:
    def __init__(self, inner: object) -> None:
        self._inner = inner

    async def open(self) -> None:
        await self._inner.open()

    async def close(self) -> None:
        await self._inner.close()

    async def __call__(self, record: dict[str, object]) -> bytes:
        return await self._inner(record["payload"])


class _VersionedSchemaRegistryPayloadSerializer:
    def __init__(self, serializers: dict[str, object]) -> None:
        self._serializers = serializers

    async def open(self) -> None:
        for serializer in self._serializers.values():
            await serializer.open()

    async def close(self) -> None:
        for serializer in self._serializers.values():
            await serializer.close()

    async def __call__(self, record: dict[str, object]) -> bytes:
        schema_version = str(record["schema_version"])
        serializer = self._serializers[schema_version]
        return await serializer(record["payload"])


def _customer_transform(record: dict[str, object]) -> dict[str, object]:
    payload = record["payload"]
    metadata = record["metadata"]
    assert isinstance(payload, dict)
    assert isinstance(metadata, dict)
    headers = metadata["headers"]
    assert isinstance(headers, list)
    name = payload["name"]
    if name == "reject-me":
        raise ValueError("unsupported customer name")
    return {
        "event_id": payload["id"],
        "display_name": str(name).upper(),
        "tenant": _header_value(headers, "tenant"),
        "event_type": _header_value(headers, "event_type"),
        "kafka_topic": metadata["topic"],
        "kafka_partition": metadata["partition"],
        "kafka_offset": metadata["offset"],
    }


def _partitioned_customer_records(
    *,
    partitions: int,
    records_per_partition: int,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for partition in range(partitions):
        for sequence in range(records_per_partition):
            event_type = "customer.created" if sequence % 2 == 0 else "customer.updated"
            records.append(
                {
                    "partition": partition,
                    "sequence": sequence,
                    "key": f"customer-{partition}-{sequence}",
                    "headers": [("tenant", "acme"), ("event_type", event_type)],
                    "payload": {
                        "id": partition * 100 + sequence + 1,
                        "name": f"customer-{partition}-{sequence}",
                    },
                }
            )
    return records


def _expected_customer_rows(
    topic: str,
    source_records: list[dict[str, object]],
) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for record in source_records:
        payload = record["payload"]
        assert isinstance(payload, dict)
        event_type = str(dict(record["headers"])["event_type"])
        name = str(payload["name"]).upper()
        rows.append(
            (
                int(payload["id"]),
                name,
                "acme",
                event_type,
                topic,
                int(record["partition"]),
                int(record["sequence"]),
            )
        )
    return sorted(rows)


def _partition_offset_pairs(records: list[dict[str, object]]) -> list[tuple[int, int]]:
    return sorted(
        (
            int(record["metadata"]["partition"]),
            int(record["metadata"]["offset"]),
        )
        for record in records
    )


def _records_at_or_after_partition_offsets(
    source_records: list[dict[str, object]],
    offsets_by_partition: dict[int, int],
) -> list[dict[str, object]]:
    return [
        record
        for record in source_records
        if int(record["sequence"]) >= offsets_by_partition[int(record["partition"])]
    ]


def _records_for_partitions(
    source_records: list[dict[str, object]],
    partitions: set[int],
) -> list[dict[str, object]]:
    return [record for record in source_records if int(record["partition"]) in partitions]


async def _insert_customer_rows(
    conn: object,
    table: str,
    rows: list[tuple[object, ...]],
) -> None:
    async with conn.cursor() as cur:
        await cur.executemany(
            f"""
            INSERT INTO "{table}" (
                event_id,
                display_name,
                tenant,
                event_type,
                kafka_topic,
                kafka_partition,
                kafka_offset
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


async def _write_customer_record_to_postgres(
    source: KafkaSource[dict[str, object]],
    sink: PostgresSink[dict[str, object]],
    record: dict[str, object],
) -> None:
    runtime = KafkaTransformSinkRuntime(source, sink, transform=_customer_transform)
    await runtime.deliver(record)


async def _drain_source_to_postgres(
    source: KafkaSource[dict[str, object]],
    sink: PostgresSink[dict[str, object]],
    *,
    max_records: int | None = None,
) -> list[dict[str, object]]:
    runtime = KafkaTransformSinkRuntime(source, sink, transform=_customer_transform)
    return await runtime.drain(max_records=max_records)


async def _write_customer_record_to_postgres_with_delivery_key(
    source: KafkaSource[dict[str, object]],
    sink: PostgresSink[dict[str, object]],
    record: dict[str, object],
) -> None:
    runtime = KafkaPostgresRuntime(source, sink, transform=_customer_transform)
    await runtime.deliver(record)


async def _drain_source_to_postgres_with_delivery_key(
    source: KafkaSource[dict[str, object]],
    sink: PostgresSink[dict[str, object]],
    *,
    max_records: int | None = None,
) -> list[dict[str, object]]:
    runtime = KafkaPostgresRuntime(source, sink, transform=_customer_transform)
    return await runtime.drain(max_records=max_records)


def _delivery_key_customer_row(
    runtime: KafkaPostgresRuntime[dict[str, object]],
    record: dict[str, object],
) -> dict[str, object]:
    row = _customer_transform(record)
    delivery_key = runtime.source_runtime.delivery_key()
    delivery_context = runtime.source_runtime.delivery_context()
    assert delivery_key is not None
    assert delivery_context is not None
    row["kafka_delivery_key"] = delivery_key
    row["kafka_metadata"] = delivery_context
    return row


def _record_delivery_key(
    topic: str,
    record: dict[str, object],
) -> str:
    metadata = cast("dict[str, object]", record["metadata"])
    return f"{topic}:{int(metadata['partition'])}:{int(metadata['offset'])}"


async def _fetch_customer_rows(conn, table: str) -> list[tuple[object, ...]]:
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT
                event_id,
                display_name,
                tenant,
                event_type,
                kafka_topic,
                kafka_partition,
                kafka_offset
            FROM "{table}"
            ORDER BY event_id
            """
        )
        return await cur.fetchall()


async def _fetch_delivery_key_rows(conn, table: str) -> list[tuple[object, ...]]:
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT
                kafka_delivery_key,
                event_id,
                display_name,
                kafka_topic,
                kafka_partition,
                kafka_offset
            FROM "{table}"
            ORDER BY kafka_delivery_key
            """
        )
        return await cur.fetchall()


def _expected_delivery_key_rows(
    topic: str,
    source_records: list[dict[str, object]],
) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for record in source_records:
        payload = cast("dict[str, object]", record["payload"])
        rows.append(
            (
                f"{topic}:{int(record['partition'])}:{int(record['sequence'])}",
                int(payload["id"]),
                str(payload["name"]).upper(),
                topic,
                int(record["partition"]),
                int(record["sequence"]),
            )
        )
    return sorted(rows)


@pytest.mark.asyncio
async def test_kafka_transform_postgres_wedge_persists_transformed_rows(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-{unique_suffix}"
    table = f"agora_kafka_pg_{unique_suffix}"
    source_records = [
        {
            "key": "customer-1",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 1, "name": "alpha"},
        },
        {
            "key": "customer-2",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 2, "name": "bravo"},
        },
    ]

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )

        await asyncio.sleep(1.0)

        consumer_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(
                    MapMiddleware(
                        _customer_transform,
                        name="kafka_to_postgres_row",
                    )
                )
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    )
                )
                .run(max_records=2)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        rows = await _fetch_customer_rows(conn, table)
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert consumer_summary.records_consumed == 2
    assert consumer_summary.records_written == 2
    assert rows == [
        (1, "ALPHA", "acme", "customer.created", topic, 0, 0),
        (2, "BRAVO", "acme", "customer.updated", topic, 0, 1),
    ]


@pytest.mark.asyncio
async def test_kafka_transform_postgres_wedge_routes_transform_errors_to_postgres_dlq(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-dlq-{unique_suffix}"
    table = f"agora_kafka_pg_dlq_{unique_suffix}"
    dlq_table = f"agora_kafka_pg_dlq_store_{unique_suffix}"
    source_records = [
        {
            "key": "customer-1",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 1, "name": "alpha"},
        },
        {
            "key": "customer-2",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 2, "name": "reject-me"},
        },
    ]

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-dlq-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    ),
                    config=DeliveryConfig(
                        dlq=PostgresDLQSink(dsn=postgres_dsn, table=dlq_table),
                    ),
                )
                .run(max_records=2)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        rows = await _fetch_customer_rows(conn, table)

        dlq_records: list[object] = []

        class _CollectDLQSink:
            sink_name = "collect_dlq"

            async def open(self) -> None:
                return None

            async def write(self, record: object) -> None:
                dlq_records.append(record)

            async def flush(self) -> None:
                return None

            async def close(self) -> None:
                return None

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink())  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    assert summary.records_consumed == 2
    assert summary.records_written == 1
    assert summary.records_errored == 1
    assert rows == [
        (1, "ALPHA", "acme", "customer.created", topic, 0, 0),
    ]
    assert len(dlq_records) == 1
    dlq_record = dlq_records[0]
    assert dlq_record.stage == "middleware"
    assert dlq_record.error_type == "ValueError"
    assert dlq_record.error_message == "unsupported customer name"
    assert dlq_record.middleware == "kafka_to_postgres_row"
    assert dlq_record.record["payload"] == {"id": 2, "name": "reject-me"}
    assert dlq_record.checkpoint["offset"] == 1


@pytest.mark.asyncio
async def test_kafka_transform_postgres_wedge_routes_schema_evolution_failures_to_poison_dlq(
    kafka_secure_schema_registry_config,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("fastavro")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-schema-evolution-{unique_suffix}"
    table = f"agora_kafka_pg_schema_evolution_{unique_suffix}"
    dlq_table = f"agora_kafka_pg_schema_evolution_dlq_{unique_suffix}"
    subject = f"{topic}-value"

    schema_v1 = {
        "type": "record",
        "name": "CustomerEvent",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
        ],
    }
    schema_v2 = {
        "type": "record",
        "name": "CustomerEvent",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": ["null", "string"], "default": None},
        ],
    }
    registry_client = kafka_secure_schema_registry_config.schema_registry_client()
    security = kafka_secure_schema_registry_config.security()
    assert security is not None

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_secure_schema_registry_config.bootstrap_servers,
                topic,
                security=security,
            ),
            timeout=10.0,
        )

        good_serializer = AvroSchemaRegistrySerializer[dict[str, object]](
            registry_client=registry_client,
            subject=subject,
            schema=schema_v1,
        )
        bad_serializer = AvroSchemaRegistrySerializer[dict[str, object]](
            registry_client=registry_client,
            subject=subject,
            schema=schema_v2,
        )
        good_payload_serializer = _SchemaRegistryPayloadSerializer(good_serializer)
        bad_payload_serializer = _SchemaRegistryPayloadSerializer(bad_serializer)

        good_records = [
            {
                "key": "customer-1",
                "headers": [("tenant", "acme"), ("event_type", "customer.created")],
                "payload": {"id": 1, "name": "alpha"},
            },
            {
                "key": "customer-2",
                "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
                "payload": {"id": 2, "name": "bravo"},
            },
        ]
        bad_record = {
            "key": "customer-3",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 3, "name": None},
        }

        good_summary = await asyncio.wait_for(
            (
                Pipeline(IterableSource(good_records))
                .build(
                    KafkaSink(
                        topic=topic,
                        bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                        serializer=good_payload_serializer,
                        key_fn=lambda record: record["key"].encode("utf-8"),
                        headers_fn=lambda record: [
                            (name, value.encode("utf-8")) for name, value in record["headers"]
                        ],
                        security=security,
                        security_protocol="SASL_SSL",
                    )
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        bad_summary = await asyncio.wait_for(
            (
                Pipeline(IterableSource([bad_record]))
                .build(
                    KafkaSink(
                        topic=topic,
                        bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                        serializer=bad_payload_serializer,
                        key_fn=lambda record: record["key"].encode("utf-8"),
                        headers_fn=lambda record: [
                            (name, value.encode("utf-8")) for name, value in record["headers"]
                        ],
                        security=security,
                        security_protocol="SASL_SSL",
                    )
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        assert good_summary.records_written == 2
        assert bad_summary.records_written == 1

        await asyncio.sleep(1.0)

        deserializer = _SchemaRegistryEnvelopeDeserializer(
            AvroSchemaRegistryDeserializer[dict[str, object]](
                registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
                reader_schema=schema_v1,
            )
        )
        summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                        group_id=f"agora-wedge-schema-evolution-{unique_suffix}",
                        deserializer=deserializer,
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                        max_idle_polls=2,
                        security=security,
                        security_protocol="SASL_SSL",
                        poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
                        poison_record_sink=PostgresDLQSink(dsn=postgres_dsn, table=dlq_table),
                        poison_record_pipeline_id="agora-kafka-schema-evolution",
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    )
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        rows = await _fetch_customer_rows(conn, table)
        dlq_records: list[object] = []

        class _CollectDLQSink:
            sink_name = "collect_dlq"

            async def open(self) -> None:
                return None

            async def write(self, record: object) -> None:
                dlq_records.append(record)

            async def flush(self) -> None:
                return None

            async def close(self) -> None:
                return None

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink())  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    assert summary.records_consumed == 2
    assert summary.records_written == 2
    assert rows == [
        (1, "ALPHA", "acme", "customer.created", topic, 0, 0),
        (2, "BRAVO", "acme", "customer.updated", topic, 0, 1),
    ]
    assert len(dlq_records) == 1
    dlq_record = dlq_records[0]
    assert dlq_record.pipeline_id == "agora-kafka-schema-evolution"
    assert dlq_record.stage == "kafka_deserialize"
    assert "schema" in dlq_record.error_type.lower() or "value" in dlq_record.error_type.lower()
    assert (
        "schema" in dlq_record.error_message.lower()
        or "string" in dlq_record.error_message.lower()
        or "null" in dlq_record.error_message.lower()
    )
    assert dlq_record.record["topic"] == topic
    assert dlq_record.record["offset"] == 2
    assert dlq_record.record["value"]["encoding"] in {"base64", "utf-8"}
    assert dlq_record.record["metadata"]["topic"] == topic


@pytest.mark.asyncio
async def test_kafka_transform_postgres_wedge_resume_and_replay_stay_idempotent(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-replay-{unique_suffix}"
    table = f"agora_kafka_pg_replay_{unique_suffix}"
    store = InMemoryCheckpointStore()
    source_records = [
        {
            "key": "customer-1",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 1, "name": "alpha"},
        },
        {
            "key": "customer-2",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 2, "name": "bravo"},
        },
        {
            "key": "customer-3",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 3, "name": "charlie"},
        },
    ]

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        first_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-resume-a-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    ),
                    config=DeliveryConfig(checkpoint=store),
                )
                .run(max_records=1)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        second_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-resume-b-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    ),
                    config=DeliveryConfig(checkpoint=store),
                )
                .run(max_records=2)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        replay_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-replay-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    )
                )
                .run(max_records=3)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        rows = await _fetch_customer_rows(conn, table)
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert first_summary.records_consumed == 1
    assert first_summary.records_written == 1
    assert first_summary.last_checkpoint is not None
    assert first_summary.last_checkpoint.value["offset"] == 0

    assert second_summary.records_consumed == 2
    assert second_summary.records_written == 2
    assert second_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint.value["offset"] == 2

    assert replay_summary.records_consumed == 3
    assert replay_summary.records_written == 3
    assert rows == [
        (1, "ALPHA", "acme", "customer.created", topic, 0, 0),
        (2, "BRAVO", "acme", "customer.updated", topic, 0, 1),
        (3, "CHARLIE", "acme", "customer.created", topic, 0, 2),
    ]


@pytest.mark.asyncio
async def test_kafka_transform_postgres_runtime_delivery_key_keeps_replay_idempotent(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-delivery-key-{unique_suffix}"
    table = f"agora_kafka_pg_delivery_key_{unique_suffix}"
    source_records = [
        {
            "key": "customer-1",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 1, "name": "alpha"},
        },
        {
            "key": "customer-2",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 2, "name": "bravo"},
        },
    ]

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        first_runtime = build_kafka_postgres_runtime(
            source=KafkaSource(
                topics=[topic],
                bootstrap_servers=kafka_bootstrap,
                group_id=f"agora-wedge-delivery-key-a-{unique_suffix}",
                deserializer=lambda value, metadata: {
                    "payload": json.loads(value.decode("utf-8")),
                    "metadata": metadata,
                },
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                commit_every=1,
            ),
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            batch_size=2,
        )
        await first_runtime.open()
        try:
            first_records = await first_runtime.drain(max_records=1)
        finally:
            await first_runtime.close()

        replay_runtime = build_kafka_postgres_runtime(
            source=KafkaSource(
                topics=[topic],
                bootstrap_servers=kafka_bootstrap,
                group_id=f"agora-wedge-delivery-key-b-{unique_suffix}",
                deserializer=lambda value, metadata: {
                    "payload": json.loads(value.decode("utf-8")),
                    "metadata": metadata,
                },
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                commit_every=1,
            ),
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            batch_size=2,
        )
        await replay_runtime.open()
        try:
            replay_records = await replay_runtime.drain(max_records=2)
        finally:
            await replay_runtime.close()

        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert len(first_records) == 1
    assert len(replay_records) == 2
    assert rows == _expected_delivery_key_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_crash_windows_recover_idempotently(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-delivery-crash-{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=1, records_per_partition=3)
    tables = {
        "before_write": f"agora_kafka_pg_crash_before_write_{unique_suffix}",
        "after_write_before_ack": f"agora_kafka_pg_crash_after_write_{unique_suffix}",
        "after_ack": f"agora_kafka_pg_crash_after_ack_{unique_suffix}",
    }

    def _build_runtime(group_id: str, table: str) -> KafkaPostgresRuntime[dict[str, object]]:
        return build_kafka_postgres_runtime(
            source=build_kafka_postgres_source(
                topics=[topic],
                bootstrap_servers=kafka_bootstrap,
                group_id=group_id,
                deserializer=lambda value: json.loads(value.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                commit_every=1,
            ),
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=2,
        )

    async def _create_table(conn: object, table: str) -> None:
        async with conn.cursor() as cur:  # type: ignore[attr-defined]
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

    async def _read_one_record_stream(
        runtime: KafkaPostgresRuntime[dict[str, object]],
    ) -> tuple[object, dict[str, object]]:
        stream = runtime.source.stream()
        return stream, await anext(stream)

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        for table in tables.values():
            await _create_table(conn, table)

        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        before_write_runtime = _build_runtime(
            f"agora-wedge-crash-before-write-{unique_suffix}",
            tables["before_write"],
        )
        await before_write_runtime.open()
        before_write_stream = None
        try:
            before_write_stream, first_before_write = await _read_one_record_stream(
                before_write_runtime
            )
        finally:
            if before_write_stream is not None:
                with contextlib.suppress(Exception):
                    await before_write_stream.aclose()
            await before_write_runtime.close()

        before_write_recovery_runtime = _build_runtime(
            f"agora-wedge-crash-before-write-{unique_suffix}",
            tables["before_write"],
        )
        await before_write_recovery_runtime.open()
        try:
            before_write_replayed = await before_write_recovery_runtime.drain(
                max_records=len(source_records)
            )
        finally:
            await before_write_recovery_runtime.close()

        after_write_runtime = _build_runtime(
            f"agora-wedge-crash-after-write-{unique_suffix}",
            tables["after_write_before_ack"],
        )
        await after_write_runtime.open()
        after_write_stream = None
        try:
            after_write_stream, first_after_write = await _read_one_record_stream(
                after_write_runtime
            )
            await after_write_runtime.sink.write(
                _delivery_key_customer_row(after_write_runtime, first_after_write)
            )
            await after_write_runtime.sink.flush()
        finally:
            if after_write_stream is not None:
                with contextlib.suppress(Exception):
                    await after_write_stream.aclose()
            await after_write_runtime.close()

        after_write_recovery_runtime = _build_runtime(
            f"agora-wedge-crash-after-write-{unique_suffix}",
            tables["after_write_before_ack"],
        )
        await after_write_recovery_runtime.open()
        try:
            after_write_replayed = await after_write_recovery_runtime.drain(
                max_records=len(source_records)
            )
        finally:
            await after_write_recovery_runtime.close()

        after_ack_runtime = _build_runtime(
            f"agora-wedge-crash-after-ack-{unique_suffix}",
            tables["after_ack"],
        )
        await after_ack_runtime.open()
        after_ack_stream = None
        try:
            after_ack_stream, first_after_ack = await _read_one_record_stream(after_ack_runtime)
            await after_ack_runtime.deliver(first_after_ack)
        finally:
            if after_ack_stream is not None:
                with contextlib.suppress(Exception):
                    await after_ack_stream.aclose()
            await after_ack_runtime.close()

        after_ack_recovery_runtime = _build_runtime(
            f"agora-wedge-crash-after-ack-{unique_suffix}",
            tables["after_ack"],
        )
        await after_ack_recovery_runtime.open()
        try:
            after_ack_replayed = await after_ack_recovery_runtime.drain(
                max_records=len(source_records) - 1
            )
        finally:
            await after_ack_recovery_runtime.close()

        before_write_rows = await _fetch_delivery_key_rows(conn, tables["before_write"])
        after_write_rows = await _fetch_delivery_key_rows(conn, tables["after_write_before_ack"])
        after_ack_rows = await _fetch_delivery_key_rows(conn, tables["after_ack"])
    finally:
        async with conn.cursor() as cur:
            for table in tables.values():
                await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert cast("dict[str, object]", first_before_write["metadata"])["offset"] == 0
    assert len(before_write_replayed) == len(source_records)
    assert before_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert cast("dict[str, object]", first_after_write["metadata"])["offset"] == 0
    assert len(after_write_replayed) == len(source_records)
    assert after_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert cast("dict[str, object]", first_after_ack["metadata"])["offset"] == 0
    assert len(after_ack_replayed) == len(source_records) - 1
    assert [
        cast("dict[str, object]", record["metadata"])["offset"] for record in after_ack_replayed
    ] == [1, 2]
    assert after_ack_rows == _expected_delivery_key_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_observability_surface_reports_live_wedge_state(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-observability-{unique_suffix}"
    table = f"agora_kafka_pg_observability_{unique_suffix}"
    source_records = [
        {
            "key": "customer-1",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 1, "name": "alpha"},
        },
        {
            "key": "customer-2",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 2, "name": "bravo"},
        },
    ]

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        runtime = build_kafka_postgres_runtime(
            source=build_kafka_postgres_source(
                topics=[topic],
                bootstrap_servers=kafka_bootstrap,
                group_id=f"agora-wedge-observability-{unique_suffix}",
                deserializer=lambda value: json.loads(value.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                commit_every=1,
                max_idle_polls=2,
            ),
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=2,
        )
        await runtime.open()
        try:
            records = await runtime.drain(max_records=2)
            snapshot, report = await assert_runtime_readiness(
                runtime,
                _RUNTIME_READINESS_THRESHOLDS,
            )
            rendered = await runtime.render_prometheus_metrics(namespace="agora_kafka_postgres")
        finally:
            await runtime.close()

        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert len(records) == 2
    assert rows == [
        (f"{topic}:0:0", 1, "ALPHA", topic, 0, 0),
        (f"{topic}:0:1", 2, "BRAVO", topic, 0, 1),
    ]
    assert snapshot.delivery_key_field == "kafka_delivery_key"
    assert snapshot.delivery_metadata_field == "kafka_metadata"
    assert snapshot.poison_dlq_enabled is False
    assert snapshot.sink.flush_count == 2
    assert snapshot.sink.flushed_row_count == 2
    assert snapshot.sink.buffered_row_count == 0
    assert snapshot.health.ready is True
    assert snapshot.health.source_ready is True
    assert snapshot.health.source_stalled is False
    assert snapshot.health.sink_connection_ready is True
    assert report.passed is True
    assert snapshot.source.health.consumer_group == f"agora-wedge-observability-{unique_suffix}"
    assert snapshot.source.runtime.record_error_count == 0
    assert snapshot.source.operational.poison_record_dlq_write_count == 0
    assert 'agora_kafka_postgres_runtime_state{consumer_group="' in rendered
    assert 'agora_kafka_postgres_runtime_config{consumer_group="' in rendered
    assert (
        f'agora_kafka_postgres_sink_events_total{{consumer_group="agora-wedge-observability-{unique_suffix}",'
        f'bootstrap_servers="{kafka_bootstrap}",table="{table}",insert_mode="sql",'
        'sink_write_safety_policy="strict",delivery_key_field="kafka_delivery_key",'
        'delivery_metadata_field="kafka_metadata",'
        'event="flush"} 2'
    ) in rendered


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_survives_multi_cycle_rebalance_broker_flap(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
    kafka_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-delivery-key-soak-{unique_suffix}"
    table = f"agora_kafka_pg_delivery_key_soak_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=4)
    cycle_targets: list[int | None] = [2, None]

    def build_source(
        listener: _RebalanceListener,
        *,
        commit_every: int,
    ) -> KafkaSource[dict[str, object]]:
        return KafkaSource(
            topics=[topic],
            bootstrap_servers=kafka_bootstrap,
            group_id=f"agora-wedge-delivery-key-soak-{unique_suffix}",
            deserializer=lambda value, metadata: {
                "payload": json.loads(value.decode("utf-8")),
                "metadata": metadata,
            },
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            commit_every=commit_every,
            rebalance_listener=listener,
            max_idle_polls=2,
        )

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    open_sources: list[KafkaSource[dict[str, object]]] = []
    listeners: list[_RebalanceListener] = []
    source_streams: dict[KafkaSource[dict[str, object]], object] = {}
    sink: PostgresSink[dict[str, object]] | None = None

    def register_source(
        listener: _RebalanceListener,
        *,
        commit_every: int,
    ) -> KafkaSource[dict[str, object]]:
        source = build_source(listener, commit_every=commit_every)
        listeners.append(listener)
        return source

    current_listener = _RebalanceListener()
    current_source = register_source(current_listener, commit_every=100)
    initial_records: list[dict[str, object]] = []
    cycle_records: list[dict[str, object]] = []
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        current_runtime = build_kafka_postgres_runtime(
            source=current_source,
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            batch_size=1,
        )
        sink = current_runtime.sink
        await sink.open()
        await current_source.open()
        open_sources.append(current_source)
        current_stream = current_source.stream()
        source_streams[current_source] = current_stream

        initial_partitions: set[int] = set()
        while len(initial_partitions) < 2:
            record = await asyncio.wait_for(
                anext(current_stream),
                timeout=_SECURE_SOAK_INITIAL_RECORD_TIMEOUT_S,
            )
            initial_records.append(record)
            initial_partitions.add(int(record["metadata"]["partition"]))
            await current_runtime.deliver(record)

        for max_records in cycle_targets:
            next_listener = _RebalanceListener()
            next_source = register_source(next_listener, commit_every=1)
            next_runtime = build_kafka_postgres_runtime(
                source=next_source,
                dsn=postgres_dsn,
                table=table,
                transform=_customer_transform,
                batch_size=1,
            )
            current_event_count = len(current_listener.events)
            await next_source.open()
            open_sources.append(next_source)
            await _wait_for_rebalance_event_counts(
                (current_listener, current_event_count + 1),
                (next_listener, 1),
                timeout_s=_SECURE_SOAK_REBALANCE_TIMEOUT_S,
            )
            next_listener_event_count = len(next_listener.events)
            await asyncio.to_thread(kafka_broker_flap_control)

            previous_stream = source_streams.pop(current_source, None)
            if previous_stream is not None:
                with contextlib.suppress(Exception):
                    await previous_stream.aclose()
            await current_source.close()
            open_sources.remove(current_source)
            await _wait_for_rebalance_event_counts(
                (next_listener, next_listener_event_count + 1),
                timeout_s=_SECURE_SOAK_REBALANCE_TIMEOUT_S,
            )

            drained_records = await asyncio.wait_for(
                next_runtime.drain(max_records=max_records),
                timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S,
            )
            cycle_records.extend(drained_records)
            current_source = next_source
            current_listener = next_listener
            current_runtime = next_runtime

            if max_records is not None:
                source_streams[current_source] = current_source.stream()
                with contextlib.suppress(Exception):
                    await source_streams[current_source].aclose()
                source_streams.pop(current_source, None)

        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        for stream in list(source_streams.values()):
            with contextlib.suppress(Exception):
                await stream.aclose()
        for source in reversed(open_sources):
            with contextlib.suppress(Exception):
                await source.close()
        if sink is not None:
            with contextlib.suppress(Exception):
                await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    delivered_pairs = _partition_offset_pairs(initial_records) + _partition_offset_pairs(
        cycle_records
    )
    delivered_pair_set = set(delivered_pairs)
    duplicate_count = len(delivered_pairs) - len(delivered_pair_set)

    assert len({int(record["metadata"]["partition"]) for record in initial_records}) == 2
    assert sorted(delivered_pair_set) == sorted(
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    )
    assert duplicate_count <= len(cycle_targets) * 2
    assert rows == _expected_delivery_key_rows(topic, source_records)
    assert (
        sum(1 for listener in listeners for event in listener.events if event[0] == "assigned")
        >= len(cycle_targets) + 1
    )
    assert sum(
        1 for listener in listeners for event in listener.events if event[0] == "revoked"
    ) >= len(cycle_targets)


@pytest.mark.asyncio
async def test_kafka_transform_postgres_wedge_checkpoint_resume_survives_broker_flap(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
    kafka_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-broker-flap-{unique_suffix}"
    table = f"agora_kafka_pg_broker_flap_{unique_suffix}"
    checkpoint_key = f"agora-wedge-broker-flap-{unique_suffix}"
    store = InMemoryCheckpointStore()
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=4)

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        first_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        assignments=[(topic, 0), (topic, 1)],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-broker-flap-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    ),
                    config=DeliveryConfig(
                        checkpoint=store,
                        checkpoint_key=checkpoint_key,
                    ),
                )
                .run(max_records=3)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        await asyncio.to_thread(kafka_broker_flap_control)
        await asyncio.sleep(3.0)

        second_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        assignments=[(topic, 0), (topic, 1)],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-broker-flap-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    ),
                    config=DeliveryConfig(
                        checkpoint=store,
                        checkpoint_key=checkpoint_key,
                    ),
                )
                .run(max_records=len(source_records) - 3)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        rows = await _fetch_customer_rows(conn, table)
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert first_summary.records_consumed == 3
    assert second_summary.records_consumed == len(source_records) - 3
    assert first_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint is not None
    assert rows == _expected_customer_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_multi_partition_transform_postgres_resumes_from_mixed_checkpoint_offsets(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-replay-{unique_suffix}"
    table = f"agora_kafka_pg_multi_replay_{unique_suffix}"
    checkpoint_key = f"agora-wedge-multi-replay-{unique_suffix}"
    store = InMemoryCheckpointStore()
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=4)
    checkpointed_records = [
        record
        for record in source_records
        if (int(record["partition"]), int(record["sequence"])) in {(0, 0), (0, 1), (1, 0)}
    ]

    await store.save(
        checkpoint_key,
        Checkpoint(
            pipeline_id="agora-wedge-multi-replay",
            run_id=unique_suffix,
            source="kafka",
            value={
                "topic": topic,
                "partition": 1,
                "offset": 0,
                "offsets": [
                    {"topic": topic, "partition": 0, "offset": 1},
                    {"topic": topic, "partition": 1, "offset": 0},
                ],
            },
        ),
    )

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await _insert_customer_rows(
            conn, table, _expected_customer_rows(topic, checkpointed_records)
        )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-multi-replay-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    ),
                    config=DeliveryConfig(
                        checkpoint=store,
                        checkpoint_key=checkpoint_key,
                    ),
                )
                .run(max_records=len(source_records) - len(checkpointed_records))
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        rows = await _fetch_customer_rows(conn, table)
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert summary.records_consumed == 5
    assert summary.records_written == 5
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value["offsets"] == [
        {"topic": topic, "partition": 0, "offset": 3},
        {"topic": topic, "partition": 1, "offset": 3},
    ]
    assert rows == _expected_customer_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_multi_partition_transform_postgres_handoff_survives_rebalance_partial_replay(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-rebalance-{unique_suffix}"
    table = f"agora_kafka_pg_multi_rebalance_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=3)
    listener_one = _RebalanceListener()
    listener_two = _RebalanceListener()

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="event_id",
        batch_size=1,
    )
    source_one = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-wedge-multi-rebalance-{unique_suffix}",
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
        group_id=f"agora-wedge-multi-rebalance-{unique_suffix}",
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
    source_two_open = False
    source_one_open = False
    sink_open = False
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        await sink.open()
        sink_open = True

        await source_one.open()
        source_one_open = True
        stream_one = source_one.stream()

        initial_records: list[dict[str, object]] = []
        initial_partitions: set[int] = set()
        while len(initial_partitions) < 2:
            record = await asyncio.wait_for(anext(stream_one), timeout=_INTEGRATION_TIMEOUT_S)
            initial_records.append(record)
            initial_partitions.add(int(record["metadata"]["partition"]))
            await _write_customer_record_to_postgres(source_one, sink, record)

        await source_two.open()
        source_two_open = True
        await asyncio.sleep(2.0)

        if stream_one is not None:
            await stream_one.aclose()
            stream_one = None
        await source_one.close()
        source_one_open = False
        await asyncio.sleep(2.0)

        handoff_records = await asyncio.wait_for(
            _drain_source_to_postgres(source_two, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        rows = await _fetch_customer_rows(conn, table)
    finally:
        if stream_one is not None:
            with contextlib.suppress(Exception):
                await stream_one.aclose()
        if source_two_open:
            await source_two.close()
        if source_one_open:
            await source_one.close()
        if sink_open:
            await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    expected_pairs = sorted(
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    )
    delivered_pairs = sorted(
        _partition_offset_pairs(initial_records) + _partition_offset_pairs(handoff_records)
    )
    assert len({int(record["metadata"]["partition"]) for record in initial_records}) == 2
    assert delivered_pairs == expected_pairs
    assert rows == _expected_customer_rows(topic, source_records)
    assert any(event[0] == "revoked" for event in listener_one.events)
    assert any(event[0] == "assigned" for event in listener_two.events)


@pytest.mark.asyncio
async def test_kafka_multi_partition_transform_postgres_handoff_survives_rebalance_and_broker_flap(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
    kafka_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-rebalance-flap-{unique_suffix}"
    table = f"agora_kafka_pg_multi_rebalance_flap_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=4)
    listener_one = _RebalanceListener()
    listener_two = _RebalanceListener()

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="event_id",
        batch_size=1,
    )
    source_one = KafkaSource(
        topics=[topic],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-wedge-multi-rebalance-flap-{unique_suffix}",
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
        group_id=f"agora-wedge-multi-rebalance-flap-{unique_suffix}",
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
    source_two_open = False
    source_one_open = False
    sink_open = False
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        await sink.open()
        sink_open = True

        await source_one.open()
        source_one_open = True
        stream_one = source_one.stream()

        initial_records: list[dict[str, object]] = []
        initial_partitions: set[int] = set()
        while len(initial_partitions) < 2:
            record = await asyncio.wait_for(anext(stream_one), timeout=_INTEGRATION_TIMEOUT_S)
            initial_records.append(record)
            initial_partitions.add(int(record["metadata"]["partition"]))
            await _write_customer_record_to_postgres(source_one, sink, record)

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
            _drain_source_to_postgres(source_two, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        rows = await _fetch_customer_rows(conn, table)
    finally:
        if stream_one is not None:
            with contextlib.suppress(Exception):
                await stream_one.aclose()
        if source_two_open:
            await source_two.close()
        if source_one_open:
            await source_one.close()
        if sink_open:
            await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    expected_pairs = sorted(
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    )
    delivered_pairs = sorted(
        _partition_offset_pairs(initial_records) + _partition_offset_pairs(handoff_records)
    )
    assert len({int(record["metadata"]["partition"]) for record in initial_records}) == 2
    assert delivered_pairs == expected_pairs
    assert rows == _expected_customer_rows(topic, source_records)
    assert any(event[0] == "revoked" for event in listener_one.events)
    assert any(event[0] == "assigned" for event in listener_two.events)


@pytest.mark.asyncio
async def test_kafka_multi_partition_transform_postgres_manual_assignments_honor_exact_start_offsets(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-assign-{unique_suffix}"
    table = f"agora_kafka_pg_multi_assign_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=4)
    offsets_by_partition = {0: 1, 1: 2}
    expected_records = _records_at_or_after_partition_offsets(source_records, offsets_by_partition)

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        assignments=[(topic, 0), (topic, 1)],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-wedge-multi-assign-{unique_suffix}",
                        deserializer=lambda value, metadata: {
                            "payload": json.loads(value.decode("utf-8")),
                            "metadata": metadata,
                        },
                        start_offsets={
                            (topic, partition): offset
                            for partition, offset in offsets_by_partition.items()
                        },
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=1,
                        max_idle_polls=2,
                    )
                )
                .pipe(MapMiddleware(_customer_transform, name="kafka_to_postgres_row"))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="event_id",
                        batch_size=2,
                    )
                )
                .run(max_records=len(expected_records))
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        rows = await _fetch_customer_rows(conn, table)
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert summary.records_consumed == len(expected_records)
    assert summary.records_written == len(expected_records)
    assert _partition_offset_pairs(
        [
            {
                "metadata": {
                    "partition": int(record["partition"]),
                    "offset": int(record["sequence"]),
                }
            }
            for record in expected_records
        ]
    ) == [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3)]
    assert rows == _expected_customer_rows(topic, expected_records)


@pytest.mark.asyncio
async def test_kafka_multi_partition_transform_postgres_seek_to_offsets_controls_replay_window(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-seek-{unique_suffix}"
    table = f"agora_kafka_pg_multi_seek_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=4)
    offsets_by_partition = {0: 2, 1: 1}
    expected_records = _records_at_or_after_partition_offsets(source_records, offsets_by_partition)

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="event_id",
        batch_size=1,
    )
    source = KafkaSource(
        assignments=[(topic, 0), (topic, 1)],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-wedge-multi-seek-{unique_suffix}",
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        max_idle_polls=2,
    )

    source_open = False
    sink_open = False
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        await sink.open()
        sink_open = True
        await source.open()
        source_open = True

        await source.seek_to_offsets(
            {(topic, partition): offset for partition, offset in offsets_by_partition.items()}
        )
        consumed_records = await asyncio.wait_for(
            _drain_source_to_postgres(source, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        rows = await _fetch_customer_rows(conn, table)
    finally:
        if source_open:
            await source.close()
        if sink_open:
            await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert _partition_offset_pairs(consumed_records) == [
        (0, 2),
        (0, 3),
        (1, 1),
        (1, 2),
        (1, 3),
    ]
    assert rows == _expected_customer_rows(topic, expected_records)


@pytest.mark.asyncio
async def test_kafka_multi_partition_transform_postgres_seek_helpers_work_with_pause_resume(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-seek-controls-{unique_suffix}"
    table = f"agora_kafka_pg_multi_seek_controls_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=3)
    partition_zero_records = _records_for_partitions(source_records, {0})
    partition_one_records = _records_for_partitions(source_records, {1})

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="event_id",
        batch_size=1,
    )
    source = KafkaSource(
        assignments=[(topic, 0), (topic, 1)],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-wedge-multi-seek-controls-{unique_suffix}",
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        max_idle_polls=2,
    )

    source_open = False
    sink_open = False
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(1.0)

        await sink.open()
        sink_open = True
        await source.open()
        source_open = True

        await source.seek_to_end()
        no_history_records = await asyncio.wait_for(
            _drain_source_to_postgres(source, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        await source.seek_to_beginning([(topic, 0), (topic, 1)])
        source.pause([(topic, 1)])
        first_pass_records = await asyncio.wait_for(
            _drain_source_to_postgres(source, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        source.resume([(topic, 1)])
        await source.seek_to_end([(topic, 0)])
        await source.seek_to_beginning([(topic, 1)])
        second_pass_records = await asyncio.wait_for(
            _drain_source_to_postgres(source, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        rows = await _fetch_customer_rows(conn, table)
    finally:
        if source_open:
            await source.close()
        if sink_open:
            await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert no_history_records == []
    assert _partition_offset_pairs(first_pass_records) == [(0, 0), (0, 1), (0, 2)]
    assert _partition_offset_pairs(second_pass_records) == [(1, 0), (1, 1), (1, 2)]
    assert rows == _expected_customer_rows(topic, partition_zero_records + partition_one_records)


@pytest.mark.asyncio
async def test_kafka_multi_partition_transform_postgres_live_tail_after_seek_to_end_supports_replay_window(
    kafka_bootstrap: str,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-live-tail-{unique_suffix}"
    table = f"agora_kafka_pg_multi_live_tail_{unique_suffix}"
    backlog_records = _partitioned_customer_records(partitions=2, records_per_partition=3)
    live_records = [
        record
        for record in _partitioned_customer_records(partitions=2, records_per_partition=5)
        if int(record["sequence"]) >= 3
    ]

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="event_id",
        batch_size=1,
    )
    source = KafkaSource(
        assignments=[(topic, 0), (topic, 1)],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-wedge-multi-live-tail-{unique_suffix}",
        deserializer=lambda value, metadata: {
            "payload": json.loads(value.decode("utf-8")),
            "metadata": metadata,
        },
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        max_idle_polls=2,
    )

    source_open = False
    sink_open = False
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=backlog_records,
        )
        await asyncio.sleep(1.0)

        await sink.open()
        sink_open = True
        await source.open()
        source_open = True

        await source.seek_to_end()
        skipped_backlog_records = await asyncio.wait_for(
            _drain_source_to_postgres(source, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=live_records,
        )
        await asyncio.sleep(1.0)

        live_tail_records = await asyncio.wait_for(
            _drain_source_to_postgres(source, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        await source.seek_to_offsets(
            {
                (topic, 0): 4,
                (topic, 1): 4,
            }
        )
        replay_window_records = await asyncio.wait_for(
            _drain_source_to_postgres(source, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        rows = await _fetch_customer_rows(conn, table)
    finally:
        if source_open:
            await source.close()
        if sink_open:
            await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert skipped_backlog_records == []
    assert _partition_offset_pairs(live_tail_records) == [
        (0, 3),
        (0, 4),
        (1, 3),
        (1, 4),
    ]
    assert _partition_offset_pairs(replay_window_records) == [
        (0, 4),
        (1, 4),
    ]
    assert rows == _expected_customer_rows(topic, live_records)


@pytest.mark.asyncio
async def test_kafka_secure_schema_registry_wedge_handoff_survives_rebalance_broker_flap_and_routes_poison_dlq(
    kafka_secure_schema_registry_config,
    postgres_dsn: str,
    unique_suffix: str,
    kafka_secure_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("fastavro")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-secure-wedge-rebalance-dlq-{unique_suffix}"
    table = f"agora_secure_wedge_rebalance_dlq_{unique_suffix}"
    dlq_table = f"agora_secure_wedge_rebalance_dlq_store_{unique_suffix}"
    subject = f"{topic}-value"
    listener_one = _RebalanceListener()
    listener_two = _RebalanceListener()

    schema_v1 = {
        "type": "record",
        "name": "CustomerEvent",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
        ],
    }
    schema_v2 = {
        "type": "record",
        "name": "CustomerEvent",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": ["null", "string"], "default": None},
        ],
    }
    source_records = [
        {
            "partition": 0,
            "sequence": 0,
            "schema_version": "v1",
            "key": "customer-0-0",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 1, "name": "alpha"},
        },
        {
            "partition": 1,
            "sequence": 0,
            "schema_version": "v1",
            "key": "customer-1-0",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 101, "name": "bravo"},
        },
        {
            "partition": 0,
            "sequence": 1,
            "schema_version": "v2",
            "key": "customer-0-1",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 2, "name": None},
        },
        {
            "partition": 1,
            "sequence": 1,
            "schema_version": "v1",
            "key": "customer-1-1",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 102, "name": "charlie"},
        },
        {
            "partition": 0,
            "sequence": 2,
            "schema_version": "v1",
            "key": "customer-0-2",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 3, "name": "delta"},
        },
        {
            "partition": 1,
            "sequence": 2,
            "schema_version": "v2",
            "key": "customer-1-2",
            "headers": [("tenant", "acme"), ("event_type", "customer.updated")],
            "payload": {"id": 103, "name": None},
        },
        {
            "partition": 1,
            "sequence": 3,
            "schema_version": "v1",
            "key": "customer-1-3",
            "headers": [("tenant", "acme"), ("event_type", "customer.created")],
            "payload": {"id": 104, "name": "echo"},
        },
    ]
    good_source_records = [
        record
        for record in source_records
        if cast("dict[str, object]", record["payload"])["name"] is not None
    ]
    expected_good_pairs = sorted(
        (int(record["partition"]), int(record["sequence"])) for record in good_source_records
    )
    expected_bad_pairs = sorted(
        (int(record["partition"]), int(record["sequence"]))
        for record in source_records
        if cast("dict[str, object]", record["payload"])["name"] is None
    )

    registry_client = kafka_secure_schema_registry_config.schema_registry_client()
    security = kafka_secure_schema_registry_config.security()
    assert security is not None

    serializer = _VersionedSchemaRegistryPayloadSerializer(
        {
            "v1": AvroSchemaRegistrySerializer[dict[str, object]](
                registry_client=registry_client,
                subject=subject,
                schema=schema_v1,
            ),
            "v2": AvroSchemaRegistrySerializer[dict[str, object]](
                registry_client=registry_client,
                subject=subject,
                schema=schema_v2,
            ),
        }
    )
    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="event_id",
        batch_size=1,
    )
    source_one = build_kafka_postgres_source(
        topics=[topic],
        bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
        group_id=f"agora-secure-wedge-rebalance-dlq-{unique_suffix}",
        deserializer=AvroSchemaRegistryDeserializer[dict[str, object]](
            registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
            reader_schema=schema_v1,
        ),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=100,
        rebalance_listener=listener_one,
        max_idle_polls=2,
        security=security,
        security_protocol="SASL_SSL",
        poison_dlq=KafkaPostgresPoisonDLQConfig(
            dsn=postgres_dsn,
            table=dlq_table,
            policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
            pipeline_id="agora-secure-wedge-rebalance-dlq",
        ),
    )
    source_two = build_kafka_postgres_source(
        topics=[topic],
        bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
        group_id=f"agora-secure-wedge-rebalance-dlq-{unique_suffix}",
        deserializer=AvroSchemaRegistryDeserializer[dict[str, object]](
            registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
            reader_schema=schema_v1,
        ),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        rebalance_listener=listener_two,
        max_idle_polls=2,
        security=security,
        security_protocol="SASL_SSL",
        poison_dlq=KafkaPostgresPoisonDLQConfig(
            dsn=postgres_dsn,
            table=dlq_table,
            policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
            pipeline_id="agora-secure-wedge-rebalance-dlq",
        ),
    )

    stream_one = None
    source_one_open = False
    source_two_open = False
    sink_open = False
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_secure_schema_registry_config.bootstrap_servers,
                topic,
                num_partitions=2,
                security=security,
            ),
            timeout=10.0,
        )
        await _produce_schema_registry_customer_records(
            kafka_bootstrap=kafka_secure_schema_registry_config.bootstrap_servers,
            security=security,
            topic=topic,
            source_records=source_records,
            serializer=serializer,
        )
        await asyncio.sleep(1.0)

        await sink.open()
        sink_open = True

        await source_one.open()
        source_one_open = True
        stream_one = source_one.stream()

        initial_records: list[dict[str, object]] = []
        initial_partitions: set[int] = set()
        while len(initial_partitions) < 2:
            record = await asyncio.wait_for(anext(stream_one), timeout=_INTEGRATION_TIMEOUT_S)
            initial_records.append(record)
            initial_partitions.add(int(record["metadata"]["partition"]))
            await _write_customer_record_to_postgres(source_one, sink, record)

        await source_two.open()
        source_two_open = True
        await _wait_for_rebalance_events(listener_one, listener_two)
        listener_two_event_count = len(listener_two.events)
        await asyncio.to_thread(kafka_secure_broker_flap_control)

        if stream_one is not None:
            await stream_one.aclose()
            stream_one = None
        await source_one.close()
        source_one_open = False
        await _wait_for_rebalance_event_counts((listener_two, listener_two_event_count + 1))

        handoff_records = await asyncio.wait_for(
            _drain_source_to_postgres(source_two, sink),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        checkpoint = source_two.current_checkpoint()
        rows = await _fetch_customer_rows(conn, table)

        dlq_records: list[object] = []

        class _CollectDLQSink:
            sink_name = "collect_dlq"

            async def open(self) -> None:
                return None

            async def write(self, record: object) -> None:
                dlq_records.append(record)

            async def flush(self) -> None:
                return None

            async def close(self) -> None:
                return None

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink())  # type: ignore[arg-type]
                .run()
            ),
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
        if sink_open:
            await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    delivered_pairs = _partition_offset_pairs(initial_records) + _partition_offset_pairs(
        handoff_records
    )
    delivered_pair_set = set(delivered_pairs)
    duplicate_count = len(delivered_pairs) - len(delivered_pair_set)

    assert len({int(record["metadata"]["partition"]) for record in initial_records}) == 2
    assert sorted(delivered_pair_set) == expected_good_pairs
    assert duplicate_count <= 2
    assert rows == _expected_customer_rows(topic, good_source_records)
    assert checkpoint is not None
    assert checkpoint["topic"] == topic
    assert checkpoint["offsets"]
    assert all(entry["topic"] == topic for entry in checkpoint["offsets"])
    assert len(dlq_records) == 2
    assert (
        sorted(
            (
                int(record.record["partition"]),
                int(record.record["offset"]),
            )
            for record in dlq_records
        )
        == expected_bad_pairs
    )
    assert all(record.stage == "kafka_deserialize" for record in dlq_records)
    assert any(event[0] == "revoked" for event in listener_one.events)
    assert any(event[0] == "assigned" for event in listener_two.events)


@pytest.mark.asyncio
async def test_kafka_secure_schema_registry_wedge_survives_multi_cycle_rebalance_flap_and_poison_dlq(
    kafka_secure_schema_registry_config,
    postgres_dsn: str,
    unique_suffix: str,
    kafka_secure_broker_flap_control,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("fastavro")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-secure-wedge-soak-{unique_suffix}"
    table = f"agora_secure_wedge_soak_{unique_suffix}"
    dlq_table = f"agora_secure_wedge_soak_dlq_{unique_suffix}"
    subject = f"{topic}-value"
    bad_pairs = {(0, 1), (1, 2), (0, 3)}
    source_records: list[dict[str, object]] = []
    for partition in range(2):
        for sequence in range(4):
            event_type = "customer.created" if sequence % 2 == 0 else "customer.updated"
            payload_name: str | None = (
                None if (partition, sequence) in bad_pairs else f"customer-{partition}-{sequence}"
            )
            source_records.append(
                {
                    "partition": partition,
                    "sequence": sequence,
                    "schema_version": "v2" if payload_name is None else "v1",
                    "key": f"customer-{partition}-{sequence}",
                    "headers": [("tenant", "acme"), ("event_type", event_type)],
                    "payload": {
                        "id": partition * 100 + sequence + 1,
                        "name": payload_name,
                    },
                }
            )
    good_source_records = [
        record
        for record in source_records
        if cast("dict[str, object]", record["payload"])["name"] is not None
    ]
    expected_good_pairs = sorted(
        (int(record["partition"]), int(record["sequence"])) for record in good_source_records
    )
    expected_bad_pairs = sorted(bad_pairs)

    schema_v1 = {
        "type": "record",
        "name": "CustomerEvent",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
        ],
    }
    schema_v2 = {
        "type": "record",
        "name": "CustomerEvent",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": ["null", "string"], "default": None},
        ],
    }
    security = kafka_secure_schema_registry_config.security()
    assert security is not None
    serializer = _VersionedSchemaRegistryPayloadSerializer(
        {
            "v1": AvroSchemaRegistrySerializer[dict[str, object]](
                registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
                subject=subject,
                schema=schema_v1,
            ),
            "v2": AvroSchemaRegistrySerializer[dict[str, object]](
                registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
                subject=subject,
                schema=schema_v2,
            ),
        }
    )

    def build_source(
        listener: _RebalanceListener, *, commit_every: int
    ) -> KafkaSource[dict[str, object]]:
        return build_kafka_postgres_source(
            topics=[topic],
            bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
            group_id=f"agora-secure-wedge-soak-{unique_suffix}",
            deserializer=AvroSchemaRegistryDeserializer[dict[str, object]](
                registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
                reader_schema=schema_v1,
            ),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            commit_every=commit_every,
            rebalance_listener=listener,
            max_idle_polls=2,
            security=security,
            security_protocol="SASL_SSL",
            poison_dlq=KafkaPostgresPoisonDLQConfig(
                dsn=postgres_dsn,
                table=dlq_table,
                policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
                pipeline_id="agora-secure-wedge-soak-dlq",
            ),
        )

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="event_id",
        batch_size=1,
    )
    open_sources: list[KafkaSource[dict[str, object]]] = []
    listeners: list[_RebalanceListener] = []
    source_streams: dict[KafkaSource[dict[str, object]], object] = {}

    def register_source(
        listener: _RebalanceListener, *, commit_every: int
    ) -> KafkaSource[dict[str, object]]:
        source = build_source(listener, commit_every=commit_every)
        listeners.append(listener)
        return source

    current_listener = _RebalanceListener()
    current_source = register_source(current_listener, commit_every=100)
    initial_records: list[dict[str, object]] = []
    cycle_records: list[dict[str, object]] = []
    cycle_targets: list[int | None] = [2, None]
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    event_id BIGINT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_secure_schema_registry_config.bootstrap_servers,
                topic,
                num_partitions=2,
                security=security,
            ),
            timeout=10.0,
        )
        await _produce_schema_registry_customer_records(
            kafka_bootstrap=kafka_secure_schema_registry_config.bootstrap_servers,
            security=security,
            topic=topic,
            source_records=source_records,
            serializer=serializer,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        await sink.open()
        await current_source.open()
        open_sources.append(current_source)
        current_stream = current_source.stream()
        source_streams[current_source] = current_stream

        initial_partitions: set[int] = set()
        while len(initial_partitions) < 2:
            record = await asyncio.wait_for(
                anext(current_stream),
                timeout=_SECURE_SOAK_INITIAL_RECORD_TIMEOUT_S,
            )
            initial_records.append(record)
            initial_partitions.add(int(record["metadata"]["partition"]))
            await _write_customer_record_to_postgres(current_source, sink, record)

        for _cycle_index, max_records in enumerate(cycle_targets, start=1):
            next_listener = _RebalanceListener()
            next_source = register_source(next_listener, commit_every=1)
            current_event_count = len(current_listener.events)
            await next_source.open()
            open_sources.append(next_source)
            await _wait_for_rebalance_event_counts(
                (current_listener, current_event_count + 1),
                (next_listener, 1),
                timeout_s=_SECURE_SOAK_REBALANCE_TIMEOUT_S,
            )
            next_listener_event_count = len(next_listener.events)
            await asyncio.to_thread(kafka_secure_broker_flap_control)

            previous_stream = source_streams.pop(current_source, None)
            if previous_stream is not None:
                with contextlib.suppress(Exception):
                    await previous_stream.aclose()
            await current_source.close()
            open_sources.remove(current_source)
            await _wait_for_rebalance_event_counts(
                (next_listener, next_listener_event_count + 1),
                timeout_s=_SECURE_SOAK_REBALANCE_TIMEOUT_S,
            )

            drained_records = await asyncio.wait_for(
                _drain_source_to_postgres(next_source, sink, max_records=max_records),
                timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S,
            )
            cycle_records.extend(drained_records)
            current_source = next_source
            current_listener = next_listener

            if max_records is not None:
                source_streams[current_source] = current_source.stream()
                with contextlib.suppress(Exception):
                    await source_streams[current_source].aclose()
                source_streams.pop(current_source, None)

        rows = await _fetch_customer_rows(conn, table)
        dlq_records: list[object] = []

        class _CollectDLQSink:
            sink_name = "collect_dlq"

            async def open(self) -> None:
                return None

            async def write(self, record: object) -> None:
                dlq_records.append(record)

            async def flush(self) -> None:
                return None

            async def close(self) -> None:
                return None

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink())  # type: ignore[arg-type]
                .run()
            ),
            timeout=_SECURE_SOAK_DLQ_READ_TIMEOUT_S,
        )
    finally:
        for stream in list(source_streams.values()):
            with contextlib.suppress(Exception):
                await stream.aclose()
        for source in reversed(open_sources):
            with contextlib.suppress(Exception):
                await source.close()
        with contextlib.suppress(Exception):
            await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    delivered_pairs = _partition_offset_pairs(initial_records) + _partition_offset_pairs(
        cycle_records
    )
    delivered_pair_set = set(delivered_pairs)
    duplicate_count = len(delivered_pairs) - len(delivered_pair_set)

    assert len({int(record["metadata"]["partition"]) for record in initial_records}) == 2
    assert sorted(delivered_pair_set) == expected_good_pairs
    assert duplicate_count <= len(cycle_targets) * 2
    assert rows == _expected_customer_rows(topic, good_source_records)
    assert len(dlq_records) == len(expected_bad_pairs)
    assert (
        sorted(
            (
                int(record.record["partition"]),
                int(record.record["offset"]),
            )
            for record in dlq_records
        )
        == expected_bad_pairs
    )
    assert all(record.stage == "kafka_deserialize" for record in dlq_records)
    assert (
        sum(1 for listener in listeners for event in listener.events if event[0] == "assigned")
        >= len(cycle_targets) + 1
    )
    assert sum(
        1 for listener in listeners for event in listener.events if event[0] == "revoked"
    ) >= len(cycle_targets)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_survives_multi_broker_leader_failover(
    kafka_cluster_bootstrap: str,
    kafka_cluster_control,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-multi-broker-failover-{unique_suffix}"
    table = f"agora_kafka_pg_multi_broker_failover_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=3, records_per_partition=3)
    expected_pairs = {
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    }

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    runtime: KafkaPostgresRuntime[dict[str, object]] | None = None
    stream = None
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_cluster_bootstrap,
                topic,
                num_partitions=3,
                replication_factor=3,
            ),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        source = build_kafka_postgres_source(
            topics=[topic],
            bootstrap_servers=kafka_cluster_bootstrap,
            group_id=f"agora-wedge-multi-broker-failover-{unique_suffix}",
            deserializer=lambda value: json.loads(value.decode("utf-8")),
            commit_every=1,
            max_idle_polls=200,
        )
        runtime = build_kafka_postgres_runtime(
            source=source,
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
        )

        await runtime.open()
        stream = source.stream()

        observed_pairs: set[tuple[int, int]] = set()
        all_records: list[dict[str, object]] = []
        leader_restart_completed = False
        previous_leader_id: int | None = None
        next_leader_id: int | None = None

        while observed_pairs != expected_pairs:
            record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
            partition_offset = (
                int(record["metadata"]["partition"]),
                int(record["metadata"]["offset"]),
            )
            observed_pairs.add(partition_offset)
            all_records.append(record)
            await runtime.deliver(record)

            if not leader_restart_completed and partition_offset[0] == 0:
                previous_leader_id = await _topic_partition_leader_id(
                    kafka_cluster_bootstrap,
                    topic,
                    0,
                )
                await asyncio.to_thread(
                    kafka_cluster_control.restart_broker,
                    previous_leader_id,
                )
                next_leader_id = await _wait_for_topic_partition_leader_change(
                    kafka_cluster_bootstrap,
                    topic,
                    0,
                    previous_leader_id=previous_leader_id,
                )
                leader_restart_completed = True

        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        if stream is not None:
            with contextlib.suppress(Exception):
                await stream.aclose()
        if runtime is not None:
            await runtime.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert leader_restart_completed is True
    assert previous_leader_id is not None
    assert next_leader_id is not None
    assert next_leader_id != previous_leader_id
    assert observed_pairs == expected_pairs
    assert rows == _expected_delivery_key_rows(topic, source_records)
    assert any(int(record["metadata"]["partition"]) == 0 for record in all_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_survives_multi_cycle_rolling_restart(
    kafka_cluster_bootstrap: str,
    kafka_cluster_control,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-rolling-restart-{unique_suffix}"
    table = f"agora_kafka_pg_rolling_restart_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=3, records_per_partition=6)
    expected_pairs = {
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    }
    restart_broker_ids = _cluster_restart_sequence(default_cycles=2)

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    runtime: KafkaPostgresRuntime[dict[str, object]] | None = None
    stream = None
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_cluster_bootstrap,
                topic,
                num_partitions=3,
                replication_factor=3,
            ),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        source = build_kafka_postgres_source(
            topics=[topic],
            bootstrap_servers=kafka_cluster_bootstrap,
            group_id=f"agora-wedge-rolling-restart-{unique_suffix}",
            deserializer=lambda value: json.loads(value.decode("utf-8")),
            commit_every=1,
            max_idle_polls=200,
        )
        runtime = build_kafka_postgres_runtime(
            source=source,
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
        )

        await runtime.open()
        stream = source.stream()

        observed_pairs: set[tuple[int, int]] = set()
        all_records: list[dict[str, object]] = []
        initial_partitions: set[int] = set()

        while len(initial_partitions) < 3:
            record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
            partition_offset = (
                int(record["metadata"]["partition"]),
                int(record["metadata"]["offset"]),
            )
            observed_pairs.add(partition_offset)
            all_records.append(record)
            initial_partitions.add(partition_offset[0])
            await runtime.deliver(record)

        for broker_id in restart_broker_ids:
            if observed_pairs == expected_pairs:
                break
            record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
            partition_offset = (
                int(record["metadata"]["partition"]),
                int(record["metadata"]["offset"]),
            )
            observed_pairs.add(partition_offset)
            all_records.append(record)
            await runtime.deliver(record)
            await asyncio.to_thread(kafka_cluster_control.restart_broker, broker_id)

        while observed_pairs != expected_pairs:
            record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
            partition_offset = (
                int(record["metadata"]["partition"]),
                int(record["metadata"]["offset"]),
            )
            observed_pairs.add(partition_offset)
            all_records.append(record)
            await runtime.deliver(record)

        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        if stream is not None:
            with contextlib.suppress(Exception):
                await stream.aclose()
        if runtime is not None:
            await runtime.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert observed_pairs == expected_pairs
    assert rows == _expected_delivery_key_rows(topic, source_records)
    assert len(all_records) == len(source_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_survives_multi_cycle_postgres_failover(
    kafka_bootstrap: str,
    postgres_ha_dsn: str,
    postgres_ha_control,
    postgres_ha_soak_cycles: int,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-postgres-failover-{unique_suffix}"
    table = f"agora_kafka_pg_postgres_failover_{unique_suffix}"
    records_per_partition = postgres_ha_soak_cycles + 1
    source_records = _partitioned_customer_records(
        partitions=2,
        records_per_partition=records_per_partition,
    )
    expected_pairs = {
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    }
    failover_checkpoints = {checkpoint * 2 for checkpoint in range(1, postgres_ha_soak_cycles + 1)}

    await asyncio.to_thread(postgres_ha_control.reset_cluster)

    conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
    runtime: KafkaPostgresRuntime[dict[str, object]] | None = None
    stream = None
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(kafka_bootstrap, topic, num_partitions=2),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        source = build_kafka_postgres_source(
            topics=[topic],
            bootstrap_servers=kafka_bootstrap,
            group_id=f"agora-wedge-postgres-failover-{unique_suffix}",
            deserializer=lambda value: json.loads(value.decode("utf-8")),
            commit_every=1,
            max_idle_polls=200,
        )
        runtime = build_kafka_postgres_runtime(
            source=source,
            dsn=postgres_ha_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
            retry_policy=RetryPolicy(
                max_attempts=8,
                initial_backoff_s=0.25,
                max_backoff_s=2.0,
                retry_exceptions=(psycopg.Error,),
            ),
        )

        await runtime.open()
        stream = source.stream()

        observed_pairs: set[tuple[int, int]] = set()
        all_records: list[dict[str, object]] = []
        transitions: list[tuple[str, str]] = []
        readiness_snapshots = []
        acceptance_reports = []

        while observed_pairs != expected_pairs:
            record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
            partition_offset = (
                int(record["metadata"]["partition"]),
                int(record["metadata"]["offset"]),
            )
            observed_pairs.add(partition_offset)
            all_records.append(record)
            await runtime.deliver(record)
            snapshot, report = await assert_runtime_readiness(
                runtime,
                _RUNTIME_READINESS_THRESHOLDS,
            )
            readiness_snapshots.append(snapshot)
            acceptance_reports.append(report)

            delivered_count = len(observed_pairs)
            if delivered_count in failover_checkpoints:
                standby_node = await asyncio.to_thread(postgres_ha_control.current_standby)
                await asyncio.to_thread(
                    postgres_ha_control.wait_for_table_row_count,
                    standby_node,
                    table,
                    expected_count=delivered_count,
                )
                transitions.append(await asyncio.to_thread(postgres_ha_control.failover_cycle))

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        rows = await _fetch_delivery_key_rows(conn, table)
        sink_metrics = runtime.sink_metrics()
    finally:
        if stream is not None:
            with contextlib.suppress(Exception):
                await stream.aclose()
        if runtime is not None:
            await runtime.close()
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()
        await asyncio.to_thread(postgres_ha_control.restore_cluster_for_teardown)

    assert observed_pairs == expected_pairs
    assert rows == _expected_delivery_key_rows(topic, source_records)
    assert len(all_records) == len(source_records)
    assert len(transitions) == postgres_ha_soak_cycles
    assert {node for transition in transitions for node in transition} == {
        "postgres-primary",
        "postgres-standby",
    }
    assert sink_metrics.retry_count >= postgres_ha_soak_cycles
    assert len(readiness_snapshots) == len(source_records)
    assert all(snapshot.health.ready for snapshot in readiness_snapshots)
    assert all(snapshot.health.sink_connection_ready for snapshot in readiness_snapshots)
    assert all(not snapshot.health.source_stalled for snapshot in readiness_snapshots)
    assert all(report.passed for report in acceptance_reports)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_crash_windows_survive_postgres_failover(
    kafka_bootstrap: str,
    postgres_ha_dsn: str,
    postgres_ha_control,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-postgres-crash-window-{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=1, records_per_partition=3)
    tables = {
        "before_write": f"agora_kafka_pg_pg_failover_before_write_{unique_suffix}",
        "after_write_before_ack": f"agora_kafka_pg_pg_failover_after_write_{unique_suffix}",
        "after_ack": f"agora_kafka_pg_pg_failover_after_ack_{unique_suffix}",
    }

    def _build_runtime(group_id: str, table: str) -> KafkaPostgresRuntime[dict[str, object]]:
        return build_kafka_postgres_runtime(
            source=build_kafka_postgres_source(
                topics=[topic],
                bootstrap_servers=kafka_bootstrap,
                group_id=group_id,
                deserializer=lambda value: json.loads(value.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                commit_every=1,
                max_idle_polls=200,
            ),
            dsn=postgres_ha_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=2,
            retry_policy=RetryPolicy(
                max_attempts=8,
                initial_backoff_s=0.25,
                max_backoff_s=2.0,
                retry_exceptions=(psycopg.Error,),
            ),
        )

    async def _create_table(conn: object, table: str) -> None:
        async with conn.cursor() as cur:  # type: ignore[attr-defined]
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

    async def _read_one_record_stream(
        runtime: KafkaPostgresRuntime[dict[str, object]],
    ) -> tuple[object, dict[str, object]]:
        stream = runtime.source.stream()
        return stream, await anext(stream)

    async def _drain_with_postgres_failover(
        runtime: KafkaPostgresRuntime[dict[str, object]],
        *,
        max_records: int,
        table: str | None = None,
        failover_after_row_count: int | None = None,
    ) -> list[dict[str, object]]:
        stream = runtime.source.stream()
        drained_records: list[dict[str, object]] = []
        failover_completed = False
        try:
            while len(drained_records) < max_records:
                record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
                drained_records.append(record)
                await runtime.deliver(record)
                if (
                    not failover_completed
                    and failover_after_row_count is not None
                    and table is not None
                    and len(drained_records) >= failover_after_row_count
                ):
                    standby_node = await asyncio.to_thread(postgres_ha_control.current_standby)
                    await asyncio.to_thread(
                        postgres_ha_control.wait_for_table_row_count,
                        standby_node,
                        table,
                        expected_count=failover_after_row_count,
                    )
                    await asyncio.to_thread(postgres_ha_control.failover_primary)
                    failover_completed = True
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
        return drained_records

    async def _reset_phase_table(table: str) -> object:
        await asyncio.to_thread(postgres_ha_control.reset_cluster)
        phase_conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        await _create_table(phase_conn, table)
        return phase_conn

    conn = None
    try:
        await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
        await _produce_customer_records(
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        conn = await _reset_phase_table(tables["before_write"])
        before_write_runtime = _build_runtime(
            f"agora-wedge-pg-failover-crash-before-write-{unique_suffix}",
            tables["before_write"],
        )
        await before_write_runtime.open()
        before_write_stream = None
        try:
            before_write_stream, first_before_write = await _read_one_record_stream(
                before_write_runtime
            )
            first_before_write_key = _record_delivery_key(topic, first_before_write)
        finally:
            if before_write_stream is not None:
                with contextlib.suppress(Exception):
                    await before_write_stream.aclose()
            await before_write_runtime.close()

        await asyncio.to_thread(postgres_ha_control.failover_primary)

        before_write_recovery_runtime = _build_runtime(
            f"agora-wedge-pg-failover-crash-before-write-{unique_suffix}",
            tables["before_write"],
        )
        await before_write_recovery_runtime.open()
        try:
            before_write_replayed = await _drain_with_postgres_failover(
                before_write_recovery_runtime,
                max_records=len(source_records),
            )
        finally:
            await before_write_recovery_runtime.close()

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        before_write_rows = await _fetch_delivery_key_rows(conn, tables["before_write"])
        await conn.close()

        conn = await _reset_phase_table(tables["after_write_before_ack"])
        after_write_runtime = _build_runtime(
            f"agora-wedge-pg-failover-crash-after-write-{unique_suffix}",
            tables["after_write_before_ack"],
        )
        await after_write_runtime.open()
        after_write_stream = None
        try:
            after_write_stream, first_after_write = await _read_one_record_stream(
                after_write_runtime
            )
            first_after_write_key = _record_delivery_key(topic, first_after_write)
            await after_write_runtime.sink.write(
                _delivery_key_customer_row(after_write_runtime, first_after_write)
            )
            await after_write_runtime.sink.flush()
        finally:
            if after_write_stream is not None:
                with contextlib.suppress(Exception):
                    await after_write_stream.aclose()
            await after_write_runtime.close()

        standby_node = await asyncio.to_thread(postgres_ha_control.current_standby)
        await asyncio.to_thread(
            postgres_ha_control.wait_for_table_row_count,
            standby_node,
            tables["after_write_before_ack"],
            expected_count=1,
        )
        await asyncio.to_thread(postgres_ha_control.failover_primary)

        after_write_recovery_runtime = _build_runtime(
            f"agora-wedge-pg-failover-crash-after-write-{unique_suffix}",
            tables["after_write_before_ack"],
        )
        await after_write_recovery_runtime.open()
        try:
            after_write_replayed = await _drain_with_postgres_failover(
                after_write_recovery_runtime,
                max_records=len(source_records),
            )
        finally:
            await after_write_recovery_runtime.close()

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        after_write_rows = await _fetch_delivery_key_rows(conn, tables["after_write_before_ack"])
        await conn.close()

        conn = await _reset_phase_table(tables["after_ack"])
        after_ack_runtime = _build_runtime(
            f"agora-wedge-pg-failover-crash-after-ack-{unique_suffix}",
            tables["after_ack"],
        )
        await after_ack_runtime.open()
        after_ack_stream = None
        try:
            after_ack_stream, first_after_ack = await _read_one_record_stream(after_ack_runtime)
            first_after_ack_key = _record_delivery_key(topic, first_after_ack)
            await after_ack_runtime.deliver(first_after_ack)
        finally:
            if after_ack_stream is not None:
                with contextlib.suppress(Exception):
                    await after_ack_stream.aclose()
            await after_ack_runtime.close()

        standby_node = await asyncio.to_thread(postgres_ha_control.current_standby)
        await asyncio.to_thread(
            postgres_ha_control.wait_for_table_row_count,
            standby_node,
            tables["after_ack"],
            expected_count=1,
        )
        await asyncio.to_thread(postgres_ha_control.failover_primary)

        after_ack_recovery_runtime = _build_runtime(
            f"agora-wedge-pg-failover-crash-after-ack-{unique_suffix}",
            tables["after_ack"],
        )
        await after_ack_recovery_runtime.open()
        try:
            after_ack_replayed = await _drain_with_postgres_failover(
                after_ack_recovery_runtime,
                max_records=len(source_records) - 1,
            )
        finally:
            await after_ack_recovery_runtime.close()

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        after_ack_rows = await _fetch_delivery_key_rows(conn, tables["after_ack"])
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()
        await asyncio.to_thread(postgres_ha_control.restore_cluster_for_teardown)

    assert first_before_write_key in {
        _record_delivery_key(topic, record) for record in before_write_replayed
    }
    assert before_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert first_after_write_key in {
        _record_delivery_key(topic, record) for record in after_write_replayed
    }
    assert after_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert first_after_ack_key not in {
        _record_delivery_key(topic, record) for record in after_ack_replayed
    }
    assert after_ack_rows == _expected_delivery_key_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_crash_windows_survive_multi_broker_rolling_restart(
    kafka_cluster_bootstrap: str,
    kafka_cluster_control,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-cluster-crash-window-{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=3, records_per_partition=2)
    tables = {
        "before_write": f"agora_kafka_pg_cluster_crash_before_write_{unique_suffix}",
        "after_write_before_ack": f"agora_kafka_pg_cluster_crash_after_write_{unique_suffix}",
        "after_ack": f"agora_kafka_pg_cluster_crash_after_ack_{unique_suffix}",
    }

    def _build_runtime(group_id: str, table: str) -> KafkaPostgresRuntime[dict[str, object]]:
        return build_kafka_postgres_runtime(
            source=build_kafka_postgres_source(
                topics=[topic],
                bootstrap_servers=kafka_cluster_bootstrap,
                group_id=group_id,
                deserializer=lambda value: json.loads(value.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                commit_every=1,
                max_idle_polls=200,
            ),
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
        )

    async def _create_table(conn: object, table: str) -> None:
        async with conn.cursor() as cur:  # type: ignore[attr-defined]
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

    async def _read_one_record_stream(
        runtime: KafkaPostgresRuntime[dict[str, object]],
    ) -> tuple[object, dict[str, object]]:
        stream = runtime.source.stream()
        return stream, await anext(stream)

    async def _drain_with_rolling_restart(
        runtime: KafkaPostgresRuntime[dict[str, object]],
        *,
        max_records: int,
        restart_broker_ids: list[int],
    ) -> list[dict[str, object]]:
        stream = runtime.source.stream()
        drained_records: list[dict[str, object]] = []
        try:
            while len(drained_records) < max_records:
                record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
                drained_records.append(record)
                await runtime.deliver(record)
                if restart_broker_ids:
                    broker_id = restart_broker_ids.pop(0)
                    await asyncio.to_thread(kafka_cluster_control.restart_broker, broker_id)
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
        return drained_records

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        for table in tables.values():
            await _create_table(conn, table)

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_cluster_bootstrap,
                topic,
                num_partitions=3,
                replication_factor=3,
            ),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        before_write_runtime = _build_runtime(
            f"agora-wedge-cluster-crash-before-write-{unique_suffix}",
            tables["before_write"],
        )
        await before_write_runtime.open()
        before_write_stream = None
        try:
            before_write_stream, first_before_write = await _read_one_record_stream(
                before_write_runtime
            )
            first_before_write_key = _record_delivery_key(topic, first_before_write)
        finally:
            if before_write_stream is not None:
                with contextlib.suppress(Exception):
                    await before_write_stream.aclose()
            await before_write_runtime.close()

        before_write_recovery_runtime = _build_runtime(
            f"agora-wedge-cluster-crash-before-write-{unique_suffix}",
            tables["before_write"],
        )
        await before_write_recovery_runtime.open()
        try:
            before_write_replayed = await _drain_with_rolling_restart(
                before_write_recovery_runtime,
                max_records=len(source_records),
                restart_broker_ids=[1],
            )
        finally:
            await before_write_recovery_runtime.close()

        after_write_runtime = _build_runtime(
            f"agora-wedge-cluster-crash-after-write-{unique_suffix}",
            tables["after_write_before_ack"],
        )
        await after_write_runtime.open()
        after_write_stream = None
        try:
            after_write_stream, first_after_write = await _read_one_record_stream(
                after_write_runtime
            )
            first_after_write_key = _record_delivery_key(topic, first_after_write)
            await after_write_runtime.sink.write(
                _delivery_key_customer_row(after_write_runtime, first_after_write)
            )
            await after_write_runtime.sink.flush()
        finally:
            if after_write_stream is not None:
                with contextlib.suppress(Exception):
                    await after_write_stream.aclose()
            await after_write_runtime.close()

        after_write_recovery_runtime = _build_runtime(
            f"agora-wedge-cluster-crash-after-write-{unique_suffix}",
            tables["after_write_before_ack"],
        )
        await after_write_recovery_runtime.open()
        try:
            after_write_replayed = await _drain_with_rolling_restart(
                after_write_recovery_runtime,
                max_records=len(source_records),
                restart_broker_ids=[2],
            )
        finally:
            await after_write_recovery_runtime.close()

        after_ack_runtime = _build_runtime(
            f"agora-wedge-cluster-crash-after-ack-{unique_suffix}",
            tables["after_ack"],
        )
        await after_ack_runtime.open()
        after_ack_stream = None
        try:
            after_ack_stream, first_after_ack = await _read_one_record_stream(after_ack_runtime)
            first_after_ack_key = _record_delivery_key(topic, first_after_ack)
            await after_ack_runtime.deliver(first_after_ack)
        finally:
            if after_ack_stream is not None:
                with contextlib.suppress(Exception):
                    await after_ack_stream.aclose()
            await after_ack_runtime.close()

        after_ack_recovery_runtime = _build_runtime(
            f"agora-wedge-cluster-crash-after-ack-{unique_suffix}",
            tables["after_ack"],
        )
        await after_ack_recovery_runtime.open()
        try:
            after_ack_replayed = await _drain_with_rolling_restart(
                after_ack_recovery_runtime,
                max_records=len(source_records) - 1,
                restart_broker_ids=[3],
            )
        finally:
            await after_ack_recovery_runtime.close()

        before_write_rows = await _fetch_delivery_key_rows(conn, tables["before_write"])
        after_write_rows = await _fetch_delivery_key_rows(conn, tables["after_write_before_ack"])
        after_ack_rows = await _fetch_delivery_key_rows(conn, tables["after_ack"])
    finally:
        async with conn.cursor() as cur:
            for table in tables.values():
                await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert first_before_write_key in {
        _record_delivery_key(topic, record) for record in before_write_replayed
    }
    assert before_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert first_after_write_key in {
        _record_delivery_key(topic, record) for record in after_write_replayed
    }
    assert after_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert first_after_ack_key not in {
        _record_delivery_key(topic, record) for record in after_ack_replayed
    }
    assert after_ack_rows == _expected_delivery_key_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_delivery_key_crash_windows_survive_coordinator_failover(
    kafka_cluster_bootstrap: str,
    kafka_cluster_control,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-cluster-coordinator-crash-window-{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=3, records_per_partition=2)
    tables = {
        "before_write": f"agora_kafka_pg_cluster_coord_before_write_{unique_suffix}",
        "after_write_before_ack": f"agora_kafka_pg_cluster_coord_after_write_{unique_suffix}",
        "after_ack": f"agora_kafka_pg_cluster_coord_after_ack_{unique_suffix}",
    }

    def _build_runtime(group_id: str, table: str) -> KafkaPostgresRuntime[dict[str, object]]:
        return build_kafka_postgres_runtime(
            source=build_kafka_postgres_source(
                topics=[topic],
                bootstrap_servers=kafka_cluster_bootstrap,
                group_id=group_id,
                deserializer=lambda value: json.loads(value.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                commit_every=1,
                max_idle_polls=200,
            ),
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
        )

    async def _create_table(conn: object, table: str) -> None:
        async with conn.cursor() as cur:  # type: ignore[attr-defined]
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

    async def _read_one_record_stream(
        runtime: KafkaPostgresRuntime[dict[str, object]],
    ) -> tuple[object, dict[str, object]]:
        stream = runtime.source.stream()
        return stream, await anext(stream)

    async def _drain_with_coordinator_failover(
        runtime: KafkaPostgresRuntime[dict[str, object]],
        *,
        group_id: str,
        max_records: int,
    ) -> tuple[list[dict[str, object]], int, int]:
        stream = runtime.source.stream()
        drained_records: list[dict[str, object]] = []
        stopped_broker_id: int | None = None
        next_coordinator_id: int | None = None
        try:
            first_record = await asyncio.wait_for(
                anext(stream), timeout=_SECURE_SOAK_INITIAL_RECORD_TIMEOUT_S
            )
            stopped_broker_id = await _consumer_group_coordinator_id(
                kafka_cluster_bootstrap,
                group_id,
            )
            await asyncio.to_thread(kafka_cluster_control.stop_broker, stopped_broker_id)
            next_coordinator_id = await _wait_for_consumer_group_coordinator_change(
                kafka_cluster_bootstrap,
                group_id,
                previous_coordinator_id=stopped_broker_id,
            )
            drained_records.append(first_record)
            await runtime.deliver(first_record)
            while len(drained_records) < max_records:
                record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
                drained_records.append(record)
                await runtime.deliver(record)
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
            if stopped_broker_id is not None:
                await asyncio.to_thread(kafka_cluster_control.start_broker, stopped_broker_id)
        assert stopped_broker_id is not None
        assert next_coordinator_id is not None
        return drained_records, stopped_broker_id, next_coordinator_id

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        for table in tables.values():
            await _create_table(conn, table)

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_cluster_bootstrap,
                topic,
                num_partitions=3,
                replication_factor=3,
            ),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        before_write_group_id = f"agora-wedge-cluster-coord-before-write-{unique_suffix}"
        before_write_runtime = _build_runtime(before_write_group_id, tables["before_write"])
        await before_write_runtime.open()
        before_write_stream = None
        try:
            before_write_stream, first_before_write = await _read_one_record_stream(
                before_write_runtime
            )
            first_before_write_key = _record_delivery_key(topic, first_before_write)
        finally:
            if before_write_stream is not None:
                with contextlib.suppress(Exception):
                    await before_write_stream.aclose()
            await before_write_runtime.close()

        before_write_recovery_runtime = _build_runtime(
            before_write_group_id,
            tables["before_write"],
        )
        await before_write_recovery_runtime.open()
        try:
            (
                before_write_replayed,
                before_write_stopped_broker_id,
                before_write_next_coordinator_id,
            ) = await _drain_with_coordinator_failover(
                before_write_recovery_runtime,
                group_id=before_write_group_id,
                max_records=len(source_records),
            )
        finally:
            await before_write_recovery_runtime.close()

        after_write_group_id = f"agora-wedge-cluster-coord-after-write-{unique_suffix}"
        after_write_runtime = _build_runtime(after_write_group_id, tables["after_write_before_ack"])
        await after_write_runtime.open()
        after_write_stream = None
        try:
            after_write_stream, first_after_write = await _read_one_record_stream(
                after_write_runtime
            )
            first_after_write_key = _record_delivery_key(topic, first_after_write)
            await after_write_runtime.sink.write(
                _delivery_key_customer_row(after_write_runtime, first_after_write)
            )
            await after_write_runtime.sink.flush()
        finally:
            if after_write_stream is not None:
                with contextlib.suppress(Exception):
                    await after_write_stream.aclose()
            await after_write_runtime.close()

        after_write_recovery_runtime = _build_runtime(
            after_write_group_id,
            tables["after_write_before_ack"],
        )
        await after_write_recovery_runtime.open()
        try:
            (
                after_write_replayed,
                after_write_stopped_broker_id,
                after_write_next_coordinator_id,
            ) = await _drain_with_coordinator_failover(
                after_write_recovery_runtime,
                group_id=after_write_group_id,
                max_records=len(source_records),
            )
        finally:
            await after_write_recovery_runtime.close()

        after_ack_group_id = f"agora-wedge-cluster-coord-after-ack-{unique_suffix}"
        after_ack_runtime = _build_runtime(after_ack_group_id, tables["after_ack"])
        await after_ack_runtime.open()
        after_ack_stream = None
        try:
            after_ack_stream, first_after_ack = await _read_one_record_stream(after_ack_runtime)
            first_after_ack_key = _record_delivery_key(topic, first_after_ack)
            await after_ack_runtime.deliver(first_after_ack)
        finally:
            if after_ack_stream is not None:
                with contextlib.suppress(Exception):
                    await after_ack_stream.aclose()
            await after_ack_runtime.close()

        after_ack_recovery_runtime = _build_runtime(
            after_ack_group_id,
            tables["after_ack"],
        )
        await after_ack_recovery_runtime.open()
        try:
            (
                after_ack_replayed,
                after_ack_stopped_broker_id,
                after_ack_next_coordinator_id,
            ) = await _drain_with_coordinator_failover(
                after_ack_recovery_runtime,
                group_id=after_ack_group_id,
                max_records=len(source_records) - 1,
            )
        finally:
            await after_ack_recovery_runtime.close()

        before_write_rows = await _fetch_delivery_key_rows(conn, tables["before_write"])
        after_write_rows = await _fetch_delivery_key_rows(conn, tables["after_write_before_ack"])
        after_ack_rows = await _fetch_delivery_key_rows(conn, tables["after_ack"])
    finally:
        async with conn.cursor() as cur:
            for table in tables.values():
                await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert before_write_stopped_broker_id != before_write_next_coordinator_id
    assert first_before_write_key in {
        _record_delivery_key(topic, record) for record in before_write_replayed
    }
    assert before_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert after_write_stopped_broker_id != after_write_next_coordinator_id
    assert first_after_write_key in {
        _record_delivery_key(topic, record) for record in after_write_replayed
    }
    assert after_write_rows == _expected_delivery_key_rows(topic, source_records)

    assert after_ack_stopped_broker_id != after_ack_next_coordinator_id
    assert first_after_ack_key not in {
        _record_delivery_key(topic, record) for record in after_ack_replayed
    }
    assert after_ack_rows == _expected_delivery_key_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_seek_to_offsets_partial_replay_survives_multi_broker_rolling_restart(
    kafka_cluster_bootstrap: str,
    kafka_cluster_control,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-cluster-seek-replay-{unique_suffix}"
    table = f"agora_kafka_pg_cluster_seek_replay_{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=3, records_per_partition=4)
    offsets_by_partition = {0: 2, 1: 1, 2: 3}
    expected_replay_records = _records_at_or_after_partition_offsets(
        source_records,
        offsets_by_partition,
    )

    async def _drain_with_rolling_restart(
        runtime: KafkaPostgresRuntime[dict[str, object]],
        *,
        max_records: int,
        restart_broker_ids: list[int],
    ) -> list[dict[str, object]]:
        stream = runtime.source.stream()
        drained_records: list[dict[str, object]] = []
        try:
            while len(drained_records) < max_records:
                record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
                drained_records.append(record)
                await runtime.deliver(record)
                if restart_broker_ids:
                    broker_id = restart_broker_ids.pop(0)
                    await asyncio.to_thread(kafka_cluster_control.restart_broker, broker_id)
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
        return drained_records

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    runtime: KafkaPostgresRuntime[dict[str, object]] | None = None
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_cluster_bootstrap,
                topic,
                num_partitions=3,
                replication_factor=3,
            ),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        source = build_kafka_postgres_source(
            topics=[topic],
            bootstrap_servers=kafka_cluster_bootstrap,
            group_id=f"agora-wedge-cluster-seek-replay-{unique_suffix}",
            deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            commit_every=1,
            max_idle_polls=200,
        )
        runtime = build_kafka_postgres_runtime(
            source=source,
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
        )

        await runtime.open()
        initial_records = await runtime.drain(max_records=len(source_records))
        await source.seek_to_offsets(
            {(topic, partition): offset for partition, offset in offsets_by_partition.items()}
        )
        replayed_records = await _drain_with_rolling_restart(
            runtime,
            max_records=len(expected_replay_records),
            restart_broker_ids=_cluster_restart_sequence(default_cycles=1),
        )

        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        if runtime is not None:
            await runtime.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert _partition_offset_pairs(initial_records) == sorted(
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    )
    assert _partition_offset_pairs(replayed_records) == sorted(
        (int(record["partition"]), int(record["sequence"])) for record in expected_replay_records
    )
    assert rows == _expected_delivery_key_rows(topic, source_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_live_tail_after_seek_to_end_survives_multi_broker_rolling_restart(
    kafka_cluster_bootstrap: str,
    kafka_cluster_control,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-cluster-live-tail-{unique_suffix}"
    table = f"agora_kafka_pg_cluster_live_tail_{unique_suffix}"
    backlog_records = _partitioned_customer_records(partitions=3, records_per_partition=3)
    live_records = [
        record
        for record in _partitioned_customer_records(partitions=3, records_per_partition=5)
        if int(record["sequence"]) >= 3
    ]

    async def _drain_with_rolling_restart(
        runtime: KafkaPostgresRuntime[dict[str, object]],
        *,
        max_records: int,
        restart_broker_ids: list[int],
    ) -> list[dict[str, object]]:
        stream = runtime.source.stream()
        drained_records: list[dict[str, object]] = []
        try:
            while len(drained_records) < max_records:
                record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
                drained_records.append(record)
                await runtime.deliver(record)
                if restart_broker_ids:
                    broker_id = restart_broker_ids.pop(0)
                    await asyncio.to_thread(kafka_cluster_control.restart_broker, broker_id)
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
        return drained_records

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    runtime: KafkaPostgresRuntime[dict[str, object]] | None = None
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_cluster_bootstrap,
                topic,
                num_partitions=3,
                replication_factor=3,
            ),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=backlog_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        source = build_kafka_postgres_source(
            topics=[topic],
            bootstrap_servers=kafka_cluster_bootstrap,
            group_id=f"agora-wedge-cluster-live-tail-{unique_suffix}",
            deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            commit_every=1,
            max_idle_polls=200,
        )
        runtime = build_kafka_postgres_runtime(
            source=source,
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
        )

        await runtime.open()
        await source.seek_to_end()

        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=live_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        live_tail_records = await _drain_with_rolling_restart(
            runtime,
            max_records=len(live_records),
            restart_broker_ids=_cluster_restart_sequence(default_cycles=1),
        )
        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        if runtime is not None:
            await runtime.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert _partition_offset_pairs(live_tail_records) == sorted(
        (int(record["partition"]), int(record["sequence"])) for record in live_records
    )
    assert rows == _expected_delivery_key_rows(topic, live_records)


@pytest.mark.asyncio
async def test_kafka_postgres_runtime_survives_consumer_group_coordinator_failover(
    kafka_cluster_bootstrap: str,
    kafka_cluster_control,
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    psycopg = pytest.importorskip("psycopg")

    topic = f"agora-wedge-group-coordinator-failover-{unique_suffix}"
    table = f"agora_kafka_pg_group_coordinator_failover_{unique_suffix}"
    group_id = f"agora-wedge-group-coordinator-failover-{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=3, records_per_partition=4)
    expected_pairs = {
        (int(record["partition"]), int(record["sequence"])) for record in source_records
    }

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    runtime: KafkaPostgresRuntime[dict[str, object]] | None = None
    stream = None
    stopped_broker_id: int | None = None
    next_coordinator_id: int | None = None
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    kafka_delivery_key TEXT PRIMARY KEY,
                    event_id BIGINT NOT NULL,
                    display_name TEXT NOT NULL,
                    tenant TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    kafka_metadata JSONB NOT NULL
                )
                """
            )

        await asyncio.wait_for(
            _ensure_topic_exists(
                kafka_cluster_bootstrap,
                topic,
                num_partitions=3,
                replication_factor=3,
            ),
            timeout=10.0,
        )
        await _produce_customer_records(
            kafka_bootstrap=kafka_cluster_bootstrap,
            topic=topic,
            source_records=source_records,
        )
        await asyncio.sleep(_SECURE_SOAK_PRODUCER_SETTLE_S)

        source = build_kafka_postgres_source(
            topics=[topic],
            bootstrap_servers=kafka_cluster_bootstrap,
            group_id=group_id,
            deserializer=lambda value: json.loads(value.decode("utf-8")),
            commit_every=1,
            max_idle_polls=200,
        )
        runtime = build_kafka_postgres_runtime(
            source=source,
            dsn=postgres_dsn,
            table=table,
            transform=_customer_transform,
            row_mapper=_kafka_postgres_json_row_mapper,
            batch_size=1,
        )

        await runtime.open()
        stream = source.stream()

        observed_pairs: set[tuple[int, int]] = set()
        all_records: list[dict[str, object]] = []

        record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
        partition_offset = (
            int(record["metadata"]["partition"]),
            int(record["metadata"]["offset"]),
        )
        observed_pairs.add(partition_offset)
        all_records.append(record)
        await runtime.deliver(record)

        stopped_broker_id = await _consumer_group_coordinator_id(
            kafka_cluster_bootstrap,
            group_id,
        )
        await asyncio.to_thread(kafka_cluster_control.stop_broker, stopped_broker_id)
        next_coordinator_id = await _wait_for_consumer_group_coordinator_change(
            kafka_cluster_bootstrap,
            group_id,
            previous_coordinator_id=stopped_broker_id,
        )

        while observed_pairs != expected_pairs:
            record = await asyncio.wait_for(anext(stream), timeout=_SECURE_SOAK_DRAIN_TIMEOUT_S)
            partition_offset = (
                int(record["metadata"]["partition"]),
                int(record["metadata"]["offset"]),
            )
            observed_pairs.add(partition_offset)
            all_records.append(record)
            await runtime.deliver(record)

        rows = await _fetch_delivery_key_rows(conn, table)
    finally:
        if stopped_broker_id is not None:
            await asyncio.to_thread(kafka_cluster_control.start_broker, stopped_broker_id)
        if stream is not None:
            with contextlib.suppress(Exception):
                await stream.aclose()
        if runtime is not None:
            await runtime.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert stopped_broker_id is not None
    assert next_coordinator_id is not None
    assert next_coordinator_id != stopped_broker_id
    assert observed_pairs == expected_pairs
    assert rows == _expected_delivery_key_rows(topic, source_records)
    assert len(all_records) == len(source_records)
