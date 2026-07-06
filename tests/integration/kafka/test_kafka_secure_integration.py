from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from inspect import Parameter, signature
from typing import Any

import pytest
from agora import IterableSource, Pipeline
from agora.core.dlq import DLQRecord

from agora_plugins.kafka import (
    AvroSchemaRegistryDeserializer,
    AvroSchemaRegistrySerializer,
    JsonSchemaRegistryDeserializer,
    JsonSchemaRegistrySerializer,
    KafkaDLQSink,
    KafkaDLQSource,
    KafkaPluginConfig,
    KafkaPoisonRecordPolicy,
    KafkaSink,
    KafkaSource,
    ProtobufSchemaRegistryDeserializer,
    ProtobufSchemaRegistrySerializer,
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


class _CollectDLQSink:
    sink_name = "collect-dlq"

    def __init__(self) -> None:
        self.records: list[DLQRecord] = []

    async def open(self) -> None:
        return None

    async def write(self, record: DLQRecord) -> None:
        self.records.append(record)

    async def write_batch(self, records: list[DLQRecord]) -> None:
        self.records.extend(records)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


async def _ensure_topic_exists(
    bootstrap_servers: str,
    topic: str,
    *,
    num_partitions: int = 1,
    security: object | None = None,
) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin_kwargs: dict[str, Any] = {}
    if security is not None:
        raw_kwargs = security.to_aiokafka_admin_kwargs()
        parameters = signature(AIOKafkaAdminClient.__init__).parameters
        if any(parameter.kind is Parameter.VAR_KEYWORD for parameter in parameters.values()):
            admin_kwargs = raw_kwargs
        else:
            admin_kwargs = {key: value for key, value in raw_kwargs.items() if key in parameters}
    admin = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        **admin_kwargs,
    )
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


def _assert_schema_registry_mtls_rejected(message: str) -> None:
    normalized = message.lower()
    assert any(
        marker in normalized
        for marker in (
            "certificate required",
            "bad certificate",
            "no required ssl certificate was sent",
            "the ssl certificate error",
            "unknown ca",
            "tlsv13 alert certificate required",
            "tlsv1 alert unknown ca",
            "eof occurred in violation of protocol",
            "handshake failure",
        )
    ), message


def _assert_schema_registry_auth_rejected(message: str) -> None:
    normalized = message.lower()
    assert "401" in message, message
    assert any(
        marker in normalized
        for marker in (
            "unauthorized",
            "authorization required",
            "www-authenticate",
        )
    ), message


def _make_dlq_record(run_id: str, *, attempt: int = 0) -> DLQRecord:
    return DLQRecord(
        pipeline_id="orders",
        run_id=run_id,
        stage="middleware",
        error_type="ValueError",
        error_message="bad payload",
        record={"id": run_id},
        original_record={"id": run_id, "raw": True},
        processed_record={"id": run_id, "normalized": True},
        source="kafka",
        checkpoint={"offset": run_id},
        middleware="normalize",
        sink="postgres",
        created_at=datetime(2026, 6, 18, 12, 0, tzinfo=UTC),
        attempt=attempt,
        max_attempts=5,
    )


def _make_protobuf_message_types() -> dict[str, type[Any]]:
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "agora_secure_integration.proto"
    file_proto.package = "agora.integration"
    file_proto.syntax = "proto3"

    order_created = file_proto.message_type.add()
    order_created.name = "OrderCreated"

    order_id_field = order_created.field.add()
    order_id_field.name = "order_id"
    order_id_field.number = 1
    order_id_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    order_id_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    amount_field = order_created.field.add()
    amount_field.name = "amount"
    amount_field.number = 2
    amount_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    amount_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT32

    customer_created = file_proto.message_type.add()
    customer_created.name = "CustomerCreated"

    customer_id_field = customer_created.field.add()
    customer_id_field.name = "customer_id"
    customer_id_field.number = 1
    customer_id_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    customer_id_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    return {
        "OrderCreated": message_factory.GetMessageClass(
            pool.FindMessageTypeByName("agora.integration.OrderCreated")
        ),
        "CustomerCreated": message_factory.GetMessageClass(
            pool.FindMessageTypeByName("agora.integration.CustomerCreated")
        ),
    }


@pytest.mark.asyncio
async def test_kafka_round_trip_over_sasl_ssl_scram_with_env_file_security(
    kafka_scram_plugin_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-secure-scram-{unique_suffix}"
    security = kafka_scram_plugin_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_scram_plugin_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

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
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_scram_plugin_config.bootstrap_servers,
                    serializer=lambda record: json.dumps(record["payload"]).encode("utf-8"),
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
    assert producer_summary.records_written == 2

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_scram_plugin_config.bootstrap_servers,
                    group_id=f"agora-secure-scram-{unique_suffix}",
                    deserializer=lambda value, metadata: {
                        "payload": json.loads(value.decode("utf-8")),
                        "metadata": metadata,
                    },
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=2)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 2
    assert [record["payload"] for record in collected.records] == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "bravo"},
    ]
    assert [record["metadata"]["key"] for record in collected.records] == [
        b"order-1",
        b"order-2",
    ]


@pytest.mark.asyncio
async def test_kafka_dlq_replay_over_mtls_with_env_resolved_client_materials(
    kafka_mtls_plugin_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-secure-dlq-{unique_suffix}"
    security = kafka_mtls_plugin_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_mtls_plugin_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

    sink = KafkaDLQSink(
        topic=topic,
        bootstrap_servers=kafka_mtls_plugin_config.bootstrap_servers,
        security_protocol="SSL",
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        ssl_certfile_env="AGORA_TEST_KAFKA_CLIENT_CERT_FILE",
        ssl_keyfile_env="AGORA_TEST_KAFKA_CLIENT_KEY_FILE",
    )
    source = KafkaDLQSource(
        topic=topic,
        bootstrap_servers=kafka_mtls_plugin_config.bootstrap_servers,
        pipeline_id="orders",
        stage="middleware",
        security_protocol="SSL",
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        ssl_certfile_env="AGORA_TEST_KAFKA_CLIENT_CERT_FILE",
        ssl_keyfile_env="AGORA_TEST_KAFKA_CLIENT_KEY_FILE",
    )

    record_one = _make_dlq_record("run-1")
    record_two = _make_dlq_record("run-2")

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


@pytest.mark.asyncio
async def test_kafka_avro_round_trip_over_secure_schema_registry_and_scram(
    kafka_secure_schema_registry_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("fastavro")

    topic = f"agora-secure-avro-{unique_suffix}"
    schema = {
        "type": "record",
        "name": "OrderCreated",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "int"},
        ],
    }
    security = kafka_secure_schema_registry_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_secure_schema_registry_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

    serializer = AvroSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        subject=f"{topic}-value",
        schema=schema,
    )
    deserializer = AvroSchemaRegistryDeserializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
    )
    source_records = [
        {"order_id": "o-1", "amount": 41},
        {"order_id": "o-2", "amount": 42},
    ]

    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    serializer=serializer,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert producer_summary.records_written == 2

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    group_id=f"agora-secure-avro-{unique_suffix}",
                    deserializer=deserializer,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=2)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 2
    assert collected.records == source_records


@pytest.mark.asyncio
async def test_schema_registry_rejects_incompatible_avro_registration(
    kafka_secure_schema_registry_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("fastavro")

    subject = f"agora-secure-avro-compat-{unique_suffix}-value"
    schema_v1 = {
        "type": "record",
        "name": "OrderCreated",
        "fields": [
            {"name": "order_id", "type": "string"},
        ],
    }
    schema_v2_incompatible = {
        "type": "record",
        "name": "OrderCreated",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "int"},
        ],
    }
    client = kafka_secure_schema_registry_config.schema_registry_client()

    assert await client.set_subject_compatibility(subject, "BACKWARD") == "BACKWARD"
    await client.register_schema(subject, json.dumps(schema_v1))

    with pytest.raises(RuntimeError) as excinfo:
        await client.register_schema(subject, json.dumps(schema_v2_incompatible))

    message = str(excinfo.value)
    assert "409" in message
    assert "incompat" in message.lower()
    assert await client.get_subject_compatibility(subject) == "BACKWARD"


@pytest.mark.asyncio
async def test_kafka_avro_round_trip_over_secure_schema_registry_mtls_and_scram(
    kafka_secure_schema_registry_mtls_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("fastavro")

    topic = f"agora-secure-avro-mtls-{unique_suffix}"
    schema = {
        "type": "record",
        "name": "OrderCreated",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "int"},
        ],
    }
    security = kafka_secure_schema_registry_mtls_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_secure_schema_registry_mtls_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

    serializer = AvroSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_mtls_config.schema_registry_client(),
        subject=f"{topic}-value",
        schema=schema,
    )
    deserializer = AvroSchemaRegistryDeserializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_mtls_config.schema_registry_client(),
    )
    source_records = [
        {"order_id": "o-mtls-1", "amount": 141},
        {"order_id": "o-mtls-2", "amount": 142},
    ]

    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_mtls_config.bootstrap_servers,
                    serializer=serializer,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert producer_summary.records_written == 2

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_secure_schema_registry_mtls_config.bootstrap_servers,
                    group_id=f"agora-secure-avro-mtls-{unique_suffix}",
                    deserializer=deserializer,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=2)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 2
    assert collected.records == source_records


@pytest.mark.asyncio
async def test_schema_registry_mtls_rejects_client_without_certificate(
    kafka_secure_schema_registry_mtls_config: KafkaPluginConfig,
    kafka_secure_schema_registry_mtls_no_client_cert_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    subject = f"agora-secure-negative-no-cert-{unique_suffix}-value"
    valid_client = kafka_secure_schema_registry_mtls_config.schema_registry_client()
    await valid_client.register_schema(
        subject,
        json.dumps(
            {
                "type": "record",
                "name": "OrderCreated",
                "fields": [{"name": "order_id", "type": "string"}],
            }
        ),
    )

    invalid_client = (
        kafka_secure_schema_registry_mtls_no_client_cert_config.schema_registry_client()
    )
    with pytest.raises(RuntimeError) as excinfo:
        await invalid_client.get_latest_schema(subject)

    _assert_schema_registry_mtls_rejected(str(excinfo.value))


@pytest.mark.asyncio
async def test_schema_registry_mtls_rejects_client_with_untrusted_certificate(
    kafka_secure_schema_registry_mtls_config: KafkaPluginConfig,
    kafka_secure_schema_registry_mtls_bad_client_cert_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    subject = f"agora-secure-negative-bad-cert-{unique_suffix}-value"
    valid_client = kafka_secure_schema_registry_mtls_config.schema_registry_client()
    await valid_client.register_schema(
        subject,
        json.dumps(
            {
                "type": "record",
                "name": "OrderCreated",
                "fields": [{"name": "order_id", "type": "string"}],
            }
        ),
    )

    invalid_client = (
        kafka_secure_schema_registry_mtls_bad_client_cert_config.schema_registry_client()
    )
    with pytest.raises(RuntimeError) as excinfo:
        await invalid_client.get_latest_schema(subject)

    _assert_schema_registry_mtls_rejected(str(excinfo.value))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "username", "password"),
    [
        ("tls", None, None),
        ("tls", "agora", "definitely-wrong"),
        ("mtls", None, None),
        ("mtls", "agora", "definitely-wrong"),
    ],
)
async def test_schema_registry_auth_failures_are_rejected_clearly(
    kafka_secure_assets: dict[str, str],
    kafka_secure_schema_registry_config: KafkaPluginConfig,
    kafka_secure_schema_registry_mtls_config: KafkaPluginConfig,
    unique_suffix: str,
    mode: str,
    username: str | None,
    password: str | None,
) -> None:
    subject = f"agora-secure-auth-{mode}-{unique_suffix}-value"
    valid_client = (
        kafka_secure_schema_registry_mtls_config.schema_registry_client()
        if mode == "mtls"
        else kafka_secure_schema_registry_config.schema_registry_client()
    )
    await valid_client.register_schema(
        subject,
        json.dumps(
            {
                "type": "record",
                "name": "OrderCreated",
                "fields": [{"name": "order_id", "type": "string"}],
            }
        ),
    )

    invalid_cfg = KafkaPluginConfig(
        bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username_env="AGORA_TEST_KAFKA_SCRAM_USERNAME",
        sasl_password_file=kafka_secure_assets["scram_password_file"],
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_url=(
            kafka_secure_assets["schema_registry_mtls_url"]
            if mode == "mtls"
            else kafka_secure_assets["schema_registry_url"]
        ),
        schema_registry_username=username,
        schema_registry_password=password,
        schema_registry_ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_ssl_certfile=(
            kafka_secure_assets["client_cert_file"] if mode == "mtls" else None
        ),
        schema_registry_ssl_keyfile=(
            kafka_secure_assets["client_key_file"] if mode == "mtls" else None
        ),
    )

    invalid_client = invalid_cfg.schema_registry_client()
    with pytest.raises(RuntimeError) as excinfo:
        await invalid_client.get_latest_schema(subject)

    _assert_schema_registry_auth_rejected(str(excinfo.value))


@pytest.mark.asyncio
async def test_kafka_json_schema_round_trip_over_secure_schema_registry_and_scram(
    kafka_secure_schema_registry_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("jsonschema")

    topic = f"agora-secure-json-{unique_suffix}"
    schema = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": "integer"},
        },
        "required": ["order_id", "amount"],
    }
    security = kafka_secure_schema_registry_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_secure_schema_registry_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

    serializer = JsonSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        subject=f"{topic}-value",
        schema=schema,
    )
    deserializer = JsonSchemaRegistryDeserializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
    )
    source_records = [
        {"order_id": "o-10", "amount": 510},
        {"order_id": "o-11", "amount": 511},
    ]

    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    serializer=serializer,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert producer_summary.records_written == 2

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    group_id=f"agora-secure-json-{unique_suffix}",
                    deserializer=deserializer,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=2)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 2
    assert collected.records == source_records


@pytest.mark.asyncio
async def test_kafka_json_schema_failure_path_routes_to_poison_dlq_by_policy(
    kafka_secure_schema_registry_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("jsonschema")

    topic = f"agora-secure-json-failure-{unique_suffix}"
    subject = f"{topic}-value"
    schema_v1 = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": "integer"},
        },
        "required": ["order_id", "amount"],
    }
    schema_v2 = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": ["integer", "string"]},
        },
        "required": ["order_id", "amount"],
    }
    security = kafka_secure_schema_registry_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_secure_schema_registry_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

    good_serializer = JsonSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        subject=subject,
        schema=schema_v1,
    )
    evolving_serializer = JsonSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        subject=subject,
        schema=schema_v2,
        auto_register="always",
    )

    good_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource([{"order_id": "o-10", "amount": 510}]))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    serializer=good_serializer,
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
            Pipeline(IterableSource([{"order_id": "o-11", "amount": "511"}]))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    serializer=evolving_serializer,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert good_summary.records_written == 1
    assert bad_summary.records_written == 1

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    dlq = _CollectDLQSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    group_id=f"agora-secure-json-failure-{unique_suffix}",
                    deserializer=JsonSchemaRegistryDeserializer[dict[str, object]](
                        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
                        reader_schema=schema_v1,
                    ),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    max_idle_polls=2,
                    security=security,
                    security_protocol="SASL_SSL",
                    poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
                    poison_record_sink=dlq,
                    poison_record_pipeline_id="agora-secure-json-schema-failure",
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 1
    assert collected.records == [{"order_id": "o-10", "amount": 510}]
    assert len(dlq.records) == 1
    poison = dlq.records[0]
    assert poison.pipeline_id == "agora-secure-json-schema-failure"
    assert poison.stage == "kafka_deserialize"
    assert poison.record["topic"] == topic
    assert poison.record["poison"] == {
        "classification": "schema_validation",
        "policy": "dlq_and_continue",
    }
    assert "type" in poison.error_message.lower()


@pytest.mark.asyncio
async def test_kafka_protobuf_round_trip_over_secure_schema_registry_and_scram(
    kafka_secure_schema_registry_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("google.protobuf")

    from google.protobuf.json_format import MessageToDict

    topic = f"agora-secure-proto-{unique_suffix}"
    schema = """
        syntax = "proto3";
        package agora.integration;

        message OrderCreated {
          string order_id = 1;
          int32 amount = 2;
        }
    """
    message_types = _make_protobuf_message_types()
    message_type = message_types["OrderCreated"]
    security = kafka_secure_schema_registry_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_secure_schema_registry_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

    serializer = ProtobufSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        subject=f"{topic}-value",
        schema=schema,
        message_type=message_type,
    )
    deserializer = ProtobufSchemaRegistryDeserializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        message_type=message_type,
        record_mapper=lambda message: MessageToDict(
            message,
            preserving_proto_field_name=True,
        ),
    )
    source_records = [
        {"order_id": "o-20", "amount": 620},
        {"order_id": "o-21", "amount": 621},
    ]

    producer_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(source_records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    serializer=serializer,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert producer_summary.records_written == 2

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    group_id=f"agora-secure-proto-{unique_suffix}",
                    deserializer=deserializer,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run(max_records=2)
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 2
    assert collected.records == [
        {"order_id": "o-20", "amount": 620},
        {"order_id": "o-21", "amount": 621},
    ]


@pytest.mark.asyncio
async def test_kafka_protobuf_failure_path_routes_binding_mismatch_to_poison_dlq_by_policy(
    kafka_secure_schema_registry_config: KafkaPluginConfig,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    pytest.importorskip("google.protobuf")

    from google.protobuf.json_format import MessageToDict

    topic = f"agora-secure-proto-failure-{unique_suffix}"
    subject = f"{topic}-value"
    schema = """
        syntax = "proto3";
        package agora.integration;

        message OrderCreated {
          string order_id = 1;
          int32 amount = 2;
        }

        message CustomerCreated {
          string customer_id = 1;
        }
    """
    message_types = _make_protobuf_message_types()
    security = kafka_secure_schema_registry_config.security()
    assert security is not None

    await asyncio.wait_for(
        _ensure_topic_exists(
            kafka_secure_schema_registry_config.bootstrap_servers,
            topic,
            security=security,
        ),
        timeout=10.0,
    )

    order_serializer = ProtobufSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        subject=subject,
        schema=schema,
        message_type=message_types["OrderCreated"],
        message_indexes=(0,),
    )
    customer_serializer = ProtobufSchemaRegistrySerializer[dict[str, object]](
        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
        subject=subject,
        schema=schema,
        message_type=message_types["CustomerCreated"],
        message_indexes=(1,),
    )

    good_summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource([{"order_id": "o-20", "amount": 620}]))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    serializer=order_serializer,
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
            Pipeline(IterableSource([{"customer_id": "c-1"}]))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    serializer=customer_serializer,
                    security=security,
                    security_protocol="SASL_SSL",
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert good_summary.records_written == 1
    assert bad_summary.records_written == 1

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    dlq = _CollectDLQSink()
    consumer_summary = await asyncio.wait_for(
        (
            Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=kafka_secure_schema_registry_config.bootstrap_servers,
                    group_id=f"agora-secure-proto-failure-{unique_suffix}",
                    deserializer=ProtobufSchemaRegistryDeserializer[dict[str, object]](
                        registry_client=kafka_secure_schema_registry_config.schema_registry_client(),
                        message_type=message_types["OrderCreated"],
                        record_mapper=lambda message: MessageToDict(
                            message,
                            preserving_proto_field_name=True,
                        ),
                    ),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=1,
                    max_idle_polls=2,
                    security=security,
                    security_protocol="SASL_SSL",
                    poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
                    poison_record_sink=dlq,
                    poison_record_pipeline_id="agora-secure-proto-binding-failure",
                )
            )
            .build(collected)  # type: ignore[arg-type]
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    assert consumer_summary.records_consumed == 1
    assert collected.records == [{"order_id": "o-20", "amount": 620}]
    assert len(dlq.records) == 1
    poison = dlq.records[0]
    assert poison.pipeline_id == "agora-secure-proto-binding-failure"
    assert poison.stage == "kafka_deserialize"
    assert poison.record["topic"] == topic
    assert poison.record["poison"] == {
        "classification": "schema_registry_binding_mismatch",
        "policy": "dlq_and_continue",
    }
    assert "binding mismatch" in poison.error_message.lower()
