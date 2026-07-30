"""Public delivery-semantics declarations for first-party sinks."""

from __future__ import annotations

import pytest
from agora import DeliveryConfig, Pipeline
from agora.core.checkpoint import InMemoryCheckpointStore
from agora.core.delivery import DeliveryPolicy, IdempotencyMode
from agora.sources.file import CsvSource

from agora_plugins.bigquery import BigQuerySink, BigQueryStorageWriteSink
from agora_plugins.kafka import KafkaSink
from agora_plugins.postgres import PostgresSink
from agora_plugins.redis import RedisSink
from agora_plugins.s3 import S3Sink


def test_redis_delivery_capability_is_mode_specific() -> None:
    set_capability = RedisSink(
        url="redis://localhost:6379",
        key_fn=str,
        mode="set",
    ).delivery_capability()
    explicit_set_capability = RedisSink(
        url="redis://localhost:6379",
        key_fn=str,
        mode="set",
        replay_safe_key_contract=True,
    ).delivery_capability()
    list_capability = RedisSink(
        url="redis://localhost:6379",
        key_fn=str,
        mode="lpush",
    ).delivery_capability()
    stream_capability = RedisSink(
        url="redis://localhost:6379",
        key_fn=str,
        mode="xadd",
    ).delivery_capability()

    assert (set_capability.idempotency, set_capability.replay_safe) == (
        IdempotencyMode.APPLICATION_MANAGED,
        False,
    )
    assert (explicit_set_capability.idempotency, explicit_set_capability.replay_safe) == (
        IdempotencyMode.APPLICATION_MANAGED,
        True,
    )
    assert (list_capability.idempotency, list_capability.replay_safe) == (
        IdempotencyMode.SINK_NATIVE,
        False,
    )
    assert (stream_capability.idempotency, stream_capability.replay_safe) == (
        IdempotencyMode.NONE,
        False,
    )


def test_kafka_delivery_capability_does_not_claim_checkpoint_coupling() -> None:
    idempotent = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda value: value.encode(),
    ).delivery_capability()
    transactional = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda value: value.encode(),
        transactional_id="agora-test",
        transaction_per_batch=True,
    ).delivery_capability()

    assert (idempotent.idempotency, idempotent.replay_safe) == (
        IdempotencyMode.SINK_NATIVE,
        False,
    )
    assert (transactional.idempotency, transactional.replay_safe) == (
        IdempotencyMode.TRANSACTIONAL,
        False,
    )
    assert transactional.transactionally_coupled_checkpoint is False


def test_postgres_delivery_capability_requires_upsert() -> None:
    upsert = PostgresSink(
        dsn="postgresql://localhost/agora",
        table="events",
        row_mapper=lambda value: {"id": value},
        conflict_key="id",
        upsert=True,
    ).delivery_capability()
    explicit_upsert = PostgresSink(
        dsn="postgresql://localhost/agora",
        table="events",
        row_mapper=lambda value: {"id": value},
        conflict_key="id",
        upsert=True,
        replay_safe_key_contract=True,
    ).delivery_capability()
    append = PostgresSink(
        dsn="postgresql://localhost/agora",
        table="events",
        row_mapper=lambda value: {"id": value},
        conflict_key="id",
        upsert=False,
    ).delivery_capability()

    assert (upsert.idempotency, upsert.replay_safe) == (
        IdempotencyMode.APPLICATION_MANAGED,
        False,
    )
    assert (explicit_upsert.idempotency, explicit_upsert.replay_safe) == (
        IdempotencyMode.APPLICATION_MANAGED,
        True,
    )
    assert (append.idempotency, append.replay_safe) == (IdempotencyMode.NONE, False)


def test_replay_safe_key_contract_requires_a_compatible_write_mode() -> None:
    with pytest.raises(TypeError, match="must be a bool"):
        PostgresSink(
            dsn="postgresql://localhost/agora",
            table="events",
            row_mapper=lambda value: {"id": value},
            conflict_key="id",
            replay_safe_key_contract="true",  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="requires upsert=True"):
        PostgresSink(
            dsn="postgresql://localhost/agora",
            table="events",
            row_mapper=lambda value: {"id": value},
            conflict_key="id",
            upsert=False,
            replay_safe_key_contract=True,
        )

    with pytest.raises(TypeError, match="must be a bool"):
        RedisSink(
            url="redis://localhost:6379",
            key_fn=str,
            replay_safe_key_contract="true",  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="requires mode='set'"):
        RedisSink(
            url="redis://localhost:6379",
            key_fn=str,
            mode="xadd",
            replay_safe_key_contract=True,
        )


def test_bigquery_and_s3_sinks_report_replay_risk() -> None:
    capabilities = (
        BigQuerySink(table="analytics.events").delivery_capability(),
        BigQueryStorageWriteSink(
            table="projects/demo/datasets/analytics/tables/events",
            stream_factory=lambda *_args: object(),
            validate_table_access=False,
        ).delivery_capability(),
        S3Sink(bucket="demo-bucket", format="jsonl", client=object()).delivery_capability(),
    )

    assert all(capability.idempotency is IdempotencyMode.NONE for capability in capabilities)
    assert all(capability.replay_safe is False for capability in capabilities)


def test_delivery_policy_rejects_unverified_application_keys(tmp_path) -> None:
    path = tmp_path / "records.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    source = CsvSource(path=path, row_mapper=lambda row: row)
    config = DeliveryConfig(
        checkpoint=InMemoryCheckpointStore(),
        delivery_policy=DeliveryPolicy(require_replay_safe=True),
    )

    postgres = PostgresSink(
        dsn="postgresql://localhost/agora",
        table="events",
        row_mapper=lambda value: {"id": value["id"]},
        conflict_key="id",
    )
    redis = RedisSink(url="redis://localhost:6379", key_fn=lambda value: value["id"])

    assert [
        finding.code
        for finding in Pipeline(source)
        .build(postgres, config=config)
        .explain()
        .delivery.policy_mismatches
    ] == ["sink_not_replay_safe"]
    assert [
        finding.code
        for finding in Pipeline(source)
        .build(redis, config=config)
        .explain()
        .delivery.policy_mismatches
    ] == ["sink_not_replay_safe"]
