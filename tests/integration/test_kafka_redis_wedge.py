from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from agora import IterableSource, Pipeline

from agora_plugins.kafka import KafkaSink
from agora_plugins.redis import (
    KafkaRedisEnterpriseAcceptanceThresholds,
    build_kafka_redis_runtime,
    build_kafka_redis_source,
)
from tests.integration._runtime_readiness import assert_runtime_readiness

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 20.0
_RUNTIME_READINESS_THRESHOLDS = KafkaRedisEnterpriseAcceptanceThresholds(
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
) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        try:
            await admin.create_topics(
                [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)]
            )
        except TopicAlreadyExistsError:
            return
    finally:
        await admin.close()


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


def _records_at_or_after_partition_offsets(
    source_records: list[dict[str, object]],
    offsets_by_partition: dict[int, int],
) -> list[dict[str, object]]:
    return [
        record
        for record in source_records
        if int(record["sequence"]) >= offsets_by_partition[int(record["partition"])]
    ]


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
                    partition_fn=lambda record: int(record["partition"]),
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


def _make_customer_redis_transform(
    key_prefix: str,
):
    def _transform(record: dict[str, object]) -> dict[str, object]:
        payload = record["payload"]
        metadata = record["metadata"]
        assert isinstance(payload, dict)
        assert isinstance(metadata, dict)
        headers = metadata["headers"]
        assert isinstance(headers, list)
        event_id = int(payload["id"])
        return {
            "redis_key": f"{key_prefix}:customer:{event_id}",
            "value": {
                "event_id": event_id,
                "display_name": str(payload["name"]).upper(),
                "tenant": _header_value(headers, "tenant"),
                "event_type": _header_value(headers, "event_type"),
                "kafka_topic": metadata["topic"],
                "kafka_partition": metadata["partition"],
                "kafka_offset": metadata["offset"],
            },
        }

    return _transform


def _expected_redis_entries(
    key_prefix: str,
    topic: str,
    source_records: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    expected: dict[str, dict[str, object]] = {}
    for record in source_records:
        payload = record["payload"]
        assert isinstance(payload, dict)
        event_id = int(payload["id"])
        expected[f"{key_prefix}:customer:{event_id}"] = {
            "event_id": event_id,
            "display_name": str(payload["name"]).upper(),
            "tenant": "acme",
            "event_type": str(dict(record["headers"])["event_type"]),
            "kafka_topic": topic,
            "kafka_partition": int(record["partition"]),
            "kafka_offset": int(record["sequence"]),
        }
    return expected


def _fetch_redis_entries(client: Any, key_prefix: str) -> dict[str, dict[str, object]]:
    entries: dict[str, dict[str, object]] = {}
    keys = sorted(client.scan_iter(match=f"{key_prefix}:*"))
    for key in keys:
        raw = client.get(key)
        if raw is None:
            continue
        entries[str(key)] = json.loads(str(raw))
    return entries


@pytest.mark.asyncio
async def test_kafka_transform_redis_wedge_seek_and_replay_window_stay_idempotent(
    kafka_bootstrap: str,
    redis_url: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")
    redis = pytest.importorskip("redis")

    topic = f"agora-wedge-redis-{unique_suffix}"
    key_prefix = f"agora:wedge:redis:{unique_suffix}"
    source_records = _partitioned_customer_records(partitions=2, records_per_partition=4)
    first_offsets = {0: 1, 1: 2}
    first_window_records = _records_at_or_after_partition_offsets(source_records, first_offsets)
    replay_offsets = {0: 3, 1: 3}
    replay_window_records = _records_at_or_after_partition_offsets(source_records, replay_offsets)

    client = redis.Redis.from_url(redis_url, decode_responses=True)
    source = build_kafka_redis_source(
        assignments=[(topic, 0), (topic, 1)],
        bootstrap_servers=kafka_bootstrap,
        group_id=f"agora-wedge-redis-{unique_suffix}",
        deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        max_idle_polls=2,
    )
    runtime = build_kafka_redis_runtime(
        source=source,
        url=redis_url,
        transform=_make_customer_redis_transform(key_prefix),
        serializer=lambda row: json.dumps(row["value"], sort_keys=True),
    )

    try:
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

        await runtime.open()
        try:
            await runtime.seek_to_offsets(
                {(topic, partition): offset for partition, offset in first_offsets.items()}
            )
            first_pass_records = await asyncio.wait_for(
                runtime.drain(max_records=len(first_window_records)),
                timeout=_INTEGRATION_TIMEOUT_S,
            )

            await runtime.seek_to_offsets(
                {(topic, partition): offset for partition, offset in replay_offsets.items()}
            )
            replay_records = await asyncio.wait_for(
                runtime.drain(max_records=len(replay_window_records)),
                timeout=_INTEGRATION_TIMEOUT_S,
            )
            snapshot, report = await assert_runtime_readiness(
                runtime,
                _RUNTIME_READINESS_THRESHOLDS,
            )
            rendered = await runtime.render_prometheus_metrics(namespace="agora_kafka_redis")
        finally:
            await runtime.close()

        stored_entries = _fetch_redis_entries(client, key_prefix)
    finally:
        keys = list(client.scan_iter(match=f"{key_prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()

    assert sorted(
        (int(record["metadata"]["partition"]), int(record["metadata"]["offset"]))
        for record in first_pass_records
    ) == [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3)]
    assert sorted(
        (int(record["metadata"]["partition"]), int(record["metadata"]["offset"]))
        for record in replay_records
    ) == [(0, 3), (1, 3)]
    assert stored_entries == _expected_redis_entries(key_prefix, topic, first_window_records)
    assert snapshot.delivery_key_field == "kafka_delivery_key"
    assert snapshot.delivery_metadata_field == "kafka_metadata"
    assert snapshot.health.ready is True
    assert snapshot.health.source_ready is True
    assert snapshot.health.source_stalled is False
    assert snapshot.health.sink_connection_ready is True
    assert report.passed is True
    assert snapshot.sink.mode == "set"
    assert snapshot.sink.write_call_count == len(first_window_records) + len(replay_window_records)
    assert snapshot.sink.written_record_count == len(first_window_records) + len(
        replay_window_records
    )
    assert 'agora_kafka_redis_runtime_state{consumer_group="' in rendered
    assert 'agora_kafka_redis_runtime_config{consumer_group="' in rendered
