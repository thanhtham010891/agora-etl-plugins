"""Builder helpers for Kafka -> Redis wedges."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from agora_plugins.redis._kafka_envelope import wrap_kafka_redis_deserializer
from agora_plugins.redis._kafka_models import KafkaRedisDeliveryConfig, KafkaRedisStorageConfig
from agora_plugins.redis.sinks.redis import RedisSink

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from agora_plugins.kafka import KafkaSecurityConfig
    from agora_plugins.kafka.sources.kafka import KafkaSource


KafkaRedisRecord = dict[str, object]


def build_kafka_redis_sink(
    *,
    url: str,
    key_fn: Callable[[KafkaRedisRecord], str] | None = None,
    serializer: Callable[[KafkaRedisRecord], object] | None = None,
    mode: str = "set",
    ttl_seconds: int | None = None,
    maxlen: int | None = None,
    delivery: KafkaRedisDeliveryConfig | None = None,
    storage: KafkaRedisStorageConfig | None = None,
) -> RedisSink[KafkaRedisRecord]:
    """Build a Redis sink with canonical Kafka wedge defaults."""

    resolved_delivery = delivery or KafkaRedisDeliveryConfig()
    resolved_storage = storage or KafkaRedisStorageConfig()
    return RedisSink[KafkaRedisRecord](
        url=url,
        key_fn=(
            (lambda record: default_key_fn(record, storage=resolved_storage))
            if key_fn is None
            else key_fn
        ),
        serializer=(
            (
                lambda record: default_serializer(
                    record,
                    mode=mode,
                    delivery=resolved_delivery,
                    storage=resolved_storage,
                )
            )
            if serializer is None
            else serializer
        ),
        mode=mode,
        ttl_seconds=ttl_seconds,
        maxlen=maxlen,
    )


def build_kafka_redis_source(
    *,
    topics: list[str] | None = None,
    topic_pattern: str | None = None,
    assignments: Iterable[tuple[str, int]] | None = None,
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "agora-consumer",
    deserializer: Callable[..., object],
    deserializer_metadata_aware: bool = False,
    include_metadata: bool = True,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    commit_every: int = 100,
    poll_timeout_ms: int = 1000,
    max_idle_polls: int | None = None,
    max_poll_records: int = 500,
    fetch_min_bytes: int = 1,
    fetch_max_wait_ms: int = 500,
    max_partition_fetch_bytes: int = 1_048_576,
    security_protocol: str = "PLAINTEXT",
    security: KafkaSecurityConfig | None = None,
    extra_config: dict[str, object] | None = None,
    start_offsets: dict[tuple[str, int], int] | None = None,
    rebalance_listener: object | None = None,
    health_snapshot_cache_ms: int = 250,
) -> KafkaSource[dict[str, object]]:
    """Build the canonical Kafka source for Kafka -> Redis wedges."""

    from agora_plugins.kafka import KafkaSource

    source_deserializer = (
        wrap_kafka_redis_deserializer(
            deserializer,
            metadata_aware=deserializer_metadata_aware,
        )
        if include_metadata
        else deserializer
    )
    source = KafkaSource[dict[str, object]](
        topics=topics,
        topic_pattern=topic_pattern,
        assignments=assignments,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        deserializer=source_deserializer,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        commit_every=commit_every,
        poll_timeout_ms=poll_timeout_ms,
        max_idle_polls=max_idle_polls,
        max_poll_records=max_poll_records,
        fetch_min_bytes=fetch_min_bytes,
        fetch_max_wait_ms=fetch_max_wait_ms,
        max_partition_fetch_bytes=max_partition_fetch_bytes,
        security_protocol=security_protocol,
        security=security,
        extra_config=extra_config,
        start_offsets=start_offsets,
        rebalance_listener=rebalance_listener,
        health_snapshot_cache_ms=health_snapshot_cache_ms,
    )
    return source


def default_key_fn(
    record: KafkaRedisRecord,
    *,
    storage: KafkaRedisStorageConfig,
) -> str:
    if storage.redis_key_field not in record:
        raise KeyError(
            "Kafka -> Redis helper requires transformed records to include "
            f"{storage.redis_key_field!r}."
        )
    return str(record[storage.redis_key_field])


def payload_with_delivery_fields(
    record: KafkaRedisRecord,
    *,
    payload: object,
    delivery: KafkaRedisDeliveryConfig,
    storage: KafkaRedisStorageConfig,
) -> KafkaRedisRecord:
    enriched = dict(payload) if isinstance(payload, Mapping) else {storage.value_field: payload}
    delivery_key = record.get(delivery.key_field)
    if delivery_key is not None and delivery.key_field not in enriched:
        enriched[delivery.key_field] = delivery_key
    metadata_field = delivery.metadata_field
    if metadata_field is not None:
        delivery_metadata = record.get(metadata_field)
        if delivery_metadata is not None and metadata_field not in enriched:
            enriched[metadata_field] = delivery_metadata
    return enriched


def default_serializer(
    record: KafkaRedisRecord,
    *,
    mode: str,
    delivery: KafkaRedisDeliveryConfig,
    storage: KafkaRedisStorageConfig,
) -> object:
    if storage.value_field in record:
        payload = record[storage.value_field]
    else:
        payload = {key: value for key, value in record.items() if key != storage.redis_key_field}
    if storage.preserve_delivery_fields_in_value:
        payload = payload_with_delivery_fields(
            record,
            payload=payload,
            delivery=delivery,
            storage=storage,
        )
    if mode == "xadd":
        if not isinstance(payload, Mapping):
            raise TypeError(
                "Kafka -> Redis helper requires 'value' to be a mapping when mode='xadd'."
            )
        return {
            str(key): (
                value
                if isinstance(value, bytes | bytearray | memoryview | str | int | float)
                else json.dumps(value, sort_keys=True, default=str)
            )
            for key, value in payload.items()
        }
    if isinstance(payload, (bytes, bytearray, memoryview, str)):
        return payload
    return json.dumps(payload, sort_keys=True, default=str)
