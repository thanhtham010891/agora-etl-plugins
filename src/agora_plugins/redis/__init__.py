"""Official Redis plugin package for Agora.

Keep package-level exports lazy so entry-point discovery can import submodules
without tripping circular imports through ``agora_plugins.redis.__init__``.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora_plugins.redis.ai import RedisLLMCache
    from agora_plugins.redis.dedup.stores import RedisEmbeddingStore, RedisStore
    from agora_plugins.redis.dlq import RedisDLQSink, RedisDLQSource
    from agora_plugins.redis.kafka import (
        KafkaRedisDeliveryConfig,
        KafkaRedisEnterpriseAcceptanceFinding,
        KafkaRedisEnterpriseAcceptanceGate,
        KafkaRedisEnterpriseAcceptanceReport,
        KafkaRedisEnterpriseAcceptanceThresholds,
        KafkaRedisEnvelopeDeserializer,
        KafkaRedisPrometheusExporter,
        KafkaRedisRuntime,
        KafkaRedisRuntimeHealthSnapshot,
        KafkaRedisRuntimeMetricsSnapshot,
        KafkaRedisStorageConfig,
        build_kafka_redis_runtime,
        build_kafka_redis_sink,
        build_kafka_redis_source,
        wrap_kafka_redis_deserializer,
    )
    from agora_plugins.redis.observability import (
        RedisDLQSinkEnterpriseAcceptanceThresholds,
        RedisDLQSinkMetricsSnapshot,
        RedisDLQSourceEnterpriseAcceptanceThresholds,
        RedisDLQSourceMetricsSnapshot,
        RedisEnterpriseAcceptanceFinding,
        RedisEnterpriseAcceptanceGate,
        RedisEnterpriseAcceptanceReport,
        RedisPrometheusExporter,
        RedisSinkEnterpriseAcceptanceThresholds,
        RedisSourceEnterpriseAcceptanceThresholds,
        RedisSourcePoisonLoopRiskSnapshot,
        RedisStreamSourceHealthSnapshot,
        RedisStreamSourceMetricsSnapshot,
    )
    from agora_plugins.redis.plugin import MANIFEST, PluginManifest
    from agora_plugins.redis.sinks import RedisSink, RedisSinkMetricsSnapshot
    from agora_plugins.redis.sources import RedisStreamSource
    from agora_plugins.redis.state import RedisBackend

__all__ = [
    "MANIFEST",
    "KafkaRedisDeliveryConfig",
    "KafkaRedisEnterpriseAcceptanceFinding",
    "KafkaRedisEnterpriseAcceptanceGate",
    "KafkaRedisEnterpriseAcceptanceReport",
    "KafkaRedisEnterpriseAcceptanceThresholds",
    "KafkaRedisEnvelopeDeserializer",
    "KafkaRedisPrometheusExporter",
    "KafkaRedisRuntime",
    "KafkaRedisRuntimeHealthSnapshot",
    "KafkaRedisRuntimeMetricsSnapshot",
    "KafkaRedisStorageConfig",
    "PluginManifest",
    "RedisBackend",
    "RedisDLQSink",
    "RedisDLQSinkEnterpriseAcceptanceThresholds",
    "RedisDLQSinkMetricsSnapshot",
    "RedisDLQSource",
    "RedisDLQSourceEnterpriseAcceptanceThresholds",
    "RedisDLQSourceMetricsSnapshot",
    "RedisEmbeddingStore",
    "RedisEnterpriseAcceptanceFinding",
    "RedisEnterpriseAcceptanceGate",
    "RedisEnterpriseAcceptanceReport",
    "RedisLLMCache",
    "RedisPrometheusExporter",
    "RedisSink",
    "RedisSinkEnterpriseAcceptanceThresholds",
    "RedisSinkMetricsSnapshot",
    "RedisSourceEnterpriseAcceptanceThresholds",
    "RedisSourcePoisonLoopRiskSnapshot",
    "RedisStore",
    "RedisStreamSource",
    "RedisStreamSourceHealthSnapshot",
    "RedisStreamSourceMetricsSnapshot",
    "build_kafka_redis_runtime",
    "build_kafka_redis_sink",
    "build_kafka_redis_source",
    "wrap_kafka_redis_deserializer",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "KafkaRedisDeliveryConfig": ("agora_plugins.redis.kafka", "KafkaRedisDeliveryConfig"),
    "KafkaRedisEnterpriseAcceptanceFinding": (
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceFinding",
    ),
    "KafkaRedisEnterpriseAcceptanceGate": (
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceGate",
    ),
    "KafkaRedisEnterpriseAcceptanceReport": (
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceReport",
    ),
    "KafkaRedisEnterpriseAcceptanceThresholds": (
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceThresholds",
    ),
    "KafkaRedisEnvelopeDeserializer": (
        "agora_plugins.redis.kafka",
        "KafkaRedisEnvelopeDeserializer",
    ),
    "KafkaRedisPrometheusExporter": ("agora_plugins.redis.kafka", "KafkaRedisPrometheusExporter"),
    "KafkaRedisRuntime": ("agora_plugins.redis.kafka", "KafkaRedisRuntime"),
    "KafkaRedisRuntimeHealthSnapshot": (
        "agora_plugins.redis.kafka",
        "KafkaRedisRuntimeHealthSnapshot",
    ),
    "KafkaRedisRuntimeMetricsSnapshot": (
        "agora_plugins.redis.kafka",
        "KafkaRedisRuntimeMetricsSnapshot",
    ),
    "KafkaRedisStorageConfig": ("agora_plugins.redis.kafka", "KafkaRedisStorageConfig"),
    "MANIFEST": ("agora_plugins.redis.plugin", "MANIFEST"),
    "PluginManifest": ("agora_plugins.redis.plugin", "PluginManifest"),
    "RedisBackend": ("agora_plugins.redis.state", "RedisBackend"),
    "RedisDLQSinkEnterpriseAcceptanceThresholds": (
        "agora_plugins.redis.observability",
        "RedisDLQSinkEnterpriseAcceptanceThresholds",
    ),
    "RedisDLQSinkMetricsSnapshot": (
        "agora_plugins.redis.observability",
        "RedisDLQSinkMetricsSnapshot",
    ),
    "RedisDLQSink": ("agora_plugins.redis.dlq", "RedisDLQSink"),
    "RedisDLQSourceEnterpriseAcceptanceThresholds": (
        "agora_plugins.redis.observability",
        "RedisDLQSourceEnterpriseAcceptanceThresholds",
    ),
    "RedisDLQSourceMetricsSnapshot": (
        "agora_plugins.redis.observability",
        "RedisDLQSourceMetricsSnapshot",
    ),
    "RedisDLQSource": ("agora_plugins.redis.dlq", "RedisDLQSource"),
    "RedisEnterpriseAcceptanceFinding": (
        "agora_plugins.redis.observability",
        "RedisEnterpriseAcceptanceFinding",
    ),
    "RedisEnterpriseAcceptanceGate": (
        "agora_plugins.redis.observability",
        "RedisEnterpriseAcceptanceGate",
    ),
    "RedisEnterpriseAcceptanceReport": (
        "agora_plugins.redis.observability",
        "RedisEnterpriseAcceptanceReport",
    ),
    "RedisEmbeddingStore": ("agora_plugins.redis.dedup.stores", "RedisEmbeddingStore"),
    "RedisLLMCache": ("agora_plugins.redis.ai", "RedisLLMCache"),
    "RedisPrometheusExporter": ("agora_plugins.redis.observability", "RedisPrometheusExporter"),
    "RedisSourcePoisonLoopRiskSnapshot": (
        "agora_plugins.redis.observability",
        "RedisSourcePoisonLoopRiskSnapshot",
    ),
    "RedisSinkEnterpriseAcceptanceThresholds": (
        "agora_plugins.redis.observability",
        "RedisSinkEnterpriseAcceptanceThresholds",
    ),
    "RedisSink": ("agora_plugins.redis.sinks", "RedisSink"),
    "RedisSinkMetricsSnapshot": ("agora_plugins.redis.sinks", "RedisSinkMetricsSnapshot"),
    "RedisSourceEnterpriseAcceptanceThresholds": (
        "agora_plugins.redis.observability",
        "RedisSourceEnterpriseAcceptanceThresholds",
    ),
    "RedisStore": ("agora_plugins.redis.dedup.stores", "RedisStore"),
    "RedisStreamSource": ("agora_plugins.redis.sources", "RedisStreamSource"),
    "RedisStreamSourceHealthSnapshot": (
        "agora_plugins.redis.observability",
        "RedisStreamSourceHealthSnapshot",
    ),
    "RedisStreamSourceMetricsSnapshot": (
        "agora_plugins.redis.observability",
        "RedisStreamSourceMetricsSnapshot",
    ),
    "build_kafka_redis_runtime": ("agora_plugins.redis.kafka", "build_kafka_redis_runtime"),
    "build_kafka_redis_sink": ("agora_plugins.redis.kafka", "build_kafka_redis_sink"),
    "build_kafka_redis_source": ("agora_plugins.redis.kafka", "build_kafka_redis_source"),
    "wrap_kafka_redis_deserializer": (
        "agora_plugins.redis.kafka",
        "wrap_kafka_redis_deserializer",
    ),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
