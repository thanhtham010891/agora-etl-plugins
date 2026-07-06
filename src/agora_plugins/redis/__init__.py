"""Official Redis plugin package for Agora.

Keep package-level exports lazy so entry-point discovery can import submodules
without tripping circular imports through ``agora_plugins.redis.__init__``.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from agora_plugins._surface_manifest import SurfaceExport, export_target_map

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

_STABLE_PUBLIC_EXPORTS = frozenset(
    {
        "MANIFEST",
        "PluginManifest",
        "RedisBackend",
        "RedisDLQSink",
        "RedisDLQSource",
        "RedisEmbeddingStore",
        "RedisLLMCache",
        "RedisSink",
        "RedisStore",
        "RedisStreamSource",
    }
)

_SUPPORTABILITY_PUBLIC_EXPORTS = frozenset(
    {
        "RedisDLQSinkEnterpriseAcceptanceThresholds",
        "RedisDLQSinkMetricsSnapshot",
        "RedisDLQSourceEnterpriseAcceptanceThresholds",
        "RedisDLQSourceMetricsSnapshot",
        "RedisEnterpriseAcceptanceFinding",
        "RedisEnterpriseAcceptanceGate",
        "RedisEnterpriseAcceptanceReport",
        "RedisPrometheusExporter",
        "RedisSinkEnterpriseAcceptanceThresholds",
        "RedisSinkMetricsSnapshot",
        "RedisSourceEnterpriseAcceptanceThresholds",
        "RedisSourcePoisonLoopRiskSnapshot",
        "RedisStreamSourceHealthSnapshot",
        "RedisStreamSourceMetricsSnapshot",
    }
)

_PATTERN_RECIPE_EXPORTS = frozenset(
    {
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
        "build_kafka_redis_runtime",
        "build_kafka_redis_sink",
        "build_kafka_redis_source",
        "wrap_kafka_redis_deserializer",
    }
)

_INTERNAL_BRIDGE_EXPORTS = frozenset({"_doctor_readiness_provider"})


def _surface_note(name: str) -> str:
    if name in _STABLE_PUBLIC_EXPORTS:
        return "Stable Redis family primitive/cache/state public surface."
    if name in _SUPPORTABILITY_PUBLIC_EXPORTS:
        return "Redis supportability, diagnostics, or observability public surface."
    return "Redis composite Kafka wedge or pattern-oriented helper surface."


_SURFACE_EXPORTS: dict[str, SurfaceExport] = {
    "MANIFEST": SurfaceExport(
        "agora_plugins.redis.plugin",
        "MANIFEST",
        "stable_public",
        _surface_note("MANIFEST"),
    ),
    "KafkaRedisDeliveryConfig": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisDeliveryConfig",
        "pattern_recipe",
        _surface_note("KafkaRedisDeliveryConfig"),
    ),
    "KafkaRedisEnterpriseAcceptanceFinding": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceFinding",
        "pattern_recipe",
        _surface_note("KafkaRedisEnterpriseAcceptanceFinding"),
    ),
    "KafkaRedisEnterpriseAcceptanceGate": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceGate",
        "pattern_recipe",
        _surface_note("KafkaRedisEnterpriseAcceptanceGate"),
    ),
    "KafkaRedisEnterpriseAcceptanceReport": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceReport",
        "pattern_recipe",
        _surface_note("KafkaRedisEnterpriseAcceptanceReport"),
    ),
    "KafkaRedisEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisEnterpriseAcceptanceThresholds",
        "pattern_recipe",
        _surface_note("KafkaRedisEnterpriseAcceptanceThresholds"),
    ),
    "KafkaRedisEnvelopeDeserializer": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisEnvelopeDeserializer",
        "pattern_recipe",
        _surface_note("KafkaRedisEnvelopeDeserializer"),
    ),
    "KafkaRedisPrometheusExporter": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisPrometheusExporter",
        "pattern_recipe",
        _surface_note("KafkaRedisPrometheusExporter"),
    ),
    "KafkaRedisRuntime": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisRuntime",
        "pattern_recipe",
        _surface_note("KafkaRedisRuntime"),
    ),
    "KafkaRedisRuntimeHealthSnapshot": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisRuntimeHealthSnapshot",
        "pattern_recipe",
        _surface_note("KafkaRedisRuntimeHealthSnapshot"),
    ),
    "KafkaRedisRuntimeMetricsSnapshot": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisRuntimeMetricsSnapshot",
        "pattern_recipe",
        _surface_note("KafkaRedisRuntimeMetricsSnapshot"),
    ),
    "KafkaRedisStorageConfig": SurfaceExport(
        "agora_plugins.redis.kafka",
        "KafkaRedisStorageConfig",
        "pattern_recipe",
        _surface_note("KafkaRedisStorageConfig"),
    ),
    "PluginManifest": SurfaceExport(
        "agora_plugins.redis.plugin",
        "PluginManifest",
        "stable_public",
        _surface_note("PluginManifest"),
    ),
    "RedisBackend": SurfaceExport(
        "agora_plugins.redis.state",
        "RedisBackend",
        "stable_public",
        _surface_note("RedisBackend"),
    ),
    "RedisDLQSink": SurfaceExport(
        "agora_plugins.redis.dlq",
        "RedisDLQSink",
        "stable_public",
        _surface_note("RedisDLQSink"),
    ),
    "RedisDLQSinkEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisDLQSinkEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("RedisDLQSinkEnterpriseAcceptanceThresholds"),
    ),
    "RedisDLQSinkMetricsSnapshot": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisDLQSinkMetricsSnapshot",
        "supportability_public",
        _surface_note("RedisDLQSinkMetricsSnapshot"),
    ),
    "RedisDLQSource": SurfaceExport(
        "agora_plugins.redis.dlq",
        "RedisDLQSource",
        "stable_public",
        _surface_note("RedisDLQSource"),
    ),
    "RedisDLQSourceEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisDLQSourceEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("RedisDLQSourceEnterpriseAcceptanceThresholds"),
    ),
    "RedisDLQSourceMetricsSnapshot": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisDLQSourceMetricsSnapshot",
        "supportability_public",
        _surface_note("RedisDLQSourceMetricsSnapshot"),
    ),
    "RedisEmbeddingStore": SurfaceExport(
        "agora_plugins.redis.dedup.stores",
        "RedisEmbeddingStore",
        "stable_public",
        _surface_note("RedisEmbeddingStore"),
    ),
    "RedisEnterpriseAcceptanceFinding": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisEnterpriseAcceptanceFinding",
        "supportability_public",
        _surface_note("RedisEnterpriseAcceptanceFinding"),
    ),
    "RedisEnterpriseAcceptanceGate": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisEnterpriseAcceptanceGate",
        "supportability_public",
        _surface_note("RedisEnterpriseAcceptanceGate"),
    ),
    "RedisEnterpriseAcceptanceReport": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisEnterpriseAcceptanceReport",
        "supportability_public",
        _surface_note("RedisEnterpriseAcceptanceReport"),
    ),
    "RedisLLMCache": SurfaceExport(
        "agora_plugins.redis.ai",
        "RedisLLMCache",
        "stable_public",
        _surface_note("RedisLLMCache"),
    ),
    "RedisPrometheusExporter": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisPrometheusExporter",
        "supportability_public",
        _surface_note("RedisPrometheusExporter"),
    ),
    "RedisSink": SurfaceExport(
        "agora_plugins.redis.sinks",
        "RedisSink",
        "stable_public",
        _surface_note("RedisSink"),
    ),
    "RedisSinkEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisSinkEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("RedisSinkEnterpriseAcceptanceThresholds"),
    ),
    "RedisSinkMetricsSnapshot": SurfaceExport(
        "agora_plugins.redis.sinks",
        "RedisSinkMetricsSnapshot",
        "supportability_public",
        _surface_note("RedisSinkMetricsSnapshot"),
    ),
    "RedisSourceEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisSourceEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("RedisSourceEnterpriseAcceptanceThresholds"),
    ),
    "RedisSourcePoisonLoopRiskSnapshot": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisSourcePoisonLoopRiskSnapshot",
        "supportability_public",
        _surface_note("RedisSourcePoisonLoopRiskSnapshot"),
    ),
    "RedisStore": SurfaceExport(
        "agora_plugins.redis.dedup.stores",
        "RedisStore",
        "stable_public",
        _surface_note("RedisStore"),
    ),
    "RedisStreamSource": SurfaceExport(
        "agora_plugins.redis.sources",
        "RedisStreamSource",
        "stable_public",
        _surface_note("RedisStreamSource"),
    ),
    "RedisStreamSourceHealthSnapshot": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisStreamSourceHealthSnapshot",
        "supportability_public",
        _surface_note("RedisStreamSourceHealthSnapshot"),
    ),
    "RedisStreamSourceMetricsSnapshot": SurfaceExport(
        "agora_plugins.redis.observability",
        "RedisStreamSourceMetricsSnapshot",
        "supportability_public",
        _surface_note("RedisStreamSourceMetricsSnapshot"),
    ),
    "build_kafka_redis_runtime": SurfaceExport(
        "agora_plugins.redis.kafka",
        "build_kafka_redis_runtime",
        "pattern_recipe",
        _surface_note("build_kafka_redis_runtime"),
    ),
    "build_kafka_redis_sink": SurfaceExport(
        "agora_plugins.redis.kafka",
        "build_kafka_redis_sink",
        "pattern_recipe",
        _surface_note("build_kafka_redis_sink"),
    ),
    "build_kafka_redis_source": SurfaceExport(
        "agora_plugins.redis.kafka",
        "build_kafka_redis_source",
        "pattern_recipe",
        _surface_note("build_kafka_redis_source"),
    ),
    "wrap_kafka_redis_deserializer": SurfaceExport(
        "agora_plugins.redis.kafka",
        "wrap_kafka_redis_deserializer",
        "pattern_recipe",
        _surface_note("wrap_kafka_redis_deserializer"),
    ),
}

_EXPORTS = export_target_map(_SURFACE_EXPORTS)
_EXPORTS["_doctor_readiness_provider"] = (
    "agora_plugins.redis.doctor",
    "DOCTOR_READINESS_PROVIDER",
)


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
