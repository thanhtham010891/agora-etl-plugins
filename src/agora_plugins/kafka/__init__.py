"""Official Kafka plugin package for Agora."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from agora_plugins._surface_manifest import SurfaceExport, export_target_map

if TYPE_CHECKING:
    from agora_plugins.kafka.config import (
        KafkaConfig,
        KafkaPluginConfig,
        KafkaSASLConfig,
        KafkaSecurityConfig,
        KafkaTLSConfig,
    )
    from agora_plugins.kafka.dlq import (
        DLQPayloadPolicy,
        KafkaDLQPrometheusExporter,
        KafkaDLQSink,
        KafkaDLQSinkMetricsSnapshot,
        KafkaDLQSource,
        KafkaDLQSourceMetricsSnapshot,
    )
    from agora_plugins.kafka.metrics import (
        KafkaSourceMetricsSnapshot,
        KafkaSourcePrometheusExporter,
    )
    from agora_plugins.kafka.plugin import MANIFEST, PluginManifest
    from agora_plugins.kafka.runtime import KafkaSourceRuntime, KafkaTransformSinkRuntime
    from agora_plugins.kafka.schema_registry import (
        AvroSchemaRegistryDeserializer,
        AvroSchemaRegistrySerializer,
        ConfluentSchemaRegistryClient,
        JsonSchemaRegistryDeserializer,
        JsonSchemaRegistrySerializer,
        PooledConfluentSchemaRegistryClient,
        ProtobufSchemaRegistryDeserializer,
        ProtobufSchemaRegistrySerializer,
        RegisteredSchema,
        SchemaAutoRegisterMode,
        SchemaRegistryClient,
    )
    from agora_plugins.kafka.sinks import KafkaSink, KafkaSinkMessage
    from agora_plugins.kafka.sources import (
        KafkaDeliveryContext,
        KafkaPartitionHealth,
        KafkaPoisonRecordClassification,
        KafkaPoisonRecordInfo,
        KafkaPoisonRecordPolicy,
        KafkaSource,
        KafkaSourceHealthSnapshot,
        KafkaSourceOperationalMetrics,
    )
    from agora_plugins.kafka.tracing import KafkaOpenTelemetryTracing

__all__ = [
    "MANIFEST",
    "AvroSchemaRegistryDeserializer",
    "AvroSchemaRegistrySerializer",
    "ConfluentSchemaRegistryClient",
    "DLQPayloadPolicy",
    "JsonSchemaRegistryDeserializer",
    "JsonSchemaRegistrySerializer",
    "KafkaConfig",
    "KafkaDLQPrometheusExporter",
    "KafkaDLQSink",
    "KafkaDLQSinkMetricsSnapshot",
    "KafkaDLQSource",
    "KafkaDLQSourceMetricsSnapshot",
    "KafkaDeliveryContext",
    "KafkaOpenTelemetryTracing",
    "KafkaPartitionHealth",
    "KafkaPluginConfig",
    "KafkaPoisonRecordClassification",
    "KafkaPoisonRecordInfo",
    "KafkaPoisonRecordPolicy",
    "KafkaSASLConfig",
    "KafkaSecurityConfig",
    "KafkaSink",
    "KafkaSinkMessage",
    "KafkaSource",
    "KafkaSourceHealthSnapshot",
    "KafkaSourceMetricsSnapshot",
    "KafkaSourceOperationalMetrics",
    "KafkaSourcePrometheusExporter",
    "KafkaSourceRuntime",
    "KafkaTLSConfig",
    "KafkaTransformSinkRuntime",
    "PluginManifest",
    "PooledConfluentSchemaRegistryClient",
    "ProtobufSchemaRegistryDeserializer",
    "ProtobufSchemaRegistrySerializer",
    "RegisteredSchema",
    "SchemaAutoRegisterMode",
    "SchemaRegistryClient",
]

_STABLE_PUBLIC_EXPORTS = frozenset(
    {
        "MANIFEST",
        "PluginManifest",
        "KafkaConfig",
        "KafkaPluginConfig",
        "KafkaSASLConfig",
        "KafkaSecurityConfig",
        "KafkaTLSConfig",
        "KafkaSink",
        "KafkaSinkMessage",
        "KafkaSource",
        "KafkaDLQSink",
        "KafkaDLQSource",
        "DLQPayloadPolicy",
        "KafkaDeliveryContext",
        "AvroSchemaRegistryDeserializer",
        "AvroSchemaRegistrySerializer",
        "ConfluentSchemaRegistryClient",
        "JsonSchemaRegistryDeserializer",
        "JsonSchemaRegistrySerializer",
        "PooledConfluentSchemaRegistryClient",
        "ProtobufSchemaRegistryDeserializer",
        "ProtobufSchemaRegistrySerializer",
        "RegisteredSchema",
        "SchemaAutoRegisterMode",
        "SchemaRegistryClient",
    }
)

_SUPPORTABILITY_PUBLIC_EXPORTS = frozenset(
    {
        "KafkaDLQPrometheusExporter",
        "KafkaDLQSinkMetricsSnapshot",
        "KafkaDLQSourceMetricsSnapshot",
        "KafkaOpenTelemetryTracing",
        "KafkaPartitionHealth",
        "KafkaPoisonRecordClassification",
        "KafkaPoisonRecordInfo",
        "KafkaPoisonRecordPolicy",
        "KafkaSourceHealthSnapshot",
        "KafkaSourceMetricsSnapshot",
        "KafkaSourceOperationalMetrics",
        "KafkaSourcePrometheusExporter",
    }
)

_PATTERN_RECIPE_EXPORTS = frozenset(
    {
        "KafkaSourceRuntime",
        "KafkaTransformSinkRuntime",
    }
)

_INTERNAL_BRIDGE_EXPORTS = frozenset({"_doctor_readiness_provider"})


def _surface_note(name: str) -> str:
    if name in _STABLE_PUBLIC_EXPORTS:
        return "Stable Kafka family primitive/config/schema public surface."
    if name in _SUPPORTABILITY_PUBLIC_EXPORTS:
        return "Kafka supportability, tracing, or observability public surface."
    return "Kafka runtime helper surface; treat as pattern-oriented integration support."


_SURFACE_EXPORTS: dict[str, SurfaceExport] = {
    "MANIFEST": SurfaceExport(
        "agora_plugins.kafka.plugin",
        "MANIFEST",
        "stable_public",
        _surface_note("MANIFEST"),
    ),
    "PluginManifest": SurfaceExport(
        "agora_plugins.kafka.plugin",
        "PluginManifest",
        "stable_public",
        _surface_note("PluginManifest"),
    ),
    "KafkaConfig": SurfaceExport(
        "agora_plugins.kafka.config",
        "KafkaConfig",
        "stable_public",
        _surface_note("KafkaConfig"),
    ),
    "KafkaPluginConfig": SurfaceExport(
        "agora_plugins.kafka.config",
        "KafkaPluginConfig",
        "stable_public",
        _surface_note("KafkaPluginConfig"),
    ),
    "KafkaSASLConfig": SurfaceExport(
        "agora_plugins.kafka.config",
        "KafkaSASLConfig",
        "stable_public",
        _surface_note("KafkaSASLConfig"),
    ),
    "KafkaSecurityConfig": SurfaceExport(
        "agora_plugins.kafka.config",
        "KafkaSecurityConfig",
        "stable_public",
        _surface_note("KafkaSecurityConfig"),
    ),
    "KafkaTLSConfig": SurfaceExport(
        "agora_plugins.kafka.config",
        "KafkaTLSConfig",
        "stable_public",
        _surface_note("KafkaTLSConfig"),
    ),
    "DLQPayloadPolicy": SurfaceExport(
        "agora_plugins.kafka.dlq",
        "DLQPayloadPolicy",
        "stable_public",
        _surface_note("DLQPayloadPolicy"),
    ),
    "KafkaDLQPrometheusExporter": SurfaceExport(
        "agora_plugins.kafka.dlq",
        "KafkaDLQPrometheusExporter",
        "supportability_public",
        _surface_note("KafkaDLQPrometheusExporter"),
    ),
    "KafkaDLQSink": SurfaceExport(
        "agora_plugins.kafka.dlq",
        "KafkaDLQSink",
        "stable_public",
        _surface_note("KafkaDLQSink"),
    ),
    "KafkaDLQSinkMetricsSnapshot": SurfaceExport(
        "agora_plugins.kafka.dlq",
        "KafkaDLQSinkMetricsSnapshot",
        "supportability_public",
        _surface_note("KafkaDLQSinkMetricsSnapshot"),
    ),
    "KafkaDLQSource": SurfaceExport(
        "agora_plugins.kafka.dlq",
        "KafkaDLQSource",
        "stable_public",
        _surface_note("KafkaDLQSource"),
    ),
    "KafkaDLQSourceMetricsSnapshot": SurfaceExport(
        "agora_plugins.kafka.dlq",
        "KafkaDLQSourceMetricsSnapshot",
        "supportability_public",
        _surface_note("KafkaDLQSourceMetricsSnapshot"),
    ),
    "KafkaSourceMetricsSnapshot": SurfaceExport(
        "agora_plugins.kafka.metrics",
        "KafkaSourceMetricsSnapshot",
        "supportability_public",
        _surface_note("KafkaSourceMetricsSnapshot"),
    ),
    "KafkaSourcePrometheusExporter": SurfaceExport(
        "agora_plugins.kafka.metrics",
        "KafkaSourcePrometheusExporter",
        "supportability_public",
        _surface_note("KafkaSourcePrometheusExporter"),
    ),
    "KafkaSourceRuntime": SurfaceExport(
        "agora_plugins.kafka.runtime",
        "KafkaSourceRuntime",
        "pattern_recipe",
        _surface_note("KafkaSourceRuntime"),
    ),
    "KafkaTransformSinkRuntime": SurfaceExport(
        "agora_plugins.kafka.runtime",
        "KafkaTransformSinkRuntime",
        "pattern_recipe",
        _surface_note("KafkaTransformSinkRuntime"),
    ),
    "AvroSchemaRegistryDeserializer": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "AvroSchemaRegistryDeserializer",
        "stable_public",
        _surface_note("AvroSchemaRegistryDeserializer"),
    ),
    "AvroSchemaRegistrySerializer": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "AvroSchemaRegistrySerializer",
        "stable_public",
        _surface_note("AvroSchemaRegistrySerializer"),
    ),
    "ConfluentSchemaRegistryClient": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "ConfluentSchemaRegistryClient",
        "stable_public",
        _surface_note("ConfluentSchemaRegistryClient"),
    ),
    "JsonSchemaRegistryDeserializer": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "JsonSchemaRegistryDeserializer",
        "stable_public",
        _surface_note("JsonSchemaRegistryDeserializer"),
    ),
    "JsonSchemaRegistrySerializer": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "JsonSchemaRegistrySerializer",
        "stable_public",
        _surface_note("JsonSchemaRegistrySerializer"),
    ),
    "PooledConfluentSchemaRegistryClient": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "PooledConfluentSchemaRegistryClient",
        "stable_public",
        _surface_note("PooledConfluentSchemaRegistryClient"),
    ),
    "ProtobufSchemaRegistryDeserializer": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "ProtobufSchemaRegistryDeserializer",
        "stable_public",
        _surface_note("ProtobufSchemaRegistryDeserializer"),
    ),
    "ProtobufSchemaRegistrySerializer": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "ProtobufSchemaRegistrySerializer",
        "stable_public",
        _surface_note("ProtobufSchemaRegistrySerializer"),
    ),
    "RegisteredSchema": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "RegisteredSchema",
        "stable_public",
        _surface_note("RegisteredSchema"),
    ),
    "SchemaAutoRegisterMode": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "SchemaAutoRegisterMode",
        "stable_public",
        _surface_note("SchemaAutoRegisterMode"),
    ),
    "SchemaRegistryClient": SurfaceExport(
        "agora_plugins.kafka.schema_registry",
        "SchemaRegistryClient",
        "stable_public",
        _surface_note("SchemaRegistryClient"),
    ),
    "KafkaSink": SurfaceExport(
        "agora_plugins.kafka.sinks",
        "KafkaSink",
        "stable_public",
        _surface_note("KafkaSink"),
    ),
    "KafkaSinkMessage": SurfaceExport(
        "agora_plugins.kafka.sinks",
        "KafkaSinkMessage",
        "stable_public",
        _surface_note("KafkaSinkMessage"),
    ),
    "KafkaDeliveryContext": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaDeliveryContext",
        "stable_public",
        _surface_note("KafkaDeliveryContext"),
    ),
    "KafkaPartitionHealth": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaPartitionHealth",
        "supportability_public",
        _surface_note("KafkaPartitionHealth"),
    ),
    "KafkaPoisonRecordClassification": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaPoisonRecordClassification",
        "supportability_public",
        _surface_note("KafkaPoisonRecordClassification"),
    ),
    "KafkaPoisonRecordInfo": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaPoisonRecordInfo",
        "supportability_public",
        _surface_note("KafkaPoisonRecordInfo"),
    ),
    "KafkaPoisonRecordPolicy": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaPoisonRecordPolicy",
        "supportability_public",
        _surface_note("KafkaPoisonRecordPolicy"),
    ),
    "KafkaSource": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaSource",
        "stable_public",
        _surface_note("KafkaSource"),
    ),
    "KafkaSourceHealthSnapshot": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaSourceHealthSnapshot",
        "supportability_public",
        _surface_note("KafkaSourceHealthSnapshot"),
    ),
    "KafkaSourceOperationalMetrics": SurfaceExport(
        "agora_plugins.kafka.sources",
        "KafkaSourceOperationalMetrics",
        "supportability_public",
        _surface_note("KafkaSourceOperationalMetrics"),
    ),
    "KafkaOpenTelemetryTracing": SurfaceExport(
        "agora_plugins.kafka.tracing",
        "KafkaOpenTelemetryTracing",
        "supportability_public",
        _surface_note("KafkaOpenTelemetryTracing"),
    ),
}

_EXPORTS = export_target_map(_SURFACE_EXPORTS)
_EXPORTS["_doctor_readiness_provider"] = (
    "agora_plugins.kafka.doctor",
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
