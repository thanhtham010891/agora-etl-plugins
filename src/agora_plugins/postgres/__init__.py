"""Official PostgreSQL plugin package for Agora."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from agora_plugins._surface_manifest import SurfaceExport, export_target_map

if TYPE_CHECKING:
    from agora_plugins.postgres.config import (
        PostgresAuthConfig,
        PostgresConfig,
        PostgresConnectionConfig,
        PostgresPluginConfig,
        PostgresTLSConfig,
    )
    from agora_plugins.postgres.dlq import PostgresDLQSink, PostgresDLQSource
    from agora_plugins.postgres.kafka import (
        KafkaPostgresDeliveryConfig,
        KafkaPostgresEnterpriseAcceptanceFinding,
        KafkaPostgresEnterpriseAcceptanceGate,
        KafkaPostgresEnterpriseAcceptanceReport,
        KafkaPostgresEnterpriseAcceptanceThresholds,
        KafkaPostgresEnvelopeDeserializer,
        KafkaPostgresPoisonDLQConfig,
        KafkaPostgresPrometheusExporter,
        KafkaPostgresRuntime,
        KafkaPostgresRuntimeHealthSnapshot,
        KafkaPostgresRuntimeMetricsSnapshot,
        build_kafka_postgres_runtime,
        build_kafka_postgres_sink,
        build_kafka_postgres_source,
        with_kafka_delivery_fields,
        wrap_kafka_postgres_deserializer,
    )
    from agora_plugins.postgres.observability import (
        PostgresDLQSinkEnterpriseAcceptanceThresholds,
        PostgresDLQSinkMetricsSnapshot,
        PostgresDLQSourceEnterpriseAcceptanceThresholds,
        PostgresDLQSourceMetricsSnapshot,
        PostgresEnterpriseAcceptanceFinding,
        PostgresEnterpriseAcceptanceGate,
        PostgresEnterpriseAcceptanceReport,
        PostgresPrometheusExporter,
        PostgresSinkEnterpriseAcceptanceThresholds,
        PostgresSourceEnterpriseAcceptanceThresholds,
        PostgresSourceHealthSnapshot,
        PostgresSourceMetricsSnapshot,
        PostgresSourceRecoveryContractSnapshot,
        PostgresSourceRecoveryMode,
    )
    from agora_plugins.postgres.plugin import MANIFEST, PluginManifest
    from agora_plugins.postgres.sinks.postgres import (
        PostgresPoisonRecordClassification,
        PostgresPoisonRecordInfo,
        PostgresSchemaAdapter,
        PostgresSink,
        PostgresSinkMetricsSnapshot,
        PostgresSinkWriteError,
        PostgresWriteSafetyPolicy,
        QuotedIdentifier,
    )
    from agora_plugins.postgres.sources.postgres import (
        PostgresReplicaStalenessError,
        PostgresSource,
    )

__all__ = [
    "MANIFEST",
    "KafkaPostgresDeliveryConfig",
    "KafkaPostgresEnterpriseAcceptanceFinding",
    "KafkaPostgresEnterpriseAcceptanceGate",
    "KafkaPostgresEnterpriseAcceptanceReport",
    "KafkaPostgresEnterpriseAcceptanceThresholds",
    "KafkaPostgresEnvelopeDeserializer",
    "KafkaPostgresPoisonDLQConfig",
    "KafkaPostgresPrometheusExporter",
    "KafkaPostgresRuntime",
    "KafkaPostgresRuntimeHealthSnapshot",
    "KafkaPostgresRuntimeMetricsSnapshot",
    "PluginManifest",
    "PostgresAuthConfig",
    "PostgresConfig",
    "PostgresConnectionConfig",
    "PostgresDLQSink",
    "PostgresDLQSinkEnterpriseAcceptanceThresholds",
    "PostgresDLQSinkMetricsSnapshot",
    "PostgresDLQSource",
    "PostgresDLQSourceEnterpriseAcceptanceThresholds",
    "PostgresDLQSourceMetricsSnapshot",
    "PostgresEnterpriseAcceptanceFinding",
    "PostgresEnterpriseAcceptanceGate",
    "PostgresEnterpriseAcceptanceReport",
    "PostgresPluginConfig",
    "PostgresPoisonRecordClassification",
    "PostgresPoisonRecordInfo",
    "PostgresPrometheusExporter",
    "PostgresReplicaStalenessError",
    "PostgresSchemaAdapter",
    "PostgresSink",
    "PostgresSinkEnterpriseAcceptanceThresholds",
    "PostgresSinkMetricsSnapshot",
    "PostgresSinkWriteError",
    "PostgresSource",
    "PostgresSourceEnterpriseAcceptanceThresholds",
    "PostgresSourceHealthSnapshot",
    "PostgresSourceMetricsSnapshot",
    "PostgresSourceRecoveryContractSnapshot",
    "PostgresSourceRecoveryMode",
    "PostgresTLSConfig",
    "PostgresWriteSafetyPolicy",
    "QuotedIdentifier",
    "build_kafka_postgres_runtime",
    "build_kafka_postgres_sink",
    "build_kafka_postgres_source",
    "with_kafka_delivery_fields",
    "wrap_kafka_postgres_deserializer",
]

_STABLE_PUBLIC_EXPORTS = frozenset(
    {
        "MANIFEST",
        "PluginManifest",
        "PostgresAuthConfig",
        "PostgresConfig",
        "PostgresConnectionConfig",
        "PostgresPluginConfig",
        "PostgresTLSConfig",
        "PostgresSource",
        "PostgresSink",
        "PostgresSchemaAdapter",
        "PostgresDLQSink",
        "PostgresDLQSource",
    }
)

_SUPPORTABILITY_PUBLIC_EXPORTS = frozenset(
    {
        "PostgresDLQSinkEnterpriseAcceptanceThresholds",
        "PostgresDLQSinkMetricsSnapshot",
        "PostgresDLQSourceEnterpriseAcceptanceThresholds",
        "PostgresDLQSourceMetricsSnapshot",
        "PostgresEnterpriseAcceptanceFinding",
        "PostgresEnterpriseAcceptanceGate",
        "PostgresEnterpriseAcceptanceReport",
        "PostgresPoisonRecordClassification",
        "PostgresPoisonRecordInfo",
        "PostgresPrometheusExporter",
        "PostgresReplicaStalenessError",
        "PostgresSinkEnterpriseAcceptanceThresholds",
        "PostgresSinkMetricsSnapshot",
        "PostgresSinkWriteError",
        "PostgresSourceEnterpriseAcceptanceThresholds",
        "PostgresSourceHealthSnapshot",
        "PostgresSourceMetricsSnapshot",
        "PostgresSourceRecoveryContractSnapshot",
        "PostgresSourceRecoveryMode",
        "PostgresWriteSafetyPolicy",
        "QuotedIdentifier",
    }
)

_PATTERN_RECIPE_EXPORTS = frozenset(
    {
        "KafkaPostgresDeliveryConfig",
        "KafkaPostgresEnterpriseAcceptanceFinding",
        "KafkaPostgresEnterpriseAcceptanceGate",
        "KafkaPostgresEnterpriseAcceptanceReport",
        "KafkaPostgresEnterpriseAcceptanceThresholds",
        "KafkaPostgresEnvelopeDeserializer",
        "KafkaPostgresPoisonDLQConfig",
        "KafkaPostgresPrometheusExporter",
        "KafkaPostgresRuntime",
        "KafkaPostgresRuntimeHealthSnapshot",
        "KafkaPostgresRuntimeMetricsSnapshot",
        "build_kafka_postgres_runtime",
        "build_kafka_postgres_sink",
        "build_kafka_postgres_source",
        "with_kafka_delivery_fields",
        "wrap_kafka_postgres_deserializer",
    }
)

_INTERNAL_BRIDGE_EXPORTS = frozenset({"_doctor_readiness_provider"})


def _surface_note(name: str) -> str:
    if name in _STABLE_PUBLIC_EXPORTS:
        return "Stable PostgreSQL family primitive/config public surface."
    if name in _SUPPORTABILITY_PUBLIC_EXPORTS:
        return "PostgreSQL supportability, diagnostics, or observability public surface."
    return "PostgreSQL composite Kafka wedge or pattern-oriented helper surface."


_SURFACE_EXPORTS: dict[str, SurfaceExport] = {
    "MANIFEST": SurfaceExport(
        "agora_plugins.postgres.plugin",
        "MANIFEST",
        "stable_public",
        _surface_note("MANIFEST"),
    ),
    "PluginManifest": SurfaceExport(
        "agora_plugins.postgres.plugin",
        "PluginManifest",
        "stable_public",
        _surface_note("PluginManifest"),
    ),
    "PostgresAuthConfig": SurfaceExport(
        "agora_plugins.postgres.config",
        "PostgresAuthConfig",
        "stable_public",
        _surface_note("PostgresAuthConfig"),
    ),
    "PostgresConfig": SurfaceExport(
        "agora_plugins.postgres.config",
        "PostgresConfig",
        "stable_public",
        _surface_note("PostgresConfig"),
    ),
    "PostgresConnectionConfig": SurfaceExport(
        "agora_plugins.postgres.config",
        "PostgresConnectionConfig",
        "stable_public",
        _surface_note("PostgresConnectionConfig"),
    ),
    "PostgresPluginConfig": SurfaceExport(
        "agora_plugins.postgres.config",
        "PostgresPluginConfig",
        "stable_public",
        _surface_note("PostgresPluginConfig"),
    ),
    "PostgresTLSConfig": SurfaceExport(
        "agora_plugins.postgres.config",
        "PostgresTLSConfig",
        "stable_public",
        _surface_note("PostgresTLSConfig"),
    ),
    "PostgresDLQSink": SurfaceExport(
        "agora_plugins.postgres.dlq",
        "PostgresDLQSink",
        "stable_public",
        _surface_note("PostgresDLQSink"),
    ),
    "PostgresDLQSource": SurfaceExport(
        "agora_plugins.postgres.dlq",
        "PostgresDLQSource",
        "stable_public",
        _surface_note("PostgresDLQSource"),
    ),
    "KafkaPostgresDeliveryConfig": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresDeliveryConfig",
        "pattern_recipe",
        _surface_note("KafkaPostgresDeliveryConfig"),
    ),
    "KafkaPostgresEnterpriseAcceptanceFinding": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresEnterpriseAcceptanceFinding",
        "pattern_recipe",
        _surface_note("KafkaPostgresEnterpriseAcceptanceFinding"),
    ),
    "KafkaPostgresEnterpriseAcceptanceGate": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresEnterpriseAcceptanceGate",
        "pattern_recipe",
        _surface_note("KafkaPostgresEnterpriseAcceptanceGate"),
    ),
    "KafkaPostgresEnterpriseAcceptanceReport": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresEnterpriseAcceptanceReport",
        "pattern_recipe",
        _surface_note("KafkaPostgresEnterpriseAcceptanceReport"),
    ),
    "KafkaPostgresEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresEnterpriseAcceptanceThresholds",
        "pattern_recipe",
        _surface_note("KafkaPostgresEnterpriseAcceptanceThresholds"),
    ),
    "KafkaPostgresEnvelopeDeserializer": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresEnvelopeDeserializer",
        "pattern_recipe",
        _surface_note("KafkaPostgresEnvelopeDeserializer"),
    ),
    "KafkaPostgresPoisonDLQConfig": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresPoisonDLQConfig",
        "pattern_recipe",
        _surface_note("KafkaPostgresPoisonDLQConfig"),
    ),
    "KafkaPostgresPrometheusExporter": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresPrometheusExporter",
        "pattern_recipe",
        _surface_note("KafkaPostgresPrometheusExporter"),
    ),
    "KafkaPostgresRuntime": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresRuntime",
        "pattern_recipe",
        _surface_note("KafkaPostgresRuntime"),
    ),
    "KafkaPostgresRuntimeHealthSnapshot": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresRuntimeHealthSnapshot",
        "pattern_recipe",
        _surface_note("KafkaPostgresRuntimeHealthSnapshot"),
    ),
    "KafkaPostgresRuntimeMetricsSnapshot": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "KafkaPostgresRuntimeMetricsSnapshot",
        "pattern_recipe",
        _surface_note("KafkaPostgresRuntimeMetricsSnapshot"),
    ),
    "build_kafka_postgres_runtime": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "build_kafka_postgres_runtime",
        "pattern_recipe",
        _surface_note("build_kafka_postgres_runtime"),
    ),
    "build_kafka_postgres_sink": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "build_kafka_postgres_sink",
        "pattern_recipe",
        _surface_note("build_kafka_postgres_sink"),
    ),
    "build_kafka_postgres_source": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "build_kafka_postgres_source",
        "pattern_recipe",
        _surface_note("build_kafka_postgres_source"),
    ),
    "with_kafka_delivery_fields": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "with_kafka_delivery_fields",
        "pattern_recipe",
        _surface_note("with_kafka_delivery_fields"),
    ),
    "wrap_kafka_postgres_deserializer": SurfaceExport(
        "agora_plugins.postgres.kafka",
        "wrap_kafka_postgres_deserializer",
        "pattern_recipe",
        _surface_note("wrap_kafka_postgres_deserializer"),
    ),
    "PostgresDLQSinkEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresDLQSinkEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("PostgresDLQSinkEnterpriseAcceptanceThresholds"),
    ),
    "PostgresDLQSinkMetricsSnapshot": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresDLQSinkMetricsSnapshot",
        "supportability_public",
        _surface_note("PostgresDLQSinkMetricsSnapshot"),
    ),
    "PostgresDLQSourceEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresDLQSourceEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("PostgresDLQSourceEnterpriseAcceptanceThresholds"),
    ),
    "PostgresDLQSourceMetricsSnapshot": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresDLQSourceMetricsSnapshot",
        "supportability_public",
        _surface_note("PostgresDLQSourceMetricsSnapshot"),
    ),
    "PostgresEnterpriseAcceptanceFinding": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresEnterpriseAcceptanceFinding",
        "supportability_public",
        _surface_note("PostgresEnterpriseAcceptanceFinding"),
    ),
    "PostgresEnterpriseAcceptanceGate": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresEnterpriseAcceptanceGate",
        "supportability_public",
        _surface_note("PostgresEnterpriseAcceptanceGate"),
    ),
    "PostgresEnterpriseAcceptanceReport": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresEnterpriseAcceptanceReport",
        "supportability_public",
        _surface_note("PostgresEnterpriseAcceptanceReport"),
    ),
    "PostgresPrometheusExporter": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresPrometheusExporter",
        "supportability_public",
        _surface_note("PostgresPrometheusExporter"),
    ),
    "PostgresSinkEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresSinkEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("PostgresSinkEnterpriseAcceptanceThresholds"),
    ),
    "PostgresSourceEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresSourceEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("PostgresSourceEnterpriseAcceptanceThresholds"),
    ),
    "PostgresSourceHealthSnapshot": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresSourceHealthSnapshot",
        "supportability_public",
        _surface_note("PostgresSourceHealthSnapshot"),
    ),
    "PostgresSourceMetricsSnapshot": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresSourceMetricsSnapshot",
        "supportability_public",
        _surface_note("PostgresSourceMetricsSnapshot"),
    ),
    "PostgresSourceRecoveryContractSnapshot": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresSourceRecoveryContractSnapshot",
        "supportability_public",
        _surface_note("PostgresSourceRecoveryContractSnapshot"),
    ),
    "PostgresSourceRecoveryMode": SurfaceExport(
        "agora_plugins.postgres.observability",
        "PostgresSourceRecoveryMode",
        "supportability_public",
        _surface_note("PostgresSourceRecoveryMode"),
    ),
    "PostgresPoisonRecordClassification": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "PostgresPoisonRecordClassification",
        "supportability_public",
        _surface_note("PostgresPoisonRecordClassification"),
    ),
    "PostgresPoisonRecordInfo": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "PostgresPoisonRecordInfo",
        "supportability_public",
        _surface_note("PostgresPoisonRecordInfo"),
    ),
    "PostgresSchemaAdapter": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "PostgresSchemaAdapter",
        "stable_public",
        _surface_note("PostgresSchemaAdapter"),
    ),
    "PostgresSink": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "PostgresSink",
        "stable_public",
        _surface_note("PostgresSink"),
    ),
    "PostgresSinkMetricsSnapshot": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "PostgresSinkMetricsSnapshot",
        "supportability_public",
        _surface_note("PostgresSinkMetricsSnapshot"),
    ),
    "PostgresSinkWriteError": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "PostgresSinkWriteError",
        "supportability_public",
        _surface_note("PostgresSinkWriteError"),
    ),
    "PostgresWriteSafetyPolicy": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "PostgresWriteSafetyPolicy",
        "supportability_public",
        _surface_note("PostgresWriteSafetyPolicy"),
    ),
    "QuotedIdentifier": SurfaceExport(
        "agora_plugins.postgres.sinks.postgres",
        "QuotedIdentifier",
        "supportability_public",
        _surface_note("QuotedIdentifier"),
    ),
    "PostgresReplicaStalenessError": SurfaceExport(
        "agora_plugins.postgres.sources.postgres",
        "PostgresReplicaStalenessError",
        "supportability_public",
        _surface_note("PostgresReplicaStalenessError"),
    ),
    "PostgresSource": SurfaceExport(
        "agora_plugins.postgres.sources.postgres",
        "PostgresSource",
        "stable_public",
        _surface_note("PostgresSource"),
    ),
}

_EXPORTS = export_target_map(_SURFACE_EXPORTS)
_EXPORTS["_doctor_readiness_provider"] = (
    "agora_plugins.postgres.doctor",
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
