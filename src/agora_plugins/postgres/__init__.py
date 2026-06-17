"""Official PostgreSQL plugin package for Agora."""

from typing import Any

from agora_plugins.postgres.config import (
    PostgresAuthConfig,
    PostgresConfig,
    PostgresConnectionConfig,
    PostgresPluginConfig,
    PostgresTLSConfig,
)
from agora_plugins.postgres.plugin import MANIFEST, PluginManifest

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


def __getattr__(name: str) -> Any:
    if name in {
        "KafkaPostgresEnterpriseAcceptanceFinding",
        "KafkaPostgresEnterpriseAcceptanceGate",
        "KafkaPostgresEnterpriseAcceptanceReport",
        "KafkaPostgresEnterpriseAcceptanceThresholds",
        "KafkaPostgresDeliveryConfig",
        "KafkaPostgresEnvelopeDeserializer",
        "KafkaPostgresPoisonDLQConfig",
        "KafkaPostgresPrometheusExporter",
        "KafkaPostgresRuntime",
        "KafkaPostgresRuntimeHealthSnapshot",
        "KafkaPostgresRuntimeMetricsSnapshot",
        "PostgresSinkMetricsSnapshot",
        "build_kafka_postgres_source",
        "build_kafka_postgres_runtime",
        "build_kafka_postgres_sink",
        "wrap_kafka_postgres_deserializer",
        "with_kafka_delivery_fields",
    }:
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
        from agora_plugins.postgres.sinks.postgres import PostgresSinkMetricsSnapshot

        return {
            "KafkaPostgresEnterpriseAcceptanceFinding": KafkaPostgresEnterpriseAcceptanceFinding,
            "KafkaPostgresEnterpriseAcceptanceGate": KafkaPostgresEnterpriseAcceptanceGate,
            "KafkaPostgresEnterpriseAcceptanceReport": KafkaPostgresEnterpriseAcceptanceReport,
            "KafkaPostgresEnterpriseAcceptanceThresholds": (
                KafkaPostgresEnterpriseAcceptanceThresholds
            ),
            "KafkaPostgresDeliveryConfig": KafkaPostgresDeliveryConfig,
            "KafkaPostgresEnvelopeDeserializer": KafkaPostgresEnvelopeDeserializer,
            "KafkaPostgresPoisonDLQConfig": KafkaPostgresPoisonDLQConfig,
            "KafkaPostgresPrometheusExporter": KafkaPostgresPrometheusExporter,
            "KafkaPostgresRuntime": KafkaPostgresRuntime,
            "KafkaPostgresRuntimeHealthSnapshot": KafkaPostgresRuntimeHealthSnapshot,
            "KafkaPostgresRuntimeMetricsSnapshot": KafkaPostgresRuntimeMetricsSnapshot,
            "PostgresSinkMetricsSnapshot": PostgresSinkMetricsSnapshot,
            "build_kafka_postgres_source": build_kafka_postgres_source,
            "build_kafka_postgres_runtime": build_kafka_postgres_runtime,
            "build_kafka_postgres_sink": build_kafka_postgres_sink,
            "wrap_kafka_postgres_deserializer": wrap_kafka_postgres_deserializer,
            "with_kafka_delivery_fields": with_kafka_delivery_fields,
        }[name]
    if name in {
        "PostgresDLQSinkEnterpriseAcceptanceThresholds",
        "PostgresDLQSinkMetricsSnapshot",
        "PostgresDLQSourceEnterpriseAcceptanceThresholds",
        "PostgresDLQSourceMetricsSnapshot",
        "PostgresEnterpriseAcceptanceFinding",
        "PostgresEnterpriseAcceptanceGate",
        "PostgresEnterpriseAcceptanceReport",
        "PostgresPrometheusExporter",
        "PostgresSinkEnterpriseAcceptanceThresholds",
        "PostgresSourceHealthSnapshot",
        "PostgresSourceEnterpriseAcceptanceThresholds",
        "PostgresSourceMetricsSnapshot",
        "PostgresSourceRecoveryContractSnapshot",
        "PostgresSourceRecoveryMode",
    }:
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

        return {
            "PostgresDLQSinkEnterpriseAcceptanceThresholds": (
                PostgresDLQSinkEnterpriseAcceptanceThresholds
            ),
            "PostgresDLQSinkMetricsSnapshot": PostgresDLQSinkMetricsSnapshot,
            "PostgresDLQSourceEnterpriseAcceptanceThresholds": (
                PostgresDLQSourceEnterpriseAcceptanceThresholds
            ),
            "PostgresDLQSourceMetricsSnapshot": PostgresDLQSourceMetricsSnapshot,
            "PostgresEnterpriseAcceptanceFinding": PostgresEnterpriseAcceptanceFinding,
            "PostgresEnterpriseAcceptanceGate": PostgresEnterpriseAcceptanceGate,
            "PostgresEnterpriseAcceptanceReport": PostgresEnterpriseAcceptanceReport,
            "PostgresPrometheusExporter": PostgresPrometheusExporter,
            "PostgresSinkEnterpriseAcceptanceThresholds": (
                PostgresSinkEnterpriseAcceptanceThresholds
            ),
            "PostgresSourceHealthSnapshot": PostgresSourceHealthSnapshot,
            "PostgresSourceEnterpriseAcceptanceThresholds": (
                PostgresSourceEnterpriseAcceptanceThresholds
            ),
            "PostgresSourceMetricsSnapshot": PostgresSourceMetricsSnapshot,
            "PostgresSourceRecoveryContractSnapshot": PostgresSourceRecoveryContractSnapshot,
            "PostgresSourceRecoveryMode": PostgresSourceRecoveryMode,
        }[name]
    if name in {
        "PostgresPoisonRecordClassification",
        "PostgresPoisonRecordInfo",
        "PostgresSchemaAdapter",
        "PostgresSink",
        "PostgresSinkWriteError",
        "PostgresWriteSafetyPolicy",
        "QuotedIdentifier",
    }:
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

        return {
            "PostgresPoisonRecordClassification": PostgresPoisonRecordClassification,
            "PostgresPoisonRecordInfo": PostgresPoisonRecordInfo,
            "PostgresSinkMetricsSnapshot": PostgresSinkMetricsSnapshot,
            "PostgresSchemaAdapter": PostgresSchemaAdapter,
            "PostgresSink": PostgresSink,
            "PostgresSinkWriteError": PostgresSinkWriteError,
            "PostgresWriteSafetyPolicy": PostgresWriteSafetyPolicy,
            "QuotedIdentifier": QuotedIdentifier,
        }[name]
    if name in {"PostgresDLQSink", "PostgresDLQSource"}:
        from agora_plugins.postgres.dlq import PostgresDLQSink, PostgresDLQSource

        return {
            "PostgresDLQSink": PostgresDLQSink,
            "PostgresDLQSource": PostgresDLQSource,
        }[name]
    if name in {"PostgresReplicaStalenessError", "PostgresSource"}:
        from agora_plugins.postgres.sources.postgres import (
            PostgresReplicaStalenessError,
            PostgresSource,
        )

        return {
            "PostgresReplicaStalenessError": PostgresReplicaStalenessError,
            "PostgresSource": PostgresSource,
        }[name]
    raise AttributeError(name)
