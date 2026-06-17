"""Official Kafka plugin package for Agora."""

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
from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot, KafkaSourcePrometheusExporter
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
