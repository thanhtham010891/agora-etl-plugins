"""Official Kafka plugin package for Agora."""

from agora_plugins.kafka.config import KafkaConfig, KafkaPluginConfig
from agora_plugins.kafka.plugin import MANIFEST, PluginManifest
from agora_plugins.kafka.schema_registry import (
    AvroSchemaRegistryDeserializer,
    AvroSchemaRegistrySerializer,
    ConfluentSchemaRegistryClient,
    RegisteredSchema,
    SchemaRegistryClient,
)
from agora_plugins.kafka.sinks import KafkaSink
from agora_plugins.kafka.sources import KafkaSource

__all__ = [
    "MANIFEST",
    "AvroSchemaRegistryDeserializer",
    "AvroSchemaRegistrySerializer",
    "ConfluentSchemaRegistryClient",
    "KafkaConfig",
    "KafkaPluginConfig",
    "KafkaSink",
    "KafkaSource",
    "PluginManifest",
    "RegisteredSchema",
    "SchemaRegistryClient",
]
