"""Public Schema Registry surface for Kafka serializers/deserializers."""

from __future__ import annotations

from agora_plugins.kafka._schema_registry_avro import (
    AvroSchemaRegistryDeserializer,
    AvroSchemaRegistrySerializer,
)
from agora_plugins.kafka._schema_registry_client import (
    ConfluentSchemaRegistryClient,
    PooledConfluentSchemaRegistryClient,
)
from agora_plugins.kafka._schema_registry_json import (
    JsonSchemaRegistryDeserializer,
    JsonSchemaRegistrySerializer,
)
from agora_plugins.kafka._schema_registry_proto import (
    resolve_proto_message_full_name as _resolve_proto_message_full_name,
)
from agora_plugins.kafka._schema_registry_protobuf_codec import (
    ProtobufSchemaRegistryDeserializer,
    ProtobufSchemaRegistrySerializer,
)
from agora_plugins.kafka._schema_registry_types import (
    RegisteredSchema,
    SchemaAutoRegisterMode,
    SchemaRegistryClient,
)

__all__ = [
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
    "_resolve_proto_message_full_name",
]
