"""Protobuf Schema Registry serializers and deserializers."""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora_plugins.kafka._schema_registry_cache import (
    coerce_schema_cache_max_entries,
    lru_cache_get,
    lru_cache_put,
)
from agora_plugins.kafka._schema_registry_proto import (
    coerce_protobuf_message,
    normalize_proto_schema_text,
    validate_protobuf_schema_binding,
)
from agora_plugins.kafka._schema_registry_resolution import (
    coerce_auto_register_mode,
    resolve_registered_schema,
)
from agora_plugins.kafka._schema_registry_wire import (
    decode_confluent_prefix,
    encode_confluent_prefix,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from agora_plugins.kafka._schema_registry_types import (
        RegisteredSchema,
        SchemaAutoRegisterMode,
        SchemaRegistryClient,
    )

T = TypeVar("T")
_DEFAULT_SCHEMA_CACHE_MAX_ENTRIES = 256


class ProtobufSchemaRegistrySerializer(Generic[T]):
    """Encode Protobuf payloads using Confluent wire format and registry-managed schemas."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        subject: str,
        schema: str,
        message_type: type[Any],
        record_mapper: Callable[[T], Any] | None = None,
        auto_register: bool | SchemaAutoRegisterMode = "missing_subject",
        message_indexes: Sequence[int] = (0,),
    ) -> None:
        self._registry_client = registry_client
        self._subject = subject
        self._schema_text = normalize_proto_schema_text(schema)
        self._message_type = message_type
        self._record_mapper = record_mapper
        self._auto_register = coerce_auto_register_mode(auto_register)
        self._schema_id: int | None = None
        self._message_indexes = tuple(int(index) for index in message_indexes)

    async def open(self) -> None:
        validate_protobuf_schema_binding(
            self._schema_text,
            self._message_type,
            self._message_indexes,
        )
        registered = await resolve_registered_schema(
            self._registry_client,
            subject=self._subject,
            schema_text=self._schema_text,
            schema_type="PROTOBUF",
            auto_register=self._auto_register,
            normalize_schema=normalize_proto_schema_text,
        )
        if registered.schema_type != "PROTOBUF":
            raise ValueError(
                f"Subject '{self._subject}' is registered as {registered.schema_type!r}, not 'PROTOBUF'."
            )
        validate_protobuf_schema_binding(
            registered.schema,
            self._message_type,
            self._message_indexes,
        )
        self._schema_id = registered.schema_id

    async def close(self) -> None:
        return None

    async def __call__(self, record: T) -> bytes:
        if self._schema_id is None:
            raise RuntimeError("ProtobufSchemaRegistrySerializer.open() must be called before use.")
        message = coerce_protobuf_message(
            record if self._record_mapper is None else self._record_mapper(record),
            self._message_type,
        )
        prefix = encode_confluent_prefix(
            self._schema_id,
            message_indexes=self._message_indexes,
        )
        return cast("bytes", prefix + message.SerializeToString())


class ProtobufSchemaRegistryDeserializer(Generic[T]):
    """Decode Confluent wire-format Protobuf payloads via a schema registry."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        message_type: type[Any],
        record_mapper: Callable[[Any], T] | None = None,
        schema_cache_max_entries: int = _DEFAULT_SCHEMA_CACHE_MAX_ENTRIES,
    ) -> None:
        self._registry_client = registry_client
        self._message_type = message_type
        self._record_mapper = record_mapper
        self._schema_cache_max_entries = coerce_schema_cache_max_entries(schema_cache_max_entries)
        self._registered_schemas: OrderedDict[int, RegisteredSchema] = OrderedDict()
        self._validated_bindings: OrderedDict[tuple[int, tuple[int, ...]], None] = OrderedDict()

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def __call__(self, value: bytes) -> T:
        schema_id, payload_offset, message_indexes = decode_confluent_prefix(
            value,
            expect_message_indexes=True,
        )
        binding_key = (schema_id, message_indexes or (0,))
        registered = lru_cache_get(self._registered_schemas, schema_id)
        if registered is None:
            registered = await self._registry_client.get_schema(schema_id)
            lru_cache_put(
                self._registered_schemas,
                schema_id,
                registered,
                max_entries=self._schema_cache_max_entries,
            )
        if registered.schema_type != "PROTOBUF":
            raise ValueError(
                f"Schema id {schema_id} is registered as {registered.schema_type!r}, not 'PROTOBUF'."
            )
        if lru_cache_get(self._validated_bindings, binding_key) is None:
            validate_protobuf_schema_binding(
                registered.schema,
                self._message_type,
                binding_key[1],
            )
            lru_cache_put(
                self._validated_bindings,
                binding_key,
                None,
                max_entries=self._schema_cache_max_entries,
            )
        message = self._message_type()
        message.ParseFromString(value[payload_offset:])
        if self._record_mapper is None:
            return cast("T", message)
        return self._record_mapper(message)


__all__ = ["ProtobufSchemaRegistryDeserializer", "ProtobufSchemaRegistrySerializer"]
