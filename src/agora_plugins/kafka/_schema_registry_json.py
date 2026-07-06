"""JSON Schema Registry serializers and deserializers."""

from __future__ import annotations

import json
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora_plugins.kafka._schema_registry_cache import (
    coerce_schema_cache_max_entries,
    lru_cache_get,
    lru_cache_put,
)
from agora_plugins.kafka._schema_registry_common import (
    identity_record_mapper,
    jsonschema_validate,
    normalize_schema_text,
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
    from collections.abc import Callable

    from agora_plugins.kafka._schema_registry_types import (
        SchemaAutoRegisterMode,
        SchemaRegistryClient,
    )

T = TypeVar("T")
_DEFAULT_SCHEMA_CACHE_MAX_ENTRIES = 256


class JsonSchemaRegistrySerializer(Generic[T]):
    """Encode JSON payloads using Confluent wire format and registry-managed schemas."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        subject: str,
        schema: dict[str, Any] | list[Any] | str,
        record_mapper: Callable[[T], Any] | None = None,
        auto_register: bool | SchemaAutoRegisterMode = "missing_subject",
        validate_payload: bool = True,
    ) -> None:
        self._registry_client = registry_client
        self._subject = subject
        self._schema_text = normalize_schema_text(schema)
        self._record_mapper = record_mapper or identity_record_mapper
        self._auto_register = coerce_auto_register_mode(auto_register)
        self._validate_payload = validate_payload
        self._schema_id: int | None = None
        self._schema_object: Any = None

    async def open(self) -> None:
        registered = await resolve_registered_schema(
            self._registry_client,
            subject=self._subject,
            schema_text=self._schema_text,
            schema_type="JSON",
            auto_register=self._auto_register,
            normalize_schema=normalize_schema_text,
        )
        self._schema_id = registered.schema_id
        self._schema_object = json.loads(self._schema_text)

    async def close(self) -> None:
        return None

    async def __call__(self, record: T) -> bytes:
        if self._schema_id is None or self._schema_object is None:
            raise RuntimeError("JsonSchemaRegistrySerializer.open() must be called before use.")
        payload = self._record_mapper(record)
        if self._validate_payload:
            jsonschema_validate(payload, self._schema_object)
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return encode_confluent_prefix(self._schema_id) + encoded


class JsonSchemaRegistryDeserializer(Generic[T]):
    """Decode Confluent wire-format JSON payloads via a schema registry."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        record_mapper: Callable[[Any], T] | None = None,
        reader_schema: dict[str, Any] | list[Any] | str | None = None,
        validate_payload: bool = True,
        schema_cache_max_entries: int = _DEFAULT_SCHEMA_CACHE_MAX_ENTRIES,
    ) -> None:
        self._registry_client = registry_client
        self._record_mapper = record_mapper
        self._reader_schema_text = normalize_schema_text(reader_schema) if reader_schema else None
        self._reader_schema: Any = None
        self._schema_cache_max_entries = coerce_schema_cache_max_entries(schema_cache_max_entries)
        self._writer_schemas: OrderedDict[int, Any] = OrderedDict()
        self._validate_payload = validate_payload

    async def open(self) -> None:
        if self._reader_schema_text is None:
            return
        self._reader_schema = json.loads(self._reader_schema_text)

    async def close(self) -> None:
        return None

    async def __call__(self, value: bytes) -> T:
        schema_id, payload_offset, _ = decode_confluent_prefix(value)
        writer_schema = lru_cache_get(self._writer_schemas, schema_id)
        if writer_schema is None:
            registered = await self._registry_client.get_schema(schema_id)
            writer_schema = json.loads(normalize_schema_text(registered.schema))
            lru_cache_put(
                self._writer_schemas,
                schema_id,
                writer_schema,
                max_entries=self._schema_cache_max_entries,
            )
        payload = json.loads(value[payload_offset:].decode("utf-8"))
        if self._validate_payload:
            jsonschema_validate(payload, self._reader_schema or writer_schema)
        if self._record_mapper is None:
            return cast("T", payload)
        return self._record_mapper(payload)


__all__ = ["JsonSchemaRegistryDeserializer", "JsonSchemaRegistrySerializer"]
