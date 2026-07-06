"""Avro Schema Registry serializers and deserializers."""

from __future__ import annotations

import io
import json
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora_plugins.kafka._schema_registry_cache import (
    coerce_schema_cache_max_entries,
    lru_cache_get,
    lru_cache_put,
)
from agora_plugins.kafka._schema_registry_common import (
    default_record_mapper,
    normalize_avro_schema_text,
)
from agora_plugins.kafka._schema_registry_resolution import (
    coerce_auto_register_mode,
    resolve_registered_schema,
)
from agora_plugins.kafka._schema_registry_wire import (
    CONFLUENT_MAGIC_BYTE,
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


class AvroSchemaRegistrySerializer(Generic[T]):
    """Encode records using Confluent wire format and registry-managed Avro schemas."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        subject: str,
        schema: dict[str, Any] | list[Any] | str,
        record_mapper: Callable[[T], dict[str, Any]] | None = None,
        auto_register: bool | SchemaAutoRegisterMode = "missing_subject",
    ) -> None:
        self._registry_client = registry_client
        self._subject = subject
        self._schema_text = normalize_avro_schema_text(schema)
        self._record_mapper = record_mapper or default_record_mapper
        self._auto_register = coerce_auto_register_mode(auto_register)
        self._schema_id: int | None = None
        self._parsed_schema: Any = None

    async def open(self) -> None:
        from fastavro import parse_schema

        registered = await resolve_registered_schema(
            self._registry_client,
            subject=self._subject,
            schema_text=self._schema_text,
            schema_type="AVRO",
            auto_register=self._auto_register,
            normalize_schema=normalize_avro_schema_text,
        )
        self._schema_id = registered.schema_id
        self._parsed_schema = parse_schema(json.loads(self._schema_text))

    async def close(self) -> None:
        return None

    async def __call__(self, record: T) -> bytes:
        from fastavro import schemaless_writer

        if self._schema_id is None or self._parsed_schema is None:
            raise RuntimeError("AvroSchemaRegistrySerializer.open() must be called before use.")
        payload = self._record_mapper(record)
        buffer = io.BytesIO()
        buffer.write(encode_confluent_prefix(self._schema_id))
        schemaless_writer(buffer, self._parsed_schema, payload)
        return buffer.getvalue()


class AvroSchemaRegistryDeserializer(Generic[T]):
    """Decode Confluent wire-format Avro payloads via a schema registry."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        record_mapper: Callable[[dict[str, Any]], T] | None = None,
        reader_schema: dict[str, Any] | list[Any] | str | None = None,
        schema_cache_max_entries: int = _DEFAULT_SCHEMA_CACHE_MAX_ENTRIES,
    ) -> None:
        self._registry_client = registry_client
        self._record_mapper = record_mapper
        self._reader_schema_text = (
            normalize_avro_schema_text(reader_schema) if reader_schema else None
        )
        self._reader_schema: Any = None
        self._schema_cache_max_entries = coerce_schema_cache_max_entries(schema_cache_max_entries)
        self._writer_schemas: OrderedDict[int, Any] = OrderedDict()

    async def open(self) -> None:
        if self._reader_schema_text is None:
            return
        from fastavro import parse_schema

        self._reader_schema = parse_schema(json.loads(self._reader_schema_text))

    async def close(self) -> None:
        return None

    async def __call__(self, value: bytes) -> T:
        from fastavro import parse_schema, schemaless_reader

        if len(value) < 5:
            raise ValueError("Schema-registry Avro payload must be at least 5 bytes long.")
        if value[0] != CONFLUENT_MAGIC_BYTE:
            raise ValueError("Unsupported schema-registry payload magic byte.")

        schema_id, payload_offset, _ = decode_confluent_prefix(value)
        writer_schema = lru_cache_get(self._writer_schemas, schema_id)
        if writer_schema is None:
            registered = await self._registry_client.get_schema(schema_id)
            writer_schema = parse_schema(json.loads(normalize_avro_schema_text(registered.schema)))
            lru_cache_put(
                self._writer_schemas,
                schema_id,
                writer_schema,
                max_entries=self._schema_cache_max_entries,
            )

        record = schemaless_reader(
            io.BytesIO(value[payload_offset:]),
            writer_schema,
            self._reader_schema,
        )
        if self._record_mapper is None:
            return cast("T", record)
        if not isinstance(record, dict):
            raise TypeError(
                "Schema-registry Avro deserializer expected a record object for record_mapper."
            )
        return self._record_mapper(cast("dict[str, Any]", record))


__all__ = ["AvroSchemaRegistryDeserializer", "AvroSchemaRegistrySerializer"]
