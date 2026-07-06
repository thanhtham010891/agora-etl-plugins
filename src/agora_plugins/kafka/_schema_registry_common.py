"""Shared codec helpers for Schema Registry serializers."""

from __future__ import annotations

import importlib
import json
from typing import Any, TypeVar

T = TypeVar("T")


def normalize_schema_text(schema: dict[str, Any] | list[Any] | str) -> str:
    if isinstance(schema, str):
        return json.dumps(json.loads(schema), sort_keys=True, separators=(",", ":"))
    return json.dumps(schema, sort_keys=True, separators=(",", ":"))


def normalize_avro_schema_text(schema: dict[str, Any] | list[Any] | str) -> str:
    schema_object = json.loads(schema) if isinstance(schema, str) else schema
    try:
        from fastavro.schema import to_parsing_canonical_form
    except ImportError:
        return json.dumps(schema_object, sort_keys=True, separators=(",", ":"))
    canonical = to_parsing_canonical_form(schema_object)
    if isinstance(canonical, str):
        return canonical
    return str(canonical)


def jsonschema_validate(instance: Any, schema: Any) -> None:
    jsonschema = importlib.import_module("jsonschema")
    jsonschema.validate(instance=instance, schema=schema)


def default_record_mapper(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise TypeError(
            "AvroSchemaRegistrySerializer requires a mapping record by default. "
            "Provide record_mapper=... for custom objects."
        )
    return record


def identity_record_mapper(record: T) -> T:
    return record


__all__ = [
    "default_record_mapper",
    "identity_record_mapper",
    "jsonschema_validate",
    "normalize_avro_schema_text",
    "normalize_schema_text",
]
