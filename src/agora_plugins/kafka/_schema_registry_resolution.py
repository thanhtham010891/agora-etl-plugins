"""Schema Registry registration and subject-resolution helpers."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora_plugins.kafka._schema_registry_types import (
        RegisteredSchema,
        SchemaAutoRegisterMode,
        SchemaRegistryClient,
    )

SCHEMA_AUTO_REGISTER_MODES: set[str] = {"disabled", "missing_subject", "always"}


def coerce_auto_register_mode(
    auto_register: bool | SchemaAutoRegisterMode,
) -> SchemaAutoRegisterMode:
    if isinstance(auto_register, bool):
        warnings.warn(
            "Passing bool auto_register is deprecated; pass one of "
            "'always', 'missing_subject', or 'disabled' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return "always" if auto_register else "disabled"
    if auto_register not in SCHEMA_AUTO_REGISTER_MODES:
        raise ValueError("auto_register must be one of 'always', 'missing_subject', or 'disabled'.")
    return auto_register


async def resolve_registered_schema(
    registry_client: SchemaRegistryClient,
    *,
    subject: str,
    schema_text: str,
    schema_type: str,
    auto_register: SchemaAutoRegisterMode,
    normalize_schema: Callable[[str], str],
) -> RegisteredSchema:
    normalized_schema_text = normalize_schema(schema_text)
    if auto_register == "always":
        return await registry_client.register_schema(
            subject,
            normalized_schema_text,
            schema_type=schema_type,
        )

    try:
        registered = await registry_client.get_latest_schema(subject)
    except Exception as exc:
        if auto_register == "missing_subject" and schema_registry_subject_is_missing(exc):
            return await registry_client.register_schema(
                subject,
                normalized_schema_text,
                schema_type=schema_type,
            )
        raise

    if normalize_schema(registered.schema) != normalized_schema_text:
        raise ValueError(f"Latest schema for subject '{subject}' does not match serializer schema.")
    return registered


def schema_registry_subject_is_missing(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code is None:
        status_code = getattr(exc, "code", None)
    if status_code is not None:
        try:
            if int(status_code) == 404:
                return True
        except (TypeError, ValueError):
            pass
    message = str(exc).lower()
    return "404" in message and (
        "schema registry request failed" in message
        or "subject" in message
        or "not found" in message
    )
