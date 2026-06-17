"""PostgreSQL identifier validation, quoting, and schema-lock helpers."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from agora.schema.types import DataType

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCHEMA_ADVISORY_LOCK_PERSON = b"agora_pg"


@dataclass(frozen=True, slots=True)
class QuotedIdentifier:
    """Explicit opt-in wrapper for PostgreSQL quoted identifiers."""

    parts: tuple[str, ...]

    def __init__(self, *parts: str) -> None:
        if not parts:
            raise ValueError("QuotedIdentifier requires at least one identifier part.")
        for part in parts:
            _validate_raw_quoted_identifier_part(part)
        object.__setattr__(self, "parts", tuple(parts))

    def __str__(self) -> str:
        return ".".join(self.parts)


def _validate_raw_quoted_identifier_part(part: str) -> None:
    if part == "" or "\x00" in part:
        raise ValueError(f"Invalid SQL identifier part: {part!r}")


def _quote_raw_identifier_part(part: str) -> str:
    _validate_raw_quoted_identifier_part(part)
    return '"' + part.replace('"', '""') + '"'


def _split_identifier_path(identifier: str, *, allow_path: bool) -> list[str]:
    if not allow_path:
        return [identifier]

    parts: list[str] = []
    current: list[str] = []
    in_quote = False
    index = 0
    while index < len(identifier):
        char = identifier[index]
        if char == '"':
            current.append(char)
            if in_quote and index + 1 < len(identifier) and identifier[index + 1] == '"':
                current.append(identifier[index + 1])
                index += 2
                continue
            in_quote = not in_quote
        elif char == "." and not in_quote:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    if in_quote:
        raise ValueError(f"Invalid SQL identifier: {identifier!r}. Unterminated quoted part.")
    parts.append("".join(current))
    return parts


def _unquote_identifier_part(identifier: str, part: str) -> str:
    if not (part.startswith('"') and part.endswith('"')) or len(part) < 2:
        raise ValueError(
            f"Invalid SQL identifier: {identifier!r}. "
            "Quoted identifier parts must start and end with a double quote."
        )

    raw: list[str] = []
    index = 1
    end = len(part) - 1
    while index < end:
        char = part[index]
        if char != '"':
            raw.append(char)
            index += 1
            continue
        if index + 1 < end and part[index + 1] == '"':
            raw.append('"')
            index += 2
            continue
        raise ValueError(
            f"Invalid SQL identifier: {identifier!r}. "
            "Embedded double quotes must be escaped as two double quotes."
        )
    return "".join(raw)


def _identifier_parts(
    identifier: str | QuotedIdentifier,
    *,
    allow_path: bool = False,
    allow_quoted: bool = False,
) -> tuple[str, ...]:
    if isinstance(identifier, QuotedIdentifier):
        parts = list(identifier.parts)
    else:
        parts = (
            _split_identifier_path(identifier, allow_path=allow_path)
            if allow_quoted
            else (identifier.split(".") if allow_path else [identifier])
        )

    if not parts or any(not part for part in parts):
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    if allow_path and len(parts) > 2:
        raise ValueError(
            f"Invalid SQL identifier: {identifier!r}. "
            "Only schema.table paths are supported (max 2 parts)."
        )
    if not allow_path and len(parts) > 1:
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")

    normalized: list[str] = []
    for part in parts:
        if isinstance(identifier, str) and part.startswith('"'):
            if not allow_quoted:
                raise ValueError(
                    f"Invalid SQL identifier: {identifier!r}. "
                    "Quoted identifiers require allow_quoted_identifiers=True."
                )
            normalized.append(_unquote_identifier_part(identifier, part))
            continue
        if isinstance(identifier, str) and part.endswith('"'):
            raise ValueError(f"Invalid SQL identifier: {identifier!r}")
        if isinstance(identifier, str) and not _IDENTIFIER_RE.fullmatch(part):
            raise ValueError(
                f"Invalid SQL identifier: {identifier!r}. "
                "Only letters, numbers, and underscores are allowed, "
                "and identifiers must not start with a number."
            )
        normalized.append(part)
    for part in normalized:
        _validate_raw_quoted_identifier_part(part)
    return tuple(normalized)


def _quote_identifier(
    identifier: str | QuotedIdentifier,
    *,
    allow_path: bool = False,
    allow_quoted: bool = False,
) -> str:
    return ".".join(
        _quote_raw_identifier_part(part)
        for part in _identifier_parts(
            identifier,
            allow_path=allow_path,
            allow_quoted=allow_quoted,
        )
    )


def _postgres_type(data_type: DataType) -> str:
    """Map Agora DataType to Postgres type."""
    mapping = {
        DataType.STRING: "TEXT",
        DataType.INTEGER: "BIGINT",
        DataType.FLOAT: "DOUBLE PRECISION",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.TIMESTAMP: "TIMESTAMPTZ",
        DataType.JSON: "JSONB",
        DataType.BYTES: "BYTEA",
        DataType.NULL: "TEXT",
    }
    return mapping.get(data_type, "TEXT")


def _table_lookup_condition(
    table_name: str | QuotedIdentifier,
    *,
    allow_quoted: bool = False,
) -> tuple[str, tuple[str, ...]]:
    parts = _identifier_parts(table_name, allow_path=True, allow_quoted=allow_quoted)
    if len(parts) == 2:
        schema_name, relation_name = parts
        return (
            "table_schema = %s AND table_name = %s",
            (schema_name, relation_name),
        )
    return (
        "table_schema = ANY(current_schemas(false)) AND table_name = %s",
        (parts[0],),
    )


def _table_catalog_condition(
    table_name: str | QuotedIdentifier,
    *,
    namespace_alias: str,
    table_alias: str,
    allow_quoted: bool = False,
) -> tuple[str, tuple[str, ...]]:
    parts = _identifier_parts(table_name, allow_path=True, allow_quoted=allow_quoted)
    if len(parts) == 2:
        schema_name, relation_name = parts
        return (
            f"{namespace_alias}.nspname = %s AND {table_alias}.relname = %s",
            (schema_name, relation_name),
        )
    return (
        f"{namespace_alias}.nspname = ANY(current_schemas(false)) AND {table_alias}.relname = %s",
        (parts[0],),
    )


def _schema_advisory_lock_key(
    table_name: str | QuotedIdentifier,
    *,
    allow_quoted: bool = False,
) -> int:
    parts = _identifier_parts(table_name, allow_path=True, allow_quoted=allow_quoted)
    lock_name = "\x00".join(parts)
    digest = hashlib.blake2b(
        lock_name.encode("utf-8"),
        digest_size=8,
        person=_SCHEMA_ADVISORY_LOCK_PERSON,
    ).digest()
    key = int.from_bytes(digest, byteorder="big", signed=False) & ((1 << 63) - 1)
    return key or 1


__all__ = ["QuotedIdentifier"]
