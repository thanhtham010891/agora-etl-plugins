from __future__ import annotations

import importlib
import re
import textwrap
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

_PROTO_IDENTIFIER_RE = re.compile(r"[A-Za-z_][\w.]*")
_PROTO_BLOCK_KEYWORDS = {"message", "enum", "oneof", "service", "extend", "group"}


@dataclass(slots=True)
class ProtoMessageNode:
    name: str
    children: list[ProtoMessageNode] = field(default_factory=list)


def normalize_proto_schema_text(schema: str) -> str:
    normalized = textwrap.dedent(schema).strip()
    return "\n".join(line.strip() for line in normalized.splitlines() if line.strip())


def coerce_protobuf_message(value: Any, message_type: type[Any]) -> Any:
    if isinstance(value, message_type):
        return value
    if isinstance(value, dict):
        json_format = importlib.import_module("google.protobuf.json_format")
        message = message_type()
        json_format.ParseDict(value, message)
        return message
    if hasattr(value, "SerializeToString") and hasattr(value, "ParseFromString"):
        return value
    raise TypeError(
        "ProtobufSchemaRegistrySerializer requires a protobuf message instance or dict payload. "
        "Provide record_mapper=... for custom objects."
    )


def validate_protobuf_schema_binding(
    schema_text: str,
    message_type: type[Any],
    message_indexes: Sequence[int],
) -> None:
    expected_full_name = resolve_proto_message_full_name(schema_text, message_indexes)
    actual_full_name = protobuf_message_full_name(message_type)
    if expected_full_name != actual_full_name:
        raise ValueError(
            "Protobuf schema-registry binding mismatch: "
            f"payload indexes {tuple(int(index) for index in message_indexes)!r} resolve to "
            f"{expected_full_name!r}, but local message_type is {actual_full_name!r}."
        )


def protobuf_message_full_name(message_type: type[Any]) -> str:
    descriptor = getattr(message_type, "DESCRIPTOR", None)
    if descriptor is None:
        raise TypeError(
            "Protobuf schema-registry integration requires a protobuf message class with DESCRIPTOR."
        )
    full_name = getattr(descriptor, "full_name", None)
    if not isinstance(full_name, str) or not full_name:
        raise TypeError("Protobuf message class DESCRIPTOR.full_name must be a non-empty string.")
    return full_name


def resolve_proto_message_full_name(schema_text: str, message_indexes: Sequence[int]) -> str:
    package_name, root_messages = parse_proto_message_tree(schema_text)
    indexes = tuple(int(index) for index in message_indexes)
    if not indexes:
        raise ValueError("Protobuf schema-registry message indexes cannot be empty.")

    node: ProtoMessageNode | None = None
    path: list[str] = []
    siblings = root_messages
    for index in indexes:
        if index < 0 or index >= len(siblings):
            raise ValueError(
                f"Protobuf schema-registry message index {index} is out of range for path {indexes!r}."
            )
        node = siblings[index]
        path.append(node.name)
        siblings = node.children
    if node is None:
        raise ValueError(f"Unable to resolve protobuf message indexes {indexes!r}.")
    return ".".join([package_name, *path]) if package_name else ".".join(path)


def parse_proto_message_tree(schema_text: str) -> tuple[str, list[ProtoMessageNode]]:
    package_name = ""
    token_stream = tokenize_proto_schema(schema_text)

    root_messages: list[ProtoMessageNode] = []
    message_stack: list[ProtoMessageNode] = []
    brace_stack: list[str] = []
    pending_block_kind: str | None = None
    pending_message_name: str | None = None
    cursor = 0
    while cursor < len(token_stream):
        token = token_stream[cursor]
        if token == "package":
            if cursor + 1 < len(token_stream):
                package_name = token_stream[cursor + 1]
            cursor += 1
            while cursor < len(token_stream) and token_stream[cursor] != ";":
                cursor += 1
        elif token in _PROTO_BLOCK_KEYWORDS:
            pending_block_kind = token
            pending_message_name = (
                token_stream[cursor + 1] if cursor + 1 < len(token_stream) else None
            )
            cursor += 1
        elif token == "{":
            if pending_block_kind in {"message", "group"}:
                if pending_message_name is None:
                    raise ValueError("Malformed protobuf schema: message block is missing a name.")
                node = ProtoMessageNode(name=pending_message_name)
                if message_stack:
                    message_stack[-1].children.append(node)
                else:
                    root_messages.append(node)
                message_stack.append(node)
                brace_stack.append(pending_block_kind)
            elif pending_block_kind is not None:
                brace_stack.append(pending_block_kind)
            else:
                brace_stack.append("block")
            pending_block_kind = None
            pending_message_name = None
        elif token == "}":
            if not brace_stack:
                raise ValueError("Malformed protobuf schema: unmatched closing brace.")
            kind = brace_stack.pop()
            if kind in {"message", "group"} and message_stack:
                message_stack.pop()
            pending_block_kind = None
            pending_message_name = None
        cursor += 1
    if brace_stack:
        raise ValueError("Malformed protobuf schema: unclosed block.")
    if not root_messages:
        raise ValueError("Malformed protobuf schema: no message definitions found.")
    return package_name, root_messages


def tokenize_proto_schema(schema_text: str) -> list[str]:
    tokens: list[str] = []
    cursor = 0
    length = len(schema_text)
    while cursor < length:
        char = schema_text[cursor]
        if char.isspace():
            cursor += 1
            continue
        if schema_text.startswith("//", cursor):
            cursor = skip_line_comment(schema_text, cursor + 2)
            continue
        if schema_text.startswith("/*", cursor):
            cursor = skip_block_comment(schema_text, cursor + 2)
            continue
        if char in {'"', "'"}:
            cursor = skip_quoted_string(schema_text, cursor)
            continue
        if char in "{};":
            tokens.append(char)
            cursor += 1
            continue
        match = _PROTO_IDENTIFIER_RE.match(schema_text, cursor)
        if match is not None:
            tokens.append(match.group(0))
            cursor = match.end()
            continue
        cursor += 1
    return tokens


def skip_line_comment(schema_text: str, cursor: int) -> int:
    newline = schema_text.find("\n", cursor)
    return len(schema_text) if newline < 0 else newline + 1


def skip_block_comment(schema_text: str, cursor: int) -> int:
    end = schema_text.find("*/", cursor)
    if end < 0:
        raise ValueError("Malformed protobuf schema: unclosed block comment.")
    return end + 2


def skip_quoted_string(schema_text: str, cursor: int) -> int:
    quote = schema_text[cursor]
    cursor += 1
    while cursor < len(schema_text):
        char = schema_text[cursor]
        if char == "\\":
            cursor += 2
            continue
        if char == quote:
            return cursor + 1
        cursor += 1
    raise ValueError("Malformed protobuf schema: unclosed quoted string.")


__all__ = [
    "ProtoMessageNode",
    "coerce_protobuf_message",
    "normalize_proto_schema_text",
    "parse_proto_message_tree",
    "protobuf_message_full_name",
    "resolve_proto_message_full_name",
    "tokenize_proto_schema",
    "validate_protobuf_schema_binding",
]
