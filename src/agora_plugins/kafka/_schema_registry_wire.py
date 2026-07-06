"""Confluent Schema Registry wire-format helpers."""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

CONFLUENT_MAGIC_BYTE = 0


def encode_confluent_prefix(
    schema_id: int,
    *,
    message_indexes: Sequence[int] | None = None,
) -> bytes:
    prefix = bytearray()
    prefix.append(CONFLUENT_MAGIC_BYTE)
    prefix.extend(struct.pack(">I", int(schema_id)))
    if message_indexes is not None:
        prefix.extend(encode_message_indexes(message_indexes))
    return bytes(prefix)


def decode_confluent_prefix(
    value: bytes,
    *,
    expect_message_indexes: bool = False,
) -> tuple[int, int, tuple[int, ...] | None]:
    if len(value) < 5:
        raise ValueError("Schema-registry payload must be at least 5 bytes long.")
    if value[0] != CONFLUENT_MAGIC_BYTE:
        raise ValueError("Unsupported schema-registry payload magic byte.")
    schema_id = struct.unpack(">I", value[1:5])[0]
    payload_offset = 5
    message_indexes: tuple[int, ...] | None = None
    if expect_message_indexes:
        message_indexes, payload_offset = decode_message_indexes(value, payload_offset)
    return schema_id, payload_offset, message_indexes


def encode_message_indexes(indexes: Sequence[int]) -> bytes:
    normalized = tuple(int(index) for index in indexes)
    if normalized == (0,):
        return b"\x00"
    encoded = bytearray()
    encoded.extend(encode_zigzag_varint(len(normalized)))
    for index in normalized:
        if index < 0:
            raise ValueError("Protobuf message indexes must be >= 0.")
        encoded.extend(encode_zigzag_varint(index))
    return bytes(encoded)


def decode_message_indexes(value: bytes, offset: int) -> tuple[tuple[int, ...], int]:
    if offset >= len(value):
        raise ValueError("Schema-registry Protobuf payload is missing message indexes.")
    if value[offset] == 0:
        return (0,), offset + 1
    length, offset = decode_zigzag_varint(value, offset)
    indexes: list[int] = []
    for _ in range(length):
        item, offset = decode_zigzag_varint(value, offset)
        indexes.append(item)
    return tuple(indexes), offset


def encode_zigzag_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("Varint value must be >= 0.")
    return encode_unsigned_varint(value << 1)


def decode_zigzag_varint(value: bytes, offset: int) -> tuple[int, int]:
    encoded, offset = decode_unsigned_varint(value, offset)
    decoded = (encoded >> 1) ^ -(encoded & 1)
    if decoded < 0:
        raise ValueError("Decoded zigzag varint must be >= 0 for message indexes.")
    return decoded, offset


def encode_unsigned_varint(value: int) -> bytes:
    encoded = bytearray()
    remaining = value
    while True:
        bits = remaining & 0x7F
        remaining >>= 7
        if remaining:
            encoded.append(bits | 0x80)
        else:
            encoded.append(bits)
            return bytes(encoded)


def decode_unsigned_varint(value: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    decoded = 0
    cursor = offset
    while cursor < len(value):
        item = value[cursor]
        cursor += 1
        decoded |= (item & 0x7F) << shift
        if not item & 0x80:
            return decoded, cursor
        shift += 7
    raise ValueError("Unexpected end of schema-registry varint payload.")
