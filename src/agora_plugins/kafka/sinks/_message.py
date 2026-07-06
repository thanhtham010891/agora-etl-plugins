"""Kafka sink publish message models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterable

UNSET = object()


@dataclass(frozen=True, slots=True)
class KafkaSinkMessage:
    """Per-record Kafka publish envelope."""

    value: bytes | None | object = UNSET
    topic: str | object = UNSET
    key: bytes | None | object = UNSET
    partition: int | None | object = UNSET
    headers: Iterable[tuple[str, bytes]] | None | object = UNSET
    timestamp_ms: int | None | object = UNSET


@dataclass(frozen=True, slots=True)
class ResolvedKafkaSinkMessage:
    topic: str
    value: bytes
    key: bytes | None
    partition: int | None
    headers: list[tuple[str, bytes]] | None
    timestamp_ms: int | None


def coerce_headers(
    headers: Iterable[tuple[str, bytes]] | None | object,
) -> list[tuple[str, bytes]] | None:
    if headers is None or headers is UNSET:
        return None
    return list(cast("Iterable[tuple[str, bytes]]", headers))
