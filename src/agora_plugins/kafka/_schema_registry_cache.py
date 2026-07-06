"""Small cache helpers used by Schema Registry codecs."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections import OrderedDict

K = TypeVar("K")
V = TypeVar("V")


def coerce_schema_cache_max_entries(schema_cache_max_entries: int) -> int:
    if schema_cache_max_entries < 1:
        raise ValueError("schema_cache_max_entries must be >= 1.")
    return schema_cache_max_entries


def lru_cache_get(cache: OrderedDict[K, V], key: K) -> V | None:
    value = cache.get(key)
    if value is None:
        return None
    cache.move_to_end(key)
    return value


def lru_cache_put(
    cache: OrderedDict[K, V],
    key: K,
    value: V,
    *,
    max_entries: int,
) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_entries:
        cache.popitem(last=False)
