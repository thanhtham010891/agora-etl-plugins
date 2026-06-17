"""Small offset normalization helpers shared by Kafka source operations."""

from __future__ import annotations


def normalize_offset_map(
    value: dict[tuple[str, int], int],
) -> dict[tuple[str, int], int]:
    return {
        (str(topic), int(partition)): int(offset) for (topic, partition), offset in value.items()
    }
