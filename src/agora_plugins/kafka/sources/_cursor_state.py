"""Cursor and checkpoint state for Kafka source operator controls."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from agora_plugins.kafka.sources._offsets import normalize_offset_map


class KafkaCursorState:
    """Tracks delivery, commit, checkpoint, and resume offsets for a source."""

    def __init__(
        self,
        *,
        start_offsets: Mapping[tuple[str, int], int],
        on_change: Callable[[], None],
    ) -> None:
        self._on_change = on_change
        self._start_offsets = normalize_offset_map(dict(start_offsets))
        self._resume_offsets: dict[tuple[str, int], int] = {}
        self._positioning_applied = False
        self._pending_commit_count = 0
        self._processed_offsets: dict[tuple[str, int], int] = {}
        self._committable_offsets: dict[tuple[str, int], int] = {}
        self._last_seen: tuple[str, int, int] | None = None
        self._checkpoint_cache: dict[str, Any] | None = None
        self._checkpoint_dirty = False

    @property
    def start_offsets(self) -> dict[tuple[str, int], int]:
        return self._start_offsets

    @start_offsets.setter
    def start_offsets(self, offsets: Mapping[tuple[str, int], int]) -> None:
        self._start_offsets = normalize_offset_map(dict(offsets))
        self._on_change()

    @property
    def resume_offsets(self) -> dict[tuple[str, int], int]:
        return self._resume_offsets

    @resume_offsets.setter
    def resume_offsets(self, offsets: Mapping[tuple[str, int], int]) -> None:
        self._resume_offsets = normalize_offset_map(dict(offsets))
        self._on_change()

    @property
    def positioning_applied(self) -> bool:
        return self._positioning_applied

    @positioning_applied.setter
    def positioning_applied(self, value: bool) -> None:
        self._positioning_applied = bool(value)
        self._on_change()

    @property
    def pending_commit_count(self) -> int:
        return self._pending_commit_count

    @pending_commit_count.setter
    def pending_commit_count(self, value: int) -> None:
        self._pending_commit_count = int(value)
        self._on_change()

    @property
    def processed_offsets(self) -> dict[tuple[str, int], int]:
        return self._processed_offsets

    @processed_offsets.setter
    def processed_offsets(self, offsets: Mapping[tuple[str, int], int]) -> None:
        self._processed_offsets = normalize_offset_map(dict(offsets))
        self._checkpoint_dirty = True
        if self._processed_offsets:
            (topic, partition), offset = sorted(self._processed_offsets.items())[-1]
            self._last_seen = (topic, partition, offset)
        if not self._processed_offsets:
            self._last_seen = None
            self._checkpoint_cache = None
        self._on_change()

    @property
    def committable_offsets(self) -> dict[tuple[str, int], int]:
        return self._committable_offsets

    @committable_offsets.setter
    def committable_offsets(self, offsets: Mapping[tuple[str, int], int]) -> None:
        self._committable_offsets = normalize_offset_map(dict(offsets))
        self._on_change()

    @property
    def last_seen(self) -> tuple[str, int, int] | None:
        return self._last_seen

    @last_seen.setter
    def last_seen(self, value: tuple[str, int, int] | None) -> None:
        if value is None:
            self._last_seen = None
        else:
            topic, partition, offset = value
            self._last_seen = (str(topic), int(partition), int(offset))
        self._checkpoint_dirty = True
        self._on_change()

    def reset_for_stream(self) -> None:
        self._pending_commit_count = 0
        self._processed_offsets = {}
        self._committable_offsets = {}
        self._last_seen = None
        self._checkpoint_cache = None
        self._checkpoint_dirty = False
        self._positioning_applied = False
        self._on_change()

    def prepare_resume(self, offsets: Mapping[tuple[str, int], int]) -> None:
        self._positioning_applied = False
        self._resume_offsets = normalize_offset_map(dict(offsets))
        self._on_change()

    def positioning_targets(self) -> dict[tuple[str, int], int]:
        return self._start_offsets or self._resume_offsets

    def has_exact_start_offsets(self) -> bool:
        return bool(self._start_offsets)

    def mark_positioning_applied(self) -> None:
        self._positioning_applied = True
        self._on_change()

    def apply_exact_positioning(self, offsets: Mapping[tuple[str, int], int]) -> None:
        self._start_offsets = normalize_offset_map(dict(offsets))
        self._resume_offsets = {}
        self._positioning_applied = True
        self.discard_tracking_state()

    def clear_positioning_targets(self) -> None:
        self._start_offsets = {}
        self._resume_offsets = {}
        self._positioning_applied = True
        self.discard_tracking_state()

    def remember_processed_offset(self, topic: str, partition: int, offset: int) -> None:
        self._processed_offsets[(str(topic), int(partition))] = int(offset)
        self._last_seen = (str(topic), int(partition), int(offset))
        self._checkpoint_dirty = True
        self._on_change()

    def remember_committable_offset(self, topic: str, partition: int, offset: int) -> None:
        self._committable_offsets[(str(topic), int(partition))] = int(offset)
        self._on_change()

    def increment_pending_commit_count(self, count: int = 1) -> None:
        if count <= 0:
            return
        self._pending_commit_count += int(count)
        self._on_change()

    def clear_pending_commit_count(self) -> None:
        self._pending_commit_count = 0
        self._on_change()

    def mark_delivered_message(
        self,
        topic: str,
        partition: int,
        offset: int,
        *,
        enable_auto_commit: bool,
    ) -> None:
        self.remember_processed_offset(topic, partition, offset)
        self.remember_committable_offset(topic, partition, offset)
        if not enable_auto_commit:
            self.increment_pending_commit_count()

    def mark_transaction_committed(self, topic: str, partition: int, offset: int) -> None:
        self.remember_processed_offset(topic, partition, offset)
        self.remember_committable_offset(topic, partition, offset)
        self.clear_pending_commit_count()

    def build_commit_offsets(
        self,
        build_topic_partition: Callable[[str, int], object],
    ) -> dict[object, int]:
        return {
            build_topic_partition(topic, partition): offset + 1
            for (topic, partition), offset in self._committable_offsets.items()
        }

    def discard_tracking_state(self) -> None:
        self._pending_commit_count = 0
        self._processed_offsets = {}
        self._committable_offsets = {}
        self._last_seen = None
        self._checkpoint_cache = None
        self._checkpoint_dirty = False
        self._on_change()

    def drop_revoked(self, revoked: set[tuple[str, int]]) -> None:
        for key in revoked:
            self._committable_offsets.pop(key, None)
            self._processed_offsets.pop(key, None)
        if self._last_seen is not None and (self._last_seen[0], self._last_seen[1]) in revoked:
            if self._processed_offsets:
                (topic, partition), offset = sorted(self._processed_offsets.items())[-1]
                self._last_seen = (topic, partition, offset)
            else:
                self._last_seen = None
        self._checkpoint_dirty = True
        self._on_change()

    def current_checkpoint(self) -> dict[str, Any] | None:
        if not self._processed_offsets or self._last_seen is None:
            return None
        if not self._checkpoint_dirty and self._checkpoint_cache is not None:
            return self._checkpoint_cache
        last_topic, last_partition, last_offset = self._last_seen
        self._checkpoint_cache = {
            "topic": last_topic,
            "partition": last_partition,
            "offset": last_offset,
            "offsets": [
                {"topic": topic, "partition": partition, "offset": offset}
                for (topic, partition), offset in sorted(self._processed_offsets.items())
            ],
        }
        self._checkpoint_dirty = False
        return self._checkpoint_cache


def normalize_checkpoint_offsets(value: dict[str, Any]) -> dict[tuple[str, int], int]:
    offsets: dict[tuple[str, int], int] = {}
    raw_offsets = value.get("offsets")
    if isinstance(raw_offsets, Iterable) and not isinstance(raw_offsets, (str, bytes, dict)):
        for item in raw_offsets:
            if not isinstance(item, dict):
                continue
            if {"topic", "partition", "offset"} - set(item):
                continue
            offsets[(str(item["topic"]), int(item["partition"]))] = int(item["offset"])

    if offsets:
        return offsets

    if {"topic", "partition", "offset"} - set(value):
        return {}
    return {
        (str(value["topic"]), int(value["partition"])): int(value["offset"]),
    }
