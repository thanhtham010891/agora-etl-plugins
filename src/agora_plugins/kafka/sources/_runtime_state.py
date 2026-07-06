"""Ephemeral runtime counters and timestamps for Kafka sources."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


class KafkaRuntimeState:
    """Owns mutable runtime counters without tying them to source orchestration."""

    def __init__(self, *, on_change: Callable[[], None]) -> None:
        self._on_change = on_change
        self._rebalance_count = 0
        self._idle_poll_count = 0
        self._last_poll_monotonic: float | None = None
        self._last_message_monotonic: float | None = None
        self._last_commit_monotonic: float | None = None
        self._last_rebalance_monotonic: float | None = None

    @property
    def rebalance_count(self) -> int:
        return self._rebalance_count

    @rebalance_count.setter
    def rebalance_count(self, value: int) -> None:
        self._rebalance_count = int(value)
        self._on_change()

    @property
    def idle_poll_count(self) -> int:
        return self._idle_poll_count

    @idle_poll_count.setter
    def idle_poll_count(self, value: int) -> None:
        self._idle_poll_count = int(value)
        self._on_change()

    @property
    def last_poll_monotonic(self) -> float | None:
        return self._last_poll_monotonic

    @property
    def last_message_monotonic(self) -> float | None:
        return self._last_message_monotonic

    @property
    def last_commit_monotonic(self) -> float | None:
        return self._last_commit_monotonic

    @property
    def last_rebalance_monotonic(self) -> float | None:
        return self._last_rebalance_monotonic

    def record_poll(self) -> None:
        self._last_poll_monotonic = time.monotonic()
        self._on_change()

    def record_delivery_progress(self) -> None:
        self._last_message_monotonic = time.monotonic()
        self._on_change()

    def record_commit_progress(self) -> None:
        self._last_commit_monotonic = time.monotonic()
        self._on_change()

    def record_rebalance(self) -> None:
        self._rebalance_count += 1
        self._last_rebalance_monotonic = time.monotonic()
        self._on_change()
