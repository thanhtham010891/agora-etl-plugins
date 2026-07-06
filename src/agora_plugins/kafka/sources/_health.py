"""Health snapshot caching and projection helpers for Kafka sources."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from agora_plugins.kafka.sources._models import KafkaPartitionHealth, KafkaSourceHealthSnapshot

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from agora_plugins.kafka.sources._consumer_runtime import KafkaPartitionProbeSnapshot
    from agora_plugins.kafka.sources._cursor_state import KafkaCursorState
    from agora_plugins.kafka.sources._delivery import KafkaDeliveryController
    from agora_plugins.kafka.sources._operator_controls import KafkaOperatorController


class KafkaHealthMonitor:
    """Owns cached health snapshots for a Kafka source."""

    def __init__(
        self,
        *,
        cache_ms: int,
        group_id: str,
        bootstrap_servers: str,
        subscription_mode: Callable[[], str],
        cursor_state: KafkaCursorState,
        operator_controls: KafkaOperatorController,
        delivery_controller: KafkaDeliveryController,
        active_assignment: Callable[[], Iterable[tuple[str, int]]],
        refresh_assignment_state: Callable[[Any | None], Awaitable[None]],
        probe_partitions: Callable[[Any | None], Awaitable[KafkaPartitionProbeSnapshot]],
    ) -> None:
        self._cache_ms = cache_ms
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._subscription_mode = subscription_mode
        self._cursor_state = cursor_state
        self._operator_controls = operator_controls
        self._delivery_controller = delivery_controller
        self._active_assignment = active_assignment
        self._refresh_assignment_state = refresh_assignment_state
        self._probe_partitions = probe_partitions
        self._snapshot: KafkaSourceHealthSnapshot | None = None
        self._snapshot_monotonic: float | None = None

    def invalidate(self) -> None:
        self._snapshot = None
        self._snapshot_monotonic = None

    async def snapshot(
        self,
        *,
        force_refresh: bool,
        consumer: Any | None,
        max_idle_polls: int | None,
        idle_poll_count: int,
        rebalance_count: int,
        last_poll_monotonic: float | None,
        last_message_monotonic: float | None,
        last_commit_monotonic: float | None,
        last_rebalance_monotonic: float | None,
    ) -> KafkaSourceHealthSnapshot:
        cached = self._cached_snapshot(force_refresh=force_refresh)
        if cached is not None:
            return cached

        if consumer is not None:
            await self._refresh_assignment_state(consumer)

        probes = await self._probe_partitions(consumer)
        positions = probes.positions
        committed_offsets = probes.committed_offsets
        end_offsets = probes.end_offsets
        active_assignment = sorted(self._active_assignment())

        partitions: list[KafkaPartitionHealth] = []
        total_lag = 0
        any_lag = False
        lagging_partition_count = 0
        max_lag = 0
        total_commit_lag = 0
        any_commit_lag = False
        max_commit_lag = 0
        for topic, partition in active_assignment:
            key = (topic, partition)
            current_offset = positions.get(key)
            committed_offset = committed_offsets.get(key)
            processed_offset = self._cursor_state.processed_offsets.get(key)
            committable_offset = self._cursor_state.committable_offsets.get(key)
            end_offset = end_offsets.get(key)
            lag = None
            if current_offset is not None and end_offset is not None:
                lag = max(0, end_offset - current_offset)
                total_lag += lag
                any_lag = True
                if lag > 0:
                    lagging_partition_count += 1
                max_lag = max(max_lag, lag)
            commit_lag = None
            if committed_offset is not None and end_offset is not None:
                commit_lag = max(0, end_offset - committed_offset)
                total_commit_lag += commit_lag
                any_commit_lag = True
                max_commit_lag = max(max_commit_lag, commit_lag)
            delivery_gap = None
            if processed_offset is not None and committable_offset is not None:
                delivery_gap = max(0, processed_offset - committable_offset)
            commit_gap = None
            if committable_offset is not None and committed_offset is not None:
                commit_gap = max(0, (committable_offset + 1) - committed_offset)
            partitions.append(
                KafkaPartitionHealth(
                    topic=topic,
                    partition=partition,
                    current_offset=current_offset,
                    committed_offset=committed_offset,
                    processed_offset=processed_offset,
                    committable_offset=committable_offset,
                    end_offset=end_offset,
                    lag=lag,
                    commit_lag=commit_lag,
                    delivery_gap=delivery_gap,
                    commit_gap=commit_gap,
                    paused=(
                        self._operator_controls.pause_all_requested
                        or key in self._operator_controls.paused_partitions
                    ),
                )
            )

        now = time.monotonic()
        snapshot = KafkaSourceHealthSnapshot(
            ready=consumer is not None and bool(active_assignment),
            stalled=(
                consumer is not None
                and max_idle_polls is not None
                and idle_poll_count >= max_idle_polls
            ),
            consumer_group=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            subscription_mode=self._subscription_mode(),
            assignment_count=len(active_assignment),
            paused_partition_count=self._operator_controls.paused_partition_count(),
            pending_commit_count=self._cursor_state.pending_commit_count,
            rebalance_count=rebalance_count,
            idle_poll_count=idle_poll_count,
            record_error_count=self._delivery_controller.record_error_count,
            record_drop_count=self._delivery_controller.record_drop_count,
            last_poll_age_ms=_age_ms(last_poll_monotonic, now),
            last_message_age_ms=_age_ms(last_message_monotonic, now),
            last_commit_age_ms=_age_ms(last_commit_monotonic, now),
            last_rebalance_age_ms=_age_ms(last_rebalance_monotonic, now),
            total_lag=total_lag if any_lag else None,
            lagging_partition_count=lagging_partition_count,
            max_lag=max_lag if any_lag else None,
            total_commit_lag=total_commit_lag if any_commit_lag else None,
            max_commit_lag=max_commit_lag if any_commit_lag else None,
            partitions=tuple(partitions),
        )
        self._snapshot = snapshot
        self._snapshot_monotonic = now
        return snapshot

    def _cached_snapshot(
        self,
        *,
        force_refresh: bool,
    ) -> KafkaSourceHealthSnapshot | None:
        if (
            force_refresh
            or self._cache_ms <= 0
            or self._snapshot is None
            or self._snapshot_monotonic is None
        ):
            return None
        age_ms = (time.monotonic() - self._snapshot_monotonic) * 1000.0
        if age_ms > self._cache_ms:
            return None
        return self._snapshot


def _age_ms(start: float | None, end: float) -> float | None:
    if start is None:
        return None
    return max(0.0, (end - start) * 1000.0)
