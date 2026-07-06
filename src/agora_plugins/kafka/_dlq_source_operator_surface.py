"""Operator-facing wrappers for Kafka DLQ source replay/backlog surfaces."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class KafkaDLQSourceOperatorSurface:
    """Owns replay filtering and observability snapshots for Kafka DLQ sources."""

    def __init__(
        self,
        source: Any,
        *,
        snapshot_cls: Any,
        exporter_cls: Any,
    ) -> None:
        self._source = source
        self._snapshot_cls = snapshot_cls
        self._exporter_cls = exporter_cls

    async def stream(self) -> AsyncGenerator[Any, None]:
        self._source._replayable_record_count = 0
        async for record in self._source._iter_records():
            if record.max_attempts is not None and record.attempt >= record.max_attempts:
                self._source._retry_filtered_count += 1
                continue
            self._source._replayable_record_count += 1
            yield record

    def subscription_mode(self) -> str:
        if self._source._assignments:
            return "manual_assign"
        if self._source._topic_pattern is not None:
            return "pattern"
        return "topics"

    def metrics_snapshot(self) -> Any:
        return self._snapshot_cls(
            consumer_group=self._source._group_id,
            bootstrap_servers=self._source._bootstrap_servers,
            subscription_mode=self.subscription_mode(),
            scan_count=self._source._scan_count,
            scanned_message_count=self._source._scanned_message_count,
            upsert_event_count=self._source._upsert_event_count,
            delete_event_count=self._source._delete_event_count,
            start_offset_seek_count=self._source._start_offset_seek_count,
            highwater_stop_count=self._source._highwater_stop_count,
            live_record_count=self._source._live_record_count,
            matched_record_count=self._source._matched_record_count,
            replayable_record_count=self._source._replayable_record_count,
            retry_filtered_count=self._source._retry_filtered_count,
            last_scan_completed_at=self._source._last_scan_completed_at,
            last_record_seen_at=self._source._last_record_seen_at,
        )

    def backlog_snapshot(self) -> Any:
        return self.metrics_snapshot()

    def render_prometheus_metrics(self, namespace: str = "agora_kafka_dlq") -> str:
        return self._exporter_cls(namespace=namespace).render_source(self.metrics_snapshot())
