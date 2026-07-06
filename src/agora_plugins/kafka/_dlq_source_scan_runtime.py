"""Scan/runtime helpers for Kafka-backed DLQ sources."""

from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable


class KafkaDLQSourceScanRuntime:
    """Owns assignment waiting, offset bounds, and DLQ compaction scan flow."""

    def __init__(
        self,
        source: Any,
        *,
        now_utc: Callable[[], Any],
        compaction_state_cls: Any,
        decode_envelope: Callable[..., tuple[str, str, Any | None]],
    ) -> None:
        self._source = source
        self._now_utc = now_utc
        self._compaction_state_cls = compaction_state_cls
        self._decode_envelope = decode_envelope

    async def iter_records(self) -> AsyncGenerator[Any, None]:
        consumer = self._source._require_consumer()
        assignment = await self.wait_for_assignment(consumer)
        if not assignment:
            self._source._scan_count += 1
            self._source._last_scan_completed_at = self._now_utc()
            return

        await self.apply_start_offsets(consumer)
        highwater_offsets = await self.partition_highwater_offsets(consumer, assignment)
        compaction = self._compaction_state_cls(
            spill_threshold=self._source._compaction_spill_threshold,
            payload_policy=self._source._payload_policy,
        )
        sequence = 0
        idle_polls = 0

        try:
            while idle_polls < self._source._scan_idle_polls:
                if self._source._assignment_prefetch_batches:
                    batches = self._source._assignment_prefetch_batches.pop(0)
                else:
                    batches = await consumer.getmany(timeout_ms=self._source._poll_timeout_ms)
                non_empty = False
                for messages in batches.values():
                    if not messages:
                        continue
                    non_empty = True
                    for message in messages:
                        operation, storage_key, record = self._decode_envelope(
                            message.value,
                            payload_policy=self._source._payload_policy,
                        )
                        self._source._scanned_message_count += 1
                        self._source._last_record_seen_at = self._now_utc()
                        if operation == "delete":
                            self._source._delete_event_count += 1
                        else:
                            self._source._upsert_event_count += 1
                        compaction.update(
                            sequence=sequence,
                            storage_key=storage_key,
                            record=None if operation == "delete" else record,
                        )
                        sequence += 1
                if non_empty:
                    idle_polls = 0
                else:
                    idle_polls += 1
                if highwater_offsets and await self.positions_reached_highwater(
                    consumer,
                    highwater_offsets,
                ):
                    self._source._highwater_stop_count += 1
                    break

            yielded = 0
            live_records = compaction.live_records()
            self._source._scan_count += 1
            self._source._live_record_count = len(live_records)
            self._source._matched_record_count = 0
            for _, record in live_records:
                if (
                    self._source._pipeline_id is not None
                    and record.pipeline_id != self._source._pipeline_id
                ):
                    continue
                if self._source._stage is not None and record.stage != self._source._stage:
                    continue
                self._source._matched_record_count += 1
                yield record
                yielded += 1
                if self._source._limit is not None and yielded >= self._source._limit:
                    self._source._last_scan_completed_at = self._now_utc()
                    return
            self._source._last_scan_completed_at = self._now_utc()
        finally:
            compaction.close()

    async def wait_for_assignment(self, consumer: Any) -> set[object]:
        self._source._assignment_prefetch_batches.clear()
        for _ in range(20):
            assignment = set(cast("set[object]", consumer.assignment()))
            if assignment:
                return assignment
            batches = await consumer.getmany(timeout_ms=self._source._poll_timeout_ms)
            if batches:
                self._source._assignment_prefetch_batches.append(batches)
        return set()

    async def apply_start_offsets(self, consumer: Any) -> None:
        if not self._source._start_offsets:
            return
        seek = getattr(consumer, "seek", None)
        if not callable(seek):
            return
        for (topic, partition), offset in sorted(self._source._start_offsets.items()):
            result = seek(self._source._build_topic_partition(topic, partition), offset)
            self._source._start_offset_seek_count += 1
            if isawaitable(result):
                await result

    async def partition_highwater_offsets(
        self,
        consumer: Any,
        assignment: set[object],
    ) -> dict[object, int]:
        if not self._source._stop_at_highwater:
            return {}
        end_offsets = getattr(consumer, "end_offsets", None)
        if not callable(end_offsets):
            return {}
        try:
            result = end_offsets(list(assignment))
            if isawaitable(result):
                result = await result
        except Exception:
            return {}
        if not isinstance(result, dict):
            return {}
        highwater: dict[object, int] = {}
        for partition, offset in result.items():
            try:
                highwater[partition] = int(offset)
            except (TypeError, ValueError):
                continue
        return highwater

    async def positions_reached_highwater(
        self,
        consumer: Any,
        highwater_offsets: dict[object, int],
    ) -> bool:
        position = getattr(consumer, "position", None)
        if not callable(position):
            return False
        for partition, highwater in highwater_offsets.items():
            current = position(partition)
            if isawaitable(current):
                current = await current
            try:
                if int(current) < highwater:
                    return False
            except (TypeError, ValueError):
                return False
        return True
