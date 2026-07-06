"""Public-facing operation and observability helpers for Redis DLQ sinks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class RedisDLQSinkSurface:
    """Owns write/replay/ack flows plus sink-side observability snapshots."""

    def __init__(
        self,
        sink: Any,
        *,
        now_utc: Callable[[], Any],
        snapshot_cls: Any,
        acceptance_gate_cls: Any,
        exporter_cls: Any,
    ) -> None:
        self._sink = sink
        self._now_utc = now_utc
        self._snapshot_cls = snapshot_cls
        self._acceptance_gate_cls = acceptance_gate_cls
        self._exporter_cls = exporter_cls

    async def write(self, record: Any) -> None:
        client = self._sink._require_client()
        self._sink._write_call_count += 1
        record_key = self._sink._record_key(record)
        object.__setattr__(record, "_storage_id", record_key)
        should_index = await self._sink._write_record(client, record, record_key)
        self._sink._upserted_record_count += 1
        if should_index:
            self._sink._inserted_record_count += 1
        else:
            self._sink._updated_record_count += 1
        self._sink._last_write_at = self._now_utc()

    async def write_batch(self, records: list[Any]) -> None:
        client = self._sink._require_client()
        if not records:
            return
        self._sink._write_batch_call_count += 1
        inserted_count = 0
        for record in records:
            record_key = self._sink._record_key(record)
            object.__setattr__(record, "_storage_id", record_key)
            if await self._sink._write_record(client, record, record_key):
                inserted_count += 1
        self._sink._inserted_record_count += inserted_count
        self._sink._updated_record_count += len(records) - inserted_count
        self._sink._upserted_record_count += len(records)
        self._sink._last_write_at = self._now_utc()

    async def replay(
        self,
        record: Any,
        *,
        replay_record: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        client = self._sink._require_client()
        record_key = self._sink._existing_record_key(record)
        updated = await replay_record(record)
        object.__setattr__(updated, "_storage_id", record_key)
        await client.hset(record_key, mapping={"attempt": str(updated.attempt)})
        self._sink._replay_count += 1
        self._sink._replayed_record_count += 1
        self._sink._last_replay_at = self._now_utc()
        return updated

    async def acknowledge(self, record: Any) -> None:
        client = self._sink._require_client()
        record_key = self._sink._existing_record_key(record)
        await self._sink._acknowledge_record(client, record, record_key)
        self._sink._acknowledge_count += 1
        self._sink._acknowledged_record_count += 1
        self._sink._last_acknowledge_at = self._now_utc()

    def metrics_snapshot(self) -> Any:
        return self._snapshot_cls(
            key_prefix=self._sink._key_prefix,
            connection_ready=self._sink._client is not None,
            write_call_count=self._sink._write_call_count,
            write_batch_call_count=self._sink._write_batch_call_count,
            inserted_record_count=self._sink._inserted_record_count,
            upserted_record_count=self._sink._upserted_record_count,
            updated_record_count=self._sink._updated_record_count,
            replay_count=self._sink._replay_count,
            replayed_record_count=self._sink._replayed_record_count,
            acknowledge_count=self._sink._acknowledge_count,
            acknowledged_record_count=self._sink._acknowledged_record_count,
            last_write_at=self._sink._last_write_at,
            last_replay_at=self._sink._last_replay_at,
            last_acknowledge_at=self._sink._last_acknowledge_at,
        )

    def acceptance_report(self, thresholds: Any = None) -> Any:
        return self._acceptance_gate_cls().evaluate_dlq_sink(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return self._exporter_cls(namespace=namespace).render_dlq_sink(self.metrics_snapshot())
