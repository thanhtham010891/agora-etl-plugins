"""Public-facing sink helpers for Kafka DLQ operations and metrics."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class KafkaDLQSinkSurface:
    """Owns sink-side DLQ envelopes, counters, and Prometheus snapshots."""

    def __init__(
        self,
        sink: Any,
        *,
        now_utc: Callable[[], Any],
        snapshot_cls: Any,
        exporter_cls: Any,
        codec: Any,
    ) -> None:
        self._sink = sink
        self._now_utc = now_utc
        self._snapshot_cls = snapshot_cls
        self._exporter_cls = exporter_cls
        self._codec = codec

    async def write(self, record: Any) -> None:
        await self._sink._sink.write(self.build_upsert_envelope(record))
        self._sink._write_count += 1
        self._sink._upsert_count += 1
        self._sink._last_write_at = self._now_utc()

    async def write_batch(self, records: list[Any]) -> None:
        if not records:
            return
        await self._sink._sink.write_batch(
            [self.build_upsert_envelope(record) for record in records]
        )
        self._sink._write_batch_count += 1
        self._sink._upsert_count += len(records)
        self._sink._last_write_at = self._now_utc()

    async def replay(
        self,
        record: Any,
        *,
        replay_record: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        updated = await replay_record(record)
        await self._sink._sink.write(self.build_upsert_envelope(updated))
        self._sink._replay_count += 1
        self._sink._upsert_count += 1
        self._sink._last_replay_at = self._now_utc()
        return updated

    async def acknowledge(self, record: Any) -> None:
        await self._sink._sink.write(self.build_delete_envelope(record))
        self._sink._acknowledge_count += 1
        self._sink._delete_count += 1
        self._sink._last_acknowledge_at = self._now_utc()

    def build_upsert_envelope(self, record: Any) -> dict[str, Any]:
        return self._codec.build_upsert_envelope(record)

    def build_delete_envelope(self, record: Any) -> dict[str, Any]:
        return self._codec.build_delete_envelope(record)

    def metrics_snapshot(self) -> Any:
        return self._snapshot_cls(
            topic=self._sink._topic,
            bootstrap_servers=self._sink._bootstrap_servers,
            write_count=self._sink._write_count,
            write_batch_count=self._sink._write_batch_count,
            replay_count=self._sink._replay_count,
            acknowledge_count=self._sink._acknowledge_count,
            upsert_count=self._sink._upsert_count,
            delete_count=self._sink._delete_count,
            last_write_at=self._sink._last_write_at,
            last_replay_at=self._sink._last_replay_at,
            last_acknowledge_at=self._sink._last_acknowledge_at,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_kafka_dlq") -> str:
        return self._exporter_cls(namespace=namespace).render_sink(self.metrics_snapshot())
