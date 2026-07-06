"""Read-loop and observability helpers for Redis DLQ sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable


class RedisDLQSourceSurface:
    """Owns chunked DLQ scans plus source-side observability snapshots."""

    def __init__(
        self,
        source: Any,
        *,
        now_utc: Callable[[], Any],
        hash_to_record: Callable[..., Any],
        snapshot_cls: Any,
        acceptance_gate_cls: Any,
        exporter_cls: Any,
    ) -> None:
        self._source = source
        self._now_utc = now_utc
        self._hash_to_record = hash_to_record
        self._snapshot_cls = snapshot_cls
        self._acceptance_gate_cls = acceptance_gate_cls
        self._exporter_cls = exporter_cls

    async def iter_records(self) -> AsyncGenerator[Any, None]:
        client = self._source._require_client()
        self._source._scan_count += 1
        self._source._last_scan_at = self._now_utc()

        chunk_size = 100
        yielded = 0
        index_key = await self._source._resolve_index_key(client)
        index_snapshot = await client.lrange(index_key, 0, -1)
        for start in range(0, len(index_snapshot), chunk_size):
            keys = index_snapshot[start : start + chunk_size]

            # Batch fetch chunked hashes to avoid N+1 round trips without
            # offset-based pagination over a mutable index list.
            async with client.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.hgetall(key)
                payloads = await pipe.execute()

            for _record_key, payload in zip(keys, payloads, strict=True):
                if not payload:
                    continue
                record = self._hash_to_record(payload, payload_policy=self._source._payload_policy)
                if (
                    self._source._pipeline_id is not None
                    and record.pipeline_id != self._source._pipeline_id
                ):
                    continue
                if self._source._stage is not None and record.stage != self._source._stage:
                    continue
                yield record
                yielded += 1
                self._source._emitted_record_count += 1
                self._source._last_record_at = self._now_utc()
                if self._source._limit is not None and yielded >= self._source._limit:
                    return

    def metrics_snapshot(self) -> Any:
        return self._snapshot_cls(
            key_prefix=self._source._key_prefix,
            pipeline_id=self._source._pipeline_id,
            stage=self._source._stage,
            limit=self._source._limit,
            connection_ready=self._source._client is not None,
            scan_count=self._source._scan_count,
            emitted_record_count=self._source._emitted_record_count,
            last_scan_at=self._source._last_scan_at,
            last_record_at=self._source._last_record_at,
        )

    def acceptance_report(self, thresholds: Any = None) -> Any:
        return self._acceptance_gate_cls().evaluate_dlq_source(
            self.metrics_snapshot(),
            thresholds=thresholds,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_redis") -> str:
        return self._exporter_cls(namespace=namespace).render_dlq_source(self.metrics_snapshot())
