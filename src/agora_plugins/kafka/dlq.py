"""Kafka-backed DLQ sink/source implementations."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from agora.core.dlq import DLQRecord, DLQSink, DLQSource

from agora_plugins.dlq_policy import DLQPayloadPolicy
from agora_plugins.kafka._dlq_compaction import KafkaDLQCompactionState
from agora_plugins.kafka._dlq_metrics import (
    KafkaDLQPrometheusExporter,
    KafkaDLQSinkMetricsSnapshot,
    KafkaDLQSourceMetricsSnapshot,
)
from agora_plugins.kafka._dlq_payloads import (
    decode_dlq_envelope as _decode_dlq_envelope,
)
from agora_plugins.kafka._dlq_payloads import (
    decode_stored_payload as _decode_stored_payload,
)
from agora_plugins.kafka._dlq_payloads import (
    encode_dlq_envelope as _encode_dlq_envelope,
)
from agora_plugins.kafka._dlq_payloads import (
    payload_to_record as _payload_to_record,
)
from agora_plugins.kafka._dlq_payloads import (
    record_headers as _record_headers,
)
from agora_plugins.kafka._dlq_payloads import (
    record_storage_key as _record_storage_key,
)
from agora_plugins.kafka._dlq_payloads import (
    record_to_payload as _record_to_payload,
)
from agora_plugins.kafka._dlq_security import resolve_dlq_security as _resolve_dlq_security
from agora_plugins.kafka._dlq_sink_codec import KafkaDLQSinkCodec
from agora_plugins.kafka._dlq_sink_surface import KafkaDLQSinkSurface
from agora_plugins.kafka._dlq_source_consumer_surface import KafkaDLQSourceConsumerSurface
from agora_plugins.kafka._dlq_source_operator_surface import KafkaDLQSourceOperatorSurface
from agora_plugins.kafka._dlq_source_scan_runtime import KafkaDLQSourceScanRuntime
from agora_plugins.kafka._dlq_source_settings import build_kafka_dlq_source_settings
from agora_plugins.kafka.sinks.kafka import KafkaSink

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable

    from agora_plugins.kafka.config import KafkaSecurityConfig


def _now_utc() -> datetime:
    return datetime.now(UTC)


class _KafkaDLQCompactionState(KafkaDLQCompactionState):
    """Compatibility wrapper that keeps white-box patch points under dlq.py."""

    def __init__(
        self,
        *,
        spill_threshold: int | None,
        payload_policy: DLQPayloadPolicy | None = None,
    ) -> None:
        super().__init__(
            spill_threshold=spill_threshold,
            payload_policy=payload_policy,
            sqlite_module=sqlite3,
            json_module=json,
            record_to_payload=_record_to_payload,
            payload_to_record=_payload_to_record,
            decode_stored_payload=_decode_stored_payload,
        )


class KafkaDLQSink(DLQSink):
    """Publish DLQ records to a Kafka error topic."""

    sink_name = "kafka_dlq"

    def __init__(
        self,
        *,
        topic: str,
        bootstrap_servers: str,
        key_fn: Callable[[DLQRecord], bytes | str] | None = None,
        security_protocol: str = "PLAINTEXT",
        security: KafkaSecurityConfig | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_username_env: str | None = None,
        sasl_password: str | None = None,
        sasl_password_env: str | None = None,
        sasl_password_file: str | None = None,
        sasl_kerberos_service_name: str | None = None,
        sasl_kerberos_domain_name: str | None = None,
        ssl_cafile: str | None = None,
        ssl_cafile_env: str | None = None,
        ssl_certfile: str | None = None,
        ssl_certfile_env: str | None = None,
        ssl_keyfile: str | None = None,
        ssl_keyfile_env: str | None = None,
        ssl_password: str | None = None,
        ssl_password_env: str | None = None,
        ssl_password_file: str | None = None,
        ssl_check_hostname: bool = True,
        payload_policy: DLQPayloadPolicy | None = None,
        **producer_kwargs: Any,
    ) -> None:
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._key_fn = key_fn
        self._payload_policy = payload_policy
        self._write_count = 0
        self._write_batch_count = 0
        self._replay_count = 0
        self._acknowledge_count = 0
        self._upsert_count = 0
        self._delete_count = 0
        self._last_write_at: datetime | None = None
        self._last_replay_at: datetime | None = None
        self._last_acknowledge_at: datetime | None = None
        resolved_security = _resolve_dlq_security(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            security_protocol=security_protocol,
            security=security,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_username_env=sasl_username_env,
            sasl_password=sasl_password,
            sasl_password_env=sasl_password_env,
            sasl_password_file=sasl_password_file,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            ssl_cafile=ssl_cafile,
            ssl_cafile_env=ssl_cafile_env,
            ssl_certfile=ssl_certfile,
            ssl_certfile_env=ssl_certfile_env,
            ssl_keyfile=ssl_keyfile,
            ssl_keyfile_env=ssl_keyfile_env,
            ssl_password=ssl_password,
            ssl_password_env=ssl_password_env,
            ssl_password_file=ssl_password_file,
            ssl_check_hostname=ssl_check_hostname,
        )
        self._codec = KafkaDLQSinkCodec(
            key_fn=key_fn,
            payload_policy=payload_policy,
            encode_envelope=_encode_dlq_envelope,
            record_storage_key=_record_storage_key,
            record_headers=_record_headers,
            payload_to_record=_payload_to_record,
        )
        self._sink = KafkaSink[dict[str, Any]](
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            serializer=self._codec.serialize,
            key_fn=self._codec.partition_key,
            headers_fn=self._codec.headers,
            security_protocol=security_protocol,
            security=resolved_security,
            **producer_kwargs,
        )
        self._surface = KafkaDLQSinkSurface(
            self,
            now_utc=_now_utc,
            snapshot_cls=KafkaDLQSinkMetricsSnapshot,
            exporter_cls=KafkaDLQPrometheusExporter,
            codec=self._codec,
        )

    async def open(self) -> None:
        await self._sink.open()

    async def close(self) -> None:
        await self._sink.close()

    async def write(self, record: DLQRecord) -> None:
        await self._surface.write(record)

    async def write_batch(self, records: list[DLQRecord]) -> None:
        await self._surface.write_batch(records)

    async def replay(self, record: DLQRecord) -> DLQRecord:
        return cast(
            "DLQRecord",
            await self._surface.replay(
                record,
                replay_record=lambda candidate: super(KafkaDLQSink, self).replay(candidate),
            ),
        )

    async def acknowledge(self, record: DLQRecord) -> None:
        await self._surface.acknowledge(record)

    def _build_upsert_envelope(self, record: DLQRecord) -> dict[str, Any]:
        return self._surface.build_upsert_envelope(record)

    def metrics_snapshot(self) -> KafkaDLQSinkMetricsSnapshot:
        return cast("KafkaDLQSinkMetricsSnapshot", self._surface.metrics_snapshot())

    def render_prometheus_metrics(self, namespace: str = "agora_kafka_dlq") -> str:
        return self._surface.render_prometheus_metrics(namespace=namespace)


class KafkaDLQSource(DLQSource):
    """Read the current replayable DLQ state from a Kafka error topic."""

    source_name = "kafka_dlq_source"

    def __init__(
        self,
        *,
        topic: str | None = None,
        topics: list[str] | None = None,
        topic_pattern: str | None = None,
        assignments: Iterable[tuple[str, int]] | None = None,
        bootstrap_servers: str,
        group_id: str | None = None,
        pipeline_id: str | None = None,
        stage: str | None = None,
        limit: int | None = None,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        poll_timeout_ms: int = 500,
        scan_idle_polls: int = 2,
        stop_at_highwater: bool = True,
        security_protocol: str = "PLAINTEXT",
        security: KafkaSecurityConfig | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_username_env: str | None = None,
        sasl_password: str | None = None,
        sasl_password_env: str | None = None,
        sasl_password_file: str | None = None,
        sasl_kerberos_service_name: str | None = None,
        sasl_kerberos_domain_name: str | None = None,
        ssl_cafile: str | None = None,
        ssl_cafile_env: str | None = None,
        ssl_certfile: str | None = None,
        ssl_certfile_env: str | None = None,
        ssl_keyfile: str | None = None,
        ssl_keyfile_env: str | None = None,
        ssl_password: str | None = None,
        ssl_password_env: str | None = None,
        ssl_password_file: str | None = None,
        ssl_check_hostname: bool = True,
        extra_config: dict[str, Any] | None = None,
        start_offsets: dict[tuple[str, int], int] | None = None,
        compaction_spill_threshold: int | None = 100_000,
        payload_policy: DLQPayloadPolicy | None = None,
    ) -> None:
        settings = build_kafka_dlq_source_settings(
            topic=topic,
            topics=topics,
            topic_pattern=topic_pattern,
            assignments=assignments,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            pipeline_id=pipeline_id,
            stage=stage,
            limit=limit,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            poll_timeout_ms=poll_timeout_ms,
            scan_idle_polls=scan_idle_polls,
            stop_at_highwater=stop_at_highwater,
            security_protocol=security_protocol,
            security=security,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_username_env=sasl_username_env,
            sasl_password=sasl_password,
            sasl_password_env=sasl_password_env,
            sasl_password_file=sasl_password_file,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            ssl_cafile=ssl_cafile,
            ssl_cafile_env=ssl_cafile_env,
            ssl_certfile=ssl_certfile,
            ssl_certfile_env=ssl_certfile_env,
            ssl_keyfile=ssl_keyfile,
            ssl_keyfile_env=ssl_keyfile_env,
            ssl_password=ssl_password,
            ssl_password_env=ssl_password_env,
            ssl_password_file=ssl_password_file,
            ssl_check_hostname=ssl_check_hostname,
            extra_config=extra_config,
            start_offsets=start_offsets,
            compaction_spill_threshold=compaction_spill_threshold,
            payload_policy=payload_policy,
            resolve_security=_resolve_dlq_security,
        )
        self._topics = settings.topics
        self._topic_pattern = settings.topic_pattern
        self._assignments = settings.assignments
        self._bootstrap_servers = settings.bootstrap_servers
        self._group_id = settings.group_id
        self._pipeline_id = settings.pipeline_id
        self._stage = settings.stage
        self._limit = settings.limit
        self._auto_offset_reset = settings.auto_offset_reset
        self._enable_auto_commit = settings.enable_auto_commit
        self._poll_timeout_ms = settings.poll_timeout_ms
        self._scan_idle_polls = settings.scan_idle_polls
        self._stop_at_highwater = settings.stop_at_highwater
        self._security = settings.security
        self._security_protocol = settings.security_protocol
        self._extra_config = settings.extra_config
        self._start_offsets = settings.start_offsets
        self._compaction_spill_threshold = settings.compaction_spill_threshold
        self._payload_policy = settings.payload_policy
        self._scan_count = 0
        self._scanned_message_count = 0
        self._upsert_event_count = 0
        self._delete_event_count = 0
        self._start_offset_seek_count = 0
        self._highwater_stop_count = 0
        self._live_record_count = 0
        self._matched_record_count = 0
        self._replayable_record_count = 0
        self._retry_filtered_count = 0
        self._last_scan_completed_at: datetime | None = None
        self._last_record_seen_at: datetime | None = None
        self._consumer: Any | None = None
        self._topic_partition_cls: Any | None = None
        self._assignment_prefetch_batches: list[dict[object, list[Any]]] = []
        self._consumer_surface = KafkaDLQSourceConsumerSurface(self)
        self._scan_runtime = KafkaDLQSourceScanRuntime(
            self,
            now_utc=_now_utc,
            compaction_state_cls=_KafkaDLQCompactionState,
            decode_envelope=_decode_dlq_envelope,
        )
        self._operator_surface = KafkaDLQSourceOperatorSurface(
            self,
            snapshot_cls=KafkaDLQSourceMetricsSnapshot,
            exporter_cls=KafkaDLQPrometheusExporter,
        )

    async def open(self) -> None:
        await self._consumer_surface.open()

    async def close(self) -> None:
        await self._consumer_surface.close()

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        async for record in self._scan_runtime.iter_records():
            yield record

    async def stream(self) -> AsyncGenerator[DLQRecord, None]:
        async for record in self._operator_surface.stream():
            yield record

    def _require_consumer(self) -> Any:
        return self._consumer_surface.require_consumer()

    def _build_topic_partition(self, topic: str, partition: int) -> object:
        return self._consumer_surface.build_topic_partition(topic, partition)

    def _security_kwargs(self) -> dict[str, Any]:
        return self._consumer_surface.security_kwargs()

    async def _wait_for_assignment(self, consumer: Any) -> set[object]:
        return await self._scan_runtime.wait_for_assignment(consumer)

    async def _apply_start_offsets(self, consumer: Any) -> None:
        await self._scan_runtime.apply_start_offsets(consumer)

    async def _partition_highwater_offsets(
        self,
        consumer: Any,
        assignment: set[object],
    ) -> dict[object, int]:
        return await self._scan_runtime.partition_highwater_offsets(
            consumer,
            assignment,
        )

    async def _positions_reached_highwater(
        self,
        consumer: Any,
        highwater_offsets: dict[object, int],
    ) -> bool:
        return await self._scan_runtime.positions_reached_highwater(
            consumer,
            highwater_offsets,
        )

    def _subscription_mode(self) -> str:
        return self._operator_surface.subscription_mode()

    def metrics_snapshot(self) -> KafkaDLQSourceMetricsSnapshot:
        return cast("KafkaDLQSourceMetricsSnapshot", self._operator_surface.metrics_snapshot())

    def backlog_snapshot(self) -> KafkaDLQSourceMetricsSnapshot:
        return cast("KafkaDLQSourceMetricsSnapshot", self._operator_surface.backlog_snapshot())

    def render_prometheus_metrics(self, namespace: str = "agora_kafka_dlq") -> str:
        return self._operator_surface.render_prometheus_metrics(namespace=namespace)


__all__ = [
    "DLQPayloadPolicy",
    "KafkaDLQPrometheusExporter",
    "KafkaDLQSink",
    "KafkaDLQSinkMetricsSnapshot",
    "KafkaDLQSource",
    "KafkaDLQSourceMetricsSnapshot",
    "_decode_dlq_envelope",
    "_encode_dlq_envelope",
    "_payload_to_record",
    "_record_to_payload",
]
