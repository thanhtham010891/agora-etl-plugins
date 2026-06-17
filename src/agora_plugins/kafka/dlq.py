"""Kafka-backed DLQ sink/source implementations."""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

from agora.core.dlq import DLQRecord, DLQSink, DLQSource
from agora.metrics.exporters import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)
from pydantic import SecretStr

from agora_plugins.dlq_policy import DLQPayloadPolicy
from agora_plugins.kafka.config import KafkaPluginConfig, KafkaSecurityConfig
from agora_plugins.kafka.sinks.kafka import KafkaSink

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable


def _serialize_json(value: Any) -> Any:
    if value is None:
        return None
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value)


def _record_to_payload(
    record: DLQRecord,
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> dict[str, Any]:
    payload = {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": _serialize_json(record.record),
        "original_record": _serialize_json(record.original_record),
        "processed_record": _serialize_json(record.processed_record),
        "source": record.source,
        "checkpoint": _serialize_json(record.checkpoint),
        "details": _serialize_json(record.details),
        "middleware": record.middleware,
        "sink": record.sink,
        "created_at": record.created_at.isoformat(),
        "attempt": record.attempt,
        "max_attempts": record.max_attempts,
    }
    if payload_policy is None:
        return payload
    return payload_policy.apply(payload)


def _payload_to_record(payload: dict[str, Any]) -> DLQRecord:
    return DLQRecord(
        pipeline_id=str(payload["pipeline_id"]),
        run_id=str(payload["run_id"]),
        stage=str(payload["stage"]),
        error_type=str(payload["error_type"]),
        error_message=str(payload["error_message"]),
        record=payload.get("record"),
        original_record=payload.get("original_record"),
        processed_record=payload.get("processed_record"),
        source=(str(payload["source"]) if payload.get("source") is not None else None),
        checkpoint=payload.get("checkpoint"),
        details=payload.get("details"),
        middleware=(str(payload["middleware"]) if payload.get("middleware") is not None else None),
        sink=(str(payload["sink"]) if payload.get("sink") is not None else None),
        created_at=_coerce_datetime(str(payload["created_at"])),
        attempt=int(payload.get("attempt", 0)),
        max_attempts=(
            int(payload["max_attempts"]) if payload.get("max_attempts") is not None else None
        ),
    )


def _storage_key_from_value(value: bytes | str) -> str:
    if isinstance(value, str):
        return value
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.hex()


def _default_storage_key(record: DLQRecord) -> str:
    return (
        f"{record.pipeline_id}:{record.run_id}:{record.stage}:"
        f"{record.created_at.isoformat()}:{uuid.uuid4().hex}"
    )


def _legacy_storage_key(record: DLQRecord) -> str:
    payload = json.dumps(
        _record_to_payload(record),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return (
        f"{record.pipeline_id}:{record.run_id}:{record.stage}:"
        f"{record.created_at.isoformat()}:{digest}"
    )


def _record_storage_key(
    record: DLQRecord,
    key_fn: Callable[[DLQRecord], bytes | str] | None = None,
) -> str:
    storage_id = record._storage_id
    if isinstance(storage_id, str) and storage_id:
        return storage_id
    if key_fn is None:
        return _default_storage_key(record)
    return _storage_key_from_value(key_fn(record))


def _resolve_dlq_security(
    *,
    bootstrap_servers: str,
    topic: str | None,
    security_protocol: str,
    security: KafkaSecurityConfig | None,
    sasl_mechanism: str | None,
    sasl_username: str | None,
    sasl_username_env: str | None,
    sasl_password: str | None,
    sasl_password_env: str | None,
    sasl_password_file: str | None,
    sasl_kerberos_service_name: str | None,
    sasl_kerberos_domain_name: str | None,
    ssl_cafile: str | None,
    ssl_cafile_env: str | None,
    ssl_certfile: str | None,
    ssl_certfile_env: str | None,
    ssl_keyfile: str | None,
    ssl_keyfile_env: str | None,
    ssl_password: str | None,
    ssl_password_env: str | None,
    ssl_password_file: str | None,
    ssl_check_hostname: bool,
) -> KafkaSecurityConfig | None:
    if security is not None:
        if security.security_protocol != security_protocol:
            raise ValueError(
                "Kafka DLQ security_protocol must match security.security_protocol when both are set."
            )
        return security
    return KafkaPluginConfig(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_username_env=sasl_username_env,
        sasl_password=SecretStr(sasl_password) if sasl_password is not None else None,
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
        ssl_password=SecretStr(ssl_password) if ssl_password is not None else None,
        ssl_password_env=ssl_password_env,
        ssl_password_file=ssl_password_file,
        ssl_check_hostname=ssl_check_hostname,
    ).security()


def _record_headers(
    storage_key: str,
    record: DLQRecord | None,
    *,
    operation: str,
) -> list[tuple[str, bytes]]:
    headers = [
        ("dlq_operation", operation.encode("utf-8")),
        ("dlq_storage_key", storage_key.encode("utf-8")),
    ]
    if record is not None:
        headers.extend(
            [
                ("pipeline_id", record.pipeline_id.encode("utf-8")),
                ("stage", record.stage.encode("utf-8")),
                ("error_type", record.error_type.encode("utf-8")),
            ]
        )
    return headers


def _encode_dlq_envelope(
    storage_key: str,
    *,
    operation: str,
    record: DLQRecord | None = None,
    payload_policy: DLQPayloadPolicy | None = None,
) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "op": operation,
        "storage_key": storage_key,
    }
    if record is not None:
        payload = _record_to_payload(record, payload_policy=payload_policy)
        if payload_policy is not None and payload_policy.mode == "encrypted":
            envelope.update(payload_policy.encrypt_payload(payload))
        else:
            envelope["payload"] = payload
    return envelope


def _decode_dlq_envelope(
    payload: bytes,
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> tuple[str, str, DLQRecord | None]:
    decoded = cast("dict[str, Any]", json.loads(payload.decode("utf-8")))
    if "op" in decoded:
        operation = str(decoded["op"])
        storage_key = str(decoded["storage_key"])
        record_payload = _decode_stored_payload(decoded, payload_policy=payload_policy)
        record = None if record_payload is None else _payload_to_record(record_payload)
        if record is not None:
            object.__setattr__(record, "_storage_id", storage_key)
        return operation, storage_key, record

    record = _payload_to_record(decoded)
    storage_key = _legacy_storage_key(record)
    object.__setattr__(record, "_storage_id", storage_key)
    return "put", storage_key, record


def _decode_stored_payload(
    payload: dict[str, Any],
    *,
    payload_policy: DLQPayloadPolicy | None,
) -> dict[str, Any] | None:
    if "payload" in payload:
        record_payload = payload.get("payload")
        return None if record_payload is None else cast("dict[str, Any]", record_payload)
    if payload.get("payload_encoding") == "encrypted":
        if payload_policy is None:
            raise ValueError("Encrypted Kafka DLQ payload requires a DLQPayloadPolicy.")
        return payload_policy.decrypt_payload(payload)
    return None


class _KafkaDLQCompactionState:
    def __init__(
        self,
        *,
        spill_threshold: int | None,
        payload_policy: DLQPayloadPolicy | None = None,
    ) -> None:
        self._spill_threshold = spill_threshold
        self._payload_policy = payload_policy
        self._memory: dict[str, tuple[int, DLQRecord | None]] = {}
        self._conn: sqlite3.Connection | None = None

    def update(self, *, sequence: int, storage_key: str, record: DLQRecord | None) -> None:
        stored_record = self._apply_payload_policy(record)
        if self._conn is None:
            should_spill = (
                self._spill_threshold is not None
                and len(self._memory) >= self._spill_threshold
                and storage_key not in self._memory
            )
            if should_spill:
                self._spill_to_sqlite()
        if self._conn is None:
            self._memory[storage_key] = (sequence, stored_record)
            return

        payload = None if stored_record is None else self._record_payload_json(stored_record)
        self._conn.execute(
            """
            INSERT INTO dlq_compaction(storage_key, sequence, payload, deleted)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(storage_key) DO UPDATE SET
                sequence=excluded.sequence,
                payload=excluded.payload,
                deleted=excluded.deleted
            """,
            (storage_key, sequence, payload, int(stored_record is None)),
        )

    def live_records(self) -> list[tuple[int, DLQRecord]]:
        if self._conn is None:
            return sorted(
                ((order, record) for order, record in self._memory.values() if record is not None),
                key=lambda item: item[0],
            )

        rows = self._conn.execute(
            """
            SELECT storage_key, sequence, payload
            FROM dlq_compaction
            WHERE deleted = 0 AND payload IS NOT NULL
            ORDER BY sequence ASC
            """
        ).fetchall()
        records: list[tuple[int, DLQRecord]] = []
        for storage_key, sequence, payload in rows:
            record_payload = self._decode_record_payload_json(cast("str", payload))
            record = _payload_to_record(record_payload)
            object.__setattr__(record, "_storage_id", storage_key)
            records.append((int(sequence), record))
        return records

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _spill_to_sqlite(self) -> None:
        self._conn = sqlite3.connect("")
        self._conn.execute(
            """
            CREATE TABLE dlq_compaction (
                storage_key TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL,
                payload TEXT,
                deleted INTEGER NOT NULL
            )
            """
        )
        for storage_key, (sequence, record) in self._memory.items():
            payload = None if record is None else self._record_payload_json(record)
            self._conn.execute(
                """
                INSERT INTO dlq_compaction(storage_key, sequence, payload, deleted)
                VALUES (?, ?, ?, ?)
                """,
                (storage_key, sequence, payload, int(record is None)),
            )
        self._memory.clear()

    def _apply_payload_policy(self, record: DLQRecord | None) -> DLQRecord | None:
        if (
            record is None
            or self._payload_policy is None
            or self._payload_policy.mode in {"raw", "encrypted"}
        ):
            return record
        payload = _record_to_payload(record, payload_policy=self._payload_policy)
        redacted = _payload_to_record(payload)
        object.__setattr__(redacted, "_storage_id", record._storage_id)
        return redacted

    def _record_payload_json(self, record: DLQRecord) -> str:
        payload = _record_to_payload(record, payload_policy=self._payload_policy)
        if self._payload_policy is not None and self._payload_policy.mode == "encrypted":
            return json.dumps(
                self._payload_policy.encrypt_payload(payload),
                ensure_ascii=False,
                sort_keys=True,
            )
        return json.dumps(payload, ensure_ascii=False)

    def _decode_record_payload_json(self, payload_json: str) -> dict[str, Any]:
        payload = cast("dict[str, Any]", json.loads(payload_json))
        if "pipeline_id" in payload:
            return payload
        decoded = _decode_stored_payload(payload, payload_policy=self._payload_policy)
        if decoded is None:
            raise ValueError("Kafka DLQ compaction payload is missing.")
        return decoded


def _age_ms(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return max((_now_utc() - timestamp).total_seconds() * 1000.0, 0.0)


@dataclass(frozen=True, slots=True)
class KafkaDLQSinkMetricsSnapshot:
    """Operational counters for Kafka DLQ sink activity."""

    topic: str
    bootstrap_servers: str
    write_count: int = 0
    write_batch_count: int = 0
    replay_count: int = 0
    acknowledge_count: int = 0
    upsert_count: int = 0
    delete_count: int = 0
    last_write_at: datetime | None = None
    last_replay_at: datetime | None = None
    last_acknowledge_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "bootstrap_servers": self.bootstrap_servers,
            "write_count": self.write_count,
            "write_batch_count": self.write_batch_count,
            "replay_count": self.replay_count,
            "acknowledge_count": self.acknowledge_count,
            "upsert_count": self.upsert_count,
            "delete_count": self.delete_count,
            "last_write_at": (
                None if self.last_write_at is None else self.last_write_at.isoformat()
            ),
            "last_replay_at": (
                None if self.last_replay_at is None else self.last_replay_at.isoformat()
            ),
            "last_acknowledge_at": (
                None if self.last_acknowledge_at is None else self.last_acknowledge_at.isoformat()
            ),
        }


@dataclass(frozen=True, slots=True)
class KafkaDLQSourceMetricsSnapshot:
    """Replay/backlog observability for Kafka DLQ source scans."""

    consumer_group: str
    bootstrap_servers: str
    subscription_mode: str
    scan_count: int = 0
    scanned_message_count: int = 0
    upsert_event_count: int = 0
    delete_event_count: int = 0
    start_offset_seek_count: int = 0
    highwater_stop_count: int = 0
    live_record_count: int = 0
    matched_record_count: int = 0
    replayable_record_count: int = 0
    retry_filtered_count: int = 0
    last_scan_completed_at: datetime | None = None
    last_record_seen_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "consumer_group": self.consumer_group,
            "bootstrap_servers": self.bootstrap_servers,
            "subscription_mode": self.subscription_mode,
            "scan_count": self.scan_count,
            "scanned_message_count": self.scanned_message_count,
            "upsert_event_count": self.upsert_event_count,
            "delete_event_count": self.delete_event_count,
            "start_offset_seek_count": self.start_offset_seek_count,
            "highwater_stop_count": self.highwater_stop_count,
            "live_record_count": self.live_record_count,
            "matched_record_count": self.matched_record_count,
            "replayable_record_count": self.replayable_record_count,
            "retry_filtered_count": self.retry_filtered_count,
            "last_scan_completed_at": (
                None
                if self.last_scan_completed_at is None
                else self.last_scan_completed_at.isoformat()
            ),
            "last_record_seen_at": (
                None if self.last_record_seen_at is None else self.last_record_seen_at.isoformat()
            ),
        }


class KafkaDLQPrometheusExporter:
    """Zero-dependency Prometheus renderer for Kafka DLQ sink/source metrics."""

    def __init__(self, namespace: str = "agora_kafka_dlq") -> None:
        self._ns = namespace

    def render_sink(self, snapshot: KafkaDLQSinkMetricsSnapshot) -> str:
        labels = ",".join(
            [
                f'topic="{escape_label_value(snapshot.topic)}"',
                f'bootstrap_servers="{escape_label_value(snapshot.bootstrap_servers)}"',
            ]
        )
        lines: list[str] = []
        ns = self._ns

        append_metric_header(
            lines,
            help_text="Kafka DLQ sink monotonic event counters",
            metric_type="counter",
            name=f"{ns}_sink_events_total",
        )
        for event_name, value in (
            ("write", snapshot.write_count),
            ("write_batch", snapshot.write_batch_count),
            ("replay", snapshot.replay_count),
            ("acknowledge", snapshot.acknowledge_count),
            ("upsert", snapshot.upsert_count),
            ("delete", snapshot.delete_count),
        ):
            lines.append(f'{ns}_sink_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka DLQ sink last-activity age in milliseconds",
            metric_type="gauge",
            name=f"{ns}_sink_age_ms",
        )
        for activity_name, age_value in (
            ("write", _age_ms(snapshot.last_write_at)),
            ("replay", _age_ms(snapshot.last_replay_at)),
            ("acknowledge", _age_ms(snapshot.last_acknowledge_at)),
        ):
            if age_value is None:
                continue
            lines.append(f'{ns}_sink_age_ms{{{labels},activity="{activity_name}"}} {age_value:.6f}')

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"

    def render_source(self, snapshot: KafkaDLQSourceMetricsSnapshot) -> str:
        labels = ",".join(
            [
                f'consumer_group="{escape_label_value(snapshot.consumer_group)}"',
                f'bootstrap_servers="{escape_label_value(snapshot.bootstrap_servers)}"',
                f'subscription_mode="{escape_label_value(snapshot.subscription_mode)}"',
            ]
        )
        lines: list[str] = []
        ns = self._ns

        append_metric_header(
            lines,
            help_text="Kafka DLQ source backlog gauges from the latest scan",
            metric_type="gauge",
            name=f"{ns}_source_backlog",
        )
        for gauge_name, value in (
            ("live_record_count", snapshot.live_record_count),
            ("matched_record_count", snapshot.matched_record_count),
            ("replayable_record_count", snapshot.replayable_record_count),
        ):
            lines.append(f'{ns}_source_backlog{{{labels},gauge="{gauge_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka DLQ source monotonic scan and replay counters",
            metric_type="counter",
            name=f"{ns}_source_events_total",
        )
        for event_name, value in (
            ("scan", snapshot.scan_count),
            ("scanned_message", snapshot.scanned_message_count),
            ("upsert", snapshot.upsert_event_count),
            ("delete", snapshot.delete_event_count),
            ("start_offset_seek", snapshot.start_offset_seek_count),
            ("highwater_stop", snapshot.highwater_stop_count),
            ("retry_filtered", snapshot.retry_filtered_count),
        ):
            lines.append(f'{ns}_source_events_total{{{labels},event="{event_name}"}} {value}')

        append_metric_header(
            lines,
            help_text="Kafka DLQ source last-activity age in milliseconds",
            metric_type="gauge",
            name=f"{ns}_source_age_ms",
        )
        for activity_name, age_value in (
            ("scan", _age_ms(snapshot.last_scan_completed_at)),
            ("record_seen", _age_ms(snapshot.last_record_seen_at)),
        ):
            if age_value is None:
                continue
            lines.append(
                f'{ns}_source_age_ms{{{labels},activity="{activity_name}"}} {age_value:.6f}'
            )

        lines.append(render_scrape_time_line())
        return "\n".join(lines) + "\n"


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
        self._sink = KafkaSink[dict[str, Any]](
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            serializer=lambda envelope: json.dumps(
                envelope,
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8"),
            key_fn=lambda envelope: str(envelope["storage_key"]).encode("utf-8"),
            headers_fn=lambda envelope: _record_headers(
                str(envelope["storage_key"]),
                (
                    _payload_to_record(cast("dict[str, Any]", envelope["payload"]))
                    if envelope.get("payload") is not None
                    else None
                ),
                operation=str(envelope["op"]),
            ),
            security_protocol=security_protocol,
            security=resolved_security,
            **producer_kwargs,
        )

    async def open(self) -> None:
        await self._sink.open()

    async def close(self) -> None:
        await self._sink.close()

    async def write(self, record: DLQRecord) -> None:
        await self._sink.write(self._build_upsert_envelope(record))
        self._write_count += 1
        self._upsert_count += 1
        self._last_write_at = _now_utc()

    async def write_batch(self, records: list[DLQRecord]) -> None:
        if not records:
            return
        await self._sink.write_batch([self._build_upsert_envelope(record) for record in records])
        self._write_batch_count += 1
        self._upsert_count += len(records)
        self._last_write_at = _now_utc()

    async def replay(self, record: DLQRecord) -> DLQRecord:
        updated = await super().replay(record)
        await self._sink.write(self._build_upsert_envelope(updated))
        self._replay_count += 1
        self._upsert_count += 1
        self._last_replay_at = _now_utc()
        return updated

    async def acknowledge(self, record: DLQRecord) -> None:
        storage_key = _record_storage_key(record, self._key_fn)
        object.__setattr__(record, "_storage_id", storage_key)
        await self._sink.write(_encode_dlq_envelope(storage_key, operation="delete"))
        self._acknowledge_count += 1
        self._delete_count += 1
        self._last_acknowledge_at = _now_utc()

    def _build_upsert_envelope(self, record: DLQRecord) -> dict[str, Any]:
        storage_key = _record_storage_key(record, self._key_fn)
        object.__setattr__(record, "_storage_id", storage_key)
        return _encode_dlq_envelope(
            storage_key,
            operation="put",
            record=record,
            payload_policy=self._payload_policy,
        )

    def metrics_snapshot(self) -> KafkaDLQSinkMetricsSnapshot:
        return KafkaDLQSinkMetricsSnapshot(
            topic=self._topic,
            bootstrap_servers=self._bootstrap_servers,
            write_count=self._write_count,
            write_batch_count=self._write_batch_count,
            replay_count=self._replay_count,
            acknowledge_count=self._acknowledge_count,
            upsert_count=self._upsert_count,
            delete_count=self._delete_count,
            last_write_at=self._last_write_at,
            last_replay_at=self._last_replay_at,
            last_acknowledge_at=self._last_acknowledge_at,
        )

    def render_prometheus_metrics(self, namespace: str = "agora_kafka_dlq") -> str:
        return KafkaDLQPrometheusExporter(namespace=namespace).render_sink(self.metrics_snapshot())


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
        if topic is not None:
            if topics is not None:
                raise ValueError("KafkaDLQSource accepts either `topic` or `topics`, not both.")
            topics = [topic]
        self._topics = list(topics or [])
        self._topic_pattern = topic_pattern
        self._assignments = sorted(
            {(str(topic), int(partition)) for topic, partition in (assignments or ())}
        )
        if not self._topics and self._topic_pattern is None and not self._assignments:
            raise ValueError("KafkaDLQSource requires `topics`, `topic_pattern`, or `assignments`.")
        if self._topics and self._topic_pattern is not None:
            raise ValueError("KafkaDLQSource accepts either `topics` or `topic_pattern`, not both.")
        if self._assignments and (self._topics or self._topic_pattern is not None):
            raise ValueError(
                "KafkaDLQSource accepts `assignments` only when `topics` and `topic_pattern` are unset."
            )
        if limit is not None and limit < 1:
            raise ValueError("KafkaDLQSource limit must be >= 1 when provided.")

        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id or f"{pipeline_id or 'agora'}-kafka-dlq-replay"
        self._pipeline_id = pipeline_id
        self._stage = stage
        self._limit = limit
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._poll_timeout_ms = max(poll_timeout_ms, 1)
        self._scan_idle_polls = max(scan_idle_polls, 1)
        self._stop_at_highwater = stop_at_highwater
        self._security = _resolve_dlq_security(
            bootstrap_servers=bootstrap_servers,
            topic=topic
            if topic is not None
            else (self._topics[0] if len(self._topics) == 1 else None),
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
        self._security_protocol = (
            self._security.security_protocol if self._security is not None else security_protocol
        )
        self._extra_config = extra_config or {}
        self._start_offsets = {
            (str(topic), int(partition)): int(offset)
            for (topic, partition), offset in (start_offsets or {}).items()
        }
        if compaction_spill_threshold is not None and compaction_spill_threshold < 0:
            raise ValueError("compaction_spill_threshold must be non-negative or None.")
        self._compaction_spill_threshold = compaction_spill_threshold
        self._payload_policy = payload_policy
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

    async def open(self) -> None:
        try:
            import aiokafka
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            raise ImportError(
                "KafkaDLQSource requires aiokafka. Install via: pip install 'agora-etl-plugins[kafka]'"
            ) from None

        self._topic_partition_cls = getattr(aiokafka, "TopicPartition", None)
        consumer_args: list[str] = []
        self._consumer = AIOKafkaConsumer(
            *consumer_args,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=self._enable_auto_commit,
            **self._security_kwargs(),
            **self._extra_config,
        )
        consumer = self._consumer
        if self._assignments:
            consumer.assign(
                [
                    self._build_topic_partition(topic, partition)
                    for topic, partition in self._assignments
                ]
            )
        elif self._topic_pattern is not None:
            consumer.subscribe(pattern=self._topic_pattern)
        else:
            consumer.subscribe(topics=self._topics)
        try:
            await consumer.start()
        except Exception:
            with contextlib.suppress(Exception):
                await consumer.stop()
            self._consumer = None
            raise

    async def close(self) -> None:
        if self._consumer is not None:
            consumer = self._consumer
            self._consumer = None
            await consumer.stop()

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        consumer = self._require_consumer()
        assignment = await self._wait_for_assignment(consumer)
        if not assignment:
            self._scan_count += 1
            self._last_scan_completed_at = _now_utc()
            return

        await self._apply_start_offsets(consumer)
        highwater_offsets = await self._partition_highwater_offsets(consumer, assignment)
        compaction = _KafkaDLQCompactionState(
            spill_threshold=self._compaction_spill_threshold,
            payload_policy=self._payload_policy,
        )
        sequence = 0
        idle_polls = 0

        try:
            while idle_polls < self._scan_idle_polls:
                if self._assignment_prefetch_batches:
                    batches = self._assignment_prefetch_batches.pop(0)
                else:
                    batches = await consumer.getmany(timeout_ms=self._poll_timeout_ms)
                non_empty = False
                for messages in batches.values():
                    if not messages:
                        continue
                    non_empty = True
                    for message in messages:
                        operation, storage_key, record = _decode_dlq_envelope(
                            message.value,
                            payload_policy=self._payload_policy,
                        )
                        self._scanned_message_count += 1
                        self._last_record_seen_at = _now_utc()
                        if operation == "delete":
                            self._delete_event_count += 1
                        else:
                            self._upsert_event_count += 1
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
                if highwater_offsets and await self._positions_reached_highwater(
                    consumer,
                    highwater_offsets,
                ):
                    self._highwater_stop_count += 1
                    break

            yielded = 0
            live_records = compaction.live_records()
            self._scan_count += 1
            self._live_record_count = len(live_records)
            self._matched_record_count = 0
            for _, record in live_records:
                if self._pipeline_id is not None and record.pipeline_id != self._pipeline_id:
                    continue
                if self._stage is not None and record.stage != self._stage:
                    continue
                self._matched_record_count += 1
                yield record
                yielded += 1
                if self._limit is not None and yielded >= self._limit:
                    self._last_scan_completed_at = _now_utc()
                    return
            self._last_scan_completed_at = _now_utc()
        finally:
            compaction.close()

    async def stream(self) -> AsyncGenerator[DLQRecord, None]:
        self._replayable_record_count = 0
        async for record in self._iter_records():
            if record.max_attempts is not None and record.attempt >= record.max_attempts:
                self._retry_filtered_count += 1
                continue
            self._replayable_record_count += 1
            yield record

    def _require_consumer(self) -> Any:
        if self._consumer is None:
            raise RuntimeError("KafkaDLQSource.open() was not called")
        return self._consumer

    def _build_topic_partition(self, topic: str, partition: int) -> object:
        if self._topic_partition_cls is not None:
            return self._topic_partition_cls(topic, partition)
        return (topic, partition)

    def _security_kwargs(self) -> dict[str, Any]:
        if self._security is None:
            return {"security_protocol": self._security_protocol}
        return self._security.to_aiokafka_client_kwargs()

    async def _wait_for_assignment(self, consumer: Any) -> set[object]:
        self._assignment_prefetch_batches.clear()
        for _ in range(20):
            assignment = set(cast("set[object]", consumer.assignment()))
            if assignment:
                return assignment
            batches = await consumer.getmany(timeout_ms=self._poll_timeout_ms)
            if batches:
                self._assignment_prefetch_batches.append(batches)
        return set()

    async def _apply_start_offsets(self, consumer: Any) -> None:
        if not self._start_offsets:
            return
        seek = getattr(consumer, "seek", None)
        if not callable(seek):
            return
        for (topic, partition), offset in sorted(self._start_offsets.items()):
            result = seek(self._build_topic_partition(topic, partition), offset)
            self._start_offset_seek_count += 1
            if isawaitable(result):
                await result

    async def _partition_highwater_offsets(
        self,
        consumer: Any,
        assignment: set[object],
    ) -> dict[object, int]:
        if not self._stop_at_highwater:
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

    async def _positions_reached_highwater(
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

    def _subscription_mode(self) -> str:
        if self._assignments:
            return "manual_assign"
        if self._topic_pattern is not None:
            return "pattern"
        return "topics"

    def metrics_snapshot(self) -> KafkaDLQSourceMetricsSnapshot:
        return KafkaDLQSourceMetricsSnapshot(
            consumer_group=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            subscription_mode=self._subscription_mode(),
            scan_count=self._scan_count,
            scanned_message_count=self._scanned_message_count,
            upsert_event_count=self._upsert_event_count,
            delete_event_count=self._delete_event_count,
            start_offset_seek_count=self._start_offset_seek_count,
            highwater_stop_count=self._highwater_stop_count,
            live_record_count=self._live_record_count,
            matched_record_count=self._matched_record_count,
            replayable_record_count=self._replayable_record_count,
            retry_filtered_count=self._retry_filtered_count,
            last_scan_completed_at=self._last_scan_completed_at,
            last_record_seen_at=self._last_record_seen_at,
        )

    def backlog_snapshot(self) -> KafkaDLQSourceMetricsSnapshot:
        return self.metrics_snapshot()

    def render_prometheus_metrics(self, namespace: str = "agora_kafka_dlq") -> str:
        return KafkaDLQPrometheusExporter(namespace=namespace).render_source(
            self.metrics_snapshot()
        )


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
