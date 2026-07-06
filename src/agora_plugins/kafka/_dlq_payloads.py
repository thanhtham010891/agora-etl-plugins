"""Payload, envelope, and storage-key helpers for Kafka DLQ records."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from agora.core.dlq import DLQRecord

if TYPE_CHECKING:
    from agora_plugins.dlq_policy import DLQPayloadPolicy


def serialize_json(value: Any) -> Any:
    if value is None:
        return None
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value)


def record_to_payload(
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
        "record": serialize_json(record.record),
        "original_record": serialize_json(record.original_record),
        "processed_record": serialize_json(record.processed_record),
        "source": record.source,
        "checkpoint": serialize_json(record.checkpoint),
        "details": serialize_json(record.details),
        "middleware": record.middleware,
        "sink": record.sink,
        "created_at": record.created_at.isoformat(),
        "attempt": record.attempt,
        "max_attempts": record.max_attempts,
    }
    if payload_policy is None:
        return payload
    return payload_policy.apply(payload)


def payload_to_record(payload: dict[str, Any]) -> DLQRecord:
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
        created_at=coerce_datetime(str(payload["created_at"])),
        attempt=int(payload.get("attempt", 0)),
        max_attempts=(
            int(payload["max_attempts"]) if payload.get("max_attempts") is not None else None
        ),
    )


def storage_key_from_value(value: bytes | str) -> str:
    if isinstance(value, str):
        return value
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.hex()


def default_storage_key(record: DLQRecord) -> str:
    return (
        f"{record.pipeline_id}:{record.run_id}:{record.stage}:"
        f"{record.created_at.isoformat()}:{uuid.uuid4().hex}"
    )


def legacy_storage_key(record: DLQRecord) -> str:
    payload = json.dumps(
        record_to_payload(record),
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


def record_storage_key(
    record: DLQRecord,
    key_fn: Any = None,
) -> str:
    storage_id = record._storage_id
    if isinstance(storage_id, str) and storage_id:
        return storage_id
    if key_fn is None:
        return default_storage_key(record)
    return storage_key_from_value(key_fn(record))


def record_headers(
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


def encode_dlq_envelope(
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
        payload = record_to_payload(record, payload_policy=payload_policy)
        if payload_policy is not None and payload_policy.mode == "encrypted":
            envelope.update(payload_policy.encrypt_payload(payload))
        else:
            envelope["payload"] = payload
    return envelope


def decode_stored_payload(
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


def decode_dlq_envelope(
    payload: bytes,
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> tuple[str, str, DLQRecord | None]:
    decoded = cast("dict[str, Any]", json.loads(payload.decode("utf-8")))
    if "op" in decoded:
        operation = str(decoded["op"])
        storage_key = str(decoded["storage_key"])
        record_payload = decode_stored_payload(decoded, payload_policy=payload_policy)
        record = None if record_payload is None else payload_to_record(record_payload)
        if record is not None:
            object.__setattr__(record, "_storage_id", storage_key)
        return operation, storage_key, record

    record = payload_to_record(decoded)
    storage_key = legacy_storage_key(record)
    object.__setattr__(record, "_storage_id", storage_key)
    return "put", storage_key, record
