"""Payload and hash conversion helpers for Redis-backed DLQ records."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from agora.core.dlq import DLQRecord

if TYPE_CHECKING:
    from agora_plugins.dlq_policy import DLQPayloadPolicy


def coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value)


def serialize_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def record_to_payload(record: DLQRecord) -> dict[str, Any]:
    return {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": record.record,
        "original_record": record.original_record,
        "processed_record": record.processed_record,
        "source": record.source or "",
        "checkpoint": record.checkpoint,
        "details": record.details,
        "middleware": record.middleware or "",
        "sink": record.sink or "",
        "created_at": record.created_at.isoformat(),
        "attempt": record.attempt,
        "max_attempts": record.max_attempts,
    }


def payload_to_hash(payload: dict[str, Any]) -> dict[str, str]:
    return {
        "pipeline_id": str(payload["pipeline_id"]),
        "run_id": str(payload["run_id"]),
        "stage": str(payload["stage"]),
        "error_type": str(payload["error_type"]),
        "error_message": str(payload["error_message"]),
        "record": serialize_value(payload.get("record")),
        "original_record": serialize_value(payload.get("original_record")),
        "processed_record": serialize_value(payload.get("processed_record")),
        "source": str(payload.get("source") or ""),
        "checkpoint": serialize_value(payload.get("checkpoint")),
        "details": serialize_value(payload.get("details")),
        "middleware": str(payload.get("middleware") or ""),
        "sink": str(payload.get("sink") or ""),
        "created_at": coerce_datetime(cast("datetime | str", payload["created_at"])).isoformat(),
        "attempt": str(int(payload.get("attempt", 0))),
        "max_attempts": (
            "" if payload.get("max_attempts") is None else str(int(payload["max_attempts"]))
        ),
    }


def record_to_hash(
    record: DLQRecord,
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> dict[str, str]:
    payload = record_to_payload(record)
    if payload_policy is not None:
        payload = payload_policy.apply(payload)
    record_hash = payload_to_hash(payload)
    if payload_policy is not None and payload_policy.mode == "encrypted":
        record_hash.update(
            {
                "record": serialize_value(payload_policy.encrypt_payload(payload)),
                "original_record": "",
                "processed_record": "",
                "checkpoint": "",
                "details": "",
            }
        )
    return record_hash


def decode_json(value: str | None) -> Any:
    if value is None or value == "":
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def hash_to_payload(payload: dict[str, str]) -> dict[str, Any]:
    return {
        "pipeline_id": payload["pipeline_id"],
        "run_id": payload["run_id"],
        "stage": payload["stage"],
        "error_type": payload["error_type"],
        "error_message": payload["error_message"],
        "record": decode_json(payload.get("record")),
        "original_record": decode_json(payload.get("original_record")),
        "processed_record": decode_json(payload.get("processed_record")),
        "source": payload.get("source") or None,
        "checkpoint": decode_json(payload.get("checkpoint")),
        "details": decode_json(payload.get("details")),
        "middleware": payload.get("middleware") or None,
        "sink": payload.get("sink") or None,
        "created_at": coerce_datetime(payload["created_at"]),
        "attempt": int(payload.get("attempt", "0") or 0),
        "max_attempts": (
            int(payload["max_attempts"]) if payload.get("max_attempts") not in (None, "") else None
        ),
    }


def payload_to_record(payload: dict[str, Any], *, storage_key: str | None = None) -> DLQRecord:
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
        created_at=coerce_datetime(cast("datetime | str", payload["created_at"])),
        attempt=int(payload.get("attempt", 0)),
        max_attempts=(
            int(payload["max_attempts"]) if payload.get("max_attempts") is not None else None
        ),
        _storage_id=cast("Any", storage_key),
    )


def hash_to_record(
    payload: dict[str, str],
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> DLQRecord:
    record_payload = decode_json(payload.get("record"))
    storage_key = payload.get("storage_key")
    if isinstance(record_payload, dict) and record_payload.get("payload_encoding") == "encrypted":
        if payload_policy is None:
            raise ValueError("Encrypted Redis DLQ payload requires a DLQPayloadPolicy.")
        return payload_to_record(
            payload_policy.decrypt_payload(record_payload),
            storage_key=storage_key,
        )
    return payload_to_record(hash_to_payload(payload), storage_key=storage_key)
