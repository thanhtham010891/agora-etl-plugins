"""Payload encoding helpers for PostgreSQL DLQ records."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from agora.core.dlq import DLQRecord

if TYPE_CHECKING:
    from agora_plugins.dlq_policy import DLQPayloadPolicy

DLQ_COLUMNS = (
    "pipeline_id",
    "run_id",
    "stage",
    "error_type",
    "error_message",
    "record",
    "original_record",
    "processed_record",
    "source",
    "checkpoint",
    "details",
    "middleware",
    "sink",
    "created_at",
    "attempt",
    "max_attempts",
)
DLQ_INSERT_COLUMNS = ("dedupe_key", *DLQ_COLUMNS)


def serialize_json(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value)


def record_to_payload(record: DLQRecord) -> dict[str, object]:
    return {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": record.record,
        "original_record": record.original_record,
        "processed_record": record.processed_record,
        "source": record.source,
        "checkpoint": record.checkpoint,
        "details": record.details,
        "middleware": record.middleware,
        "sink": record.sink,
        "created_at": record.created_at,
        "attempt": record.attempt,
        "max_attempts": record.max_attempts,
    }


def payload_to_row(payload: dict[str, object]) -> dict[str, object]:
    return {
        "pipeline_id": payload["pipeline_id"],
        "run_id": payload["run_id"],
        "stage": payload["stage"],
        "error_type": payload["error_type"],
        "error_message": payload["error_message"],
        "record": serialize_json(payload.get("record")),
        "original_record": serialize_json(payload.get("original_record")),
        "processed_record": serialize_json(payload.get("processed_record")),
        "source": payload.get("source"),
        "checkpoint": serialize_json(payload.get("checkpoint")),
        "details": serialize_json(payload.get("details")),
        "middleware": payload.get("middleware"),
        "sink": payload.get("sink"),
        "created_at": coerce_datetime(cast("datetime | str", payload["created_at"])),
        "attempt": int(payload.get("attempt", 0)),
        "max_attempts": (
            int(payload["max_attempts"]) if payload.get("max_attempts") is not None else None
        ),
    }


def record_to_row(
    record: DLQRecord,
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> dict[str, object]:
    payload = record_to_payload(record)
    if payload_policy is not None:
        payload = payload_policy.apply(payload)
    row = payload_to_row(payload)
    dedupe_key = dedupe_key_for_row(row)
    if payload_policy is not None and payload_policy.mode == "encrypted":
        row.update(
            {
                "record": serialize_json(payload_policy.encrypt_payload(payload)),
                "original_record": None,
                "processed_record": None,
                "checkpoint": None,
                "details": None,
            }
        )
    row["dedupe_key"] = dedupe_key
    return row


def dedupe_key_for_row(row: dict[str, object]) -> str:
    payload = {column: row.get(column) for column in DLQ_COLUMNS}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=json_default)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def decode_json(value: object) -> object:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value


def payload_to_record(payload: dict[str, object]) -> DLQRecord:
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
    )


def row_to_payload(row: dict[str, object]) -> dict[str, object]:
    return {
        "pipeline_id": row["pipeline_id"],
        "run_id": row["run_id"],
        "stage": row["stage"],
        "error_type": row["error_type"],
        "error_message": row["error_message"],
        "record": decode_json(row["record"]),
        "original_record": decode_json(row.get("original_record")),
        "processed_record": decode_json(row.get("processed_record")),
        "source": row.get("source"),
        "checkpoint": decode_json(row.get("checkpoint")),
        "details": decode_json(row.get("details")),
        "middleware": row.get("middleware"),
        "sink": row.get("sink"),
        "created_at": coerce_datetime(cast("datetime | str", row["created_at"])),
        "attempt": int(row.get("attempt", 0)),
        "max_attempts": (int(row["max_attempts"]) if row.get("max_attempts") is not None else None),
    }


def row_to_record(
    row: dict[str, object],
    *,
    payload_policy: DLQPayloadPolicy | None = None,
) -> DLQRecord:
    record_payload = decode_json(row["record"])
    if isinstance(record_payload, dict) and record_payload.get("payload_encoding") == "encrypted":
        if payload_policy is None:
            raise ValueError("Encrypted Postgres DLQ payload requires a DLQPayloadPolicy.")
        return payload_to_record(payload_policy.decrypt_payload(record_payload))
    return payload_to_record(row_to_payload(row))
