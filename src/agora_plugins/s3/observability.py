"""Observability snapshots for S3 plugin surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from agora.core.recovery import SourceRecoveryContractSnapshot, SourceRecoveryMode

S3SourceRecoveryMode = SourceRecoveryMode


def _isoformat_or_none(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


@dataclass(frozen=True, slots=True)
class S3SourceRecoveryContractSnapshot(SourceRecoveryContractSnapshot):
    """Machine-readable recovery contract for S3Source."""


@dataclass(frozen=True, slots=True)
class S3SourceMetricsSnapshot:
    """Operational metrics and recovery contract for S3Source."""

    bucket: str
    prefix: str
    format: str
    supports_checkpoint: bool
    listed_object_count: int
    completed_object_count: int
    emitted_record_count: int
    record_error_count: int
    record_drop_count: int
    last_listed_key: str | None
    last_completed_key: str | None
    last_error: str | None
    recovery_contract: S3SourceRecoveryContractSnapshot

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "prefix": self.prefix,
            "format": self.format,
            "supports_checkpoint": self.supports_checkpoint,
            "listed_object_count": self.listed_object_count,
            "completed_object_count": self.completed_object_count,
            "emitted_record_count": self.emitted_record_count,
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
            "last_listed_key": self.last_listed_key,
            "last_completed_key": self.last_completed_key,
            "last_error": self.last_error,
            "recovery_contract": self.recovery_contract.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class S3SinkMetricsSnapshot:
    """Operational metrics for S3 sink activity."""

    bucket: str
    prefix: str
    run_id: str
    format: str
    flush_every: int
    max_records_per_file: int
    connection_ready: bool
    uploaded_object_count: int
    uploaded_record_count: int
    last_uploaded_key: str | None
    last_upload_at: datetime | None
    last_error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "prefix": self.prefix,
            "run_id": self.run_id,
            "format": self.format,
            "flush_every": self.flush_every,
            "max_records_per_file": self.max_records_per_file,
            "connection_ready": self.connection_ready,
            "uploaded_object_count": self.uploaded_object_count,
            "uploaded_record_count": self.uploaded_record_count,
            "last_uploaded_key": self.last_uploaded_key,
            "last_upload_at": _isoformat_or_none(self.last_upload_at),
            "last_error": self.last_error,
        }
