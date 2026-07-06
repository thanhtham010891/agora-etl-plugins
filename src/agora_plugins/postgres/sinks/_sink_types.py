"""Public PostgreSQL sink policy and error types."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from agora.core.failures import PoisonRecordClassification, PoisonRecordInfo


def now_utc() -> datetime:
    return datetime.now(UTC)


class PostgresWriteSafetyPolicy(StrEnum):
    """Controls how PostgresSink reacts to live target-schema drift."""

    STRICT = "strict"
    ALIGN_TO_TARGET = "align_to_target"


def resolve_write_safety_policy(
    value: PostgresWriteSafetyPolicy | str,
) -> PostgresWriteSafetyPolicy:
    if isinstance(value, PostgresWriteSafetyPolicy):
        return value
    try:
        return PostgresWriteSafetyPolicy(value)
    except ValueError as exc:
        allowed = ", ".join(policy.value for policy in PostgresWriteSafetyPolicy)
        raise ValueError(f"write_safety_policy must be one of: {allowed}. Got {value!r}.") from exc


PostgresPoisonRecordClassification = PoisonRecordClassification


@dataclass(frozen=True, slots=True, kw_only=True)
class PostgresPoisonRecordInfo(PoisonRecordInfo):
    """Structured poison metadata for DLQ and incident tooling."""

    classification: PostgresPoisonRecordClassification
    reason: str
    details: dict[str, Any]


class PostgresSinkWriteError(RuntimeError):
    """Structured sink write error carrying Postgres poison metadata."""

    def __init__(
        self,
        message: str,
        *,
        poison_info: PostgresPoisonRecordInfo,
    ) -> None:
        super().__init__(message)
        self.poison_info = poison_info
        self.dlq_details = {"postgres": poison_info.to_dict()}
