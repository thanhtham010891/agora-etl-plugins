"""Poison-record routing for :mod:`agora_plugins.kafka.sources.kafka`."""

from __future__ import annotations

import base64
import time
from datetime import UTC, datetime
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Protocol

import logstruct
from agora.core.dlq import DLQRecord, DLQSink
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.kafka.sources._models import (
    BatchMessageContext,
    KafkaPoisonRecordClassification,
    KafkaPoisonRecordInfo,
    KafkaPoisonRecordPolicy,
)

logger = logstruct.getLogger("agora_plugins.kafka.sources.kafka")

if TYPE_CHECKING:
    from collections.abc import Iterable


class KafkaPoisonOwner(Protocol):
    """State required by poison routing without coupling to ``KafkaSource``."""

    source_name: str
    _poison_record_policy: KafkaPoisonRecordPolicy
    _poison_record_sink: DLQSink | None
    _poison_record_pipeline_id: str
    _poison_record_max_attempts: int | None
    _poison_record_dlq_write_count: int
    _poison_record_dlq_write_failure_count: int
    _poison_record_log_only_count: int
    _poison_record_fail_closed_count: int
    _poison_record_classification_counts: dict[KafkaPoisonRecordClassification, int]

    def _invalidate_health_snapshot_cache(self) -> None: ...


def should_continue_after_poison_record(owner: KafkaPoisonOwner) -> bool:
    return owner._poison_record_policy in {
        KafkaPoisonRecordPolicy.LOG_AND_CONTINUE,
        KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
    }


def resolve_poison_record_policy(
    policy: KafkaPoisonRecordPolicy | str | None,
    *,
    on_deserialize_error: SourceRecordFailurePolicy,
) -> KafkaPoisonRecordPolicy:
    if policy is None:
        return (
            KafkaPoisonRecordPolicy.LOG_AND_CONTINUE
            if on_deserialize_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE
            else KafkaPoisonRecordPolicy.FAIL_CLOSED
        )
    return KafkaPoisonRecordPolicy(policy)


async def capture_poison_batch(
    owner: KafkaPoisonOwner,
    exc: Exception,
    messages: list[Any],
    batch_contexts: list[BatchMessageContext],
    *,
    stage: str,
    poison_info: KafkaPoisonRecordInfo,
) -> None:
    if owner._poison_record_policy not in {
        KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        KafkaPoisonRecordPolicy.DLQ_AND_FAIL_CLOSED,
    }:
        return
    sink = owner._poison_record_sink
    if sink is None:
        return
    records = [
        build_poison_dlq_record(
            owner,
            exc,
            message,
            batch_context.metadata,
            stage=stage,
            poison_info=poison_info,
        )
        for message, batch_context in zip(messages, batch_contexts, strict=False)
    ]
    write_batch = getattr(sink, "write_batch", None)
    try:
        if callable(write_batch):
            result = write_batch(records)
            if isawaitable(result):
                await result
            owner._poison_record_dlq_write_count += len(records)
            owner._invalidate_health_snapshot_cache()
            return
        for record in records:
            await sink.write(record)
    except Exception as dlq_exc:
        handle_poison_dlq_write_error(
            owner,
            dlq_exc,
            record_count=len(records),
            stage=stage,
        )
        return
    owner._poison_record_dlq_write_count += len(records)
    owner._invalidate_health_snapshot_cache()


async def capture_poison_record(
    owner: KafkaPoisonOwner,
    exc: Exception,
    message: Any,
    metadata: dict[str, Any],
    *,
    stage: str,
    poison_info: KafkaPoisonRecordInfo,
) -> None:
    if owner._poison_record_policy not in {
        KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        KafkaPoisonRecordPolicy.DLQ_AND_FAIL_CLOSED,
    }:
        return
    sink = owner._poison_record_sink
    if sink is None:
        return
    try:
        await sink.write(
            build_poison_dlq_record(
                owner,
                exc,
                message,
                metadata,
                stage=stage,
                poison_info=poison_info,
            )
        )
    except Exception as dlq_exc:
        handle_poison_dlq_write_error(owner, dlq_exc, record_count=1, stage=stage)
        return
    owner._poison_record_dlq_write_count += 1
    owner._invalidate_health_snapshot_cache()


def handle_poison_dlq_write_error(
    owner: KafkaPoisonOwner,
    exc: Exception,
    *,
    record_count: int,
    stage: str,
) -> None:
    owner._poison_record_dlq_write_failure_count += record_count
    owner._invalidate_health_snapshot_cache()
    logger.exception(
        "kafka_poison_dlq_write_error",
        stage=stage,
        record_count=record_count,
        policy=owner._poison_record_policy.value,
        error=str(exc),
    )
    if owner._poison_record_policy in {
        KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
        KafkaPoisonRecordPolicy.DLQ_AND_FAIL_CLOSED,
    }:
        raise exc


def build_poison_dlq_record(
    owner: KafkaPoisonOwner,
    exc: Exception,
    message: Any,
    metadata: dict[str, Any],
    *,
    stage: str,
    poison_info: KafkaPoisonRecordInfo,
) -> DLQRecord:
    topic = str(metadata.get("topic", getattr(message, "topic", "unknown")))
    partition = int(metadata.get("partition", getattr(message, "partition", 0)))
    offset = int(metadata.get("offset", getattr(message, "offset", -1)))
    poison_payload = {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "key": encode_kafka_value(getattr(message, "key", None)),
        "value": encode_kafka_value(getattr(message, "value", None)),
        "headers": encode_kafka_headers(getattr(message, "headers", ()) or ()),
        "timestamp": getattr(message, "timestamp", None),
        "timestamp_type": getattr(message, "timestamp_type", None),
        "metadata": dict(metadata),
        "poison": poison_info.to_dict(),
    }
    return DLQRecord(
        pipeline_id=owner._poison_record_pipeline_id,
        run_id=f"{topic}:{partition}:{offset}:{int(time.time() * 1000)}",
        stage=stage,
        error_type=type(exc).__name__,
        error_message=str(exc),
        record=poison_payload,
        original_record=poison_payload,
        processed_record=None,
        source=owner.source_name,
        checkpoint={
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "offsets": [{"topic": topic, "partition": partition, "offset": offset}],
        },
        created_at=datetime.now(UTC),
        max_attempts=owner._poison_record_max_attempts,
    )


def observe_poison_records(
    owner: KafkaPoisonOwner,
    exc: Exception,
    *,
    count: int,
) -> KafkaPoisonRecordInfo:
    classification = classify_poison_record(exc)
    owner._poison_record_classification_counts[classification] += count
    if owner._poison_record_policy == KafkaPoisonRecordPolicy.LOG_AND_CONTINUE:
        owner._poison_record_log_only_count += count
    elif owner._poison_record_policy in {
        KafkaPoisonRecordPolicy.FAIL_CLOSED,
        KafkaPoisonRecordPolicy.DLQ_AND_FAIL_CLOSED,
    }:
        owner._poison_record_fail_closed_count += count
    return KafkaPoisonRecordInfo(
        classification=classification,
        policy=owner._poison_record_policy,
    )


def classify_poison_record(exc: Exception) -> KafkaPoisonRecordClassification:
    error_type = type(exc).__name__.lower()
    message = str(exc).lower()
    if "binding mismatch" in message and "protobuf" in message:
        return KafkaPoisonRecordClassification.SCHEMA_REGISTRY_BINDING_MISMATCH
    if "schemaresolutionerror" in error_type or (
        "schema" in message
        and any(marker in message for marker in ("mismatch", "reader", "writer", "union"))
    ):
        return KafkaPoisonRecordClassification.SCHEMA_EVOLUTION
    if (
        "validationerror" in error_type
        or "jsonschema" in error_type
        or any(
            marker in message
            for marker in (
                "is not of type",
                "is a required property",
                "failed validating",
                "oneof",
                "anyof",
                "allof",
            )
        )
    ):
        return KafkaPoisonRecordClassification.SCHEMA_VALIDATION
    if any(
        marker in message
        for marker in (
            "payload",
            "deserialize",
            "magic byte",
            "varint",
            "utf-8",
            "bad batch",
            "bad payload",
        )
    ):
        return KafkaPoisonRecordClassification.DESERIALIZATION
    return KafkaPoisonRecordClassification.UNKNOWN


def encode_kafka_value(value: bytes | None) -> dict[str, str] | None:
    if value is None:
        return None
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError:
        return {"encoding": "base64", "data": base64.b64encode(value).decode("ascii")}
    if "\x00" in decoded:
        return {"encoding": "base64", "data": base64.b64encode(value).decode("ascii")}
    return {"encoding": "utf-8", "data": decoded}


def encode_kafka_headers(headers: Iterable[tuple[str, bytes]]) -> list[dict[str, Any]]:
    return [
        {
            "key": str(key),
            "value": encode_kafka_value(value),
        }
        for key, value in headers
    ]
