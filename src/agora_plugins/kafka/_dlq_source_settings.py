"""Settings normalization for Kafka DLQ source construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from agora_plugins.dlq_policy import DLQPayloadPolicy
    from agora_plugins.kafka.config import KafkaSecurityConfig


@dataclass(frozen=True, slots=True)
class KafkaDLQSourceSettings:
    """Normalized constructor inputs for Kafka DLQ sources."""

    topics: list[str]
    topic_pattern: str | None
    assignments: list[tuple[str, int]]
    bootstrap_servers: str
    group_id: str
    pipeline_id: str | None
    stage: str | None
    limit: int | None
    auto_offset_reset: str
    enable_auto_commit: bool
    poll_timeout_ms: int
    scan_idle_polls: int
    stop_at_highwater: bool
    security: KafkaSecurityConfig | None
    security_protocol: str
    extra_config: dict[str, Any]
    start_offsets: dict[tuple[str, int], int]
    compaction_spill_threshold: int | None
    payload_policy: DLQPayloadPolicy | None


def build_kafka_dlq_source_settings(
    *,
    topic: str | None,
    topics: list[str] | None,
    topic_pattern: str | None,
    assignments: Iterable[tuple[str, int]] | None,
    bootstrap_servers: str,
    group_id: str | None,
    pipeline_id: str | None,
    stage: str | None,
    limit: int | None,
    auto_offset_reset: str,
    enable_auto_commit: bool,
    poll_timeout_ms: int,
    scan_idle_polls: int,
    stop_at_highwater: bool,
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
    extra_config: dict[str, Any] | None,
    start_offsets: dict[tuple[str, int], int] | None,
    compaction_spill_threshold: int | None,
    payload_policy: DLQPayloadPolicy | None,
    resolve_security: Callable[..., KafkaSecurityConfig | None],
) -> KafkaDLQSourceSettings:
    normalized_topics = _normalize_topics(topic=topic, topics=topics)
    normalized_assignments = _normalize_assignments(assignments)
    _validate_subscription_inputs(
        topics=normalized_topics,
        topic_pattern=topic_pattern,
        assignments=normalized_assignments,
    )
    if limit is not None and limit < 1:
        raise ValueError("KafkaDLQSource limit must be >= 1 when provided.")
    if compaction_spill_threshold is not None and compaction_spill_threshold < 0:
        raise ValueError("compaction_spill_threshold must be non-negative or None.")

    resolved_security = resolve_security(
        bootstrap_servers=bootstrap_servers,
        topic=topic if topic is not None else _single_topic_or_none(normalized_topics),
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
    effective_security_protocol = (
        resolved_security.security_protocol if resolved_security is not None else security_protocol
    )

    return KafkaDLQSourceSettings(
        topics=normalized_topics,
        topic_pattern=topic_pattern,
        assignments=normalized_assignments,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id or f"{pipeline_id or 'agora'}-kafka-dlq-replay",
        pipeline_id=pipeline_id,
        stage=stage,
        limit=limit,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        poll_timeout_ms=max(poll_timeout_ms, 1),
        scan_idle_polls=max(scan_idle_polls, 1),
        stop_at_highwater=stop_at_highwater,
        security=resolved_security,
        security_protocol=effective_security_protocol,
        extra_config=dict(extra_config or {}),
        start_offsets={
            (str(offset_topic), int(partition)): int(offset)
            for (offset_topic, partition), offset in (start_offsets or {}).items()
        },
        compaction_spill_threshold=compaction_spill_threshold,
        payload_policy=payload_policy,
    )


def _normalize_topics(*, topic: str | None, topics: list[str] | None) -> list[str]:
    if topic is not None:
        if topics is not None:
            raise ValueError("KafkaDLQSource accepts either `topic` or `topics`, not both.")
        return [topic]
    return list(topics or [])


def _normalize_assignments(
    assignments: Iterable[tuple[str, int]] | None,
) -> list[tuple[str, int]]:
    return sorted({(str(topic), int(partition)) for topic, partition in (assignments or ())})


def _validate_subscription_inputs(
    *,
    topics: list[str],
    topic_pattern: str | None,
    assignments: list[tuple[str, int]],
) -> None:
    if not topics and topic_pattern is None and not assignments:
        raise ValueError("KafkaDLQSource requires `topics`, `topic_pattern`, or `assignments`.")
    if topics and topic_pattern is not None:
        raise ValueError("KafkaDLQSource accepts either `topics` or `topic_pattern`, not both.")
    if assignments and (topics or topic_pattern is not None):
        raise ValueError(
            "KafkaDLQSource accepts `assignments` only when `topics` and `topic_pattern` are unset."
        )


def _single_topic_or_none(topics: list[str]) -> str | None:
    if len(topics) == 1:
        return topics[0]
    return None
