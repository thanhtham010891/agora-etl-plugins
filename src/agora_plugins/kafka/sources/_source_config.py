from __future__ import annotations

from typing import Any

from agora_plugins.kafka.config import KafkaSecurityConfig

CONSUMER_POSITIVE_INT_CONFIGS = frozenset(
    {
        "auto_commit_interval_ms",
        "connections_max_idle_ms",
        "fetch_max_bytes",
        "heartbeat_interval_ms",
        "max_poll_interval_ms",
        "metadata_max_age_ms",
        "request_timeout_ms",
        "session_timeout_ms",
    }
)
CONSUMER_NON_NEGATIVE_INT_CONFIGS = frozenset({"retry_backoff_ms"})


def validate_int_config(
    name: str,
    value: object,
    *,
    minimum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"KafkaSource {name} must be an integer >= {minimum}.")
    if value < minimum:
        raise ValueError(f"KafkaSource {name} must be >= {minimum}.")
    return value


def validate_extra_consumer_config(extra_config: dict[str, Any]) -> None:
    for name in sorted(CONSUMER_POSITIVE_INT_CONFIGS):
        if name in extra_config:
            validate_int_config(name, extra_config[name], minimum=1)
    for name in sorted(CONSUMER_NON_NEGATIVE_INT_CONFIGS):
        if name in extra_config:
            validate_int_config(name, extra_config[name], minimum=0)


def resolve_security(
    security_protocol: str,
    security: KafkaSecurityConfig | None,
) -> KafkaSecurityConfig | None:
    if security is None:
        return (
            None
            if security_protocol == "PLAINTEXT"
            else KafkaSecurityConfig(security_protocol=security_protocol)
        )
    if security.security_protocol != security_protocol:
        raise ValueError(
            "KafkaSource security_protocol must match security.security_protocol when both are set."
        )
    return security


def security_kwargs(
    *,
    security_protocol: str,
    security: KafkaSecurityConfig | None,
) -> dict[str, Any]:
    if security is None:
        return {"security_protocol": security_protocol}
    return security.to_aiokafka_client_kwargs()


__all__ = [
    "resolve_security",
    "security_kwargs",
    "validate_extra_consumer_config",
    "validate_int_config",
]
