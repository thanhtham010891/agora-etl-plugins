"""Kafka-specific input to Agora's shared failure-decision contract."""

from __future__ import annotations

from agora.core.failures import (
    AlertSeverity,
    FailureClassification,
    FailureDecision,
    classify_failure,
)


def classify_kafka_failure(exc: Exception) -> FailureDecision:
    """Classify broker failures without coupling the core to aiokafka."""
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    markers = f"{name} {message}"
    if any(marker in markers for marker in ("authorization", "authentication", "sasl", "security")):
        return FailureDecision(
            classification=FailureClassification.AUTHORIZATION,
            retryable=False,
            dlq_eligible=False,
            alert_severity=AlertSeverity.CRITICAL,
            reason=type(exc).__name__,
        )
    if "timeout" in markers:
        return FailureDecision(
            classification=FailureClassification.TIMEOUT,
            retryable=True,
            dlq_eligible=False,
            alert_severity=AlertSeverity.WARNING,
            reason=type(exc).__name__,
        )
    if any(
        marker in markers
        for marker in (
            "broker",
            "connection",
            "network",
            "leadernotavailable",
            "leader not available",
            "notleader",
            "not leader",
            "requesttimedout",
            "request timed out",
        )
    ):
        return FailureDecision(
            classification=FailureClassification.CONNECTIVITY,
            retryable=True,
            dlq_eligible=False,
            alert_severity=AlertSeverity.WARNING,
            reason=type(exc).__name__,
        )
    if "serializ" in markers:
        return FailureDecision(
            classification=FailureClassification.SERIALIZATION,
            retryable=False,
            dlq_eligible=True,
            alert_severity=AlertSeverity.ERROR,
            reason=type(exc).__name__,
        )
    return classify_failure(exc)


__all__ = ["classify_kafka_failure"]
