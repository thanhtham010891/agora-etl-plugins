"""Redis-specific input to Agora's shared failure-decision contract."""

from __future__ import annotations

from agora.core.failures import (
    AlertSeverity,
    FailureClassification,
    FailureDecision,
    classify_failure,
)


def classify_redis_failure(exc: Exception) -> FailureDecision:
    """Classify Redis command failures without importing optional redis types."""
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    markers = f"{name} {message}"
    if any(marker in markers for marker in ("authentication", "authorization", "noperm", "acl")):
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
    if any(marker in markers for marker in ("connection", "readonly", "loading", "tryagain")):
        return FailureDecision(
            classification=FailureClassification.CONNECTIVITY,
            retryable=True,
            dlq_eligible=False,
            alert_severity=AlertSeverity.WARNING,
            reason=type(exc).__name__,
        )
    return classify_failure(exc)


__all__ = ["classify_redis_failure"]
