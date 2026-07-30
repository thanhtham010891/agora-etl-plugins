"""PostgreSQL-specific input to Agora's shared failure-decision contract."""

from __future__ import annotations

from agora.core.failures import (
    AlertSeverity,
    FailureClassification,
    FailureDecision,
    classify_failure,
)


def classify_postgres_failure(exc: Exception) -> FailureDecision:
    """Classify SQLSTATE and driver errors for retry and DLQ routing."""
    sqlstate = getattr(exc, "sqlstate", None)
    state = sqlstate if isinstance(sqlstate, str) else ""
    if state.startswith("28"):
        return _decision(
            FailureClassification.AUTHORIZATION,
            False,
            False,
            AlertSeverity.CRITICAL,
            exc,
        )
    if state.startswith("23"):
        return _decision(
            FailureClassification.CONSTRAINT_VIOLATION,
            False,
            True,
            AlertSeverity.ERROR,
            exc,
        )
    if state.startswith("22"):
        return _decision(
            FailureClassification.TYPE_MISMATCH,
            False,
            True,
            AlertSeverity.ERROR,
            exc,
        )
    if state == "57014" or isinstance(exc, TimeoutError):
        return _decision(
            FailureClassification.TIMEOUT,
            True,
            False,
            AlertSeverity.WARNING,
            exc,
        )
    if state.startswith("08") or state in {"57P01", "57P02", "57P03", "40001", "40P01"}:
        return _decision(
            FailureClassification.CONNECTIVITY,
            True,
            False,
            AlertSeverity.WARNING,
            exc,
        )

    name = type(exc).__name__.lower()
    message = str(exc).lower()
    markers = f"{name} {message}"
    if any(
        marker in markers for marker in ("connection", "connect", "operational", "admin shutdown")
    ):
        return _decision(
            FailureClassification.CONNECTIVITY,
            True,
            False,
            AlertSeverity.WARNING,
            exc,
        )
    if "timeout" in markers:
        return _decision(
            FailureClassification.TIMEOUT,
            True,
            False,
            AlertSeverity.WARNING,
            exc,
        )
    return classify_failure(exc)


def _decision(
    classification: FailureClassification,
    retryable: bool,
    dlq_eligible: bool,
    alert_severity: AlertSeverity,
    exc: Exception,
) -> FailureDecision:
    return FailureDecision(
        classification=classification,
        retryable=retryable,
        dlq_eligible=dlq_eligible,
        alert_severity=alert_severity,
        reason=type(exc).__name__,
        details=(
            {"sqlstate": exc.sqlstate} if isinstance(getattr(exc, "sqlstate", None), str) else {}
        ),
    )


__all__ = ["classify_postgres_failure"]
