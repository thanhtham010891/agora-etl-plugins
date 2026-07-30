from __future__ import annotations

from agora.core.failures import AlertSeverity, FailureClassification

from agora_plugins.kafka.failures import classify_kafka_failure
from agora_plugins.postgres.failures import classify_postgres_failure
from agora_plugins.redis.failures import classify_redis_failure


def test_kafka_failure_adapter_marks_broker_outage_retryable() -> None:
    error = RuntimeError("leader not available at broker")

    decision = classify_kafka_failure(error)

    assert decision.classification == FailureClassification.CONNECTIVITY
    assert decision.retryable is True
    assert decision.dlq_eligible is False
    assert decision.alert_severity == AlertSeverity.WARNING


def test_redis_failure_adapter_keeps_auth_failure_out_of_record_dlq() -> None:
    error = RuntimeError("NOPERM this user has no permissions")

    decision = classify_redis_failure(error)

    assert decision.classification == FailureClassification.AUTHORIZATION
    assert decision.retryable is False
    assert decision.dlq_eligible is False
    assert decision.alert_severity == AlertSeverity.CRITICAL


def test_postgres_failure_adapter_classifies_sqlstate() -> None:
    class _PostgresError(RuntimeError):
        sqlstate = "23505"

    decision = classify_postgres_failure(_PostgresError("duplicate key"))

    assert decision.classification == FailureClassification.CONSTRAINT_VIOLATION
    assert decision.retryable is False
    assert decision.dlq_eligible is True
