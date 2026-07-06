"""Enterprise acceptance gate for Kafka -> Redis runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport

if TYPE_CHECKING:
    from agora_plugins.redis._kafka_models import KafkaRedisRuntimeMetricsSnapshot


@dataclass(frozen=True, slots=True)
class KafkaRedisEnterpriseAcceptanceThresholds:
    """Production gate thresholds for Kafka -> Redis runtime health."""

    require_runtime_ready: bool = True
    require_source_ready: bool = True
    require_source_not_stalled: bool = True
    require_sink_connection_ready: bool = True
    max_pending_commit_count: int | None = 0
    max_idle_poll_count: int | None = 0
    max_total_lag: int | None = 0
    max_max_lag: int | None = 0
    max_total_commit_lag: int | None = 0
    max_max_commit_lag: int | None = 0
    max_last_poll_age_ms: float | None = 5_000.0
    max_last_message_age_ms: float | None = 5_000.0
    max_last_commit_age_ms: float | None = 10_000.0
    max_record_error_count: int | None = 0
    max_record_drop_count: int | None = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "require_runtime_ready": self.require_runtime_ready,
            "require_source_ready": self.require_source_ready,
            "require_source_not_stalled": self.require_source_not_stalled,
            "require_sink_connection_ready": self.require_sink_connection_ready,
            "max_pending_commit_count": self.max_pending_commit_count,
            "max_idle_poll_count": self.max_idle_poll_count,
            "max_total_lag": self.max_total_lag,
            "max_max_lag": self.max_max_lag,
            "max_total_commit_lag": self.max_total_commit_lag,
            "max_max_commit_lag": self.max_max_commit_lag,
            "max_last_poll_age_ms": self.max_last_poll_age_ms,
            "max_last_message_age_ms": self.max_last_message_age_ms,
            "max_last_commit_age_ms": self.max_last_commit_age_ms,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
        }


@dataclass(frozen=True, slots=True)
class KafkaRedisEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single threshold violation from enterprise acceptance evaluation."""


@dataclass(frozen=True, slots=True)
class KafkaRedisEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise acceptance verdict for a runtime snapshot."""

    findings: tuple[KafkaRedisEnterpriseAcceptanceFinding, ...] = ()


class KafkaRedisEnterpriseAcceptanceGate:
    """Evaluate Kafka -> Redis runtime snapshots against ops-grade thresholds."""

    def __init__(
        self,
        thresholds: KafkaRedisEnterpriseAcceptanceThresholds | None = None,
    ) -> None:
        self._thresholds = (
            KafkaRedisEnterpriseAcceptanceThresholds() if thresholds is None else thresholds
        )

    async def evaluate_runtime(
        self,
        runtime: object,
    ) -> KafkaRedisEnterpriseAcceptanceReport:
        return self.evaluate(await runtime.observability_snapshot())

    def evaluate(
        self,
        snapshot: KafkaRedisRuntimeMetricsSnapshot,
    ) -> KafkaRedisEnterpriseAcceptanceReport:
        thresholds = self._thresholds
        findings: list[KafkaRedisEnterpriseAcceptanceFinding] = []
        runtime_health = snapshot.health
        source = snapshot.source
        health = source.health
        runtime_metrics = source.runtime

        if thresholds.require_runtime_ready and not runtime_health.ready:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="runtime.ready",
                    message="Kafka -> Redis runtime is not ready.",
                    value=runtime_health.ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_ready and not runtime_health.source_ready:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="source.ready",
                    message="Kafka source is not ready.",
                    value=runtime_health.source_ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_not_stalled and runtime_health.source_stalled:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="source.stalled",
                    message="Kafka source is stalled.",
                    value=runtime_health.source_stalled,
                    threshold=False,
                )
            )
        if thresholds.require_sink_connection_ready and not runtime_health.sink_connection_ready:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric="sink.connection_ready",
                    message="Redis sink connection is not ready.",
                    value=runtime_health.sink_connection_ready,
                    threshold=True,
                )
            )

        self._check_max(
            findings,
            "source.pending_commit_count",
            health.pending_commit_count,
            thresholds.max_pending_commit_count,
        )
        self._check_max(
            findings,
            "source.idle_poll_count",
            health.idle_poll_count,
            thresholds.max_idle_poll_count,
        )
        self._check_max(findings, "source.total_lag", health.total_lag, thresholds.max_total_lag)
        self._check_max(findings, "source.max_lag", health.max_lag, thresholds.max_max_lag)
        self._check_max(
            findings,
            "source.total_commit_lag",
            health.total_commit_lag,
            thresholds.max_total_commit_lag,
        )
        self._check_max(
            findings, "source.max_commit_lag", health.max_commit_lag, thresholds.max_max_commit_lag
        )
        self._check_max(
            findings,
            "source.last_poll_age_ms",
            health.last_poll_age_ms,
            thresholds.max_last_poll_age_ms,
        )
        self._check_max(
            findings,
            "source.last_message_age_ms",
            health.last_message_age_ms,
            thresholds.max_last_message_age_ms,
        )
        self._check_max(
            findings,
            "source.last_commit_age_ms",
            health.last_commit_age_ms,
            thresholds.max_last_commit_age_ms,
        )
        self._check_max(
            findings,
            "source.record_error_count",
            runtime_metrics.record_error_count,
            thresholds.max_record_error_count,
        )
        self._check_max(
            findings,
            "source.record_drop_count",
            runtime_metrics.record_drop_count,
            thresholds.max_record_drop_count,
        )

        return KafkaRedisEnterpriseAcceptanceReport(
            passed=not findings,
            thresholds=thresholds,
            findings=tuple(findings),
        )

    @staticmethod
    def _check_max(
        findings: list[KafkaRedisEnterpriseAcceptanceFinding],
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                KafkaRedisEnterpriseAcceptanceFinding(
                    metric=metric,
                    message=f"{metric} exceeded enterprise threshold.",
                    value=value,
                    threshold=threshold,
                )
            )
