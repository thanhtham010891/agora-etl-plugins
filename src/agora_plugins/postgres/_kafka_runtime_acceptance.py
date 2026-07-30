"""Acceptance gates for Kafka -> PostgreSQL runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport

if TYPE_CHECKING:
    from agora_plugins.postgres._kafka_runtime_snapshots import KafkaPostgresRuntimeMetricsSnapshot


@dataclass(frozen=True, slots=True)
class KafkaPostgresEnterpriseAcceptanceThresholds:
    """Production gate thresholds for Kafka -> PostgreSQL runtime health."""

    require_runtime_ready: bool = True
    require_source_ready: bool = True
    require_source_not_stalled: bool = True
    require_sink_connection_ready: bool = True
    require_delivery_key_conflict: bool = True
    require_sink_upsert: bool = True
    require_strict_sink_write_safety: bool = True
    require_poison_dlq_ready: bool = False
    max_pending_commit_count: int | None = 0
    max_idle_poll_count: int | None = 0
    max_total_lag: int | None = 0
    max_max_lag: int | None = 0
    max_total_commit_lag: int | None = 0
    max_max_commit_lag: int | None = 0
    max_last_poll_age_ms: float | None = 5_000.0
    max_last_message_age_ms: float | None = 5_000.0
    max_last_commit_age_ms: float | None = 10_000.0
    max_buffered_row_count: int | None = 0
    max_sink_retry_count: int | None = 0
    max_poison_dlq_write_count: int | None = 0
    max_record_error_count: int | None = 0
    max_record_drop_count: int | None = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_runtime_ready": self.require_runtime_ready,
            "require_source_ready": self.require_source_ready,
            "require_source_not_stalled": self.require_source_not_stalled,
            "require_sink_connection_ready": self.require_sink_connection_ready,
            "require_delivery_key_conflict": self.require_delivery_key_conflict,
            "require_sink_upsert": self.require_sink_upsert,
            "require_strict_sink_write_safety": self.require_strict_sink_write_safety,
            "require_poison_dlq_ready": self.require_poison_dlq_ready,
            "max_pending_commit_count": self.max_pending_commit_count,
            "max_idle_poll_count": self.max_idle_poll_count,
            "max_total_lag": self.max_total_lag,
            "max_max_lag": self.max_max_lag,
            "max_total_commit_lag": self.max_total_commit_lag,
            "max_max_commit_lag": self.max_max_commit_lag,
            "max_last_poll_age_ms": self.max_last_poll_age_ms,
            "max_last_message_age_ms": self.max_last_message_age_ms,
            "max_last_commit_age_ms": self.max_last_commit_age_ms,
            "max_buffered_row_count": self.max_buffered_row_count,
            "max_sink_retry_count": self.max_sink_retry_count,
            "max_poison_dlq_write_count": self.max_poison_dlq_write_count,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
        }


@dataclass(frozen=True, slots=True)
class KafkaPostgresEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single threshold violation from enterprise acceptance evaluation."""


@dataclass(frozen=True, slots=True)
class KafkaPostgresEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise acceptance verdict for a runtime snapshot."""

    findings: tuple[KafkaPostgresEnterpriseAcceptanceFinding, ...] = ()


class KafkaPostgresEnterpriseAcceptanceGate:
    """Evaluate Kafka -> PostgreSQL runtime snapshots against ops-grade thresholds."""

    def __init__(
        self,
        thresholds: KafkaPostgresEnterpriseAcceptanceThresholds | None = None,
    ) -> None:
        self._thresholds = (
            KafkaPostgresEnterpriseAcceptanceThresholds() if thresholds is None else thresholds
        )

    async def evaluate_runtime(
        self,
        runtime: object,
    ) -> KafkaPostgresEnterpriseAcceptanceReport:
        return self.evaluate(await runtime.observability_snapshot())

    def evaluate(
        self,
        snapshot: KafkaPostgresRuntimeMetricsSnapshot,
    ) -> KafkaPostgresEnterpriseAcceptanceReport:
        thresholds = self._thresholds
        findings: list[KafkaPostgresEnterpriseAcceptanceFinding] = []
        runtime_health = snapshot.health
        source = snapshot.source
        health = source.health
        operational = source.operational
        runtime_metrics = source.runtime
        sink = snapshot.sink

        if thresholds.require_runtime_ready and not runtime_health.ready:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="runtime.ready",
                    message="Kafka -> PostgreSQL runtime is not ready.",
                    value=runtime_health.ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_ready and not runtime_health.source_ready:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="source.ready",
                    message="Kafka source is not ready.",
                    value=runtime_health.source_ready,
                    threshold=True,
                )
            )
        if thresholds.require_source_not_stalled and runtime_health.source_stalled:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="source.stalled",
                    message="Kafka source is stalled.",
                    value=runtime_health.source_stalled,
                    threshold=False,
                )
            )
        if thresholds.require_sink_connection_ready and not runtime_health.sink_connection_ready:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="sink.connection_ready",
                    message="PostgreSQL sink connection is not ready.",
                    value=runtime_health.sink_connection_ready,
                    threshold=True,
                )
            )
        if (
            thresholds.require_delivery_key_conflict
            and snapshot.delivery_key_field not in sink.conflict_keys
        ):
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="sink.delivery_key_conflict",
                    message=(
                        "PostgreSQL conflict keys must include the Kafka delivery key "
                        "to make recovery replay idempotent."
                    ),
                    value=list(sink.conflict_keys),
                    threshold=snapshot.delivery_key_field,
                )
            )
        if thresholds.require_sink_upsert and not sink.upsert:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="sink.upsert",
                    message="PostgreSQL upsert must be enabled for replay-safe delivery.",
                    value=sink.upsert,
                    threshold=True,
                )
            )
        if thresholds.require_strict_sink_write_safety and sink.write_safety_policy != "strict":
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="sink.write_safety_policy",
                    message=(
                        "Kafka -> PostgreSQL acceptance requires strict sink write safety "
                        "to preserve the delivery-key replay boundary."
                    ),
                    value=sink.write_safety_policy,
                    threshold="strict",
                )
            )
        if (
            thresholds.require_poison_dlq_ready
            and runtime_health.poison_dlq_enabled
            and runtime_health.poison_dlq_ready is not True
        ):
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric="poison_dlq.ready",
                    message="PostgreSQL poison DLQ is not ready.",
                    value=runtime_health.poison_dlq_ready,
                    threshold=True,
                )
            )

        for metric, value, threshold in (
            (
                "source.pending_commit_count",
                health.pending_commit_count,
                thresholds.max_pending_commit_count,
            ),
            ("source.idle_poll_count", health.idle_poll_count, thresholds.max_idle_poll_count),
            ("source.total_lag", health.total_lag, thresholds.max_total_lag),
            ("source.max_lag", health.max_lag, thresholds.max_max_lag),
            ("source.total_commit_lag", health.total_commit_lag, thresholds.max_total_commit_lag),
            ("source.max_commit_lag", health.max_commit_lag, thresholds.max_max_commit_lag),
            ("source.last_poll_age_ms", health.last_poll_age_ms, thresholds.max_last_poll_age_ms),
            (
                "source.last_message_age_ms",
                health.last_message_age_ms,
                thresholds.max_last_message_age_ms,
            ),
            (
                "source.last_commit_age_ms",
                health.last_commit_age_ms,
                thresholds.max_last_commit_age_ms,
            ),
            ("sink.buffered_row_count", sink.buffered_row_count, thresholds.max_buffered_row_count),
            ("sink.retry_count", sink.retry_count, thresholds.max_sink_retry_count),
            (
                "source.poison_record_dlq_write_count",
                operational.poison_record_dlq_write_count,
                thresholds.max_poison_dlq_write_count,
            ),
            (
                "source.record_error_count",
                runtime_metrics.record_error_count,
                thresholds.max_record_error_count,
            ),
            (
                "source.record_drop_count",
                runtime_metrics.record_drop_count,
                thresholds.max_record_drop_count,
            ),
        ):
            self._check_max(findings, metric=metric, value=value, threshold=threshold)

        return KafkaPostgresEnterpriseAcceptanceReport(
            passed=not findings,
            thresholds=thresholds,
            findings=tuple(findings),
        )

    @staticmethod
    def _check_max(
        findings: list[KafkaPostgresEnterpriseAcceptanceFinding],
        *,
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                KafkaPostgresEnterpriseAcceptanceFinding(
                    metric=metric,
                    message=f"{metric} exceeded enterprise threshold.",
                    value=value,
                    threshold=threshold,
                )
            )


__all__ = [
    "KafkaPostgresEnterpriseAcceptanceFinding",
    "KafkaPostgresEnterpriseAcceptanceGate",
    "KafkaPostgresEnterpriseAcceptanceReport",
    "KafkaPostgresEnterpriseAcceptanceThresholds",
]
