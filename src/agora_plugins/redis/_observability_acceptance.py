"""Redis enterprise acceptance gates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport

if TYPE_CHECKING:
    from agora_plugins.redis._observability_snapshots import (
        RedisDLQSinkMetricsSnapshot,
        RedisDLQSourceMetricsSnapshot,
        RedisStreamSourceMetricsSnapshot,
    )
    from agora_plugins.redis.sinks.redis import RedisSinkMetricsSnapshot


@dataclass(frozen=True, slots=True)
class RedisEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single enterprise threshold failure for a Redis component."""


@dataclass(frozen=True, slots=True)
class RedisEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise verdict for a Redis component snapshot."""

    findings: tuple[RedisEnterpriseAcceptanceFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class RedisSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisStreamSource behavior."""

    require_ready: bool = True
    require_connection_ready: bool = True
    require_group_ready: bool = True
    max_pending_ack_count: int | None = 0
    max_record_error_count: int = 0
    max_record_drop_count: int = 0
    max_poison_loop_count: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "require_ready": self.require_ready,
            "require_connection_ready": self.require_connection_ready,
            "require_group_ready": self.require_group_ready,
            "max_pending_ack_count": self.max_pending_ack_count,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
            "max_poison_loop_count": self.max_poison_loop_count,
        }


@dataclass(frozen=True, slots=True)
class RedisSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisSink behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, object]:
        return {"require_connection_ready": self.require_connection_ready}


@dataclass(frozen=True, slots=True)
class RedisDLQSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisDLQSink behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, object]:
        return {"require_connection_ready": self.require_connection_ready}


@dataclass(frozen=True, slots=True)
class RedisDLQSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for RedisDLQSource behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, object]:
        return {"require_connection_ready": self.require_connection_ready}


class RedisEnterpriseAcceptanceGate:
    """Evaluate Redis component snapshots against ops-grade thresholds."""

    def evaluate_source(
        self,
        snapshot: RedisStreamSourceMetricsSnapshot,
        thresholds: RedisSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisSourceEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        health = snapshot.health
        if resolved.require_ready and not health.ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_source",
                    metric="ready",
                    message="Redis stream source is not ready.",
                    value=health.ready,
                    threshold=True,
                )
            )
        if resolved.require_connection_ready and not health.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_source",
                    metric="connection_ready",
                    message="Redis stream source connection is not ready.",
                    value=health.connection_ready,
                    threshold=True,
                )
            )
        if resolved.require_group_ready and not health.group_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_source",
                    metric="group_ready",
                    message="Redis stream source consumer group is not ready.",
                    value=health.group_ready,
                    threshold=True,
                )
            )
        self._check_max(
            findings,
            component="redis_source",
            metric="pending_ack_count",
            value=snapshot.pending_ack_count,
            threshold=resolved.max_pending_ack_count,
        )
        self._check_max(
            findings,
            component="redis_source",
            metric="record_error_count",
            value=snapshot.record_error_count,
            threshold=resolved.max_record_error_count,
        )
        self._check_max(
            findings,
            component="redis_source",
            metric="record_drop_count",
            value=snapshot.record_drop_count,
            threshold=resolved.max_record_drop_count,
        )
        self._check_max(
            findings,
            component="redis_source",
            metric="poison_loop_count",
            value=snapshot.poison_loop_risk.loop_count,
            threshold=resolved.max_poison_loop_count,
            message=(
                "reclaimed poison record(s) are looping without acknowledgment; "
                "drain or replay the pending poison before promoting this runtime."
            ),
        )
        return RedisEnterpriseAcceptanceReport(
            component="redis_source",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_sink(
        self,
        snapshot: RedisSinkMetricsSnapshot,
        thresholds: RedisSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisSinkEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        if resolved.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_sink",
                    metric="connection_ready",
                    message="Redis sink connection is not ready.",
                    value=snapshot.connection_ready,
                    threshold=True,
                )
            )
        return RedisEnterpriseAcceptanceReport(
            component="redis_sink",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_sink(
        self,
        snapshot: RedisDLQSinkMetricsSnapshot,
        thresholds: RedisDLQSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisDLQSinkEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        if resolved.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_dlq_sink",
                    metric="connection_ready",
                    message="Redis DLQ sink connection is not ready.",
                    value=snapshot.connection_ready,
                    threshold=True,
                )
            )
        return RedisEnterpriseAcceptanceReport(
            component="redis_dlq_sink",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_source(
        self,
        snapshot: RedisDLQSourceMetricsSnapshot,
        thresholds: RedisDLQSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> RedisEnterpriseAcceptanceReport:
        resolved = thresholds or RedisDLQSourceEnterpriseAcceptanceThresholds()
        findings: list[RedisEnterpriseAcceptanceFinding] = []
        if resolved.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component="redis_dlq_source",
                    metric="connection_ready",
                    message="Redis DLQ source connection is not ready.",
                    value=snapshot.connection_ready,
                    threshold=True,
                )
            )
        return RedisEnterpriseAcceptanceReport(
            component="redis_dlq_source",
            passed=not findings,
            thresholds=resolved.to_dict(),
            findings=tuple(findings),
        )

    @staticmethod
    def _check_max(
        findings: list[RedisEnterpriseAcceptanceFinding],
        *,
        component: str,
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
        message: str | None = None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                RedisEnterpriseAcceptanceFinding(
                    component=component,
                    metric=metric,
                    message=message or f"{metric} exceeded enterprise threshold.",
                    value=value,
                    threshold=threshold,
                )
            )
