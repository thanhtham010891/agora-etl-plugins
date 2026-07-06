"""Enterprise acceptance contracts for PostgreSQL plugin observability."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport

from agora_plugins.postgres._observability_snapshots import (
    PostgresDLQSinkMetricsSnapshot,
    PostgresDLQSourceMetricsSnapshot,
    PostgresSourceMetricsSnapshot,
    PostgresSourceRecoveryMode,
)

if TYPE_CHECKING:
    from agora_plugins.postgres.sinks._metrics import PostgresSinkMetricsSnapshot


@dataclass(frozen=True, slots=True)
class PostgresEnterpriseAcceptanceFinding(AcceptanceFinding):
    """Single enterprise threshold failure for a PostgreSQL component."""


@dataclass(frozen=True, slots=True)
class PostgresEnterpriseAcceptanceReport(AcceptanceReport):
    """Machine-readable enterprise verdict for a PostgreSQL component snapshot."""

    findings: tuple[PostgresEnterpriseAcceptanceFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class PostgresSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresSource behavior."""

    require_checkpoint_support: bool = False
    require_declared_recovery_contract: bool = True
    require_pipeline_rerun_contract: bool = True
    require_nontransparent_failover: bool = True
    require_ready: bool = False
    require_connection_ready: bool = False
    require_routing_ready: bool = False
    require_staleness_guard_ready: bool = False
    max_retry_count: int | None = None
    max_record_error_count: int = 0
    max_record_drop_count: int = 0
    max_active_stream_count: int | None = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_checkpoint_support": self.require_checkpoint_support,
            "require_declared_recovery_contract": self.require_declared_recovery_contract,
            "require_pipeline_rerun_contract": self.require_pipeline_rerun_contract,
            "require_nontransparent_failover": self.require_nontransparent_failover,
            "require_ready": self.require_ready,
            "require_connection_ready": self.require_connection_ready,
            "require_routing_ready": self.require_routing_ready,
            "require_staleness_guard_ready": self.require_staleness_guard_ready,
            "max_retry_count": self.max_retry_count,
            "max_record_error_count": self.max_record_error_count,
            "max_record_drop_count": self.max_record_drop_count,
            "max_active_stream_count": self.max_active_stream_count,
        }


@dataclass(frozen=True, slots=True)
class PostgresSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresSink behavior."""

    require_connection_ready: bool = True
    max_buffered_row_count: int = 0
    max_retry_count: int = 0
    max_poison_record_count: int = 0
    max_poison_record_unknown_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
            "max_buffered_row_count": self.max_buffered_row_count,
            "max_retry_count": self.max_retry_count,
            "max_poison_record_count": self.max_poison_record_count,
            "max_poison_record_unknown_count": self.max_poison_record_unknown_count,
        }


@dataclass(frozen=True, slots=True)
class PostgresDLQSinkEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresDLQSink behavior."""

    require_connection_ready: bool = True
    require_table_ready: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "require_connection_ready": self.require_connection_ready,
            "require_table_ready": self.require_table_ready,
        }


@dataclass(frozen=True, slots=True)
class PostgresDLQSourceEnterpriseAcceptanceThresholds:
    """Enterprise-grade thresholds for PostgresDLQSource behavior."""

    require_connection_ready: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"require_connection_ready": self.require_connection_ready}


class PostgresEnterpriseAcceptanceGate:
    """Evaluate PostgreSQL plugin component snapshots against enterprise thresholds."""

    def evaluate_source(
        self,
        snapshot: PostgresSourceMetricsSnapshot,
        thresholds: PostgresSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresSourceEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        contract = snapshot.recovery_contract
        if thresholds.require_declared_recovery_contract and not isinstance(
            contract.mode, PostgresSourceRecoveryMode
        ):
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.mode",
                    "Postgres source recovery contract is not declared.",
                    str(contract.mode),
                    tuple(mode.value for mode in PostgresSourceRecoveryMode),
                )
            )
        if thresholds.require_checkpoint_support and not contract.supports_checkpoint:
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.supports_checkpoint",
                    "Postgres source does not support checkpoint-based resume.",
                    contract.supports_checkpoint,
                    True,
                )
            )
        if thresholds.require_pipeline_rerun_contract and not contract.requires_pipeline_rerun:
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.requires_pipeline_rerun",
                    "Postgres source failover contract must declare pipeline rerun semantics.",
                    contract.requires_pipeline_rerun,
                    True,
                )
            )
        if thresholds.require_nontransparent_failover and contract.transparent_failover:
            findings.append(
                self._finding(
                    "source",
                    "recovery_contract.transparent_failover",
                    "Postgres source should not advertise transparent failover semantics.",
                    contract.transparent_failover,
                    False,
                )
            )
        if thresholds.require_ready and not snapshot.ready:
            findings.append(
                self._finding(
                    "source",
                    "ready",
                    "Postgres source readiness probe is not passing.",
                    snapshot.ready,
                    True,
                )
            )
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "source",
                    "connection_ready",
                    "Postgres source connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        if thresholds.require_routing_ready and not snapshot.routing_ready:
            findings.append(
                self._finding(
                    "source",
                    "routing_ready",
                    "Postgres source routing target is not satisfied.",
                    snapshot.routing_ready,
                    True,
                )
            )
        if thresholds.require_staleness_guard_ready and not snapshot.staleness_guard_ready:
            findings.append(
                self._finding(
                    "source",
                    "staleness_guard_ready",
                    "Postgres source replica staleness guard is blocking readiness.",
                    snapshot.staleness_guard_ready,
                    True,
                )
            )
        self._check_max(
            findings,
            component="source",
            metric="retry_count",
            value=snapshot.retry_count,
            threshold=thresholds.max_retry_count,
        )
        self._check_max(
            findings,
            component="source",
            metric="record_error_count",
            value=snapshot.record_error_count,
            threshold=thresholds.max_record_error_count,
        )
        self._check_max(
            findings,
            component="source",
            metric="record_drop_count",
            value=snapshot.record_drop_count,
            threshold=thresholds.max_record_drop_count,
        )
        self._check_max(
            findings,
            component="source",
            metric="active_stream_count",
            value=snapshot.active_stream_count,
            threshold=thresholds.max_active_stream_count,
        )
        return PostgresEnterpriseAcceptanceReport(
            component="source",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_sink(
        self,
        snapshot: PostgresSinkMetricsSnapshot,
        thresholds: PostgresSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresSinkEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "sink",
                    "connection_ready",
                    "Postgres sink connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        self._check_max(
            findings,
            component="sink",
            metric="buffered_row_count",
            value=snapshot.buffered_row_count,
            threshold=thresholds.max_buffered_row_count,
        )
        self._check_max(
            findings,
            component="sink",
            metric="retry_count",
            value=snapshot.retry_count,
            threshold=thresholds.max_retry_count,
        )
        self._check_max(
            findings,
            component="sink",
            metric="poison_record_count",
            value=snapshot.poison_record_count,
            threshold=thresholds.max_poison_record_count,
        )
        self._check_max(
            findings,
            component="sink",
            metric="poison_record_unknown_count",
            value=snapshot.poison_record_unknown_count,
            threshold=thresholds.max_poison_record_unknown_count,
        )
        return PostgresEnterpriseAcceptanceReport(
            component="sink",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_sink(
        self,
        snapshot: PostgresDLQSinkMetricsSnapshot,
        thresholds: PostgresDLQSinkEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresDLQSinkEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "dlq_sink",
                    "connection_ready",
                    "Postgres DLQ sink connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        if thresholds.require_table_ready and not snapshot.table_ready:
            findings.append(
                self._finding(
                    "dlq_sink",
                    "table_ready",
                    "Postgres DLQ sink table is not ready.",
                    snapshot.table_ready,
                    True,
                )
            )
        return PostgresEnterpriseAcceptanceReport(
            component="dlq_sink",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    def evaluate_dlq_source(
        self,
        snapshot: PostgresDLQSourceMetricsSnapshot,
        thresholds: PostgresDLQSourceEnterpriseAcceptanceThresholds | None = None,
    ) -> PostgresEnterpriseAcceptanceReport:
        thresholds = thresholds or PostgresDLQSourceEnterpriseAcceptanceThresholds()
        findings: list[PostgresEnterpriseAcceptanceFinding] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "dlq_source",
                    "connection_ready",
                    "Postgres DLQ source connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        return PostgresEnterpriseAcceptanceReport(
            component="dlq_source",
            passed=not findings,
            thresholds=thresholds.to_dict(),
            findings=tuple(findings),
        )

    @staticmethod
    def _finding(
        component: str,
        metric: str,
        message: str,
        value: Any,
        threshold: Any,
    ) -> PostgresEnterpriseAcceptanceFinding:
        return PostgresEnterpriseAcceptanceFinding(
            component=component,
            metric=metric,
            message=message,
            value=value,
            threshold=threshold,
        )

    def _check_max(
        self,
        findings: list[PostgresEnterpriseAcceptanceFinding],
        *,
        component: str,
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                self._finding(
                    component,
                    metric,
                    f"{component}.{metric} exceeded enterprise threshold.",
                    value,
                    threshold,
                )
            )


__all__ = [
    "PostgresDLQSinkEnterpriseAcceptanceThresholds",
    "PostgresDLQSourceEnterpriseAcceptanceThresholds",
    "PostgresEnterpriseAcceptanceFinding",
    "PostgresEnterpriseAcceptanceGate",
    "PostgresEnterpriseAcceptanceReport",
    "PostgresSinkEnterpriseAcceptanceThresholds",
    "PostgresSourceEnterpriseAcceptanceThresholds",
]
