"""Plugin-owned `agora doctor` readiness checks for PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agora.core.container import AgoraContainer
from agora.core.doctor import CheckResult, DoctorReadinessProvider, Status
from agora.core.metrics import has_metrics_snapshot

from agora_plugins._doctor_support import parse_key_value_lines, structured_readiness_data
from agora_plugins.postgres.observability import (
    PostgresDLQSinkEnterpriseAcceptanceThresholds,
    PostgresEnterpriseAcceptanceGate,
    PostgresSinkEnterpriseAcceptanceThresholds,
    PostgresSourceEnterpriseAcceptanceThresholds,
)


@dataclass(frozen=True, slots=True)
class PostgresDoctorReadinessProvider:
    backend: str = "postgres"
    component_types: frozenset[str] = frozenset({"postgres", "postgres_dlq"})

    async def run_readiness_checks(self, pipeline_config: dict[str, Any]) -> list[CheckResult]:
        container = AgoraContainer.from_config(pipeline_config)
        gate = PostgresEnterpriseAcceptanceGate()
        results: list[CheckResult] = []

        async with container:
            pipeline = container.build_pipeline()
            source_cfg = pipeline_config.get("source", {})
            source = getattr(pipeline, "_source", None)
            if _component_type(source_cfg) == "postgres":
                if source is None or not has_metrics_snapshot(source):
                    results.append(
                        CheckResult(
                            name="Postgres source readiness",
                            status=Status.FAIL,
                            message="Configured Postgres source could not expose readiness metrics",
                            detail="Expected a live Postgres source instance with metrics_snapshot().",
                            data=structured_readiness_data(
                                backend="postgres",
                                component="source",
                                name="Postgres source readiness",
                                status=Status.FAIL.value,
                                message="Configured Postgres source could not expose readiness metrics",
                                metrics={},
                                findings=[
                                    {
                                        "metric": "metrics_snapshot",
                                        "message": "Expected a live Postgres source instance with metrics_snapshot().",
                                        "value": None,
                                        "threshold": "present",
                                    }
                                ],
                                operator_hooks=[
                                    "Verify the configured Postgres source plugin loads and starts cleanly before cutover."
                                ],
                            ),
                        )
                    )
                else:
                    snapshot = source.metrics_snapshot()
                    report = gate.evaluate_source(
                        snapshot,
                        thresholds=PostgresSourceEnterpriseAcceptanceThresholds(
                            require_checkpoint_support=True
                        ),
                    )
                    detail_lines = [
                        f"mode={snapshot.recovery_contract.mode.value}",
                        f"supports_checkpoint={snapshot.recovery_contract.supports_checkpoint}",
                        f"requires_pipeline_rerun={snapshot.recovery_contract.requires_pipeline_rerun}",
                        f"transparent_failover={snapshot.recovery_contract.transparent_failover}",
                    ]
                    results.append(
                        _postgres_readiness_result(
                            name="Postgres source readiness",
                            subject="Postgres source",
                            component="source",
                            report=report,
                            detail_lines=detail_lines,
                        )
                    )

            writer = getattr(pipeline, "_writer", None)
            sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
            sink_cfgs = pipeline_config.get("sinks", [])
            if isinstance(sink_cfgs, list):
                for index, sink_cfg in enumerate(sink_cfgs):
                    if _component_type(sink_cfg) != "postgres":
                        continue
                    sink = sink_instances[index] if index < len(sink_instances) else None
                    if sink is None or not has_metrics_snapshot(sink):
                        results.append(
                            CheckResult(
                                name=f"Postgres sink readiness #{index + 1}",
                                status=Status.FAIL,
                                message="Configured Postgres sink could not expose readiness metrics",
                                detail="Expected a live Postgres sink instance with metrics_snapshot().",
                                data=structured_readiness_data(
                                    backend="postgres",
                                    component="sink",
                                    name=f"Postgres sink readiness #{index + 1}",
                                    status=Status.FAIL.value,
                                    message="Configured Postgres sink could not expose readiness metrics",
                                    metrics={},
                                    findings=[
                                        {
                                            "metric": "metrics_snapshot",
                                            "message": "Expected a live Postgres sink instance with metrics_snapshot().",
                                            "value": None,
                                            "threshold": "present",
                                        }
                                    ],
                                    operator_hooks=[
                                        "Verify the configured Postgres sink plugin loads and starts cleanly before cutover."
                                    ],
                                ),
                            )
                        )
                        continue
                    snapshot = sink.metrics_snapshot()
                    report = gate.evaluate_sink(
                        snapshot,
                        thresholds=PostgresSinkEnterpriseAcceptanceThresholds(),
                    )
                    detail_lines = [
                        f"table={snapshot.table}",
                        f"connection_ready={snapshot.connection_ready}",
                        f"write_safety_policy={snapshot.write_safety_policy}",
                    ]
                    results.append(
                        _postgres_readiness_result(
                            name=f"Postgres sink readiness #{index + 1}",
                            subject=f"Postgres sink {snapshot.table!r}",
                            component="sink",
                            report=report,
                            detail_lines=detail_lines,
                        )
                    )

            dlq_cfg = pipeline_config.get("dlq")
            if (
                isinstance(dlq_cfg, dict)
                and dlq_cfg.get("enabled", True)
                and _component_type(dlq_cfg.get("sink")) == "postgres_dlq"
            ):
                dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
                if dlq_sink is None or not has_metrics_snapshot(dlq_sink):
                    results.append(
                        CheckResult(
                            name="Postgres DLQ readiness",
                            status=Status.FAIL,
                            message="Configured Postgres DLQ could not expose readiness metrics",
                            detail="Expected a live Postgres DLQ sink instance with metrics_snapshot().",
                            data=structured_readiness_data(
                                backend="postgres",
                                component="dlq",
                                name="Postgres DLQ readiness",
                                status=Status.FAIL.value,
                                message="Configured Postgres DLQ could not expose readiness metrics",
                                metrics={},
                                findings=[
                                    {
                                        "metric": "metrics_snapshot",
                                        "message": "Expected a live Postgres DLQ sink instance with metrics_snapshot().",
                                        "value": None,
                                        "threshold": "present",
                                    }
                                ],
                                operator_hooks=[
                                    "Verify the configured Postgres DLQ plugin loads and starts cleanly before cutover."
                                ],
                            ),
                        )
                    )
                else:
                    snapshot = dlq_sink.metrics_snapshot()
                    report = gate.evaluate_dlq_sink(
                        snapshot,
                        thresholds=PostgresDLQSinkEnterpriseAcceptanceThresholds(),
                    )
                    detail_lines = [
                        f"table={snapshot.table}",
                        f"connection_ready={snapshot.connection_ready}",
                        f"table_ready={snapshot.table_ready}",
                    ]
                    results.append(
                        _postgres_readiness_result(
                            name="Postgres DLQ readiness",
                            subject=f"Postgres DLQ {snapshot.table!r}",
                            component="dlq",
                            report=report,
                            detail_lines=detail_lines,
                        )
                    )

        return results


DOCTOR_READINESS_PROVIDER: DoctorReadinessProvider = PostgresDoctorReadinessProvider()


def _component_type(config: object) -> str | None:
    if isinstance(config, dict):
        value = config.get("type")
        return value if isinstance(value, str) else None
    return None


def _postgres_readiness_result(
    *,
    name: str,
    subject: str,
    component: str,
    report: Any,
    detail_lines: list[str],
) -> CheckResult:
    status = Status.PASS if report.passed else Status.FAIL
    detail = list(detail_lines)
    findings_payload: list[dict[str, Any]] = []
    for finding in report.findings:
        detail.append(
            f"{finding.metric}: {finding.message} (value={finding.value!r}, threshold={finding.threshold!r})"
        )
        findings_payload.append(
            {
                "metric": finding.metric,
                "message": finding.message,
                "value": finding.value,
                "threshold": finding.threshold,
            }
        )
    hooks = _postgres_operator_hooks(subject=subject, report=report)
    detail.extend(f"operator_hook={hook}" for hook in hooks)
    message = (
        f"{subject} passed enterprise readiness checks"
        if report.passed
        else f"{subject} failed enterprise readiness checks"
    )
    return CheckResult(
        name=name,
        status=status,
        message=message,
        detail="\n".join(detail),
        data=structured_readiness_data(
            backend="postgres",
            component=component,
            name=name,
            status=status.value,
            message=message,
            metrics=parse_key_value_lines(detail_lines),
            findings=findings_payload,
            operator_hooks=hooks,
        ),
    )


def _postgres_operator_hooks(*, subject: str, report: Any) -> list[str]:
    hooks: list[str] = []
    metrics = {finding.metric for finding in report.findings}
    if "recovery_contract.supports_checkpoint" in metrics:
        hooks.append(
            f"Configure checkpoint cursor fields for {subject} before relying on enterprise failover resume semantics."
        )
    if "connection_ready" in metrics:
        hooks.append(
            f"Verify DSN, credentials, TLS settings, and network reachability for {subject}."
        )
    if "table_ready" in metrics:
        hooks.append(
            f"Ensure the target table for {subject} exists and the service account can read/write it."
        )
    if "poison_record_count" in metrics or "poison_record_unknown_count" in metrics:
        hooks.append(
            f"Inspect poison-record classification for {subject} before enabling automatic replay or cutover."
        )
    if "retry_count" in metrics:
        hooks.append(
            f"Investigate repeated retries on {subject}; enterprise readiness expects a clean steady-state startup."
        )
    return hooks
