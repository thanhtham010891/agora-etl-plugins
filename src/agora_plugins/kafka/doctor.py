"""Plugin-owned `agora doctor` readiness checks for Kafka."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agora.core.container import AgoraContainer
from agora.core.doctor import CheckResult, DoctorReadinessProvider, Status
from agora.core.metrics import has_metrics_snapshot

from agora_plugins._doctor_support import (
    call_async_with_optional_kwargs,
    parse_key_value_lines,
    structured_readiness_data,
)


@dataclass(frozen=True, slots=True)
class KafkaDoctorReadinessProvider:
    backend: str = "kafka"
    component_types: frozenset[str] = frozenset({"kafka", "kafka_dlq"})

    async def run_readiness_checks(self, pipeline_config: dict[str, Any]) -> list[CheckResult]:
        container = AgoraContainer.from_config(pipeline_config)
        results: list[CheckResult] = []
        async with container:
            pipeline = container.build_pipeline()
            source_cfg = pipeline_config.get("source", {})
            source = getattr(pipeline, "_source", None)
            if _component_type(source_cfg) == "kafka":
                results.append(await _check_kafka_source_readiness(source))

            writer = getattr(pipeline, "_writer", None)
            sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
            sink_cfgs = pipeline_config.get("sinks", [])
            if isinstance(sink_cfgs, list):
                for index, sink_cfg in enumerate(sink_cfgs):
                    if _component_type(sink_cfg) != "kafka":
                        continue
                    sink = sink_instances[index] if index < len(sink_instances) else None
                    results.append(_check_kafka_sink_readiness(sink, index=index + 1))

            dlq_cfg = pipeline_config.get("dlq")
            if (
                isinstance(dlq_cfg, dict)
                and dlq_cfg.get("enabled", True)
                and _component_type(dlq_cfg.get("sink")) == "kafka_dlq"
            ):
                dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
                results.append(_check_kafka_dlq_readiness(dlq_sink))

        return results


DOCTOR_READINESS_PROVIDER: DoctorReadinessProvider = KafkaDoctorReadinessProvider()


def _component_type(config: object) -> str | None:
    if isinstance(config, dict):
        value = config.get("type")
        return value if isinstance(value, str) else None
    return None


async def _check_kafka_source_readiness(source: object) -> CheckResult:
    if source is None or not hasattr(source, "health_snapshot"):
        return CheckResult(
            name="Kafka source readiness",
            status=Status.FAIL,
            message="Configured Kafka source could not expose readiness state",
            detail="Expected a live Kafka source instance with health_snapshot().",
            data=structured_readiness_data(
                backend="kafka",
                component="source",
                name="Kafka source readiness",
                status=Status.FAIL.value,
                message="Configured Kafka source could not expose readiness state",
                metrics={},
                findings=[
                    {
                        "metric": "health_snapshot",
                        "message": "Expected a live Kafka source instance with health_snapshot().",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Kafka source plugin loads and starts cleanly before cutover."
                ],
            ),
        )

    health = await call_async_with_optional_kwargs(source, "health_snapshot", force_refresh=True)
    runtime_metrics = getattr(source, "runtime_metrics", lambda: None)()
    operational_metrics = getattr(source, "operational_metrics", lambda: None)()
    detail_lines = [
        f"consumer_group={getattr(health, 'consumer_group', 'unknown')}",
        f"subscription_mode={getattr(health, 'subscription_mode', 'unknown')}",
        f"assignment_count={getattr(health, 'assignment_count', 'unknown')}",
        f"pending_commit_count={getattr(health, 'pending_commit_count', 'unknown')}",
        f"rebalance_count={getattr(health, 'rebalance_count', 'unknown')}",
    ]
    total_lag = getattr(health, "total_lag", None)
    if total_lag is not None:
        detail_lines.append(f"total_lag={total_lag}")
    status = Status.PASS
    message = "Kafka source passed enterprise readiness checks"
    hooks: list[str] = []
    if getattr(health, "stalled", False):
        status = Status.FAIL
        message = "Kafka source is stalled"
        hooks.append(
            "Inspect broker connectivity, rebalance churn, or pause/resume orchestration before cutover."
        )
    elif not getattr(health, "ready", False):
        status = Status.WARN
        message = "Kafka source opened but has no active partition assignment yet"
        hooks.append(
            "Verify topic existence, ACLs, and consumer-group coordinator state until partition assignment becomes stable."
        )
    if getattr(runtime_metrics, "record_error_count", 0) > 0:
        status = Status.FAIL
        message = "Kafka source has source-level record errors"
        hooks.append(
            "Inspect poison-record classification counters and DLQ flow before promoting this consumer."
        )
    if getattr(health, "pending_commit_count", 0) > 0 and status == Status.PASS:
        status = Status.WARN
        message = "Kafka source has pending commits at readiness time"
        hooks.append(
            "Let commit-safe handoff drain pending acknowledgements before rolling forward."
        )
    if getattr(operational_metrics, "poison_record_fail_closed_count", 0) > 0:
        hooks.append(
            "A fail-closed poison policy has already fired; verify schema or payload fixes before restart."
        )
    detail_lines.extend(f"operator_hook={hook}" for hook in dict.fromkeys(hooks))
    rendered_hooks = list(dict.fromkeys(hooks))
    return CheckResult(
        name="Kafka source readiness",
        status=status,
        message=message,
        detail="\n".join(detail_lines),
        data=structured_readiness_data(
            backend="kafka",
            component="source",
            name="Kafka source readiness",
            status=status.value,
            message=message,
            metrics=parse_key_value_lines(
                [line for line in detail_lines if not line.startswith("operator_hook=")]
            ),
            findings=[],
            operator_hooks=rendered_hooks,
        ),
    )


def _check_kafka_sink_readiness(sink: object, *, index: int) -> CheckResult:
    if sink is None:
        return CheckResult(
            name=f"Kafka sink readiness #{index}",
            status=Status.FAIL,
            message="Configured Kafka sink instance is missing",
            data=structured_readiness_data(
                backend="kafka",
                component="sink",
                name=f"Kafka sink readiness #{index}",
                status=Status.FAIL.value,
                message="Configured Kafka sink instance is missing",
                metrics={},
                findings=[
                    {
                        "metric": "sink_instance",
                        "message": "Configured Kafka sink instance is missing",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Kafka sink plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    producer = getattr(sink, "_producer", None)
    topic = getattr(sink, "_topic", "unknown")
    bootstrap = getattr(sink, "_bootstrap", "unknown")
    ready = producer is not None
    detail_lines = [
        f"topic={topic}",
        f"bootstrap_servers={bootstrap}",
        f"producer_ready={ready}",
        "operator_hook=Verify idempotent producer auth, TLS, and topic ACLs before cutover."
        if not ready
        else "operator_hook=Producer startup succeeded; keep an eye on delivery acks during first live traffic.",
    ]
    message = (
        f"Kafka sink {topic!r} passed enterprise readiness checks"
        if ready
        else f"Kafka sink {topic!r} failed enterprise readiness checks"
    )
    hooks = [
        "Verify idempotent producer auth, TLS, and topic ACLs before cutover."
        if not ready
        else "Producer startup succeeded; keep an eye on delivery acks during first live traffic."
    ]
    status = Status.PASS if ready else Status.FAIL
    return CheckResult(
        name=f"Kafka sink readiness #{index}",
        status=status,
        message=message,
        detail="\n".join(detail_lines),
        data=structured_readiness_data(
            backend="kafka",
            component="sink",
            name=f"Kafka sink readiness #{index}",
            status=status.value,
            message=message,
            metrics=parse_key_value_lines(
                [line for line in detail_lines if not line.startswith("operator_hook=")]
            ),
            findings=[],
            operator_hooks=hooks,
        ),
    )


def _check_kafka_dlq_readiness(dlq_sink: object) -> CheckResult:
    if dlq_sink is None or not has_metrics_snapshot(dlq_sink):
        return CheckResult(
            name="Kafka DLQ readiness",
            status=Status.FAIL,
            message="Configured Kafka DLQ could not expose readiness metrics",
            detail="Expected a live Kafka DLQ sink instance with metrics_snapshot().",
            data=structured_readiness_data(
                backend="kafka",
                component="dlq",
                name="Kafka DLQ readiness",
                status=Status.FAIL.value,
                message="Configured Kafka DLQ could not expose readiness metrics",
                metrics={},
                findings=[
                    {
                        "metric": "metrics_snapshot",
                        "message": "Expected a live Kafka DLQ sink instance with metrics_snapshot().",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Kafka DLQ plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    snapshot = dlq_sink.metrics_snapshot()
    topic = getattr(snapshot, "topic", "unknown")
    bootstrap = getattr(snapshot, "bootstrap_servers", "unknown")
    message = f"Kafka DLQ {topic!r} passed enterprise readiness checks"
    hooks = [
        "Validate DLQ topic retention and replay consumers before relying on poison-record isolation."
    ]
    return CheckResult(
        name="Kafka DLQ readiness",
        status=Status.PASS,
        message=message,
        detail="\n".join(
            [
                f"topic={topic}",
                f"bootstrap_servers={bootstrap}",
                f"operator_hook={hooks[0]}",
            ]
        ),
        data=structured_readiness_data(
            backend="kafka",
            component="dlq",
            name="Kafka DLQ readiness",
            status=Status.PASS.value,
            message=message,
            metrics={"topic": topic, "bootstrap_servers": bootstrap},
            findings=[],
            operator_hooks=hooks,
        ),
    )
