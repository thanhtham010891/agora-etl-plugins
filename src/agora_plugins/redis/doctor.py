"""Plugin-owned `agora doctor` readiness checks for Redis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agora.core.container import AgoraContainer
from agora.core.doctor import CheckResult, DoctorReadinessProvider, Status
from agora.core.metrics import has_metrics_snapshot

from agora_plugins._doctor_support import parse_key_value_lines, structured_readiness_data


@dataclass(frozen=True, slots=True)
class RedisDoctorReadinessProvider:
    backend: str = "redis"
    component_types: frozenset[str] = frozenset({"redis", "redis_dlq", "redis_stream"})

    async def run_readiness_checks(self, pipeline_config: dict[str, Any]) -> list[CheckResult]:
        container = AgoraContainer.from_config(pipeline_config)
        results: list[CheckResult] = []
        async with container:
            pipeline = container.build_pipeline()
            source_cfg = pipeline_config.get("source", {})
            source = getattr(pipeline, "_source", None)
            if _component_type(source_cfg) == "redis_stream":
                results.append(_check_redis_stream_source_readiness(source))

            writer = getattr(pipeline, "_writer", None)
            sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
            sink_cfgs = pipeline_config.get("sinks", [])
            if isinstance(sink_cfgs, list):
                for index, sink_cfg in enumerate(sink_cfgs):
                    if _component_type(sink_cfg) != "redis":
                        continue
                    sink = sink_instances[index] if index < len(sink_instances) else None
                    results.append(_check_redis_sink_readiness(sink, index=index + 1))

            dlq_cfg = pipeline_config.get("dlq")
            if (
                isinstance(dlq_cfg, dict)
                and dlq_cfg.get("enabled", True)
                and _component_type(dlq_cfg.get("sink")) == "redis_dlq"
            ):
                dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
                results.append(_check_redis_dlq_readiness(dlq_sink))

        return results


DOCTOR_READINESS_PROVIDER: DoctorReadinessProvider = RedisDoctorReadinessProvider()


def _component_type(config: object) -> str | None:
    if isinstance(config, dict):
        value = config.get("type")
        return value if isinstance(value, str) else None
    return None


def _check_redis_stream_source_readiness(source: object) -> CheckResult:
    if source is None:
        return CheckResult(
            name="Redis stream source readiness",
            status=Status.FAIL,
            message="Configured Redis stream source instance is missing",
            data=structured_readiness_data(
                backend="redis",
                component="source",
                name="Redis stream source readiness",
                status=Status.FAIL.value,
                message="Configured Redis stream source instance is missing",
                metrics={},
                findings=[
                    {
                        "metric": "source_instance",
                        "message": "Configured Redis stream source instance is missing",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Redis stream source plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    ready = getattr(source, "_client", None) is not None
    runtime_metrics = getattr(source, "runtime_metrics", lambda: None)()
    supports_checkpoint = bool(getattr(source, "supports_checkpoint", False))
    detail_lines = [
        f"stream={getattr(source, '_stream', 'unknown')}",
        f"group={getattr(source, '_group', 'unknown')}",
        f"consumer={getattr(source, '_consumer', 'unknown')}",
        f"supports_checkpoint={supports_checkpoint}",
        f"connection_ready={ready}",
    ]
    status = Status.PASS if ready and supports_checkpoint else Status.FAIL
    message = (
        "Redis stream source passed enterprise readiness checks"
        if status == Status.PASS
        else "Redis stream source failed enterprise readiness checks"
    )
    hooks: list[str] = []
    if not ready:
        hooks.append("Verify Redis URL, ACLs, and stream/group existence before cutover.")
    if not supports_checkpoint:
        hooks.append(
            "Redis stream recovery expects checkpointable message IDs before enterprise cutover."
        )
    if getattr(runtime_metrics, "record_error_count", 0) > 0:
        status = Status.FAIL
        message = "Redis stream source has source-level record errors"
        hooks.append("Inspect deserializer failures or reclaim loops before restarting consumers.")
    rendered_hooks = list(dict.fromkeys(hooks))
    detail_lines.extend(f"operator_hook={hook}" for hook in rendered_hooks)
    return CheckResult(
        name="Redis stream source readiness",
        status=status,
        message=message,
        detail="\n".join(detail_lines),
        data=structured_readiness_data(
            backend="redis",
            component="source",
            name="Redis stream source readiness",
            status=status.value,
            message=message,
            metrics=parse_key_value_lines(
                [line for line in detail_lines if not line.startswith("operator_hook=")]
            ),
            findings=[],
            operator_hooks=rendered_hooks,
        ),
    )


def _check_redis_sink_readiness(sink: object, *, index: int) -> CheckResult:
    if sink is None or not has_metrics_snapshot(sink):
        return CheckResult(
            name=f"Redis sink readiness #{index}",
            status=Status.FAIL,
            message="Configured Redis sink could not expose readiness metrics",
            detail="Expected a live Redis sink instance with metrics_snapshot().",
            data=structured_readiness_data(
                backend="redis",
                component="sink",
                name=f"Redis sink readiness #{index}",
                status=Status.FAIL.value,
                message="Configured Redis sink could not expose readiness metrics",
                metrics={},
                findings=[
                    {
                        "metric": "metrics_snapshot",
                        "message": "Expected a live Redis sink instance with metrics_snapshot().",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Redis sink plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    snapshot = sink.metrics_snapshot()
    ready = bool(getattr(snapshot, "connection_ready", False))
    message = (
        f"Redis sink {snapshot.target!r} passed enterprise readiness checks"
        if ready
        else f"Redis sink {snapshot.target!r} failed enterprise readiness checks"
    )
    hooks = [
        "Verify Redis memory policy, TTL, and write mode semantics before production cutover."
        if ready
        else "Verify Redis URL, ACLs, and target database reachability before cutover."
    ]
    status = Status.PASS if ready else Status.FAIL
    return CheckResult(
        name=f"Redis sink readiness #{index}",
        status=status,
        message=message,
        detail="\n".join(
            [
                f"target={snapshot.target}",
                f"mode={snapshot.mode}",
                f"connection_ready={snapshot.connection_ready}",
                f"operator_hook={hooks[0]}",
            ]
        ),
        data=structured_readiness_data(
            backend="redis",
            component="sink",
            name=f"Redis sink readiness #{index}",
            status=status.value,
            message=message,
            metrics={
                "target": snapshot.target,
                "mode": snapshot.mode,
                "connection_ready": snapshot.connection_ready,
            },
            findings=[],
            operator_hooks=hooks,
        ),
    )


def _check_redis_dlq_readiness(dlq_sink: object) -> CheckResult:
    if dlq_sink is None:
        return CheckResult(
            name="Redis DLQ readiness",
            status=Status.FAIL,
            message="Configured Redis DLQ instance is missing",
            data=structured_readiness_data(
                backend="redis",
                component="dlq",
                name="Redis DLQ readiness",
                status=Status.FAIL.value,
                message="Configured Redis DLQ instance is missing",
                metrics={},
                findings=[
                    {
                        "metric": "dlq_instance",
                        "message": "Configured Redis DLQ instance is missing",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Redis DLQ plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    ready = getattr(dlq_sink, "_client", None) is not None
    key_prefix = getattr(dlq_sink, "_key_prefix", "agora:dlq")
    message = (
        f"Redis DLQ {key_prefix!r} passed enterprise readiness checks"
        if ready
        else f"Redis DLQ {key_prefix!r} failed enterprise readiness checks"
    )
    hooks = [
        "Validate DLQ key retention and replay cleanup rules before relying on Redis poison isolation."
        if ready
        else "Verify Redis DLQ connectivity and ACLs before enabling replay workflows."
    ]
    status = Status.PASS if ready else Status.FAIL
    return CheckResult(
        name="Redis DLQ readiness",
        status=status,
        message=message,
        detail="\n".join(
            [
                f"key_prefix={key_prefix}",
                f"connection_ready={ready}",
                f"operator_hook={hooks[0]}",
            ]
        ),
        data=structured_readiness_data(
            backend="redis",
            component="dlq",
            name="Redis DLQ readiness",
            status=status.value,
            message=message,
            metrics={
                "key_prefix": key_prefix,
                "connection_ready": ready,
            },
            findings=[],
            operator_hooks=hooks,
        ),
    )
