"""Shared helpers for plugin-owned `agora doctor` readiness providers."""

from __future__ import annotations

from typing import Any


async def call_async_with_optional_kwargs(
    instance: object,
    method_name: str,
    **kwargs: Any,
) -> Any:
    method = getattr(instance, method_name)
    try:
        return await method(**kwargs)
    except TypeError:
        return await method()


def structured_readiness_data(
    *,
    backend: str,
    component: str,
    name: str,
    status: str,
    message: str,
    metrics: dict[str, Any],
    findings: list[dict[str, Any]],
    operator_hooks: list[str],
) -> dict[str, Any]:
    return {
        "category": "enterprise_readiness",
        "backend": backend,
        "component": component,
        "name": name,
        "status": status,
        "message": message,
        "metrics": metrics,
        "findings": findings,
        "operator_hooks": operator_hooks,
    }


def parse_key_value_lines(lines: list[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for line in lines:
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        metrics[key] = coerce_scalar(raw_value)
    return metrics


def coerce_scalar(value: str) -> Any:
    normalized = value.strip()
    if normalized.lower() in {"true", "false"}:
        return normalized.lower() == "true"
    try:
        return int(normalized)
    except ValueError:
        pass
    return normalized
