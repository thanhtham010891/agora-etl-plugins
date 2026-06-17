from __future__ import annotations

from typing import Any


async def assert_runtime_readiness(
    runtime: Any,
    thresholds: Any,
) -> tuple[Any, Any]:
    health, snapshot, report = await runtime.ensure_ready(thresholds)
    assert snapshot.health == health
    return snapshot, report
