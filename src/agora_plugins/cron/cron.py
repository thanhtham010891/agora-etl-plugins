"""Cron helper functions for Agora scheduling."""

from __future__ import annotations

from typing import cast

try:
    import croniter as _croniter
except ImportError as _exc:
    raise ImportError(
        "agora_plugins.cron requires 'croniter'. "
        "Install with: pip install 'agora-etl-plugins[cron]'"
    ) from _exc


def validate_cron_expression(expression: str) -> None:
    if not _croniter.croniter.is_valid(expression):
        raise ValueError(f"Invalid cron expression: {expression!r}")


def seconds_until_next_run(expression: str, now: float) -> float:
    cron = _croniter.croniter(expression, now)
    next_run = cast("float", cron.get_next(float))
    return max(next_run - now, 0.0)


__all__ = ["seconds_until_next_run", "validate_cron_expression"]
