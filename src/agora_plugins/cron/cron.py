"""Cron helper functions for Agora scheduling."""

from __future__ import annotations

import warnings
from datetime import UTC, datetime
from typing import Literal, cast

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


CronTimezoneMode = Literal["wall-clock", "utc"]


def _cron_base(value: datetime, *, timezone_mode: CronTimezoneMode) -> datetime:
    if value.tzinfo is None:
        raise ValueError("cron helpers require timezone-aware datetimes.")
    if timezone_mode == "wall-clock":
        return value
    if timezone_mode == "utc":
        return value.astimezone(UTC)
    raise ValueError("timezone_mode must be 'wall-clock' or 'utc'.")


def _normalize_cron_result(value: datetime, *, base: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=base.tzinfo)
    return value


def seconds_until_next_run(
    expression: str,
    now: float | datetime,
    *,
    timezone_mode: CronTimezoneMode = "wall-clock",
) -> float:
    """Return seconds until the next cron fire time.

    ``timezone_mode="wall-clock"`` preserves croniter's local wall-clock
    semantics. ``timezone_mode="utc"`` evaluates the schedule in UTC, which is
    the recommended production mode when DST skip/double-run behavior is not
    desired.
    """

    if isinstance(now, datetime):
        base = _cron_base(now, timezone_mode=timezone_mode)
    else:
        base = datetime.fromtimestamp(now, UTC)

    validate_cron_expression(expression)
    cron = _croniter.croniter(expression, base)
    next_run = _normalize_cron_result(cast("datetime", cron.get_next(datetime)), base=base)
    return max((next_run - base).total_seconds(), 0.0)


def missed_run_times(
    expression: str,
    *,
    since: datetime,
    until: datetime,
    limit: int = 100,
    include_until: bool = True,
    timezone_mode: CronTimezoneMode = "wall-clock",
) -> list[datetime]:
    """Return scheduled run times after ``since`` and up to ``until``.

    ``timezone_mode="wall-clock"`` keeps local cron semantics. Use
    ``timezone_mode="utc"`` for production schedules that should not be affected
    by local DST transitions.
    """

    if limit <= 0:
        raise ValueError("limit must be greater than zero.")

    since = _cron_base(since, timezone_mode=timezone_mode)
    until = _cron_base(until, timezone_mode=timezone_mode)
    if until < since:
        raise ValueError("until must be greater than or equal to since.")
    validate_cron_expression(expression)
    cron = _croniter.croniter(expression, since)
    runs: list[datetime] = []
    while len(runs) < limit:
        run_at = _normalize_cron_result(cast("datetime", cron.get_next(datetime)), base=since)
        if run_at > until or (run_at == until and not include_until):
            break
        runs.append(run_at)
    if len(runs) == limit:
        next_run = _normalize_cron_result(cast("datetime", cron.get_next(datetime)), base=since)
        if next_run < until or (next_run == until and include_until):
            warnings.warn(
                "missed_run_times reached limit before covering the full catch-up window.",
                RuntimeWarning,
                stacklevel=2,
            )
    return runs


__all__ = [
    "CronTimezoneMode",
    "missed_run_times",
    "seconds_until_next_run",
    "validate_cron_expression",
]
