from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest
from agora.runner import Schedule

from agora_plugins.cron import missed_run_times, seconds_until_next_run, validate_cron_expression


def test_cron_helpers_validate_expressions() -> None:
    validate_cron_expression("0 9 * * 1-5")


def test_schedule_cron_uses_installed_plugin_module() -> None:
    schedule = Schedule.cron("0 9 * * 1-5")

    assert schedule._mode == "cron"
    assert seconds_until_next_run("0 9 * * 1-5", 0.0) >= 0.0


def test_seconds_until_next_run_accepts_timezone_aware_datetime() -> None:
    now = datetime(2026, 6, 22, 8, 30, tzinfo=UTC)

    assert seconds_until_next_run("0 9 * * *", now) == 30 * 60


def test_seconds_until_next_run_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        seconds_until_next_run("0 9 * * *", datetime(2026, 6, 22, 8, 30))


def test_seconds_until_next_run_rejects_invalid_expression_with_value_error() -> None:
    with pytest.raises(ValueError, match="Invalid cron expression"):
        seconds_until_next_run("not a cron", datetime(2026, 6, 22, 8, 30, tzinfo=UTC))


def test_missed_run_times_returns_bounded_catch_up_window() -> None:
    since = datetime(2026, 6, 22, 8, 0, tzinfo=UTC)
    until = datetime(2026, 6, 22, 11, 0, tzinfo=UTC)

    assert missed_run_times("0 * * * *", since=since, until=until) == [
        datetime(2026, 6, 22, 9, 0, tzinfo=UTC),
        datetime(2026, 6, 22, 10, 0, tzinfo=UTC),
        datetime(2026, 6, 22, 11, 0, tzinfo=UTC),
    ]
    with pytest.warns(RuntimeWarning, match="reached limit"):
        assert missed_run_times("0 * * * *", since=since, until=until, limit=2) == [
            datetime(2026, 6, 22, 9, 0, tzinfo=UTC),
            datetime(2026, 6, 22, 10, 0, tzinfo=UTC),
        ]


def test_missed_run_times_rejects_naive_datetimes() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        missed_run_times(
            "0 * * * *",
            since=datetime(2026, 6, 22, 8, 0),
            until=datetime(2026, 6, 22, 9, 0, tzinfo=UTC),
        )


def test_cron_helpers_can_normalize_schedule_to_utc_across_dst_window() -> None:
    eastern = ZoneInfo("America/New_York")
    since = datetime(2026, 3, 8, 1, 30, tzinfo=eastern)
    until = datetime(2026, 3, 8, 4, 30, tzinfo=eastern)

    assert missed_run_times(
        "0 * * * *",
        since=since,
        until=until,
        timezone_mode="utc",
    ) == [
        datetime(2026, 3, 8, 7, 0, tzinfo=UTC),
        datetime(2026, 3, 8, 8, 0, tzinfo=UTC),
    ]
