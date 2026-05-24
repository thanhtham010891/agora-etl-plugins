from __future__ import annotations

from agora.runner import Schedule

from agora_plugins.cron import seconds_until_next_run, validate_cron_expression


def test_cron_helpers_validate_expressions() -> None:
    validate_cron_expression("0 9 * * 1-5")


def test_schedule_cron_uses_installed_plugin_module() -> None:
    schedule = Schedule.cron("0 9 * * 1-5")

    assert schedule._mode == "cron"
    assert seconds_until_next_run("0 9 * * 1-5", 0.0) >= 0.0
