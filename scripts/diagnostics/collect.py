#!/usr/bin/env python3
"""Collect local diagnostics for integration failures."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from shlex import quote

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACT_DIR = REPO_ROOT / ".artifacts" / "integration-diagnostics"
COMPOSE_FILES = (
    "docker-compose.yaml",
    "docker-compose.kafka-cluster.yaml",
    "docker-compose.kafka-secure.yaml",
    "docker-compose.postgres-ha.yaml",
    "docker-compose.redis-cluster.yaml",
    "docker-compose.redis-redlock.yaml",
    "docker-compose.redis-secure.yaml",
    "docker-compose.redis-sentinel.yaml",
    "docker-compose.redis-stack.yaml",
    "docker-compose.s3-minio.yaml",
)


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bool_env(name: str) -> str:
    return "true" if os.getenv(name) else "false"


def _append_command_output(output_file: Path, command: list[str]) -> None:
    rendered = " ".join(quote(part) for part in command)
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    with output_file.open("a", encoding="utf-8") as handle:
        handle.write(f"$ {rendered}\n")
        if result.stdout:
            handle.write(result.stdout)
        if result.stderr:
            handle.write(result.stderr)
        handle.write("\n")


def _compose_name(compose_file: str) -> str:
    if compose_file == "docker-compose.yaml":
        return "base"
    return compose_file.removeprefix("docker-compose.").removesuffix(".yaml")


def collect(artifact_dir: Path) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir = artifact_dir.resolve()

    runner_file = artifact_dir / "runner.txt"
    docker_file = artifact_dir / "docker.txt"
    summary_file = artifact_dir / "summary.txt"

    summary_lines = [
        f"collected_at_utc={_timestamp()}",
        f"repo_root={REPO_ROOT}",
        f"pwd={REPO_ROOT}",
        f"path={os.environ.get('PATH', '')}",
        f"venv_present={'true' if (REPO_ROOT / '.venv/bin/python').exists() else 'false'}",
        f"bigquery_project_set={_bool_env('INTEGRATION_BIGQUERY_PROJECT')}",
        f"bigquery_dataset_set={_bool_env('INTEGRATION_BIGQUERY_DATASET')}",
        f"bigquery_credentials_path_set={_bool_env('INTEGRATION_BIGQUERY_CREDENTIALS_PATH')}",
        f"google_application_credentials_set={_bool_env('GOOGLE_APPLICATION_CREDENTIALS')}",
    ]
    summary_file.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    _append_command_output(runner_file, ["uname", "-a"])
    _append_command_output(runner_file, ["date", "-u"])

    system_python = shutil.which("python")
    if system_python:
        _append_command_output(runner_file, [system_python, "--version"])

    venv_python = REPO_ROOT / ".venv/bin/python"
    if venv_python.exists():
        _append_command_output(runner_file, [str(venv_python), "--version"])

    venv_pip = REPO_ROOT / ".venv/bin/pip"
    if venv_pip.exists():
        _append_command_output(runner_file, [str(venv_pip), "freeze"])

    lastfailed = REPO_ROOT / ".pytest_cache/v/cache/lastfailed"
    if lastfailed.exists():
        shutil.copy2(lastfailed, artifact_dir / "pytest-lastfailed.json")

    if not shutil.which("docker"):
        with summary_file.open("a", encoding="utf-8") as handle:
            handle.write("docker_available=false\n")
        return

    with summary_file.open("a", encoding="utf-8") as handle:
        handle.write("docker_available=true\n")

    for command in (
        ["docker", "version"],
        ["docker", "info"],
        ["docker", "ps", "-a"],
        ["docker", "network", "ls"],
        ["docker", "volume", "ls"],
    ):
        _append_command_output(docker_file, command)

    for compose_file in COMPOSE_FILES:
        if not (REPO_ROOT / compose_file).exists():
            continue
        output_file = artifact_dir / f"compose-{_compose_name(compose_file)}.txt"
        _append_command_output(output_file, ["docker", "compose", "-f", compose_file, "ps", "-a"])
        _append_command_output(
            output_file,
            ["docker", "compose", "-f", compose_file, "logs", "--no-color", "--timestamps"],
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "artifact_dir",
        nargs="?",
        default=os.environ.get("DIAGNOSTICS_DIR", str(DEFAULT_ARTIFACT_DIR)),
        help="Directory where diagnostics artifacts will be written.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    collect(Path(args.artifact_dir).expanduser())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
