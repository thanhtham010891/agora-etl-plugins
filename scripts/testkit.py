#!/usr/bin/env python3
"""Declarative test automation runner for agora-etl-plugins."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "qa" / "testkit.toml"


class TestkitError(RuntimeError):
    """Raised when manifest-driven test orchestration cannot continue."""


@dataclass(frozen=True)
class Topology:
    name: str
    description: str
    up: tuple[tuple[str, ...], ...]
    down: tuple[tuple[str, ...], ...]
    status: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Suite:
    name: str
    description: str
    commands: tuple[tuple[str, ...], ...]
    env_group: str | None
    topologies: tuple[str, ...]
    topology_policy: str
    required_env: tuple[str, ...]


@dataclass(frozen=True)
class Gate:
    name: str
    description: str
    suites: tuple[str, ...]


@dataclass(frozen=True)
class MatrixLane:
    lane_id: str
    label: str
    kind: str
    target: str
    skip_if_missing_env: tuple[str, ...]


@dataclass(frozen=True)
class Matrix:
    name: str
    description: str
    lanes: tuple[str, ...]


@dataclass(frozen=True)
class Manifest:
    default_env: dict[str, str]
    default_artifacts_dir: str
    env_groups: dict[str, dict[str, str]]
    topologies: dict[str, Topology]
    suites: dict[str, Suite]
    gates: dict[str, Gate]
    matrices: dict[str, Matrix]
    matrix_lanes: dict[str, dict[str, MatrixLane]]


class SafeFormatDict(dict[str, str]):
    def __missing__(self, key: str) -> str:
        raise TestkitError(f"Missing environment value {key!r} required by manifest template.")


def _command_context() -> dict[str, str]:
    return {
        "python": sys.executable,
    }


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    print(f"[{_timestamp()}] {message}", flush=True)


def load_manifest() -> Manifest:
    data = tomllib.loads(MANIFEST_PATH.read_text())
    defaults = data.get("defaults", {})
    default_env = {key: str(value) for key, value in defaults.get("env", {}).items()}
    env_groups = {
        group_name: {key: str(value) for key, value in values.items()}
        for group_name, values in data.get("env_groups", {}).items()
    }
    topologies = {
        name: Topology(
            name=name,
            description=str(values.get("description", "")),
            up=_command_list(values.get("up", [])),
            down=_command_list(values.get("down", [])),
            status=_command_list(values.get("status", [])),
        )
        for name, values in data.get("topologies", {}).items()
    }
    suites = {
        name: Suite(
            name=name,
            description=str(values.get("description", "")),
            commands=_command_list(values.get("commands", [])),
            env_group=_optional_str(values.get("env_group")),
            topologies=tuple(str(item) for item in values.get("topologies", [])),
            topology_policy=str(values.get("topology_policy", "reuse")),
            required_env=tuple(str(item) for item in values.get("required_env", [])),
        )
        for name, values in data.get("suites", {}).items()
    }
    gates = {
        name: Gate(
            name=name,
            description=str(values.get("description", "")),
            suites=tuple(str(item) for item in values.get("suites", [])),
        )
        for name, values in data.get("gates", {}).items()
    }
    matrices = {
        name: Matrix(
            name=name,
            description=str(values.get("description", "")),
            lanes=tuple(str(item) for item in values.get("lanes", [])),
        )
        for name, values in data.get("matrices", {}).items()
    }
    matrix_lanes = {
        matrix_name: {
            lane_id: MatrixLane(
                lane_id=lane_id,
                label=str(values.get("label", lane_id)),
                kind=str(values.get("kind", "")),
                target=str(values.get("target", "")),
                skip_if_missing_env=tuple(
                    str(item) for item in values.get("skip_if_missing_env", [])
                ),
            )
            for lane_id, values in lane_values.items()
        }
        for matrix_name, lane_values in data.get("matrix_lanes", {}).items()
    }
    return Manifest(
        default_env=default_env,
        default_artifacts_dir=str(defaults.get("artifacts_dir", ".artifacts/testkit")),
        env_groups=env_groups,
        topologies=topologies,
        suites=suites,
        gates=gates,
        matrices=matrices,
        matrix_lanes=matrix_lanes,
    )


def _command_list(raw: Iterable[Iterable[Any]]) -> tuple[tuple[str, ...], ...]:
    return tuple(tuple(str(part) for part in command) for command in raw)


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def build_base_env(manifest: Manifest) -> dict[str, str]:
    merged = dict(os.environ)
    for key, value in manifest.default_env.items():
        merged.setdefault(key, value)
    merged.setdefault(
        "INTEGRATION_BIGQUERY_CREDENTIALS_PATH", merged.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    )
    return merged


def resolve_env_group(manifest: Manifest, env_group: str | None) -> dict[str, str]:
    env = build_base_env(manifest)
    if env_group is None:
        return env
    try:
        values = manifest.env_groups[env_group]
    except KeyError as exc:
        raise TestkitError(f"Unknown env group {env_group!r}.") from exc
    formatter = SafeFormatDict(env)
    for key, template in values.items():
        env[key] = template.format_map(formatter)
        formatter[key] = env[key]
    return env


def ensure_required_env(env: dict[str, str], keys: Sequence[str]) -> None:
    missing = [key for key in keys if not env.get(key)]
    if missing:
        joined = ", ".join(missing)
        raise TestkitError(f"Missing required environment value(s): {joined}.")


def run_command(
    command: Sequence[str],
    *,
    env: dict[str, str],
    dry_run: bool,
) -> None:
    formatter = SafeFormatDict(_command_context())
    resolved_command = tuple(part.format_map(formatter) for part in command)
    rendered = " ".join(resolved_command)
    log(f"RUN {rendered}")
    if dry_run:
        return
    subprocess.run(resolved_command, cwd=REPO_ROOT, env=env, check=True)


def run_topology_action(
    manifest: Manifest,
    topology_name: str,
    action: str,
    *,
    dry_run: bool,
) -> None:
    try:
        topology = manifest.topologies[topology_name]
    except KeyError as exc:
        raise TestkitError(f"Unknown topology {topology_name!r}.") from exc
    commands = getattr(topology, action)
    env = build_base_env(manifest)
    for command in commands:
        run_command(command, env=env, dry_run=dry_run)


def run_suite(
    manifest: Manifest,
    suite_name: str,
    *,
    dry_run: bool,
    ensure_topologies_enabled: bool = True,
    cleanup_topologies: bool = True,
) -> None:
    try:
        suite = manifest.suites[suite_name]
    except KeyError as exc:
        raise TestkitError(f"Unknown suite {suite_name!r}.") from exc
    env = resolve_env_group(manifest, suite.env_group)
    ensure_required_env(env, suite.required_env)
    if ensure_topologies_enabled:
        if suite.topology_policy == "recreate":
            for topology_name in reversed(suite.topologies):
                run_topology_action(manifest, topology_name, "down", dry_run=dry_run)
        for topology_name in suite.topologies:
            run_topology_action(manifest, topology_name, "up", dry_run=dry_run)
    try:
        for command in suite.commands:
            run_command(command, env=env, dry_run=dry_run)
    finally:
        if ensure_topologies_enabled and cleanup_topologies:
            for topology_name in reversed(suite.topologies):
                run_topology_action(manifest, topology_name, "down", dry_run=dry_run)


def run_gate(manifest: Manifest, gate_name: str, *, dry_run: bool) -> None:
    try:
        gate = manifest.gates[gate_name]
    except KeyError as exc:
        raise TestkitError(f"Unknown gate {gate_name!r}.") from exc
    for suite_name in gate.suites:
        log(f"BEGIN gate={gate.name} suite={suite_name}")
        run_suite(manifest, suite_name, dry_run=dry_run)
        log(f"PASS gate={gate.name} suite={suite_name}")


def collect_diagnostics(manifest: Manifest, destination: Path, *, dry_run: bool) -> None:
    env = build_base_env(manifest)
    destination.parent.mkdir(parents=True, exist_ok=True)
    run_command(
        [sys.executable, "scripts/diagnostics/collect.py", str(destination)],
        env=env,
        dry_run=dry_run,
    )


def run_matrix(
    manifest: Manifest,
    matrix_name: str,
    *,
    selected_lanes: set[str] | None,
    artifacts_dir: Path,
    dry_run: bool,
) -> None:
    try:
        matrix = manifest.matrices[matrix_name]
        lane_map = manifest.matrix_lanes[matrix_name]
    except KeyError as exc:
        raise TestkitError(f"Unknown matrix {matrix_name!r}.") from exc
    log_file = artifacts_dir / f"{matrix_name}.log"
    status_file = artifacts_dir / "status.txt"
    if not dry_run:
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        dispatch_file = artifacts_dir / "dispatch-config.txt"
        with dispatch_file.open("w", encoding="utf-8") as handle:
            handle.write(f"matrix={matrix_name}\n")
            handle.write(
                f"selected_lanes={','.join(sorted(selected_lanes)) if selected_lanes else 'full'}\n"
            )
    exit_code = 0
    try:
        if not dry_run:
            with log_file.open("a", encoding="utf-8") as handle:
                handle.write(f"[{_timestamp()}] BEGIN matrix={matrix_name}\n")
        for lane_id in matrix.lanes:
            if selected_lanes is not None and lane_id not in selected_lanes:
                log(f"SKIP {lane_id} (not selected)")
                continue
            lane = lane_map[lane_id]
            env = build_base_env(manifest)
            missing = [key for key in lane.skip_if_missing_env if not env.get(key)]
            if missing:
                log(f"SKIP {lane.label} (missing env: {', '.join(missing)})")
                continue
            log(f"BEGIN {lane.label}")
            if lane.kind == "suite":
                run_suite(manifest, lane.target, dry_run=dry_run)
            elif lane.kind == "gate":
                run_gate(manifest, lane.target, dry_run=dry_run)
            else:
                raise TestkitError(
                    f"Unsupported matrix lane kind {lane.kind!r} for lane {lane.lane_id!r}."
                )
            log(f"PASS {lane.label}")
    except Exception:
        exit_code = 1
        raise
    finally:
        if exit_code != 0:
            if not dry_run:
                log(
                    f"FAIL matrix={matrix_name}; collecting diagnostics into {artifacts_dir / 'diagnostics'}"
                )
                collect_diagnostics(manifest, artifacts_dir / "diagnostics", dry_run=False)
                status_file.write_text("failure\n", encoding="utf-8")
        else:
            if not dry_run:
                status_file.write_text("success\n", encoding="utf-8")
            log(f"PASS matrix={matrix_name}")


CatalogEntry = Topology | Suite | Gate | Matrix


def _print_catalog_section(heading: str, entries: Mapping[str, CatalogEntry]) -> None:
    print(f"{heading}:")
    for name, entry in sorted(entries.items()):
        description = entry.description
        if isinstance(entry, Matrix):
            description = f"{description} (lanes: {', '.join(entry.lanes)})"
        print(f"  {name:<32} {description}")
    print()


def print_catalog(manifest: Manifest) -> None:
    """Print the supported declarative names without requiring TOML knowledge."""

    _print_catalog_section("Topologies", manifest.topologies)
    _print_catalog_section("Suites", manifest.suites)
    _print_catalog_section("Gates", manifest.gates)
    _print_catalog_section("Matrices", manifest.matrices)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="Print commands without executing them."
    )
    subparsers = parser.add_subparsers(dest="resource", required=True)

    topology_parser = subparsers.add_parser("topology", help="Manage a named topology.")
    topology_subparsers = topology_parser.add_subparsers(dest="action", required=True)
    for action in ("up", "down", "status"):
        parser_for_action = topology_subparsers.add_parser(action)
        parser_for_action.add_argument("name")

    suite_parser = subparsers.add_parser("suite", help="Run a named suite.")
    suite_subparsers = suite_parser.add_subparsers(dest="action", required=True)
    suite_run = suite_subparsers.add_parser("run")
    suite_run.add_argument("name")
    suite_run.add_argument(
        "--no-topology",
        action="store_true",
        help="Run the suite without managing declared topologies.",
    )
    suite_run.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Leave managed topologies running after the suite finishes.",
    )

    gate_parser = subparsers.add_parser("gate", help="Run a named gate.")
    gate_subparsers = gate_parser.add_subparsers(dest="action", required=True)
    gate_run = gate_subparsers.add_parser("run")
    gate_run.add_argument("name")

    matrix_parser = subparsers.add_parser("matrix", help="Run a named matrix.")
    matrix_subparsers = matrix_parser.add_subparsers(dest="action", required=True)
    matrix_run = matrix_subparsers.add_parser("run")
    matrix_run.add_argument("name")
    matrix_run.add_argument(
        "--lanes",
        default="",
        help="Comma-separated lane ids. Leave empty to run the full matrix.",
    )
    matrix_run.add_argument(
        "--artifacts-dir",
        default="",
        help="Directory for matrix logs, status, and diagnostics.",
    )

    subparsers.add_parser("catalog", help="List supported topology, suite, gate, and matrix names.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    manifest = load_manifest()
    try:
        if args.resource == "catalog":
            print_catalog(manifest)
            return 0
        if args.resource == "topology":
            action = "status" if args.action == "status" else args.action
            run_topology_action(manifest, args.name, action, dry_run=args.dry_run)
            return 0
        if args.resource == "suite":
            run_suite(
                manifest,
                args.name,
                dry_run=args.dry_run,
                ensure_topologies_enabled=not args.no_topology,
                cleanup_topologies=not args.no_cleanup,
            )
            return 0
        if args.resource == "gate":
            run_gate(manifest, args.name, dry_run=args.dry_run)
            return 0
        if args.resource == "matrix":
            selected_lanes = {
                lane.strip() for lane in args.lanes.split(",") if lane.strip()
            } or None
            artifacts_dir = (
                Path(args.artifacts_dir)
                if args.artifacts_dir
                else REPO_ROOT / manifest.default_artifacts_dir / args.name
            )
            run_matrix(
                manifest,
                args.name,
                selected_lanes=selected_lanes,
                artifacts_dir=artifacts_dir,
                dry_run=args.dry_run,
            )
            return 0
        raise TestkitError(f"Unsupported resource {args.resource!r}.")
    except TestkitError as exc:
        print(f"testkit error: {exc}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as exc:
        print(
            f"testkit command failed with exit code {exc.returncode}: {' '.join(exc.cmd)}",
            file=sys.stderr,
        )
        return exc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
