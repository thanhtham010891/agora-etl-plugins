"""Smoke-test the installed agora-etl-plugins wheel."""

from __future__ import annotations

import tomllib
from importlib import import_module, metadata
from pathlib import Path

EXPECTED_EXTRAS = {
    "all",
    "anthropic",
    "bigquery",
    "cron",
    "distributed",
    "kafka",
    "postgres",
    "redis",
    "s3",
}
EXPECTED_ENTRY_POINTS = {
    "agora.sources": {
        "bigquery",
        "redis_stream",
        "redis_dlq_source",
        "kafka",
        "kafka_dlq_source",
        "postgres",
        "postgres_dlq_source",
        "s3",
    },
    "agora.sinks": {
        "bigquery",
        "bigquery_storage_write",
        "redis",
        "redis_dlq",
        "kafka",
        "kafka_dlq",
        "postgres",
        "postgres_schema_adapter",
        "postgres_dlq",
        "s3",
    },
    "agora.ai.caches": {"redis"},
    "agora.ai.providers": {"anthropic"},
    "agora.middlewares.dedup.stores": {"redis", "redis_embedding"},
    "agora.state.backends": {"redis"},
}
EXPECTED_PUBLIC_IMPORTS = {
    "agora_plugins.bigquery": [
        "BigQuerySource",
        "BigQuerySink",
        "BigQueryStorageWriteSink",
    ],
    "agora_plugins.kafka": [
        "KafkaSource",
        "KafkaSink",
        "KafkaDLQSink",
        "KafkaDLQSource",
        "DLQPayloadPolicy",
    ],
    "agora_plugins.postgres": [
        "PostgresSource",
        "PostgresSink",
        "PostgresDLQSink",
        "PostgresDLQSource",
        "PostgresSchemaAdapter",
    ],
    "agora_plugins.redis": [
        "RedisStreamSource",
        "RedisSink",
        "RedisDLQSink",
        "RedisDLQSource",
        "RedisBackend",
    ],
    "agora_plugins.distributed": ["RedisWorkerCoordinator", "DistributedConfig"],
    "agora_plugins.anthropic": ["AnthropicProvider"],
    "agora_plugins.cron": [
        "missed_run_times",
        "seconds_until_next_run",
        "validate_cron_expression",
    ],
    "agora_plugins.s3": [
        "S3Source",
        "S3Sink",
    ],
}


def _repo_metadata() -> tuple[str, str]:
    pyproject = tomllib.loads((Path(__file__).resolve().parents[2] / "pyproject.toml").read_text())
    project = pyproject["project"]
    version = str(project["version"])
    core_requirement = next(
        dependency
        for dependency in project["dependencies"]
        if isinstance(dependency, str) and dependency.startswith("agora-etl>=")
    )
    core_floor = core_requirement.split(">=", 1)[1].split(",", 1)[0]
    return version, core_floor


def main() -> None:
    source_package_dir = Path(__file__).resolve().parents[2] / "src" / "agora_plugins"
    expected_version, expected_core_version = _repo_metadata()
    core_dist = metadata.distribution("agora-etl")
    core_version = core_dist.version
    if core_version != expected_core_version:
        raise SystemExit(f"Expected agora-etl {expected_core_version}, got {core_version}.")

    unexpected_core_entry_points = sorted(
        f"{entry_point.group}:{entry_point.name}"
        for entry_point in core_dist.entry_points
        if entry_point.group.startswith("agora.")
    )
    if unexpected_core_entry_points:
        raise SystemExit(
            "agora-etl must keep built-ins registered in core code instead of "
            "publishing public plugin entry points; found: "
            f"{unexpected_core_entry_points}"
        )

    dist = metadata.distribution("agora-etl-plugins")
    version = dist.version
    if version != expected_version:
        raise SystemExit(f"Expected agora-etl-plugins {expected_version}, got {version}.")

    extras = set(dist.metadata.get_all("Provides-Extra") or [])
    missing_extras = EXPECTED_EXTRAS - extras
    if missing_extras:
        raise SystemExit(f"Missing extras in installed metadata: {sorted(missing_extras)}")

    entry_points = metadata.entry_points()
    for group, expected_names in EXPECTED_ENTRY_POINTS.items():
        installed = {
            entry_point.name: entry_point for entry_point in entry_points.select(group=group)
        }
        missing = expected_names - set(installed)
        if missing:
            raise SystemExit(f"Missing entry points for {group}: {sorted(missing)}")
        for name in sorted(expected_names):
            installed[name].load()

    for module_name, names in EXPECTED_PUBLIC_IMPORTS.items():
        module = import_module(module_name)
        module_file = Path(module.__file__ or "").resolve()
        if module_file.is_relative_to(source_package_dir):
            raise SystemExit(
                f"{module_name} was imported from source tree instead of installed wheel: "
                f"{module_file}"
            )
        missing_names = [name for name in names if not hasattr(module, name)]
        if missing_names:
            raise SystemExit(f"{module_name} is missing exports: {missing_names}")

    print(f"Installed package smoke passed for agora-etl-plugins {version}.")


if __name__ == "__main__":
    main()
