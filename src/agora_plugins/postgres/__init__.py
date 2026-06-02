"""Official PostgreSQL plugin package for Agora."""

from typing import Any

from agora_plugins.postgres.config import PostgresConfig, PostgresPluginConfig
from agora_plugins.postgres.plugin import MANIFEST, PluginManifest

__all__ = [
    "MANIFEST",
    "PluginManifest",
    "PostgresConfig",
    "PostgresDLQSink",
    "PostgresDLQSource",
    "PostgresPluginConfig",
    "PostgresSchemaAdapter",
    "PostgresSink",
    "PostgresSource",
]


def __getattr__(name: str) -> Any:
    if name in {"PostgresSchemaAdapter", "PostgresSink"}:
        from agora_plugins.postgres.sinks.postgres import PostgresSchemaAdapter, PostgresSink

        return {
            "PostgresSchemaAdapter": PostgresSchemaAdapter,
            "PostgresSink": PostgresSink,
        }[name]
    if name == "PostgresDLQSink":
        from agora_plugins.postgres.dlq import PostgresDLQSink

        return PostgresDLQSink
    if name in {"PostgresDLQSource", "PostgresSource"}:
        from agora_plugins.postgres.dlq import PostgresDLQSource
        from agora_plugins.postgres.sources.postgres import PostgresSource

        return {
            "PostgresDLQSource": PostgresDLQSource,
            "PostgresSource": PostgresSource,
        }[name]
    raise AttributeError(name)
