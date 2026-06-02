"""PostgreSQL sources exposed by the official Agora plugin package."""

from typing import Any

__all__ = ["PostgresDLQSource", "PostgresSource"]


def __getattr__(name: str) -> Any:
    if name == "PostgresDLQSource":
        from agora_plugins.postgres.dlq import PostgresDLQSource

        return PostgresDLQSource
    if name == "PostgresSource":
        from agora_plugins.postgres.sources.postgres import PostgresSource

        return PostgresSource
    raise AttributeError(name)
