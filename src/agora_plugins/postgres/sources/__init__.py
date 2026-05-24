"""PostgreSQL sources exposed by the official Agora plugin package."""

__all__ = ["PostgresDLQSource", "PostgresSource"]


def __getattr__(name: str):
    if name == "PostgresDLQSource":
        from agora_plugins.postgres.dlq import PostgresDLQSource

        return PostgresDLQSource
    if name == "PostgresSource":
        from agora_plugins.postgres.sources.postgres import PostgresSource

        return PostgresSource
    raise AttributeError(name)
