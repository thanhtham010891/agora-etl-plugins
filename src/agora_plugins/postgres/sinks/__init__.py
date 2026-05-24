"""PostgreSQL sinks exposed by the official Agora plugin package."""

__all__ = ["PostgresDLQSink", "PostgresSchemaAdapter", "PostgresSink"]


def __getattr__(name: str):
    if name in {"PostgresSchemaAdapter", "PostgresSink"}:
        from agora_plugins.postgres.sinks.postgres import PostgresSchemaAdapter, PostgresSink

        return {
            "PostgresSchemaAdapter": PostgresSchemaAdapter,
            "PostgresSink": PostgresSink,
        }[name]
    if name == "PostgresDLQSink":
        from agora_plugins.postgres.dlq import PostgresDLQSink

        return PostgresDLQSink
    raise AttributeError(name)
