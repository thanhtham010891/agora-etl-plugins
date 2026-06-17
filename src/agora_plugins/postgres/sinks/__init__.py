"""PostgreSQL sinks exposed by the official Agora plugin package."""

from typing import Any

__all__ = [
    "PostgresDLQSink",
    "PostgresPoisonRecordClassification",
    "PostgresPoisonRecordInfo",
    "PostgresSchemaAdapter",
    "PostgresSink",
    "PostgresSinkMetricsSnapshot",
    "PostgresSinkWriteError",
    "PostgresWriteSafetyPolicy",
    "QuotedIdentifier",
]


def __getattr__(name: str) -> Any:
    if name in {
        "PostgresPoisonRecordClassification",
        "PostgresPoisonRecordInfo",
        "PostgresSchemaAdapter",
        "PostgresSink",
        "PostgresSinkMetricsSnapshot",
        "PostgresSinkWriteError",
        "PostgresWriteSafetyPolicy",
        "QuotedIdentifier",
    }:
        from agora_plugins.postgres.sinks.postgres import (
            PostgresPoisonRecordClassification,
            PostgresPoisonRecordInfo,
            PostgresSchemaAdapter,
            PostgresSink,
            PostgresSinkMetricsSnapshot,
            PostgresSinkWriteError,
            PostgresWriteSafetyPolicy,
            QuotedIdentifier,
        )

        return {
            "PostgresPoisonRecordClassification": PostgresPoisonRecordClassification,
            "PostgresPoisonRecordInfo": PostgresPoisonRecordInfo,
            "PostgresSchemaAdapter": PostgresSchemaAdapter,
            "PostgresSink": PostgresSink,
            "PostgresSinkMetricsSnapshot": PostgresSinkMetricsSnapshot,
            "PostgresSinkWriteError": PostgresSinkWriteError,
            "PostgresWriteSafetyPolicy": PostgresWriteSafetyPolicy,
            "QuotedIdentifier": QuotedIdentifier,
        }[name]
    if name == "PostgresDLQSink":
        from agora_plugins.postgres.dlq import PostgresDLQSink

        return PostgresDLQSink
    raise AttributeError(name)
