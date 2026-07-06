"""BigQuery sinks exposed by the official Agora plugin package."""

from typing import Any

__all__ = [
    "BigQuerySink",
    "BigQuerySinkFlushRuntime",
    "BigQuerySinkLoadJobRuntime",
    "BigQuerySinkMetricsSnapshot",
    "BigQuerySinkWriteError",
    "BigQueryStorageWriteFlushRuntime",
    "BigQueryStorageWriteRowSerializer",
    "BigQueryStorageWriteSession",
    "BigQueryStorageWriteSink",
    "BigQueryStorageWriteSinkError",
    "BigQueryStorageWriteSinkMetricsSnapshot",
    "BigQueryStorageWriteSinkOperatorSurface",
]


def __getattr__(name: str) -> Any:
    if name in {
        "BigQuerySink",
        "BigQuerySinkFlushRuntime",
        "BigQuerySinkLoadJobRuntime",
        "BigQuerySinkMetricsSnapshot",
        "BigQuerySinkWriteError",
    }:
        from agora_plugins.bigquery.sinks.bigquery import (
            BigQuerySink,
            BigQuerySinkFlushRuntime,
            BigQuerySinkLoadJobRuntime,
            BigQuerySinkMetricsSnapshot,
            BigQuerySinkWriteError,
        )

        return {
            "BigQuerySinkFlushRuntime": BigQuerySinkFlushRuntime,
            "BigQuerySinkLoadJobRuntime": BigQuerySinkLoadJobRuntime,
            "BigQuerySink": BigQuerySink,
            "BigQuerySinkMetricsSnapshot": BigQuerySinkMetricsSnapshot,
            "BigQuerySinkWriteError": BigQuerySinkWriteError,
        }[name]
    if name in {
        "BigQueryStorageWriteSink",
        "BigQueryStorageWriteSinkError",
        "BigQueryStorageWriteFlushRuntime",
        "BigQueryStorageWriteSinkOperatorSurface",
        "BigQueryStorageWriteRowSerializer",
        "BigQueryStorageWriteSession",
        "BigQueryStorageWriteSinkMetricsSnapshot",
    }:
        from agora_plugins.bigquery.sinks.storage_write import (
            BigQueryStorageWriteFlushRuntime,
            BigQueryStorageWriteRowSerializer,
            BigQueryStorageWriteSession,
            BigQueryStorageWriteSink,
            BigQueryStorageWriteSinkError,
            BigQueryStorageWriteSinkMetricsSnapshot,
            BigQueryStorageWriteSinkOperatorSurface,
        )

        return {
            "BigQueryStorageWriteFlushRuntime": BigQueryStorageWriteFlushRuntime,
            "BigQueryStorageWriteSinkOperatorSurface": BigQueryStorageWriteSinkOperatorSurface,
            "BigQueryStorageWriteRowSerializer": BigQueryStorageWriteRowSerializer,
            "BigQueryStorageWriteSink": BigQueryStorageWriteSink,
            "BigQueryStorageWriteSinkError": BigQueryStorageWriteSinkError,
            "BigQueryStorageWriteSession": BigQueryStorageWriteSession,
            "BigQueryStorageWriteSinkMetricsSnapshot": BigQueryStorageWriteSinkMetricsSnapshot,
        }[name]
    raise AttributeError(name)
