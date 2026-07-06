"""S3 sinks exposed by the official Agora plugin package."""

from typing import Any

__all__ = ["S3Sink", "S3SinkMetricsSnapshot", "S3SinkUploadRuntime"]


def __getattr__(name: str) -> Any:
    if name in {"S3Sink", "S3SinkMetricsSnapshot", "S3SinkUploadRuntime"}:
        from agora_plugins.s3.sinks.s3 import S3Sink, S3SinkMetricsSnapshot, S3SinkUploadRuntime

        return {
            "S3Sink": S3Sink,
            "S3SinkMetricsSnapshot": S3SinkMetricsSnapshot,
            "S3SinkUploadRuntime": S3SinkUploadRuntime,
        }[name]
    raise AttributeError(name)
