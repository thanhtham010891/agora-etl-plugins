"""BigQuery sources exposed by the official Agora plugin package."""

from typing import Any

__all__ = ["BigQuerySource", "BigQuerySourceQueryRuntime", "BigQuerySourceStreamRuntime"]


def __getattr__(name: str) -> Any:
    if name == "BigQuerySource":
        from agora_plugins.bigquery.sources.bigquery import BigQuerySource

        return BigQuerySource
    if name == "BigQuerySourceQueryRuntime":
        from agora_plugins.bigquery.sources.query_runtime import BigQuerySourceQueryRuntime

        return BigQuerySourceQueryRuntime
    if name == "BigQuerySourceStreamRuntime":
        from agora_plugins.bigquery.sources.stream_runtime import BigQuerySourceStreamRuntime

        return BigQuerySourceStreamRuntime
    raise AttributeError(name)
