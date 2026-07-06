"""S3 sources exposed by the official Agora plugin package."""

from typing import Any

__all__ = ["S3Source", "S3SourceObjectRuntime"]


def __getattr__(name: str) -> Any:
    if name == "S3Source":
        from agora_plugins.s3.sources.s3 import S3Source

        return S3Source
    if name == "S3SourceObjectRuntime":
        from agora_plugins.s3.sources.object_runtime import S3SourceObjectRuntime

        return S3SourceObjectRuntime
    raise AttributeError(name)
