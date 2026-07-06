"""Official S3 plugin package for Agora."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora_plugins.s3.config import S3ConnectionConfig
    from agora_plugins.s3.observability import (
        S3SinkMetricsSnapshot,
        S3SourceMetricsSnapshot,
        S3SourceRecoveryContractSnapshot,
        S3SourceRecoveryMode,
    )
    from agora_plugins.s3.plugin import MANIFEST, PluginManifest
    from agora_plugins.s3.sinks import S3Sink, S3SinkUploadRuntime
    from agora_plugins.s3.sources import S3Source, S3SourceObjectRuntime

__all__ = [
    "MANIFEST",
    "PluginManifest",
    "S3ConnectionConfig",
    "S3Sink",
    "S3SinkMetricsSnapshot",
    "S3SinkUploadRuntime",
    "S3Source",
    "S3SourceMetricsSnapshot",
    "S3SourceObjectRuntime",
    "S3SourceRecoveryContractSnapshot",
    "S3SourceRecoveryMode",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "MANIFEST": ("agora_plugins.s3.plugin", "MANIFEST"),
    "PluginManifest": ("agora_plugins.s3.plugin", "PluginManifest"),
    "S3ConnectionConfig": ("agora_plugins.s3.config", "S3ConnectionConfig"),
    "S3Sink": ("agora_plugins.s3.sinks", "S3Sink"),
    "S3SinkUploadRuntime": ("agora_plugins.s3.sinks", "S3SinkUploadRuntime"),
    "S3SinkMetricsSnapshot": ("agora_plugins.s3.sinks", "S3SinkMetricsSnapshot"),
    "S3Source": ("agora_plugins.s3.sources", "S3Source"),
    "S3SourceObjectRuntime": ("agora_plugins.s3.sources", "S3SourceObjectRuntime"),
    "S3SourceMetricsSnapshot": ("agora_plugins.s3.observability", "S3SourceMetricsSnapshot"),
    "S3SourceRecoveryContractSnapshot": (
        "agora_plugins.s3.observability",
        "S3SourceRecoveryContractSnapshot",
    ),
    "S3SourceRecoveryMode": ("agora_plugins.s3.observability", "S3SourceRecoveryMode"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
