"""Official BigQuery plugin package for Agora."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from agora_plugins._surface_manifest import SurfaceExport, export_target_map

if TYPE_CHECKING:
    from agora_plugins.bigquery.config import BigQueryConnectionConfig
    from agora_plugins.bigquery.observability import (
        BigQueryEnterpriseAcceptanceFinding,
        BigQueryEnterpriseAcceptanceGate,
        BigQueryEnterpriseAcceptanceReport,
        BigQuerySinkAcceptanceEvaluator,
        BigQuerySinkEnterpriseAcceptanceThresholds,
        BigQuerySinkHealthSnapshot,
        BigQuerySinkMetricsSnapshot,
        BigQuerySourceAcceptanceEvaluator,
        BigQuerySourceEnterpriseAcceptanceThresholds,
        BigQuerySourceHealthSnapshot,
        BigQuerySourceMetricsSnapshot,
        BigQuerySourceRecoveryContractSnapshot,
        BigQuerySourceRecoveryMode,
        BigQueryStorageWriteSinkAcceptanceEvaluator,
        BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds,
        BigQueryStorageWriteSinkHealthSnapshot,
        BigQueryStorageWriteSinkMetricsSnapshot,
    )
    from agora_plugins.bigquery.plugin import MANIFEST, PluginManifest
    from agora_plugins.bigquery.sinks import (
        BigQuerySink,
        BigQuerySinkFlushRuntime,
        BigQuerySinkLoadJobRuntime,
        BigQuerySinkWriteError,
        BigQueryStorageWriteFlushRuntime,
        BigQueryStorageWriteRowSerializer,
        BigQueryStorageWriteSession,
        BigQueryStorageWriteSink,
        BigQueryStorageWriteSinkError,
        BigQueryStorageWriteSinkOperatorSurface,
    )
    from agora_plugins.bigquery.sources import (
        BigQuerySource,
        BigQuerySourceQueryRuntime,
        BigQuerySourceStreamRuntime,
    )

__all__ = [
    "MANIFEST",
    "BigQueryConnectionConfig",
    "BigQueryEnterpriseAcceptanceFinding",
    "BigQueryEnterpriseAcceptanceGate",
    "BigQueryEnterpriseAcceptanceReport",
    "BigQuerySink",
    "BigQuerySinkAcceptanceEvaluator",
    "BigQuerySinkEnterpriseAcceptanceThresholds",
    "BigQuerySinkFlushRuntime",
    "BigQuerySinkHealthSnapshot",
    "BigQuerySinkLoadJobRuntime",
    "BigQuerySinkMetricsSnapshot",
    "BigQuerySinkWriteError",
    "BigQuerySource",
    "BigQuerySourceAcceptanceEvaluator",
    "BigQuerySourceEnterpriseAcceptanceThresholds",
    "BigQuerySourceHealthSnapshot",
    "BigQuerySourceMetricsSnapshot",
    "BigQuerySourceQueryRuntime",
    "BigQuerySourceRecoveryContractSnapshot",
    "BigQuerySourceRecoveryMode",
    "BigQuerySourceStreamRuntime",
    "BigQueryStorageWriteFlushRuntime",
    "BigQueryStorageWriteRowSerializer",
    "BigQueryStorageWriteSession",
    "BigQueryStorageWriteSink",
    "BigQueryStorageWriteSinkAcceptanceEvaluator",
    "BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds",
    "BigQueryStorageWriteSinkError",
    "BigQueryStorageWriteSinkHealthSnapshot",
    "BigQueryStorageWriteSinkMetricsSnapshot",
    "BigQueryStorageWriteSinkOperatorSurface",
    "PluginManifest",
]

_STABLE_PUBLIC_EXPORTS = frozenset(
    {
        "MANIFEST",
        "PluginManifest",
        "BigQueryConnectionConfig",
        "BigQuerySource",
        "BigQuerySink",
        "BigQueryStorageWriteSink",
    }
)

_SUPPORTABILITY_PUBLIC_EXPORTS = frozenset(
    {
        "BigQueryEnterpriseAcceptanceFinding",
        "BigQueryEnterpriseAcceptanceGate",
        "BigQueryEnterpriseAcceptanceReport",
        "BigQuerySinkAcceptanceEvaluator",
        "BigQuerySinkEnterpriseAcceptanceThresholds",
        "BigQuerySinkHealthSnapshot",
        "BigQuerySinkMetricsSnapshot",
        "BigQuerySinkWriteError",
        "BigQuerySourceAcceptanceEvaluator",
        "BigQuerySourceEnterpriseAcceptanceThresholds",
        "BigQuerySourceHealthSnapshot",
        "BigQuerySourceMetricsSnapshot",
        "BigQuerySourceRecoveryContractSnapshot",
        "BigQuerySourceRecoveryMode",
        "BigQueryStorageWriteSinkAcceptanceEvaluator",
        "BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds",
        "BigQueryStorageWriteSinkError",
        "BigQueryStorageWriteSinkHealthSnapshot",
        "BigQueryStorageWriteSinkMetricsSnapshot",
    }
)

_PATTERN_RECIPE_EXPORTS = frozenset(
    {
        "BigQuerySinkFlushRuntime",
        "BigQuerySinkLoadJobRuntime",
        "BigQuerySourceQueryRuntime",
        "BigQuerySourceStreamRuntime",
        "BigQueryStorageWriteFlushRuntime",
        "BigQueryStorageWriteRowSerializer",
        "BigQueryStorageWriteSinkOperatorSurface",
        "BigQueryStorageWriteSession",
    }
)


def _surface_note(name: str) -> str:
    if name in _STABLE_PUBLIC_EXPORTS:
        return "Stable BigQuery family primitive/config public surface."
    if name in _SUPPORTABILITY_PUBLIC_EXPORTS:
        return "BigQuery supportability, recovery, or observability public surface."
    return "BigQuery advanced runtime or recipe-oriented helper surface."


_SURFACE_EXPORTS: dict[str, SurfaceExport] = {
    "MANIFEST": SurfaceExport(
        "agora_plugins.bigquery.plugin",
        "MANIFEST",
        "stable_public",
        _surface_note("MANIFEST"),
    ),
    "BigQueryConnectionConfig": SurfaceExport(
        "agora_plugins.bigquery.config",
        "BigQueryConnectionConfig",
        "stable_public",
        _surface_note("BigQueryConnectionConfig"),
    ),
    "BigQueryEnterpriseAcceptanceFinding": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQueryEnterpriseAcceptanceFinding",
        "supportability_public",
        _surface_note("BigQueryEnterpriseAcceptanceFinding"),
    ),
    "BigQueryEnterpriseAcceptanceGate": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQueryEnterpriseAcceptanceGate",
        "supportability_public",
        _surface_note("BigQueryEnterpriseAcceptanceGate"),
    ),
    "BigQueryEnterpriseAcceptanceReport": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQueryEnterpriseAcceptanceReport",
        "supportability_public",
        _surface_note("BigQueryEnterpriseAcceptanceReport"),
    ),
    "BigQuerySinkAcceptanceEvaluator": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySinkAcceptanceEvaluator",
        "supportability_public",
        _surface_note("BigQuerySinkAcceptanceEvaluator"),
    ),
    "BigQuerySink": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQuerySink",
        "stable_public",
        _surface_note("BigQuerySink"),
    ),
    "BigQuerySinkFlushRuntime": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQuerySinkFlushRuntime",
        "pattern_recipe",
        _surface_note("BigQuerySinkFlushRuntime"),
    ),
    "BigQuerySinkLoadJobRuntime": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQuerySinkLoadJobRuntime",
        "pattern_recipe",
        _surface_note("BigQuerySinkLoadJobRuntime"),
    ),
    "BigQuerySinkEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySinkEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("BigQuerySinkEnterpriseAcceptanceThresholds"),
    ),
    "BigQuerySinkHealthSnapshot": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySinkHealthSnapshot",
        "supportability_public",
        _surface_note("BigQuerySinkHealthSnapshot"),
    ),
    "BigQuerySinkMetricsSnapshot": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQuerySinkMetricsSnapshot",
        "supportability_public",
        _surface_note("BigQuerySinkMetricsSnapshot"),
    ),
    "BigQuerySinkWriteError": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQuerySinkWriteError",
        "supportability_public",
        _surface_note("BigQuerySinkWriteError"),
    ),
    "BigQuerySource": SurfaceExport(
        "agora_plugins.bigquery.sources",
        "BigQuerySource",
        "stable_public",
        _surface_note("BigQuerySource"),
    ),
    "BigQuerySourceAcceptanceEvaluator": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySourceAcceptanceEvaluator",
        "supportability_public",
        _surface_note("BigQuerySourceAcceptanceEvaluator"),
    ),
    "BigQuerySourceQueryRuntime": SurfaceExport(
        "agora_plugins.bigquery.sources",
        "BigQuerySourceQueryRuntime",
        "pattern_recipe",
        _surface_note("BigQuerySourceQueryRuntime"),
    ),
    "BigQuerySourceStreamRuntime": SurfaceExport(
        "agora_plugins.bigquery.sources",
        "BigQuerySourceStreamRuntime",
        "pattern_recipe",
        _surface_note("BigQuerySourceStreamRuntime"),
    ),
    "BigQuerySourceEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySourceEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("BigQuerySourceEnterpriseAcceptanceThresholds"),
    ),
    "BigQuerySourceHealthSnapshot": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySourceHealthSnapshot",
        "supportability_public",
        _surface_note("BigQuerySourceHealthSnapshot"),
    ),
    "BigQuerySourceMetricsSnapshot": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySourceMetricsSnapshot",
        "supportability_public",
        _surface_note("BigQuerySourceMetricsSnapshot"),
    ),
    "BigQuerySourceRecoveryContractSnapshot": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySourceRecoveryContractSnapshot",
        "supportability_public",
        _surface_note("BigQuerySourceRecoveryContractSnapshot"),
    ),
    "BigQuerySourceRecoveryMode": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQuerySourceRecoveryMode",
        "supportability_public",
        _surface_note("BigQuerySourceRecoveryMode"),
    ),
    "BigQueryStorageWriteSink": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQueryStorageWriteSink",
        "stable_public",
        _surface_note("BigQueryStorageWriteSink"),
    ),
    "BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds",
        "supportability_public",
        _surface_note("BigQueryStorageWriteSinkEnterpriseAcceptanceThresholds"),
    ),
    "BigQueryStorageWriteSinkError": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQueryStorageWriteSinkError",
        "supportability_public",
        _surface_note("BigQueryStorageWriteSinkError"),
    ),
    "BigQueryStorageWriteFlushRuntime": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQueryStorageWriteFlushRuntime",
        "pattern_recipe",
        _surface_note("BigQueryStorageWriteFlushRuntime"),
    ),
    "BigQueryStorageWriteSinkOperatorSurface": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQueryStorageWriteSinkOperatorSurface",
        "pattern_recipe",
        _surface_note("BigQueryStorageWriteSinkOperatorSurface"),
    ),
    "BigQueryStorageWriteSinkAcceptanceEvaluator": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQueryStorageWriteSinkAcceptanceEvaluator",
        "supportability_public",
        _surface_note("BigQueryStorageWriteSinkAcceptanceEvaluator"),
    ),
    "BigQueryStorageWriteRowSerializer": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQueryStorageWriteRowSerializer",
        "pattern_recipe",
        _surface_note("BigQueryStorageWriteRowSerializer"),
    ),
    "BigQueryStorageWriteSinkHealthSnapshot": SurfaceExport(
        "agora_plugins.bigquery.observability",
        "BigQueryStorageWriteSinkHealthSnapshot",
        "supportability_public",
        _surface_note("BigQueryStorageWriteSinkHealthSnapshot"),
    ),
    "BigQueryStorageWriteSinkMetricsSnapshot": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQueryStorageWriteSinkMetricsSnapshot",
        "supportability_public",
        _surface_note("BigQueryStorageWriteSinkMetricsSnapshot"),
    ),
    "BigQueryStorageWriteSession": SurfaceExport(
        "agora_plugins.bigquery.sinks",
        "BigQueryStorageWriteSession",
        "pattern_recipe",
        _surface_note("BigQueryStorageWriteSession"),
    ),
    "PluginManifest": SurfaceExport(
        "agora_plugins.bigquery.plugin",
        "PluginManifest",
        "stable_public",
        _surface_note("PluginManifest"),
    ),
}

_EXPORTS = export_target_map(_SURFACE_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
