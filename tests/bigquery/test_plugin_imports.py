from __future__ import annotations

import importlib
import sys
import tomllib
from pathlib import Path


def test_bigquery_root_import_is_lazy() -> None:
    for module_name in [
        "agora_plugins.bigquery",
        "agora_plugins.bigquery.sources",
        "agora_plugins.bigquery.sinks",
    ]:
        sys.modules.pop(module_name, None)

    package = importlib.import_module("agora_plugins.bigquery")

    assert "agora_plugins.bigquery.sources" not in sys.modules
    assert "agora_plugins.bigquery.sinks" not in sys.modules
    assert package.BigQuerySource.__name__ == "BigQuerySource"
    assert package.BigQuerySourceAcceptanceEvaluator.__name__ == "BigQuerySourceAcceptanceEvaluator"
    assert package.BigQuerySourceQueryRuntime.__name__ == "BigQuerySourceQueryRuntime"
    assert package.BigQuerySourceStreamRuntime.__name__ == "BigQuerySourceStreamRuntime"
    assert package.BigQuerySink.__name__ == "BigQuerySink"
    assert package.BigQuerySinkAcceptanceEvaluator.__name__ == "BigQuerySinkAcceptanceEvaluator"
    assert package.BigQuerySinkFlushRuntime.__name__ == "BigQuerySinkFlushRuntime"
    assert package.BigQuerySinkLoadJobRuntime.__name__ == "BigQuerySinkLoadJobRuntime"
    assert package.BigQueryStorageWriteFlushRuntime.__name__ == "BigQueryStorageWriteFlushRuntime"
    assert (
        package.BigQueryStorageWriteSinkOperatorSurface.__name__
        == "BigQueryStorageWriteSinkOperatorSurface"
    )
    assert (
        package.BigQueryStorageWriteSinkAcceptanceEvaluator.__name__
        == "BigQueryStorageWriteSinkAcceptanceEvaluator"
    )
    assert package.BigQueryStorageWriteRowSerializer.__name__ == "BigQueryStorageWriteRowSerializer"
    assert package.BigQueryStorageWriteSink.__name__ == "BigQueryStorageWriteSink"
    assert package.BigQueryStorageWriteSession.__name__ == "BigQueryStorageWriteSession"
    assert package.BigQueryEnterpriseAcceptanceGate.__name__ == "BigQueryEnterpriseAcceptanceGate"
    assert (
        package.BigQuerySourceEnterpriseAcceptanceThresholds.__name__
        == "BigQuerySourceEnterpriseAcceptanceThresholds"
    )


def test_bigquery_entrypoints_target_leaf_modules() -> None:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    entrypoints = data["project"]["entry-points"]

    assert (
        entrypoints["agora.sources"]["bigquery"]
        == "agora_plugins.bigquery.sources.bigquery:BigQuerySource"
    )
    assert (
        entrypoints["agora.sinks"]["bigquery"]
        == "agora_plugins.bigquery.sinks.bigquery:BigQuerySink"
    )
    assert (
        entrypoints["agora.sinks"]["bigquery_storage_write"]
        == "agora_plugins.bigquery.sinks.storage_write:BigQueryStorageWriteSink"
    )


def test_bigquery_manifest_lists_storage_write_capability() -> None:
    package = importlib.import_module("agora_plugins.bigquery")

    assert "sink:bigquery_storage_write" in package.MANIFEST.capabilities
