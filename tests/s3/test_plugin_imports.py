from __future__ import annotations

import importlib
import sys
import tomllib
from pathlib import Path


def test_s3_root_import_is_lazy() -> None:
    for module_name in [
        "agora_plugins.s3",
        "agora_plugins.s3.sources",
        "agora_plugins.s3.sinks",
    ]:
        sys.modules.pop(module_name, None)

    package = importlib.import_module("agora_plugins.s3")

    assert "agora_plugins.s3.sources" not in sys.modules
    assert "agora_plugins.s3.sinks" not in sys.modules
    assert package.S3Source.__name__ == "S3Source"
    assert package.S3SourceObjectRuntime.__name__ == "S3SourceObjectRuntime"
    assert package.S3Sink.__name__ == "S3Sink"
    assert package.S3SinkUploadRuntime.__name__ == "S3SinkUploadRuntime"


def test_s3_entrypoints_target_leaf_modules() -> None:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    entrypoints = data["project"]["entry-points"]

    assert entrypoints["agora.sources"]["s3"] == "agora_plugins.s3.sources.s3:S3Source"
    assert entrypoints["agora.sinks"]["s3"] == "agora_plugins.s3.sinks.s3:S3Sink"
