"""Plugin metadata for the official Anthropic integration."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

from agora.core.registry import AGORA_CORE_API_COMPATIBILITY, AGORA_PLUGIN_MANIFEST_VERSION


@dataclass(frozen=True)
class PluginManifest:
    """Lightweight manifest for a first-party Agora plugin package."""

    name: str
    version: str
    agora_api_version: str
    agora_core_api_range: str
    package: str
    capabilities: tuple[str, ...]


def _package_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[3] / "pyproject.toml"
    if pyproject_path.exists():
        data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        version = data.get("project", {}).get("version")
        if isinstance(version, str) and version.strip():
            return version
    try:
        return _pkg_version("agora-etl-plugins")
    except PackageNotFoundError:
        return "0+unknown"


MANIFEST = PluginManifest(
    name="anthropic",
    version=_package_version(),
    agora_api_version=AGORA_PLUGIN_MANIFEST_VERSION,
    agora_core_api_range=AGORA_CORE_API_COMPATIBILITY,
    package="agora-etl-plugins",
    capabilities=("ai_provider:anthropic",),
)
