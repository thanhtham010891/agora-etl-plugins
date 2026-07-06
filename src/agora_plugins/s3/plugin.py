"""Plugin metadata for the official S3 integration."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

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
    try:
        return _pkg_version("agora-etl-plugins")
    except PackageNotFoundError:
        return "0+unknown"


MANIFEST = PluginManifest(
    name="s3",
    version=_package_version(),
    agora_api_version=AGORA_PLUGIN_MANIFEST_VERSION,
    agora_core_api_range=AGORA_CORE_API_COMPATIBILITY,
    package="agora-etl-plugins",
    capabilities=(
        "source:s3",
        "sink:s3",
    ),
)
