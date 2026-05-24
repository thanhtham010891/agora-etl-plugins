"""Plugin metadata for the official PostgreSQL integration."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from agora.core.registry import AGORA_API_VERSION


@dataclass(frozen=True)
class PluginManifest:
    """Lightweight manifest for a first-party Agora plugin package."""

    name: str
    version: str
    agora_api_version: str
    package: str
    capabilities: tuple[str, ...]


def _package_version() -> str:
    try:
        return _pkg_version("agora-etl-plugins")
    except PackageNotFoundError:
        return "0+unknown"


MANIFEST = PluginManifest(
    name="postgres",
    version=_package_version(),
    agora_api_version=AGORA_API_VERSION,
    package="agora-etl-plugins",
    capabilities=(
        "source:postgres",
        "source:postgres_dlq_source",
        "sink:postgres",
        "sink:postgres_dlq",
        "sink:postgres_schema_adapter",
    ),
)
