"""Plugin metadata for the official Redis integration."""

from __future__ import annotations

from dataclasses import dataclass
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


MANIFEST = PluginManifest(
    name="redis",
    version=_pkg_version("agora-etl-plugins"),
    agora_api_version=AGORA_API_VERSION,
    package="agora-etl-plugins",
    capabilities=(
        "source:redis_dlq_source",
        "source:redis_stream",
        "sink:redis_dlq",
        "sink:redis",
        "state:redis",
        "dedup_store:redis",
        "dedup_store:embedding_redis",
        "ai_cache:redis",
    ),
)
