"""Internal metadata for classifying plugin root export surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SurfaceClassification = Literal[
    "stable_public",
    "supportability_public",
    "pattern_recipe",
    "internal_bridge",
]


@dataclass(frozen=True, slots=True)
class SurfaceExport:
    module_path: str
    attr_name: str
    classification: SurfaceClassification
    note: str


def export_target_map(
    surface_exports: dict[str, SurfaceExport],
) -> dict[str, tuple[str, str]]:
    return {
        export_name: (export.module_path, export.attr_name)
        for export_name, export in surface_exports.items()
    }
