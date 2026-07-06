from __future__ import annotations

import importlib
import sys

FAMILY_MODULES = (
    "agora_plugins.bigquery",
    "agora_plugins.kafka",
    "agora_plugins.postgres",
    "agora_plugins.redis",
)

ALLOWED_CLASSIFICATIONS = {
    "stable_public",
    "supportability_public",
    "pattern_recipe",
    "internal_bridge",
}


def test_family_surface_manifests_cover_public_root_exports() -> None:
    for module_name in FAMILY_MODULES:
        module = importlib.import_module(module_name)

        assert set(module.__all__) == set(module._SURFACE_EXPORTS)
        assert set(module.__all__).issubset(set(module._EXPORTS))

        for _export_name, export in module._SURFACE_EXPORTS.items():
            assert export.classification in ALLOWED_CLASSIFICATIONS
            assert export.note
            assert export.attr_name
            assert export.module_path.startswith(module_name)


def test_hidden_bridge_exports_stay_out_of_public_root_surface() -> None:
    for module_name in ("agora_plugins.kafka", "agora_plugins.postgres", "agora_plugins.redis"):
        module = importlib.import_module(module_name)
        bridge_names = module._INTERNAL_BRIDGE_EXPORTS

        assert bridge_names
        for export_name in bridge_names:
            assert export_name not in module.__all__
            assert export_name in module._EXPORTS


def test_kafka_root_import_is_lazy() -> None:
    for module_name in [
        "agora_plugins.kafka",
        "agora_plugins.kafka.runtime",
        "agora_plugins.kafka.schema_registry",
        "agora_plugins.kafka.sources",
    ]:
        sys.modules.pop(module_name, None)

    package = importlib.import_module("agora_plugins.kafka")

    assert "agora_plugins.kafka.runtime" not in sys.modules
    assert "agora_plugins.kafka.schema_registry" not in sys.modules
    assert "agora_plugins.kafka.sources" not in sys.modules

    assert package.KafkaSource.__name__ == "KafkaSource"
    assert package.KafkaSourceRuntime.__name__ == "KafkaSourceRuntime"
    assert package.ConfluentSchemaRegistryClient.__name__ == "ConfluentSchemaRegistryClient"


def test_postgres_root_import_is_lazy() -> None:
    for module_name in [
        "agora_plugins.postgres",
        "agora_plugins.postgres.config",
        "agora_plugins.postgres.kafka",
        "agora_plugins.postgres.sources.postgres",
    ]:
        sys.modules.pop(module_name, None)

    package = importlib.import_module("agora_plugins.postgres")

    assert "agora_plugins.postgres.config" not in sys.modules
    assert "agora_plugins.postgres.kafka" not in sys.modules
    assert "agora_plugins.postgres.sources.postgres" not in sys.modules

    assert package.PostgresConfig.__name__ == "PostgresConfig"
    assert package.PostgresSource.__name__ == "PostgresSource"
    assert package.KafkaPostgresRuntime.__name__ == "KafkaPostgresRuntime"
