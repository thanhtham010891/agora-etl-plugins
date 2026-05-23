from __future__ import annotations

import importlib
import sys
import tomllib
from pathlib import Path


def test_redis_root_import_is_lazy() -> None:
    for module_name in [
        "agora_plugins.redis",
        "agora_plugins.redis.ai",
        "agora_plugins.redis.dedup",
        "agora_plugins.redis.dedup.stores",
    ]:
        sys.modules.pop(module_name, None)

    package = importlib.import_module("agora_plugins.redis")

    assert "agora_plugins.redis.ai" not in sys.modules
    assert "agora_plugins.redis.dedup.stores" not in sys.modules

    assert package.RedisLLMCache.__name__ == "RedisLLMCache"
    assert package.RedisStore.__name__ == "RedisStore"


def test_redis_entrypoints_target_leaf_modules() -> None:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    entrypoints = data["project"]["entry-points"]

    assert (
        entrypoints["agora.sources"]["redis_stream"]
        == "agora_plugins.redis.sources.redis:RedisStreamSource"
    )
    assert entrypoints["agora.sinks"]["redis"] == "agora_plugins.redis.sinks.redis:RedisSink"
    assert entrypoints["agora.middlewares.dedup.stores"]["redis"] == (
        "agora_plugins.redis.dedup.stores.redis:RedisStore"
    )
    assert entrypoints["agora.middlewares.dedup.stores"]["redis_embedding"] == (
        "agora_plugins.redis.dedup.stores.embedding:RedisEmbeddingStore"
    )
    assert entrypoints["agora.ai.caches"]["redis"] == "agora_plugins.redis.ai.cache:RedisLLMCache"
