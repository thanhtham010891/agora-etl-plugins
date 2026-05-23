"""Official Redis plugin package for Agora.

Keep package-level exports lazy so entry-point discovery can import submodules
without tripping circular imports through ``agora_plugins.redis.__init__``.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora_plugins.redis.ai import RedisLLMCache
    from agora_plugins.redis.dedup.stores import RedisEmbeddingStore, RedisStore
    from agora_plugins.redis.dlq import RedisDLQSink, RedisDLQSource
    from agora_plugins.redis.plugin import MANIFEST, PluginManifest
    from agora_plugins.redis.sinks import RedisSink
    from agora_plugins.redis.sources import RedisStreamSource
    from agora_plugins.redis.state import RedisBackend

__all__ = [
    "MANIFEST",
    "PluginManifest",
    "RedisBackend",
    "RedisDLQSink",
    "RedisDLQSource",
    "RedisEmbeddingStore",
    "RedisLLMCache",
    "RedisSink",
    "RedisStore",
    "RedisStreamSource",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "MANIFEST": ("agora_plugins.redis.plugin", "MANIFEST"),
    "PluginManifest": ("agora_plugins.redis.plugin", "PluginManifest"),
    "RedisBackend": ("agora_plugins.redis.state", "RedisBackend"),
    "RedisDLQSink": ("agora_plugins.redis.dlq", "RedisDLQSink"),
    "RedisDLQSource": ("agora_plugins.redis.dlq", "RedisDLQSource"),
    "RedisEmbeddingStore": ("agora_plugins.redis.dedup.stores", "RedisEmbeddingStore"),
    "RedisLLMCache": ("agora_plugins.redis.ai", "RedisLLMCache"),
    "RedisSink": ("agora_plugins.redis.sinks", "RedisSink"),
    "RedisStore": ("agora_plugins.redis.dedup.stores", "RedisStore"),
    "RedisStreamSource": ("agora_plugins.redis.sources", "RedisStreamSource"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
