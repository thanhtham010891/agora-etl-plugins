from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from agora_plugins.redis.connection import (
    build_async_redis_client,
    build_sync_redis_client,
    has_hash_tag,
)


@pytest.mark.asyncio
async def test_build_async_redis_client_uses_sentinel_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []
    master = object()

    class _Sentinel:
        def __init__(self, hosts: list[tuple[str, int]], **kwargs: object) -> None:
            calls.append(("sentinel", hosts, kwargs))

        def master_for(self, service_name: str, **kwargs: object) -> object:
            calls.append(("master_for", service_name, kwargs))
            return master

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=SimpleNamespace()))
    monkeypatch.setitem(sys.modules, "redis.asyncio", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.asyncio.sentinel", SimpleNamespace(Sentinel=_Sentinel))

    client = await build_async_redis_client(
        url="sentinel://redis-a:26379,redis-b:26380/2?service=mymaster",
        decode_responses=True,
        socket_timeout_s=None,
        socket_connect_timeout_s=None,
        health_check_interval_s=None,
    )

    assert client is master
    assert calls == [
        ("sentinel", [("redis-a", 26379), ("redis-b", 26380)], {"decode_responses": True}),
        ("master_for", "mymaster", {"db": 2, "decode_responses": True}),
    ]


@pytest.mark.asyncio
async def test_build_async_redis_client_preserves_sentinel_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []
    master = object()

    class _Sentinel:
        def __init__(self, hosts: list[tuple[str, int]], **kwargs: object) -> None:
            calls.append(("sentinel", hosts, kwargs))

        def master_for(self, service_name: str, **kwargs: object) -> object:
            calls.append(("master_for", service_name, kwargs))
            return master

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=SimpleNamespace()))
    monkeypatch.setitem(sys.modules, "redis.asyncio", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.asyncio.sentinel", SimpleNamespace(Sentinel=_Sentinel))

    client = await build_async_redis_client(
        url="sentinel://user:secret%20value@redis-a:26379,redis-b:26380/2?service=mymaster",
        decode_responses=False,
        socket_timeout_s=None,
        socket_connect_timeout_s=None,
        health_check_interval_s=None,
    )

    assert client is master
    assert calls == [
        (
            "sentinel",
            [("redis-a", 26379), ("redis-b", 26380)],
            {
                "decode_responses": False,
                "username": "user",
                "password": "secret value",
                "sentinel_kwargs": {"username": "user", "password": "secret value"},
            },
        ),
        (
            "master_for",
            "mymaster",
            {
                "db": 2,
                "decode_responses": False,
                "username": "user",
                "password": "secret value",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_build_async_redis_client_uses_cluster_from_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    cluster = object()

    class _RedisCluster:
        @staticmethod
        def from_url(url: str, **kwargs: object) -> object:
            calls.append((url, kwargs))
            return cluster

    fake_asyncio = SimpleNamespace(RedisCluster=_RedisCluster)
    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=fake_asyncio))
    monkeypatch.setitem(sys.modules, "redis.asyncio", fake_asyncio)

    client = await build_async_redis_client(
        url="redis://redis-cluster:6379",
        decode_responses=False,
        redis_cluster=True,
        socket_timeout_s=None,
        socket_connect_timeout_s=None,
        health_check_interval_s=None,
    )

    assert client is cluster
    assert calls == [("redis://redis-cluster:6379", {"decode_responses": False})]


def test_build_sync_redis_client_uses_explicit_sentinel_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []
    master = object()

    class _Sentinel:
        def __init__(self, hosts: list[tuple[str, int]], **kwargs: object) -> None:
            calls.append(("sentinel", hosts, kwargs))

        def master_for(self, service_name: str, **kwargs: object) -> object:
            calls.append(("master_for", service_name, kwargs))
            return master

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.sentinel", SimpleNamespace(Sentinel=_Sentinel))

    client = build_sync_redis_client(
        url="redis://ignored:6379/4",
        decode_responses=True,
        sentinel_service_name="primary",
        sentinel_urls=["redis://sentinel-a:26379", "redis://sentinel-b:26380"],
        socket_timeout_s=None,
        socket_connect_timeout_s=None,
        health_check_interval_s=None,
    )

    assert client is master
    assert calls == [
        ("sentinel", [("sentinel-a", 26379), ("sentinel-b", 26380)], {"decode_responses": True}),
        ("master_for", "primary", {"db": 4, "decode_responses": True}),
    ]


def test_build_sync_redis_client_uses_primary_url_auth_with_explicit_sentinel_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []
    master = object()

    class _Sentinel:
        def __init__(self, hosts: list[tuple[str, int]], **kwargs: object) -> None:
            calls.append(("sentinel", hosts, kwargs))

        def master_for(self, service_name: str, **kwargs: object) -> object:
            calls.append(("master_for", service_name, kwargs))
            return master

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.sentinel", SimpleNamespace(Sentinel=_Sentinel))

    client = build_sync_redis_client(
        url="redis://:primary-secret@ignored:6379/4",
        decode_responses=False,
        sentinel_service_name="primary",
        sentinel_urls=["redis://sentinel-a:26379", "redis://sentinel-b:26380"],
        socket_timeout_s=None,
        socket_connect_timeout_s=None,
        health_check_interval_s=None,
    )

    assert client is master
    assert calls == [
        (
            "sentinel",
            [("sentinel-a", 26379), ("sentinel-b", 26380)],
            {
                "decode_responses": False,
                "password": "primary-secret",
                "sentinel_kwargs": {"password": "primary-secret"},
            },
        ),
        (
            "master_for",
            "primary",
            {"db": 4, "decode_responses": False, "password": "primary-secret"},
        ),
    ]


@pytest.mark.asyncio
async def test_build_async_redis_client_threads_default_timeouts_to_from_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    client = object()

    def _from_url(url: str, **kwargs: object) -> object:
        calls.append((url, kwargs))
        return client

    fake_asyncio = SimpleNamespace(from_url=_from_url)
    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=fake_asyncio))
    monkeypatch.setitem(sys.modules, "redis.asyncio", fake_asyncio)

    result = await build_async_redis_client(
        url="redis://redis.example:6379/0",
        decode_responses=False,
    )

    assert result is client
    assert calls == [
        (
            "redis://redis.example:6379/0",
            {
                "decode_responses": False,
                "socket_timeout": 5.0,
                "socket_connect_timeout": 5.0,
                "health_check_interval": 30,
            },
        )
    ]


@pytest.mark.asyncio
async def test_build_async_redis_client_does_not_pass_ssl_flag_to_rediss_from_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    client = object()

    def _from_url(url: str, **kwargs: object) -> object:
        calls.append((url, kwargs))
        return client

    fake_asyncio = SimpleNamespace(from_url=_from_url)
    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=fake_asyncio))
    monkeypatch.setitem(sys.modules, "redis.asyncio", fake_asyncio)

    result = await build_async_redis_client(
        url="rediss://redis.example:6379/0",
        decode_responses=False,
        socket_timeout_s=None,
        socket_connect_timeout_s=None,
        health_check_interval_s=None,
    )

    assert result is client
    assert calls == [("rediss://redis.example:6379/0", {"decode_responses": False})]


@pytest.mark.asyncio
async def test_build_async_redis_client_threads_timeouts_and_tls_to_sentinel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []
    master = object()

    class _Sentinel:
        def __init__(self, hosts: list[tuple[str, int]], **kwargs: object) -> None:
            calls.append(("sentinel", hosts, kwargs))

        def master_for(self, service_name: str, **kwargs: object) -> object:
            calls.append(("master_for", service_name, kwargs))
            return master

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=SimpleNamespace()))
    monkeypatch.setitem(sys.modules, "redis.asyncio", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "redis.asyncio.sentinel", SimpleNamespace(Sentinel=_Sentinel))

    client = await build_async_redis_client(
        url="rediss://redis-a:26379,redis-b:26380/2?service=mymaster",
        decode_responses=True,
    )

    assert client is master
    expected_connection_kwargs = {
        "decode_responses": True,
        "socket_timeout": 5.0,
        "socket_connect_timeout": 5.0,
        "health_check_interval": 30,
        "ssl": True,
        "sentinel_kwargs": {
            "socket_timeout": 5.0,
            "socket_connect_timeout": 5.0,
            "health_check_interval": 30,
            "ssl": True,
        },
    }
    assert calls == [
        ("sentinel", [("redis-a", 26379), ("redis-b", 26380)], expected_connection_kwargs),
        (
            "master_for",
            "mymaster",
            {
                "db": 2,
                "decode_responses": True,
                "socket_timeout": 5.0,
                "socket_connect_timeout": 5.0,
                "health_check_interval": 30,
                "ssl": True,
            },
        ),
    ]


def test_has_hash_tag_detects_non_empty_cluster_slot_tag() -> None:
    assert has_hash_tag("agora:{tenant-a}:events") is True
    assert has_hash_tag("agora:{}:events") is False
    assert has_hash_tag("agora:tenant-a:events") is False
