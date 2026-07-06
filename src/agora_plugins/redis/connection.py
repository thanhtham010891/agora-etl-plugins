"""Shared Redis connection helpers for standalone, Sentinel, and Cluster deployments."""

from __future__ import annotations

import warnings
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlparse

RedisClusterAddressRemap = Callable[[tuple[str, int]], tuple[str, int]]

_DEFAULT_SOCKET_TIMEOUT_S = 5.0
_DEFAULT_HEALTH_CHECK_INTERVAL_S = 30


def _install_redis_cluster_driver_warning_filter() -> None:
    # redis-py 7.4.0 still routes cluster node connections through deprecated
    # lib_name/lib_version kwargs and does not yet expose driver_info on the
    # async cluster constructor. Keep the filter narrowly scoped to redis
    # cluster internals until upstream exposes a non-deprecated path.
    warnings.filterwarnings(
        "ignore",
        message=r".*deprecated usage of input argument/s 'lib_name'.*",
        category=DeprecationWarning,
        module=r"redis(\.asyncio)?\.cluster",
    )
    warnings.filterwarnings(
        "ignore",
        message=r".*deprecated usage of input argument/s 'lib_version'.*",
        category=DeprecationWarning,
        module=r"redis(\.asyncio)?\.cluster",
    )


def _sentinel_urls(url: str, explicit_urls: list[str] | None) -> list[str]:
    if explicit_urls:
        return explicit_urls
    parsed = urlparse(url)
    scheme = "rediss" if parsed.scheme == "rediss" else "redis"
    return [f"{scheme}://{part}" for part in parsed.netloc.split(",")]


def _sentinel_hosts(url: str, explicit_urls: list[str] | None) -> list[tuple[str, int]]:
    urls = _sentinel_urls(url, explicit_urls)
    hosts: list[tuple[str, int]] = []
    for item in urls:
        parsed = urlparse(item)
        host = parsed.hostname or parsed.netloc
        if not host:
            continue
        hosts.append((host, parsed.port or 26379))
    if not hosts:
        raise ValueError("Redis Sentinel configuration requires at least one host.")
    return hosts


def _sentinel_auth_kwargs(url: str, explicit_urls: list[str] | None) -> dict[str, str]:
    urls = [url, *explicit_urls] if explicit_urls else _sentinel_urls(url, explicit_urls)
    for item in urls:
        parsed = urlparse(item)
        kwargs: dict[str, str] = {}
        if parsed.username:
            kwargs["username"] = unquote(parsed.username)
        if parsed.password is not None:
            kwargs["password"] = unquote(parsed.password)
        if kwargs:
            return kwargs
    return {}


def _uses_tls(url: str, explicit_urls: list[str] | None) -> bool:
    urls = [url, *explicit_urls] if explicit_urls else [url]
    return any(urlparse(item).scheme == "rediss" for item in urls)


def _redis_connection_kwargs(
    *,
    decode_responses: bool,
    url: str,
    explicit_urls: list[str] | None,
    socket_timeout_s: float | None,
    socket_connect_timeout_s: float | None,
    health_check_interval_s: int | None,
    include_ssl_flag: bool = False,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"decode_responses": decode_responses}
    if socket_timeout_s is not None:
        kwargs["socket_timeout"] = socket_timeout_s
    if socket_connect_timeout_s is not None:
        kwargs["socket_connect_timeout"] = socket_connect_timeout_s
    if health_check_interval_s is not None:
        kwargs["health_check_interval"] = health_check_interval_s
    if include_ssl_flag and _uses_tls(url, explicit_urls):
        kwargs["ssl"] = True
    return kwargs


def _sentinel_connection_kwargs(
    url: str,
    explicit_urls: list[str] | None,
    *,
    decode_responses: bool,
    socket_timeout_s: float | None,
    socket_connect_timeout_s: float | None,
    health_check_interval_s: int | None,
) -> dict[str, Any]:
    kwargs = _redis_connection_kwargs(
        decode_responses=decode_responses,
        url=url,
        explicit_urls=explicit_urls,
        socket_timeout_s=socket_timeout_s,
        socket_connect_timeout_s=socket_connect_timeout_s,
        health_check_interval_s=health_check_interval_s,
        include_ssl_flag=True,
    )
    auth_kwargs = _sentinel_auth_kwargs(url, explicit_urls)
    if auth_kwargs:
        kwargs.update(auth_kwargs)
    sentinel_kwargs = {key: value for key, value in kwargs.items() if key != "decode_responses"}
    if sentinel_kwargs:
        kwargs["sentinel_kwargs"] = sentinel_kwargs
    return kwargs


def _sentinel_service_name(url: str, explicit: str | None) -> str | None:
    if explicit:
        return explicit
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    values = query.get("service") or query.get("master")
    return values[0] if values else None


def _redis_db(url: str) -> int:
    path = urlparse(url).path.strip("/")
    if not path:
        return 0
    try:
        return int(path)
    except ValueError:
        return 0


def _parse_cluster_node_name(node_name: str) -> tuple[str, int] | None:
    host, separator, raw_port = node_name.rpartition(":")
    if not separator or not host:
        return None
    try:
        return host, int(raw_port)
    except ValueError:
        return None


def _patch_async_cluster_redirect_remap(
    client: Any,
    address_remap: RedisClusterAddressRemap,
) -> None:
    nodes_manager = getattr(client, "nodes_manager", None)
    if nodes_manager is None:
        return

    try:
        if getattr(nodes_manager, "address_remap", None) is None:
            nodes_manager.address_remap = address_remap
    except Exception:
        pass

    nodes_manager_cls = nodes_manager.__class__
    if getattr(nodes_manager_cls, "_agora_address_remap_patched", False):
        return

    original_get_node = getattr(nodes_manager_cls, "get_node", None)
    if callable(original_get_node):

        def _get_node_with_remap(
            self: Any,
            host: str | None = None,
            port: int | None = None,
            node_name: str | None = None,
        ) -> Any:
            node = original_get_node(self, host=host, port=port, node_name=node_name)
            if node is not None:
                return node

            remap = getattr(self, "address_remap", None)
            if remap is None:
                return None

            if host is not None and port is not None:
                remapped_host, remapped_port = remap((host, int(port)))
                if (remapped_host, remapped_port) != (host, int(port)):
                    return original_get_node(self, host=remapped_host, port=remapped_port)

            if node_name is not None:
                parsed = _parse_cluster_node_name(node_name)
                if parsed is None:
                    return None
                remapped_host, remapped_port = remap(parsed)
                if (remapped_host, remapped_port) != parsed:
                    return original_get_node(self, host=remapped_host, port=remapped_port)

            return None

        nodes_manager_cls.get_node = _get_node_with_remap

    original_move_slot = getattr(nodes_manager_cls, "move_slot", None)
    if callable(original_move_slot):

        def _move_slot_with_remap(self: Any, error: Any) -> Any:
            host = getattr(error, "host", None)
            port = getattr(error, "port", None)
            if host is None or port is None:
                return original_move_slot(self, error)

            remap = getattr(self, "address_remap", None)
            if remap is None:
                return original_move_slot(self, error)

            remapped_host, remapped_port = remap((host, int(port)))
            if (remapped_host, remapped_port) == (host, int(port)):
                return original_move_slot(self, error)

            redirected = SimpleNamespace(
                host=remapped_host,
                port=remapped_port,
                slot_id=getattr(error, "slot_id", None),
            )
            return original_move_slot(self, redirected)

        nodes_manager_cls.move_slot = _move_slot_with_remap

    nodes_manager_cls._agora_address_remap_patched = True


async def build_async_redis_client(
    *,
    url: str,
    decode_responses: bool,
    redis_cluster: bool = False,
    redis_cluster_address_remap: RedisClusterAddressRemap | None = None,
    sentinel_service_name: str | None = None,
    sentinel_urls: list[str] | None = None,
    socket_timeout_s: float | None = _DEFAULT_SOCKET_TIMEOUT_S,
    socket_connect_timeout_s: float | None = _DEFAULT_SOCKET_TIMEOUT_S,
    health_check_interval_s: int | None = _DEFAULT_HEALTH_CHECK_INTERVAL_S,
) -> Any:
    import redis.asyncio as aioredis

    service_name = _sentinel_service_name(url, sentinel_service_name)
    if service_name is not None:
        from redis.asyncio.sentinel import Sentinel

        sentinel_cls = cast("Any", Sentinel)
        auth_kwargs = _sentinel_auth_kwargs(url, sentinel_urls)
        sentinel = sentinel_cls(
            _sentinel_hosts(url, sentinel_urls),
            **_sentinel_connection_kwargs(
                url,
                sentinel_urls,
                decode_responses=decode_responses,
                socket_timeout_s=socket_timeout_s,
                socket_connect_timeout_s=socket_connect_timeout_s,
                health_check_interval_s=health_check_interval_s,
            ),
        )
        master_kwargs = _redis_connection_kwargs(
            decode_responses=decode_responses,
            url=url,
            explicit_urls=sentinel_urls,
            socket_timeout_s=socket_timeout_s,
            socket_connect_timeout_s=socket_connect_timeout_s,
            health_check_interval_s=health_check_interval_s,
            include_ssl_flag=True,
        )
        master_kwargs.update(auth_kwargs)
        return cast(
            "Any",
            sentinel.master_for(
                service_name,
                db=_redis_db(url),
                **master_kwargs,
            ),
        )

    if redis_cluster:
        redis_cluster_cls = getattr(aioredis, "RedisCluster", None)
        if redis_cluster_cls is None:
            from redis.asyncio import cluster as cluster_module

            redis_cluster_cls = cluster_module.RedisCluster

        kwargs = _redis_connection_kwargs(
            decode_responses=decode_responses,
            url=url,
            explicit_urls=None,
            socket_timeout_s=socket_timeout_s,
            socket_connect_timeout_s=socket_connect_timeout_s,
            health_check_interval_s=health_check_interval_s,
        )
        if redis_cluster_address_remap is not None:
            kwargs["address_remap"] = redis_cluster_address_remap
        _install_redis_cluster_driver_warning_filter()
        client = redis_cluster_cls.from_url(url, **kwargs)
        if redis_cluster_address_remap is not None:
            _patch_async_cluster_redirect_remap(client, redis_cluster_address_remap)
        return client

    return cast(
        "Any",
        aioredis.from_url(
            url,
            **_redis_connection_kwargs(
                decode_responses=decode_responses,
                url=url,
                explicit_urls=None,
                socket_timeout_s=socket_timeout_s,
                socket_connect_timeout_s=socket_connect_timeout_s,
                health_check_interval_s=health_check_interval_s,
            ),
        ),
    )


def build_sync_redis_client(
    *,
    url: str,
    decode_responses: bool,
    redis_cluster: bool = False,
    redis_cluster_address_remap: RedisClusterAddressRemap | None = None,
    sentinel_service_name: str | None = None,
    sentinel_urls: list[str] | None = None,
    socket_timeout_s: float | None = _DEFAULT_SOCKET_TIMEOUT_S,
    socket_connect_timeout_s: float | None = _DEFAULT_SOCKET_TIMEOUT_S,
    health_check_interval_s: int | None = _DEFAULT_HEALTH_CHECK_INTERVAL_S,
) -> Any:
    import redis

    service_name = _sentinel_service_name(url, sentinel_service_name)
    if service_name is not None:
        from redis.sentinel import Sentinel

        sentinel_cls = cast("Any", Sentinel)
        auth_kwargs = _sentinel_auth_kwargs(url, sentinel_urls)
        sentinel = sentinel_cls(
            _sentinel_hosts(url, sentinel_urls),
            **_sentinel_connection_kwargs(
                url,
                sentinel_urls,
                decode_responses=decode_responses,
                socket_timeout_s=socket_timeout_s,
                socket_connect_timeout_s=socket_connect_timeout_s,
                health_check_interval_s=health_check_interval_s,
            ),
        )
        master_kwargs = _redis_connection_kwargs(
            decode_responses=decode_responses,
            url=url,
            explicit_urls=sentinel_urls,
            socket_timeout_s=socket_timeout_s,
            socket_connect_timeout_s=socket_connect_timeout_s,
            health_check_interval_s=health_check_interval_s,
            include_ssl_flag=True,
        )
        master_kwargs.update(auth_kwargs)
        return cast(
            "Any",
            sentinel.master_for(
                service_name,
                db=_redis_db(url),
                **master_kwargs,
            ),
        )

    if redis_cluster:
        redis_cluster_cls = getattr(redis, "RedisCluster", None)
        if redis_cluster_cls is None:
            from redis import cluster as cluster_module

            redis_cluster_cls = cluster_module.RedisCluster

        kwargs = _redis_connection_kwargs(
            decode_responses=decode_responses,
            url=url,
            explicit_urls=None,
            socket_timeout_s=socket_timeout_s,
            socket_connect_timeout_s=socket_connect_timeout_s,
            health_check_interval_s=health_check_interval_s,
        )
        if redis_cluster_address_remap is not None:
            kwargs["address_remap"] = redis_cluster_address_remap
        _install_redis_cluster_driver_warning_filter()
        return redis_cluster_cls.from_url(url, **kwargs)

    return redis.Redis.from_url(
        url,
        **_redis_connection_kwargs(
            decode_responses=decode_responses,
            url=url,
            explicit_urls=None,
            socket_timeout_s=socket_timeout_s,
            socket_connect_timeout_s=socket_connect_timeout_s,
            health_check_interval_s=health_check_interval_s,
        ),
    )


def has_hash_tag(value: str) -> bool:
    start = value.find("{")
    end = value.find("}", start + 1)
    return start >= 0 and end > start + 1
