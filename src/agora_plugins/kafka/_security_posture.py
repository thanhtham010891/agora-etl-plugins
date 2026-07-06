"""Secure-by-default posture checks for Kafka wiring."""

from __future__ import annotations

import ipaddress
import warnings
from urllib.parse import urlparse

_LOCAL_ENV_NAMES = frozenset({"dev", "local", "test", "testing"})
_LOCAL_HOST_NAMES = frozenset({"localhost"})


def warn_if_insecure_plaintext(
    *,
    subject: str,
    security_protocol: str,
    bootstrap_servers: str | None,
    env: str | None = None,
    stacklevel: int = 3,
) -> None:
    """Warn when PLAINTEXT is used outside an obviously local posture."""

    if security_protocol != "PLAINTEXT":
        return

    reasons: list[str] = []
    if env is not None and env.strip().lower() not in _LOCAL_ENV_NAMES:
        reasons.append(f"env={env!r}")
    if bootstrap_servers is not None and bootstrap_servers_look_non_local(bootstrap_servers):
        reasons.append(f"bootstrap_servers={bootstrap_servers!r}")
    if not reasons:
        return

    warnings.warn(
        f"{subject} is using Kafka PLAINTEXT outside a clearly local development posture "
        f"({', '.join(reasons)}). Restrict PLAINTEXT to local/dev brokers or configure "
        "SSL/SASL before shared, staging, or production deployments.",
        UserWarning,
        stacklevel=stacklevel,
    )


def bootstrap_servers_look_non_local(bootstrap_servers: str) -> bool:
    """Return True when any bootstrap endpoint does not look local-only.

    Single-label names like ``kafka`` or ``broker`` are treated as local-ish to
    avoid noisy warnings for Docker Compose and other internal test topologies.
    """

    for endpoint in bootstrap_servers.split(","):
        host = _extract_host(endpoint)
        if host is None:
            continue
        if _host_looks_non_local(host):
            return True
    return False


def _extract_host(endpoint: str) -> str | None:
    value = endpoint.strip()
    if not value:
        return None
    value = value.rsplit("@", 1)[-1]
    if "://" in value:
        parsed = urlparse(value)
        if parsed.hostname is not None:
            return parsed.hostname.lower()
    if value.startswith("[") and "]" in value:
        return value[1 : value.index("]")].lower()
    if value.count(":") == 1:
        return value.split(":", 1)[0].lower()
    return value.lower()


def _host_looks_non_local(host: str) -> bool:
    if host in _LOCAL_HOST_NAMES:
        return False
    try:
        return not ipaddress.ip_address(host).is_loopback
    except ValueError:
        return "." in host


__all__ = ["bootstrap_servers_look_non_local", "warn_if_insecure_plaintext"]
