from __future__ import annotations

import json
from json import JSONDecodeError
from urllib.parse import urlparse

from agora.runner import WorkerInfo

_PROGRAMMING_ERROR_TYPES = (AssertionError, AttributeError, KeyError, TypeError, ValueError)


def raise_if_programming_error(exc: Exception) -> None:
    if isinstance(exc, _PROGRAMMING_ERROR_TYPES):
        raise exc


def redact_url(url: str) -> str:
    """Return URL with password replaced by ***."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            return parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                + (f":{parsed.port}" if parsed.port else "")
            ).geturl()
    except Exception:
        pass
    return url


def worker_info_from_raw(raw: object) -> WorkerInfo | None:
    if raw is None:
        return None
    try:
        data = json.loads(str(raw))
    except (TypeError, JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        assigned = data.get("assigned_pipelines", [])
        if not isinstance(assigned, list):
            assigned = []
        return WorkerInfo(
            worker_id=str(data.get("worker_id", "")),
            hostname=str(data.get("hostname", "")),
            pid=int(data.get("pid", 0)),
            status=str(data.get("status", "unknown")),
            assigned_pipelines=[str(item) for item in assigned],
            last_heartbeat_at=str(data.get("last_heartbeat_at", "")),
        )
    except (TypeError, ValueError):
        return None


def lease_payload_matches(raw: object, worker_id: str, fencing_token: int) -> bool:
    if raw is None:
        return False
    try:
        data = json.loads(str(raw))
    except (TypeError, JSONDecodeError):
        return False
    return data.get("worker_id") == worker_id and str(data.get("fencing_token")) == str(
        fencing_token
    )


def lease_payload_matches_redlock(
    raw: object,
    worker_id: str,
    acquired_at: str,
    fencing_token: int,
) -> bool:
    if raw is None:
        return False
    try:
        data = json.loads(str(raw))
    except (TypeError, JSONDecodeError):
        return False
    return (
        data.get("worker_id") == worker_id
        and str(data.get("acquired_at")) == acquired_at
        and str(data.get("fencing_token")) == str(fencing_token)
    )
