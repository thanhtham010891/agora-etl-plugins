"""Shared checkpoint-input identity helpers for first-party plugin sources."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any
from urllib.parse import urlparse

from agora.core.checkpoint import (
    Checkpoint,
    SourceIdentity,
    SourceIdentityMismatchError,
    SourceIdentityMismatchPolicy,
)

logger = logging.getLogger(__name__)


def fingerprint_source_identity(kind: str, attributes: dict[str, Any]) -> SourceIdentity:
    """Return a stable, secret-free identity for a configured external input."""
    canonical = json.dumps(
        attributes,
        default=_canonical_fallback,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return SourceIdentity(
        uri=f"{kind}://identity/{digest}",
        size_bytes=0,
        modified_time_ns=0,
    )


def validate_resume_checkpoint_identity(
    checkpoint: Checkpoint | None,
    *,
    current_identity: SourceIdentity,
    policy: SourceIdentityMismatchPolicy | str,
    source_name: str,
) -> Checkpoint | None:
    """Return a safe resume checkpoint or apply the configured mismatch policy."""
    if checkpoint is None:
        return None
    if checkpoint.source_identity == current_identity:
        return checkpoint

    resolved_policy = SourceIdentityMismatchPolicy(policy)
    reason = (
        "checkpoint has no source identity (legacy checkpoint)"
        if checkpoint.source_identity is None
        else "saved source identity differs from the configured input"
    )
    if resolved_policy == SourceIdentityMismatchPolicy.FAIL_CLOSED:
        raise SourceIdentityMismatchError(
            f"Cannot safely resume source {source_name!r}: {reason}. "
            "Use source_identity_mismatch_policy='reset' to start from the beginning, "
            "or 'allow' only when preserving the saved cursor is known to be safe."
        )
    if resolved_policy == SourceIdentityMismatchPolicy.RESET:
        logger.warning(
            "plugin_source_checkpoint_identity_reset",
            extra={"source_name": source_name, "reason": reason},
        )
        return None
    logger.warning(
        "plugin_source_checkpoint_identity_mismatch_allowed",
        extra={"source_name": source_name, "reason": reason},
    )
    return checkpoint


def redact_url_password(url: str) -> str:
    """Remove a URL password so credential rotation does not change identity."""
    try:
        parsed = urlparse(url)
        if parsed.password is None:
            return url
        username = parsed.username or ""
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port is not None else ""
        return parsed._replace(netloc=f"{username}:***@{host}{port}").geturl()
    except ValueError:
        return url


def _canonical_fallback(value: object) -> str:
    """Keep fingerprints deterministic for unusual but valid query parameter values."""
    return f"{type(value).__qualname__}:{value!r}"


__all__ = [
    "fingerprint_source_identity",
    "redact_url_password",
    "validate_resume_checkpoint_identity",
]
