from __future__ import annotations

import os
from pathlib import Path

from pydantic import SecretStr


def resolve_env_value(env_name: str) -> str:
    value = os.getenv(env_name)
    if value is None or value == "":
        raise ValueError(f"Kafka config env var {env_name!r} is not set or empty.")
    return value


def resolve_file_value(path: str) -> str:
    try:
        value = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Kafka config secret file {path!r} could not be read: {exc}") from exc
    value = value.strip()
    if not value:
        raise ValueError(f"Kafka config secret file {path!r} is empty.")
    return value


def resolve_string_value(
    *,
    field_name: str,
    direct: str | None,
    env_name: str | None = None,
    file_path: str | None = None,
) -> str | None:
    configured_sources = sum(1 for value in (direct, env_name, file_path) if value is not None)
    if configured_sources > 1:
        raise ValueError(
            f"Kafka config field {field_name!r} accepts only one of direct value, *_env, or *_file."
        )
    if direct is not None:
        return direct
    if env_name is not None:
        return resolve_env_value(env_name)
    if file_path is not None:
        return resolve_file_value(file_path)
    return None


def resolve_secret_value(
    *,
    field_name: str,
    direct: SecretStr | None,
    env_name: str | None = None,
    file_path: str | None = None,
) -> SecretStr | None:
    configured_sources = sum(1 for value in (direct, env_name, file_path) if value is not None)
    if configured_sources > 1:
        raise ValueError(
            f"Kafka config field {field_name!r} accepts only one of direct value, *_env, or *_file."
        )
    if direct is not None:
        return direct
    if env_name is not None:
        return SecretStr(resolve_env_value(env_name))
    if file_path is not None:
        return SecretStr(resolve_file_value(file_path))
    return None


__all__ = [
    "resolve_env_value",
    "resolve_file_value",
    "resolve_secret_value",
    "resolve_string_value",
]
