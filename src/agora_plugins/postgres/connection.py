"""Shared PostgreSQL connection/auth/TLS configuration helpers."""

from __future__ import annotations

import os
import re
import warnings
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, SecretStr, model_validator

_POSTGRES_SSLMODES = frozenset(
    {
        "disable",
        "allow",
        "prefer",
        "require",
        "verify-ca",
        "verify-full",
    }
)
_POSTGRES_INSECURE_SSLMODES = frozenset({"disable", "allow", "prefer", "require"})
_POSTGRES_TARGET_SESSION_ATTRS = frozenset(
    {
        "any",
        "read-write",
        "read-only",
        "primary",
        "standby",
        "prefer-standby",
    }
)
_LIBPQ_PASSWORD_RE = re.compile(r"(?i)(^|\s)(password\s*=\s*)(?:'[^']*'|\"[^\"]*\"|[^\s]+)")
_LIBPQ_KEYWORD_RE = re.compile(r"(?i)(^|\s)([a-z_][a-z0-9_]*)\s*=")


def _dsn_declares_option(dsn: str, option: str) -> bool:
    parsed = urlparse(dsn)
    if parsed.scheme:
        query_keys = {part.split("=", 1)[0].lower() for part in parsed.query.split("&") if part}
        return option.lower() in query_keys
    return any(
        match.group(2).lower() == option.lower() for match in _LIBPQ_KEYWORD_RE.finditer(dsn)
    )


def _resolve_env_value(env_name: str) -> str:
    value = os.getenv(env_name)
    if value is None or value == "":
        raise ValueError(f"Postgres config env var {env_name!r} is not set or empty.")
    return value


def _resolve_file_value(path: str) -> str:
    try:
        value = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Postgres config secret file {path!r} could not be read: {exc}") from exc
    value = value.strip()
    if not value:
        raise ValueError(f"Postgres config secret file {path!r} is empty.")
    return value


def _resolve_string_value(
    *,
    field_name: str,
    direct: str | None,
    env_name: str | None = None,
    file_path: str | None = None,
) -> str | None:
    configured_sources = sum(1 for value in (direct, env_name, file_path) if value is not None)
    if configured_sources > 1:
        raise ValueError(
            f"Postgres config field {field_name!r} accepts only one of direct value, *_env, or *_file."
        )
    if direct is not None:
        return direct
    if env_name is not None:
        return _resolve_env_value(env_name)
    if file_path is not None:
        return _resolve_file_value(file_path)
    return None


def _resolve_secret_value(
    *,
    field_name: str,
    direct: SecretStr | None,
    env_name: str | None = None,
    file_path: str | None = None,
) -> SecretStr | None:
    configured_sources = sum(1 for value in (direct, env_name, file_path) if value is not None)
    if configured_sources > 1:
        raise ValueError(
            f"Postgres config field {field_name!r} accepts only one of direct value, *_env, or *_file."
        )
    if direct is not None:
        return direct
    if env_name is not None:
        return SecretStr(_resolve_env_value(env_name))
    if file_path is not None:
        return SecretStr(_resolve_file_value(file_path))
    return None


class PostgresAuthConfig(BaseModel):
    """Auth settings for PostgreSQL clients."""

    username: str | None = None
    username_env: str | None = None
    password: SecretStr | None = None
    password_env: str | None = None
    password_file: str | None = None

    def resolved_username(self) -> str | None:
        return _resolve_string_value(
            field_name="username",
            direct=self.username,
            env_name=self.username_env,
        )

    def resolved_password(self) -> SecretStr | None:
        return _resolve_secret_value(
            field_name="password",
            direct=self.password,
            env_name=self.password_env,
            file_path=self.password_file,
        )

    def connect_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        username = self.resolved_username()
        password = self.resolved_password()
        if username is not None:
            kwargs["user"] = username
        if password is not None:
            kwargs["password"] = password.get_secret_value()
        return kwargs


class PostgresTLSConfig(BaseModel):
    """TLS settings for PostgreSQL clients."""

    sslmode: str = "verify-full"
    root_cert_file: str | None = None
    root_cert_env: str | None = None
    cert_file: str | None = None
    cert_env: str | None = None
    key_file: str | None = None
    key_env: str | None = None
    key_password: SecretStr | None = None
    key_password_env: str | None = None
    key_password_file: str | None = None

    @model_validator(mode="after")
    def _validate_tls(self) -> PostgresTLSConfig:
        if self.sslmode not in _POSTGRES_SSLMODES:
            supported = ", ".join(sorted(_POSTGRES_SSLMODES))
            raise ValueError(
                f"Unsupported Postgres sslmode {self.sslmode!r}. Supported: {supported}"
            )
        cert_file = _resolve_string_value(
            field_name="cert_file",
            direct=self.cert_file,
            env_name=self.cert_env,
        )
        key_file = _resolve_string_value(
            field_name="key_file",
            direct=self.key_file,
            env_name=self.key_env,
        )
        if (cert_file is None) != (key_file is None):
            raise ValueError(
                "Postgres TLS client auth requires both cert_file and key_file together."
            )
        return self

    def connect_kwargs(self) -> dict[str, Any]:
        if self.sslmode in _POSTGRES_INSECURE_SSLMODES:
            warnings.warn(
                "Postgres sslmode is configured without full server identity verification. "
                "Use sslmode='verify-full' with a trusted CA for production deployments.",
                UserWarning,
                stacklevel=2,
            )
        kwargs: dict[str, Any] = {
            "sslmode": self.sslmode,
        }
        root_cert_file = _resolve_string_value(
            field_name="root_cert_file",
            direct=self.root_cert_file,
            env_name=self.root_cert_env,
        )
        cert_file = _resolve_string_value(
            field_name="cert_file",
            direct=self.cert_file,
            env_name=self.cert_env,
        )
        key_file = _resolve_string_value(
            field_name="key_file",
            direct=self.key_file,
            env_name=self.key_env,
        )
        key_password = _resolve_secret_value(
            field_name="key_password",
            direct=self.key_password,
            env_name=self.key_password_env,
            file_path=self.key_password_file,
        )
        if root_cert_file is not None:
            kwargs["sslrootcert"] = root_cert_file
        if cert_file is not None:
            kwargs["sslcert"] = cert_file
        if key_file is not None:
            kwargs["sslkey"] = key_file
        if key_password is not None:
            kwargs["sslpassword"] = key_password.get_secret_value()
        return kwargs


class PostgresConnectionConfig(BaseModel):
    """First-class PostgreSQL connection settings with env/file secret ergonomics."""

    dsn: str | None = None
    dsn_env: str | None = None
    auth: PostgresAuthConfig | None = None
    tls: PostgresTLSConfig | None = None
    connect_timeout_s: int | None = None
    application_name: str | None = None
    application_name_env: str | None = None
    target_session_attrs: str | None = None
    target_session_attrs_env: str | None = None

    @model_validator(mode="after")
    def _validate_direct_target_session_attrs(self) -> PostgresConnectionConfig:
        if self.connect_timeout_s is not None and self.connect_timeout_s <= 0:
            raise ValueError("Postgres connect_timeout_s must be > 0 when provided.")
        if (
            self.target_session_attrs is not None
            and self.target_session_attrs not in _POSTGRES_TARGET_SESSION_ATTRS
        ):
            supported = ", ".join(sorted(_POSTGRES_TARGET_SESSION_ATTRS))
            raise ValueError(
                "Unsupported Postgres target_session_attrs "
                f"{self.target_session_attrs!r}. Supported: {supported}"
            )
        return self

    def resolve_dsn(self) -> str:
        dsn = _resolve_string_value(
            field_name="dsn",
            direct=self.dsn,
            env_name=self.dsn_env,
        )
        if dsn is None:
            raise ValueError("Postgres connection requires dsn or dsn_env.")
        return dsn

    def resolved_application_name(self) -> str | None:
        return _resolve_string_value(
            field_name="application_name",
            direct=self.application_name,
            env_name=self.application_name_env,
        )

    def resolved_target_session_attrs(self) -> str | None:
        target_session_attrs = _resolve_string_value(
            field_name="target_session_attrs",
            direct=self.target_session_attrs,
            env_name=self.target_session_attrs_env,
        )
        if (
            target_session_attrs is not None
            and target_session_attrs not in _POSTGRES_TARGET_SESSION_ATTRS
        ):
            supported = ", ".join(sorted(_POSTGRES_TARGET_SESSION_ATTRS))
            raise ValueError(
                "Unsupported Postgres target_session_attrs "
                f"{target_session_attrs!r}. Supported: {supported}"
            )
        return target_session_attrs

    def connect_kwargs(self, **overrides: Any) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "conninfo": self.resolve_dsn(),
        }
        if self.auth is not None:
            kwargs.update(self.auth.connect_kwargs())
        if self.tls is not None:
            tls_kwargs = self.tls.connect_kwargs()
            if _dsn_declares_option(kwargs["conninfo"], "sslmode"):
                tls_kwargs.pop("sslmode", None)
            kwargs.update(tls_kwargs)
        if self.connect_timeout_s is not None:
            kwargs["connect_timeout"] = self.connect_timeout_s
        application_name = self.resolved_application_name()
        if application_name is not None:
            kwargs["application_name"] = application_name
        target_session_attrs = self.resolved_target_session_attrs()
        if target_session_attrs is not None:
            kwargs["target_session_attrs"] = target_session_attrs
        kwargs.update(overrides)
        return kwargs

    def with_fallback_dsn(self, dsn: str | None) -> PostgresConnectionConfig:
        if self.dsn is not None or self.dsn_env is not None or dsn is None:
            return self
        return self.model_copy(update={"dsn": dsn})

    def redacted_dsn(self) -> str:
        return redact_postgres_dsn(self.resolve_dsn())


def coerce_connection_config(
    *,
    dsn: str | None,
    connection: PostgresConnectionConfig | None,
) -> PostgresConnectionConfig:
    if connection is None:
        if dsn is None:
            raise ValueError("Postgres connection requires dsn or connection config.")
        return PostgresConnectionConfig(dsn=dsn)
    if dsn is not None and (connection.dsn is not None or connection.dsn_env is not None):
        resolved_dsn = connection.resolve_dsn()
        if resolved_dsn != dsn:
            raise ValueError("Pass either dsn or connection.dsn, not conflicting values for both.")
    return connection.with_fallback_dsn(dsn)


def redact_postgres_dsn(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
        if parsed.password:
            netloc = f"{parsed.username}:***@{parsed.hostname}" + (
                f":{parsed.port}" if parsed.port else ""
            )
            return parsed._replace(netloc=netloc).geturl()
    except Exception:
        pass
    return _LIBPQ_PASSWORD_RE.sub(lambda match: f"{match.group(1)}{match.group(2)}***", dsn)


__all__ = [
    "PostgresAuthConfig",
    "PostgresConnectionConfig",
    "PostgresTLSConfig",
    "coerce_connection_config",
    "redact_postgres_dsn",
]
