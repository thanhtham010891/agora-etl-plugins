from __future__ import annotations

import pytest

from agora_plugins.postgres import (
    PostgresConfig,
    PostgresConnectionConfig,
    PostgresPluginConfig,
    PostgresTLSConfig,
)
from agora_plugins.postgres.connection import redact_postgres_dsn


def test_postgres_plugin_config_connection_resolves_env_and_secret_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    password_file = tmp_path / "postgres-password.txt"
    password_file.write_text("plugin-secret\n", encoding="utf-8")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://db.internal/agora")
    monkeypatch.setenv("POSTGRES_USERNAME", "agora")
    monkeypatch.setenv("POSTGRES_CA", "/etc/certs/postgres-ca.pem")
    monkeypatch.setenv("POSTGRES_CERT", "/etc/certs/postgres-client.crt")
    monkeypatch.setenv("POSTGRES_KEY", "/etc/certs/postgres-client.key")
    monkeypatch.setenv("POSTGRES_APP", "agora-plugin")
    monkeypatch.setenv("POSTGRES_ROUTE", "prefer-standby")

    config = PostgresPluginConfig(
        dsn_env="POSTGRES_DSN",
        username_env="POSTGRES_USERNAME",
        password_file=str(password_file),
        sslmode="verify-full",
        sslrootcert_env="POSTGRES_CA",
        sslcert_env="POSTGRES_CERT",
        sslkey_env="POSTGRES_KEY",
        application_name_env="POSTGRES_APP",
        target_session_attrs_env="POSTGRES_ROUTE",
        connect_timeout_s=9,
        table="events",
    )

    kwargs = config.connection().connect_kwargs()

    assert kwargs == {
        "conninfo": "postgresql://db.internal/agora",
        "user": "agora",
        "password": "plugin-secret",
        "sslmode": "verify-full",
        "sslrootcert": "/etc/certs/postgres-ca.pem",
        "sslcert": "/etc/certs/postgres-client.crt",
        "sslkey": "/etc/certs/postgres-client.key",
        "connect_timeout": 9,
        "application_name": "agora-plugin",
        "target_session_attrs": "prefer-standby",
    }


def test_postgres_plugin_config_rejects_conflicting_password_sources(tmp_path) -> None:
    password_file = tmp_path / "postgres-password.txt"
    password_file.write_text("plugin-secret\n", encoding="utf-8")
    config = PostgresPluginConfig(
        dsn="postgresql://db.internal/agora",
        password="inline-secret",
        password_file=str(password_file),
        table="events",
    )

    with pytest.raises(ValueError, match="password"):
        config.connection().connect_kwargs()


def test_postgres_tls_defaults_verify_server_identity() -> None:
    assert PostgresTLSConfig().connect_kwargs()["sslmode"] == "verify-full"
    assert (
        PostgresConfig(database_url="postgresql://db.internal/agora")
        .connection()
        .connect_kwargs()["sslmode"]
        == "verify-full"
    )
    assert (
        PostgresPluginConfig(
            dsn="postgresql://db.internal/agora",
            table="events",
        )
        .connection()
        .connect_kwargs()["sslmode"]
        == "verify-full"
    )


def test_postgres_tls_warns_when_server_identity_is_not_verified() -> None:
    with pytest.warns(UserWarning, match="without full server identity verification"):
        kwargs = PostgresTLSConfig(sslmode="require").connect_kwargs()

    assert kwargs["sslmode"] == "require"


def test_postgres_plugin_config_does_not_override_dsn_sslmode() -> None:
    kwargs = (
        PostgresPluginConfig(
            dsn="postgresql://db.internal/agora?sslmode=disable",
            table="events",
        )
        .connection()
        .connect_kwargs()
    )

    assert kwargs == {"conninfo": "postgresql://db.internal/agora?sslmode=disable"}


def test_postgres_connection_config_rejects_invalid_target_session_attrs_eagerly() -> None:
    with pytest.raises(ValueError, match="target_session_attrs"):
        PostgresConnectionConfig(
            dsn="postgresql://db.internal/agora",
            target_session_attrs="bogus",
        )


def test_postgres_connection_config_rejects_invalid_connect_timeout() -> None:
    with pytest.raises(ValueError, match="connect_timeout_s"):
        PostgresConnectionConfig(
            dsn="postgresql://db.internal/agora",
            connect_timeout_s=0,
        )


def test_postgres_config_database_url_env_is_used_when_inline_url_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://db.internal/agora")

    kwargs = PostgresConfig(database_url_env="POSTGRES_DSN").connection().connect_kwargs()

    assert kwargs["conninfo"] == "postgresql://db.internal/agora"


def test_postgres_tls_config_requires_cert_and_key_together() -> None:
    with pytest.raises(ValueError, match="cert_file and key_file"):
        PostgresTLSConfig(
            sslmode="verify-ca",
            cert_file="/etc/certs/postgres-client.crt",
        )


def test_redact_postgres_dsn_redacts_libpq_keyword_passwords() -> None:
    assert (
        redact_postgres_dsn("host=db.internal password=secret dbname=agora")
        == "host=db.internal password=*** dbname=agora"
    )
    assert (
        redact_postgres_dsn("host=db.internal password='secret value' dbname=agora")
        == "host=db.internal password=*** dbname=agora"
    )
