from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from agora_plugins.kafka.config import (
    KafkaConfig,
    KafkaPluginConfig,
    KafkaSASLConfig,
    KafkaSecurityConfig,
    KafkaTLSConfig,
)
from agora_plugins.kafka.schema_registry import (
    ConfluentSchemaRegistryClient,
    PooledConfluentSchemaRegistryClient,
)

if TYPE_CHECKING:
    from pathlib import Path


class _OAuthTokenProvider:
    async def token(self) -> str:
        return "access-token"


def test_kafka_tls_config_requires_full_client_keypair() -> None:
    with pytest.raises(ValueError, match="certfile and keyfile together"):
        KafkaTLSConfig(certfile="/tmp/client.crt")


def test_kafka_security_config_requires_sasl_credentials_for_sasl_protocols() -> None:
    with pytest.raises(ValueError, match="requires SASL credentials"):
        KafkaSecurityConfig(security_protocol="SASL_SSL")


def test_kafka_security_config_rejects_tls_for_plaintext() -> None:
    with pytest.raises(ValueError, match="PLAINTEXT"):
        KafkaSecurityConfig(
            security_protocol="PLAINTEXT",
            tls=KafkaTLSConfig(cafile="/tmp/ca.pem"),
        )


def test_kafka_security_config_builds_aiokafka_kwargs() -> None:
    config = KafkaSecurityConfig(
        security_protocol="SASL_SSL",
        sasl=KafkaSASLConfig(
            mechanism="SCRAM-SHA-512",
            username="svc",
            password="secret",
        ),
        tls=KafkaTLSConfig(
            check_hostname=False,
        ),
    )

    kwargs = config.to_aiokafka_kwargs()

    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert kwargs["sasl_plain_username"] == "svc"
    assert kwargs["sasl_plain_password"] == "secret"
    assert "ssl_context" in kwargs
    assert kwargs["ssl_context"].check_hostname is False


def test_kafka_sasl_config_builds_oauthbearer_kwargs() -> None:
    provider = _OAuthTokenProvider()
    config = KafkaSASLConfig(
        mechanism="OAUTHBEARER",
        oauth_token_provider=provider,
    )

    kwargs = config.to_aiokafka_kwargs()

    assert kwargs == {
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": provider,
    }


def test_kafka_sasl_config_rejects_oauthbearer_string_token() -> None:
    with pytest.raises(ValueError, match="provider object"):
        KafkaSASLConfig(
            mechanism="OAUTHBEARER",
            oauth_token_provider="access-token",
        )


def test_kafka_sasl_config_requires_oauthbearer_token_method() -> None:
    with pytest.raises(ValueError, match="token"):
        KafkaSASLConfig(
            mechanism="OAUTHBEARER",
            oauth_token_provider=object(),
        )


def test_kafka_sasl_config_builds_gssapi_kwargs() -> None:
    config = KafkaSASLConfig(
        mechanism="GSSAPI",
        kerberos_service_name="kafka",
        kerberos_domain_name="EXAMPLE.COM",
    )

    kwargs = config.to_aiokafka_kwargs()

    assert kwargs == {
        "sasl_mechanism": "GSSAPI",
        "sasl_kerberos_service_name": "kafka",
        "sasl_kerberos_domain_name": "EXAMPLE.COM",
    }


def test_kafka_sasl_config_rejects_plain_with_kerberos_fields() -> None:
    with pytest.raises(ValueError, match="does not accept OAuth or Kerberos"):
        KafkaSASLConfig(
            mechanism="PLAIN",
            username="svc",
            password="secret",
            kerberos_service_name="kafka",
        )


def test_kafka_security_config_builds_admin_kwargs_with_ssl_context(
    tmp_path: Path,
) -> None:
    del tmp_path
    config = KafkaSecurityConfig(
        security_protocol="SSL",
        tls=KafkaTLSConfig(
            check_hostname=False,
        ),
    )

    admin_kwargs = config.to_aiokafka_admin_kwargs()

    assert admin_kwargs["security_protocol"] == "SSL"
    assert "ssl_context" in admin_kwargs
    assert admin_kwargs["ssl_context"].check_hostname is False


def test_kafka_security_config_builds_client_kwargs_with_ssl_context() -> None:
    config = KafkaSecurityConfig(
        security_protocol="SASL_SSL",
        sasl=KafkaSASLConfig(
            mechanism="PLAIN",
            username="svc",
            password="secret",
        ),
        tls=KafkaTLSConfig(check_hostname=True),
    )

    client_kwargs = config.to_aiokafka_client_kwargs()

    assert client_kwargs["security_protocol"] == "SASL_SSL"
    assert client_kwargs["sasl_plain_username"] == "svc"
    assert "ssl_context" in client_kwargs


def test_kafka_config_builds_first_class_security_config() -> None:
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="svc",
        sasl_password="secret",
        ssl_cafile="/tmp/ca.pem",
    )

    security = config.security()

    assert security is not None
    assert security.security_protocol == "SASL_SSL"
    assert security.sasl is not None
    assert security.sasl.username == "svc"
    assert security.tls is not None
    assert security.tls.cafile == "/tmp/ca.pem"


def test_kafka_config_builds_gssapi_security_config() -> None:
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="GSSAPI",
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name="EXAMPLE.COM",
    )

    security = config.security()

    assert security is not None
    assert security.security_protocol == "SASL_SSL"
    assert security.sasl is not None
    assert security.sasl.mechanism == "GSSAPI"
    kwargs = security.to_aiokafka_client_kwargs()
    assert kwargs["sasl_mechanism"] == "GSSAPI"
    assert kwargs["sasl_kerberos_service_name"] == "kafka"
    assert kwargs["sasl_kerberos_domain_name"] == "EXAMPLE.COM"
    assert "ssl_context" in kwargs


def test_kafka_config_rejects_oauthbearer_env_style_config() -> None:
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
    )

    with pytest.raises(ValueError, match="oauth_token_provider object"):
        config.security()


def test_kafka_plugin_config_reuses_security_builder() -> None:
    config = KafkaPluginConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SSL",
        ssl_cafile="/tmp/ca.pem",
        ssl_check_hostname=False,
    )

    security = config.security()

    assert security is not None
    assert security.security_protocol == "SSL"
    assert security.tls is not None
    assert security.tls.cafile == "/tmp/ca.pem"
    assert security.tls.check_hostname is False


def test_kafka_plugin_config_passes_gssapi_fields() -> None:
    config = KafkaPluginConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="GSSAPI",
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name="EXAMPLE.COM",
    )

    security = config.security()

    assert security is not None
    assert security.sasl is not None
    assert security.sasl.to_aiokafka_kwargs() == {
        "sasl_mechanism": "GSSAPI",
        "sasl_kerberos_service_name": "kafka",
        "sasl_kerberos_domain_name": "EXAMPLE.COM",
    }


def test_kafka_config_resolves_secure_values_from_env_and_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    secret_file = tmp_path / "kafka-password.txt"
    secret_file.write_text("super-secret\n", encoding="utf-8")
    monkeypatch.setenv("KAFKA_SASL_USER", "svc")
    monkeypatch.setenv("KAFKA_SSL_CA_PATH", "/etc/certs/ca.pem")

    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username_env="KAFKA_SASL_USER",
        sasl_password_file=str(secret_file),
        ssl_cafile_env="KAFKA_SSL_CA_PATH",
    )

    security = config.security()

    assert security is not None
    assert security.security_protocol == "SASL_SSL"
    assert security.sasl is not None
    assert security.sasl.mechanism == "SCRAM-SHA-512"
    assert security.sasl.username == "svc"
    assert security.sasl.password.get_secret_value() == "super-secret"
    assert security.tls is not None
    assert security.tls.cafile == "/etc/certs/ca.pem"
    assert security.tls.check_hostname is True


def test_kafka_plugin_config_rejects_multiple_secret_sources() -> None:
    config = KafkaPluginConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="svc",
        sasl_password="inline-secret",
        sasl_password_env="KAFKA_SASL_PASSWORD",
    )

    with pytest.raises(ValueError, match="accepts only one"):
        config.security()


def test_kafka_plugin_config_resolves_schema_registry_auth_from_env_and_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    password_file = tmp_path / "schema-registry-password.txt"
    password_file.write_text("registry-secret\n", encoding="utf-8")
    monkeypatch.setenv("SCHEMA_REGISTRY_USER", "registry-user")

    config = KafkaPluginConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_username_env="SCHEMA_REGISTRY_USER",
        schema_registry_password_file=str(password_file),
    )

    assert config.schema_registry_auth() == ("registry-user", "registry-secret")


def test_kafka_plugin_config_builds_schema_registry_tls_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCHEMA_REGISTRY_CA", "/etc/ssl/registry-ca.pem")

    config = KafkaPluginConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_ssl_cafile_env="SCHEMA_REGISTRY_CA",
        schema_registry_ssl_check_hostname=False,
    )

    tls = config.schema_registry_tls()

    assert tls is not None
    assert tls.cafile == "/etc/ssl/registry-ca.pem"
    assert tls.check_hostname is False


def test_kafka_plugin_config_builds_secure_schema_registry_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    password_file = tmp_path / "schema-registry-password.txt"
    password_file.write_text("registry-secret\n", encoding="utf-8")
    monkeypatch.setenv("SCHEMA_REGISTRY_USER", "registry-user")

    config = KafkaPluginConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_url="https://registry.internal:8081",
        schema_registry_username_env="SCHEMA_REGISTRY_USER",
        schema_registry_password_file=str(password_file),
        schema_registry_ssl_check_hostname=False,
    )

    client = config.schema_registry_client(headers={"X-Test": "1"})

    assert client._base_url == "https://registry.internal:8081"  # type: ignore[attr-defined]
    assert client._username == "registry-user"  # type: ignore[attr-defined]
    assert client._password == "registry-secret"  # type: ignore[attr-defined]
    assert client._headers["X-Test"] == "1"  # type: ignore[attr-defined]
    assert client._ssl_context is not None  # type: ignore[attr-defined]
    assert client._ssl_context.check_hostname is False  # type: ignore[attr-defined]
    assert isinstance(client, ConfluentSchemaRegistryClient)
    assert not isinstance(client, PooledConfluentSchemaRegistryClient)


def test_kafka_plugin_config_builds_pooled_schema_registry_client() -> None:
    config = KafkaPluginConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_url="https://registry.internal:8081",
        schema_registry_transport="pooled",
        schema_registry_timeout_s=3.0,
    )

    client = config.schema_registry_client(headers={"X-Test": "1"})

    assert isinstance(client, PooledConfluentSchemaRegistryClient)
    assert client._base_url == "https://registry.internal:8081"  # type: ignore[attr-defined]
    assert client._headers["X-Test"] == "1"  # type: ignore[attr-defined]
    assert client._timeout_s == 3.0  # type: ignore[attr-defined]


def test_kafka_config_rejects_unknown_schema_registry_transport() -> None:
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_url="https://registry.internal:8081",
        schema_registry_transport="unknown",
    )

    with pytest.raises(ValueError, match="schema_registry_transport"):
        config.schema_registry_client()
