"""Configuration models for the Kafka plugin package."""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_SECURITY_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"})
_SASL_PROTOCOLS = frozenset({"SASL_PLAINTEXT", "SASL_SSL"})
_SSL_PROTOCOLS = frozenset({"SSL", "SASL_SSL"})
_PASSWORD_SASL_MECHANISMS = frozenset({"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})
_OAUTH_SASL_MECHANISMS = frozenset({"OAUTHBEARER"})
_KERBEROS_SASL_MECHANISMS = frozenset({"GSSAPI"})
_SASL_MECHANISMS = _PASSWORD_SASL_MECHANISMS | _OAUTH_SASL_MECHANISMS | _KERBEROS_SASL_MECHANISMS
_SCHEMA_REGISTRY_TRANSPORTS = frozenset({"stdlib", "pooled"})


def _resolve_env_value(env_name: str) -> str:
    value = os.getenv(env_name)
    if value is None or value == "":
        raise ValueError(f"Kafka config env var {env_name!r} is not set or empty.")
    return value


def _resolve_file_value(path: str) -> str:
    try:
        value = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Kafka config secret file {path!r} could not be read: {exc}") from exc
    value = value.strip()
    if not value:
        raise ValueError(f"Kafka config secret file {path!r} is empty.")
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
            f"Kafka config field {field_name!r} accepts only one of direct value, *_env, or *_file."
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
            f"Kafka config field {field_name!r} accepts only one of direct value, *_env, or *_file."
        )
    if direct is not None:
        return direct
    if env_name is not None:
        return SecretStr(_resolve_env_value(env_name))
    if file_path is not None:
        return SecretStr(_resolve_file_value(file_path))
    return None


class KafkaTLSConfig(BaseModel):
    """TLS settings for Kafka clients."""

    cafile: str | None = None
    certfile: str | None = None
    keyfile: str | None = None
    password: SecretStr | None = None
    check_hostname: bool = True

    @model_validator(mode="after")
    def _validate_keypair(self) -> KafkaTLSConfig:
        if (self.certfile is None) != (self.keyfile is None):
            raise ValueError("Kafka TLS client auth requires both certfile and keyfile together.")
        if self.password is not None and (self.certfile is None or self.keyfile is None):
            raise ValueError("Kafka TLS password requires certfile and keyfile.")
        return self

    def to_aiokafka_kwargs(self) -> dict[str, Any]:
        return {"ssl_context": self.build_ssl_context()}

    def build_ssl_context(self) -> ssl.SSLContext:
        context = ssl.create_default_context(cafile=self.cafile)
        context.check_hostname = self.check_hostname
        if self.certfile is not None and self.keyfile is not None:
            context.load_cert_chain(
                certfile=self.certfile,
                keyfile=self.keyfile,
                password=(self.password.get_secret_value() if self.password is not None else None),
            )
        return context


class KafkaSASLConfig(BaseModel):
    """SASL settings for Kafka clients."""

    mechanism: str = "PLAIN"
    username: str | None = None
    password: SecretStr | None = None
    oauth_token_provider: Any | None = None
    kerberos_service_name: str | None = None
    kerberos_domain_name: str | None = None

    @model_validator(mode="after")
    def _validate_mechanism(self) -> KafkaSASLConfig:
        if self.mechanism not in _SASL_MECHANISMS:
            supported = ", ".join(sorted(_SASL_MECHANISMS))
            raise ValueError(
                f"Unsupported Kafka SASL mechanism {self.mechanism!r}. Supported: {supported}"
            )
        if self.mechanism in _PASSWORD_SASL_MECHANISMS:
            if self.username is None or self.password is None:
                raise ValueError(
                    f"Kafka SASL mechanism {self.mechanism!r} requires username and password."
                )
            if (
                self.oauth_token_provider is not None
                or self.kerberos_service_name is not None
                or self.kerberos_domain_name is not None
            ):
                raise ValueError(
                    f"Kafka SASL mechanism {self.mechanism!r} does not accept OAuth "
                    "or Kerberos fields."
                )
        elif self.mechanism in _OAUTH_SASL_MECHANISMS:
            if self.oauth_token_provider is None:
                raise ValueError("Kafka OAUTHBEARER SASL requires oauth_token_provider.")
            if isinstance(self.oauth_token_provider, (str, bytes)):
                raise ValueError(
                    "Kafka OAUTHBEARER oauth_token_provider must be a provider object, "
                    "not a string token."
                )
            token_method = getattr(self.oauth_token_provider, "token", None)
            if not callable(token_method):
                raise ValueError("Kafka OAUTHBEARER oauth_token_provider must define token().")
            if (
                self.username is not None
                or self.password is not None
                or self.kerberos_service_name is not None
                or self.kerberos_domain_name is not None
            ):
                raise ValueError(
                    "Kafka OAUTHBEARER SASL does not accept username, password, or Kerberos fields."
                )
        elif self.mechanism in _KERBEROS_SASL_MECHANISMS:
            if (
                self.username is not None
                or self.password is not None
                or self.oauth_token_provider is not None
            ):
                raise ValueError(
                    "Kafka GSSAPI SASL does not accept username, password, "
                    "or OAuth provider fields."
                )
        return self

    def to_aiokafka_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"sasl_mechanism": self.mechanism}
        if self.mechanism in _PASSWORD_SASL_MECHANISMS:
            if self.username is None or self.password is None:
                raise ValueError(
                    f"Kafka SASL mechanism {self.mechanism!r} requires username and password."
                )
            kwargs["sasl_plain_username"] = self.username
            kwargs["sasl_plain_password"] = self.password.get_secret_value()
        elif self.mechanism in _OAUTH_SASL_MECHANISMS:
            kwargs["sasl_oauth_token_provider"] = self.oauth_token_provider
        elif self.mechanism in _KERBEROS_SASL_MECHANISMS:
            if self.kerberos_service_name is not None:
                kwargs["sasl_kerberos_service_name"] = self.kerberos_service_name
            if self.kerberos_domain_name is not None:
                kwargs["sasl_kerberos_domain_name"] = self.kerberos_domain_name
        return kwargs


class KafkaSecurityConfig(BaseModel):
    """First-class security settings for Kafka source/sink wiring."""

    security_protocol: str = "PLAINTEXT"
    sasl: KafkaSASLConfig | None = None
    tls: KafkaTLSConfig | None = None

    @model_validator(mode="after")
    def _validate_protocol(self) -> KafkaSecurityConfig:
        if self.security_protocol not in _SECURITY_PROTOCOLS:
            supported = ", ".join(sorted(_SECURITY_PROTOCOLS))
            raise ValueError(
                f"Unsupported Kafka security protocol {self.security_protocol!r}. "
                f"Supported: {supported}"
            )
        if self.security_protocol in _SASL_PROTOCOLS and self.sasl is None:
            raise ValueError(
                f"Kafka security protocol {self.security_protocol!r} requires SASL credentials "
                "or provider settings."
            )
        if self.security_protocol == "PLAINTEXT" and (
            self.sasl is not None or self.tls is not None
        ):
            raise ValueError("Kafka PLAINTEXT security does not accept SASL or TLS settings.")
        if self.security_protocol in _SSL_PROTOCOLS and self.tls is None:
            self.tls = KafkaTLSConfig()
        if self.security_protocol not in _SSL_PROTOCOLS and self.tls is not None:
            raise ValueError(
                f"Kafka security protocol {self.security_protocol!r} does not accept TLS settings."
            )
        if self.security_protocol not in _SASL_PROTOCOLS and self.sasl is not None:
            raise ValueError(
                f"Kafka security protocol {self.security_protocol!r} does not accept SASL settings."
            )
        return self

    def to_aiokafka_kwargs(self) -> dict[str, Any]:
        return self.to_aiokafka_client_kwargs()

    def to_aiokafka_client_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "security_protocol": self.security_protocol,
        }
        if self.tls is not None:
            kwargs["ssl_context"] = self.tls.build_ssl_context()
        if self.sasl is not None:
            kwargs.update(self.sasl.to_aiokafka_kwargs())
        return kwargs

    def to_aiokafka_admin_kwargs(self) -> dict[str, Any]:
        return self.to_aiokafka_client_kwargs()


class KafkaConfig(BaseSettings):
    """Kafka producer/consumer configuration."""

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        extra="ignore",
        populate_by_name=True,
    )

    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    env: str = "dev"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_username_env: str | None = None
    sasl_password: SecretStr | None = None
    sasl_password_env: str | None = None
    sasl_password_file: str | None = None
    sasl_kerberos_service_name: str | None = None
    sasl_kerberos_domain_name: str | None = None
    ssl_cafile: str | None = None
    ssl_cafile_env: str | None = None
    ssl_certfile: str | None = None
    ssl_certfile_env: str | None = None
    ssl_keyfile: str | None = None
    ssl_keyfile_env: str | None = None
    ssl_password: SecretStr | None = None
    ssl_password_env: str | None = None
    ssl_password_file: str | None = None
    ssl_check_hostname: bool = True

    producer_acks: str = "all"
    producer_linger_ms: Annotated[int, Field(ge=0)] = 5
    producer_batch_size: Annotated[int, Field(gt=0)] = 65_536
    producer_enable_idempotence: bool = True
    producer_retries: Annotated[int, Field(ge=0)] = 5

    consumer_session_timeout_ms: Annotated[int, Field(gt=0)] = 30_000
    consumer_max_poll_interval_ms: Annotated[int, Field(gt=0)] = 300_000
    consumer_max_poll_records: Annotated[int, Field(gt=0)] = 500
    consumer_fetch_min_bytes: Annotated[int, Field(ge=1)] = 1
    consumer_fetch_max_wait_ms: Annotated[int, Field(gt=0)] = 500
    consumer_max_partition_fetch_bytes: Annotated[int, Field(gt=0)] = 1_048_576
    schema_registry_url: str | None = None
    schema_registry_username: str | None = None
    schema_registry_username_env: str | None = None
    schema_registry_password: str | None = None
    schema_registry_password_env: str | None = None
    schema_registry_password_file: str | None = None
    schema_registry_ssl_cafile: str | None = None
    schema_registry_ssl_cafile_env: str | None = None
    schema_registry_ssl_certfile: str | None = None
    schema_registry_ssl_certfile_env: str | None = None
    schema_registry_ssl_keyfile: str | None = None
    schema_registry_ssl_keyfile_env: str | None = None
    schema_registry_ssl_password: SecretStr | None = None
    schema_registry_ssl_password_env: str | None = None
    schema_registry_ssl_password_file: str | None = None
    schema_registry_ssl_check_hostname: bool = True
    schema_registry_timeout_s: Annotated[float, Field(gt=0)] = 5.0
    schema_registry_transport: str = "stdlib"

    def topic(self, name: str) -> str:
        return f"{self.env}.{name}"

    def security(self) -> KafkaSecurityConfig | None:
        sasl_username = _resolve_string_value(
            field_name="sasl_username",
            direct=self.sasl_username,
            env_name=self.sasl_username_env,
        )
        sasl_password = _resolve_secret_value(
            field_name="sasl_password",
            direct=self.sasl_password,
            env_name=self.sasl_password_env,
            file_path=self.sasl_password_file,
        )
        ssl_cafile = _resolve_string_value(
            field_name="ssl_cafile",
            direct=self.ssl_cafile,
            env_name=self.ssl_cafile_env,
        )
        ssl_certfile = _resolve_string_value(
            field_name="ssl_certfile",
            direct=self.ssl_certfile,
            env_name=self.ssl_certfile_env,
        )
        ssl_keyfile = _resolve_string_value(
            field_name="ssl_keyfile",
            direct=self.ssl_keyfile,
            env_name=self.ssl_keyfile_env,
        )
        ssl_password = _resolve_secret_value(
            field_name="ssl_password",
            direct=self.ssl_password,
            env_name=self.ssl_password_env,
            file_path=self.ssl_password_file,
        )
        has_tls = any(
            value is not None for value in (ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password)
        )
        has_sasl = any(
            value is not None
            for value in (
                self.sasl_mechanism,
                sasl_username,
                sasl_password,
                self.sasl_kerberos_service_name,
                self.sasl_kerberos_domain_name,
            )
        )
        if self.security_protocol == "PLAINTEXT" and not has_tls and not has_sasl:
            return None

        sasl = None
        if has_sasl:
            if self.sasl_mechanism is None:
                raise ValueError("Kafka SASL configuration requires sasl_mechanism.")
            if self.sasl_mechanism in _OAUTH_SASL_MECHANISMS:
                raise ValueError(
                    "Kafka OAUTHBEARER SASL requires an oauth_token_provider object. "
                    "Pass KafkaSecurityConfig(..., sasl=KafkaSASLConfig(...)) instead "
                    "of env-style KafkaConfig fields."
                )
            if self.sasl_mechanism in _PASSWORD_SASL_MECHANISMS and (
                sasl_username is None or sasl_password is None
            ):
                raise ValueError(
                    "Kafka SASL configuration requires sasl_mechanism, sasl_username, and sasl_password."
                )
            sasl = KafkaSASLConfig(
                mechanism=self.sasl_mechanism,
                username=sasl_username,
                password=sasl_password,
                kerberos_service_name=self.sasl_kerberos_service_name,
                kerberos_domain_name=self.sasl_kerberos_domain_name,
            )

        tls = None
        if has_tls or self.security_protocol in _SSL_PROTOCOLS:
            tls = KafkaTLSConfig(
                cafile=ssl_cafile,
                certfile=ssl_certfile,
                keyfile=ssl_keyfile,
                password=ssl_password,
                check_hostname=self.ssl_check_hostname,
            )

        return KafkaSecurityConfig(
            security_protocol=self.security_protocol,
            sasl=sasl,
            tls=tls,
        )

    def schema_registry_auth(self) -> tuple[str | None, str | None]:
        username = _resolve_string_value(
            field_name="schema_registry_username",
            direct=self.schema_registry_username,
            env_name=self.schema_registry_username_env,
        )
        password = _resolve_string_value(
            field_name="schema_registry_password",
            direct=self.schema_registry_password,
            env_name=self.schema_registry_password_env,
            file_path=self.schema_registry_password_file,
        )
        return username, password

    def schema_registry_tls(self) -> KafkaTLSConfig | None:
        cafile = _resolve_string_value(
            field_name="schema_registry_ssl_cafile",
            direct=self.schema_registry_ssl_cafile,
            env_name=self.schema_registry_ssl_cafile_env,
        )
        certfile = _resolve_string_value(
            field_name="schema_registry_ssl_certfile",
            direct=self.schema_registry_ssl_certfile,
            env_name=self.schema_registry_ssl_certfile_env,
        )
        keyfile = _resolve_string_value(
            field_name="schema_registry_ssl_keyfile",
            direct=self.schema_registry_ssl_keyfile,
            env_name=self.schema_registry_ssl_keyfile_env,
        )
        password = _resolve_secret_value(
            field_name="schema_registry_ssl_password",
            direct=self.schema_registry_ssl_password,
            env_name=self.schema_registry_ssl_password_env,
            file_path=self.schema_registry_ssl_password_file,
        )
        if (
            all(value is None for value in (cafile, certfile, keyfile, password))
            and self.schema_registry_ssl_check_hostname
        ):
            return None
        return KafkaTLSConfig(
            cafile=cafile,
            certfile=certfile,
            keyfile=keyfile,
            password=password,
            check_hostname=self.schema_registry_ssl_check_hostname,
        )

    def schema_registry_client(
        self,
        *,
        headers: dict[str, str] | None = None,
    ) -> Any:
        if self.schema_registry_url is None:
            raise ValueError("schema_registry_url must be configured to build a registry client.")
        username, password = self.schema_registry_auth()
        if (username is None) != (password is None):
            raise ValueError("Schema registry auth requires both username and password together.")
        if self.schema_registry_transport not in _SCHEMA_REGISTRY_TRANSPORTS:
            supported = ", ".join(sorted(_SCHEMA_REGISTRY_TRANSPORTS))
            raise ValueError(
                f"Unsupported schema_registry_transport {self.schema_registry_transport!r}. "
                f"Supported: {supported}"
            )
        from agora_plugins.kafka.schema_registry import (
            ConfluentSchemaRegistryClient,
            PooledConfluentSchemaRegistryClient,
        )

        client_cls: type[Any] = (
            PooledConfluentSchemaRegistryClient
            if self.schema_registry_transport == "pooled"
            else ConfluentSchemaRegistryClient
        )

        return client_cls(
            self.schema_registry_url,
            username=username,
            password=password,
            headers=headers,
            timeout_s=self.schema_registry_timeout_s,
            tls=self.schema_registry_tls(),
        )


class KafkaPluginConfig(BaseModel):
    """Shared plugin-level settings that can be embedded in app config."""

    bootstrap_servers: str = Field(description="Kafka bootstrap servers.")
    topic: str | None = Field(default=None, description="Default topic for source or sink wiring.")
    security_protocol: str = Field(default="PLAINTEXT")
    sasl_mechanism: str | None = Field(default=None)
    sasl_username: str | None = Field(default=None)
    sasl_username_env: str | None = Field(default=None)
    sasl_password: SecretStr | None = Field(default=None)
    sasl_password_env: str | None = Field(default=None)
    sasl_password_file: str | None = Field(default=None)
    sasl_kerberos_service_name: str | None = Field(default=None)
    sasl_kerberos_domain_name: str | None = Field(default=None)
    ssl_cafile: str | None = Field(default=None)
    ssl_cafile_env: str | None = Field(default=None)
    ssl_certfile: str | None = Field(default=None)
    ssl_certfile_env: str | None = Field(default=None)
    ssl_keyfile: str | None = Field(default=None)
    ssl_keyfile_env: str | None = Field(default=None)
    ssl_password: SecretStr | None = Field(default=None)
    ssl_password_env: str | None = Field(default=None)
    ssl_password_file: str | None = Field(default=None)
    ssl_check_hostname: bool = Field(default=True)
    schema_registry_url: str | None = Field(
        default=None,
        description="Optional Confluent-compatible schema registry URL.",
    )
    schema_registry_username: str | None = Field(default=None)
    schema_registry_username_env: str | None = Field(default=None)
    schema_registry_password: str | None = Field(default=None)
    schema_registry_password_env: str | None = Field(default=None)
    schema_registry_password_file: str | None = Field(default=None)
    schema_registry_ssl_cafile: str | None = Field(default=None)
    schema_registry_ssl_cafile_env: str | None = Field(default=None)
    schema_registry_ssl_certfile: str | None = Field(default=None)
    schema_registry_ssl_certfile_env: str | None = Field(default=None)
    schema_registry_ssl_keyfile: str | None = Field(default=None)
    schema_registry_ssl_keyfile_env: str | None = Field(default=None)
    schema_registry_ssl_password: SecretStr | None = Field(default=None)
    schema_registry_ssl_password_env: str | None = Field(default=None)
    schema_registry_ssl_password_file: str | None = Field(default=None)
    schema_registry_ssl_check_hostname: bool = Field(default=True)
    schema_registry_timeout_s: Annotated[float, Field(gt=0)] = 5.0
    schema_registry_transport: str = Field(default="stdlib")

    def security(self) -> KafkaSecurityConfig | None:
        cfg = KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_username=self.sasl_username,
            sasl_username_env=self.sasl_username_env,
            sasl_password=self.sasl_password,
            sasl_password_env=self.sasl_password_env,
            sasl_password_file=self.sasl_password_file,
            sasl_kerberos_service_name=self.sasl_kerberos_service_name,
            sasl_kerberos_domain_name=self.sasl_kerberos_domain_name,
            ssl_cafile=self.ssl_cafile,
            ssl_cafile_env=self.ssl_cafile_env,
            ssl_certfile=self.ssl_certfile,
            ssl_certfile_env=self.ssl_certfile_env,
            ssl_keyfile=self.ssl_keyfile,
            ssl_keyfile_env=self.ssl_keyfile_env,
            ssl_password=self.ssl_password,
            ssl_password_env=self.ssl_password_env,
            ssl_password_file=self.ssl_password_file,
            ssl_check_hostname=self.ssl_check_hostname,
        )
        return cfg.security()

    def schema_registry_auth(self) -> tuple[str | None, str | None]:
        cfg = KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            schema_registry_username=self.schema_registry_username,
            schema_registry_username_env=self.schema_registry_username_env,
            schema_registry_password=self.schema_registry_password,
            schema_registry_password_env=self.schema_registry_password_env,
            schema_registry_password_file=self.schema_registry_password_file,
        )
        return cfg.schema_registry_auth()

    def schema_registry_tls(self) -> KafkaTLSConfig | None:
        cfg = KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            schema_registry_ssl_cafile=self.schema_registry_ssl_cafile,
            schema_registry_ssl_cafile_env=self.schema_registry_ssl_cafile_env,
            schema_registry_ssl_certfile=self.schema_registry_ssl_certfile,
            schema_registry_ssl_certfile_env=self.schema_registry_ssl_certfile_env,
            schema_registry_ssl_keyfile=self.schema_registry_ssl_keyfile,
            schema_registry_ssl_keyfile_env=self.schema_registry_ssl_keyfile_env,
            schema_registry_ssl_password=self.schema_registry_ssl_password,
            schema_registry_ssl_password_env=self.schema_registry_ssl_password_env,
            schema_registry_ssl_password_file=self.schema_registry_ssl_password_file,
            schema_registry_ssl_check_hostname=self.schema_registry_ssl_check_hostname,
        )
        return cfg.schema_registry_tls()

    def schema_registry_client(
        self,
        *,
        headers: dict[str, str] | None = None,
    ) -> Any:
        cfg = KafkaConfig(
            bootstrap_servers=self.bootstrap_servers,
            schema_registry_url=self.schema_registry_url,
            schema_registry_username=self.schema_registry_username,
            schema_registry_username_env=self.schema_registry_username_env,
            schema_registry_password=self.schema_registry_password,
            schema_registry_password_env=self.schema_registry_password_env,
            schema_registry_password_file=self.schema_registry_password_file,
            schema_registry_ssl_cafile=self.schema_registry_ssl_cafile,
            schema_registry_ssl_cafile_env=self.schema_registry_ssl_cafile_env,
            schema_registry_ssl_certfile=self.schema_registry_ssl_certfile,
            schema_registry_ssl_certfile_env=self.schema_registry_ssl_certfile_env,
            schema_registry_ssl_keyfile=self.schema_registry_ssl_keyfile,
            schema_registry_ssl_keyfile_env=self.schema_registry_ssl_keyfile_env,
            schema_registry_ssl_password=self.schema_registry_ssl_password,
            schema_registry_ssl_password_env=self.schema_registry_ssl_password_env,
            schema_registry_ssl_password_file=self.schema_registry_ssl_password_file,
            schema_registry_ssl_check_hostname=self.schema_registry_ssl_check_hostname,
            schema_registry_timeout_s=self.schema_registry_timeout_s,
            schema_registry_transport=self.schema_registry_transport,
        )
        return cfg.schema_registry_client(headers=headers)
