"""Configuration models for the Kafka plugin package."""

from __future__ import annotations

import ssl
from typing import Annotated, Any

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from agora_plugins.kafka._config_constants import (
    KERBEROS_SASL_MECHANISMS as _KERBEROS_SASL_MECHANISMS,
)
from agora_plugins.kafka._config_constants import (
    OAUTH_SASL_MECHANISMS as _OAUTH_SASL_MECHANISMS,
)
from agora_plugins.kafka._config_constants import (
    PASSWORD_SASL_MECHANISMS as _PASSWORD_SASL_MECHANISMS,
)
from agora_plugins.kafka._config_constants import (
    SASL_MECHANISMS as _SASL_MECHANISMS,
)
from agora_plugins.kafka._config_constants import (
    SASL_PROTOCOLS as _SASL_PROTOCOLS,
)
from agora_plugins.kafka._config_constants import (
    SECURITY_PROTOCOLS as _SECURITY_PROTOCOLS,
)
from agora_plugins.kafka._config_constants import (
    SSL_PROTOCOLS as _SSL_PROTOCOLS,
)
from agora_plugins.kafka._config_surface import KafkaConfigSurfaceMixin


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


class KafkaConfig(KafkaConfigSurfaceMixin, BaseSettings):
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


class KafkaPluginConfig(KafkaConfigSurfaceMixin, BaseModel):
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
