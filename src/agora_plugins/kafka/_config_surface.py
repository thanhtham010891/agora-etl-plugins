"""Shared factory surface for Kafka config models."""

from __future__ import annotations

from agora_plugins.kafka._config_constants import (
    OAUTH_SASL_MECHANISMS,
    PASSWORD_SASL_MECHANISMS,
    SCHEMA_REGISTRY_TRANSPORTS,
    SSL_PROTOCOLS,
)
from agora_plugins.kafka._config_values import resolve_secret_value, resolve_string_value
from agora_plugins.kafka._security_posture import warn_if_insecure_plaintext


class KafkaConfigSurfaceMixin:
    """Shared security and schema-registry builders for config models."""

    def security(self):
        from agora_plugins.kafka.config import KafkaSASLConfig, KafkaSecurityConfig, KafkaTLSConfig

        sasl_username = resolve_string_value(
            field_name="sasl_username",
            direct=self.sasl_username,
            env_name=self.sasl_username_env,
        )
        sasl_password = resolve_secret_value(
            field_name="sasl_password",
            direct=self.sasl_password,
            env_name=self.sasl_password_env,
            file_path=self.sasl_password_file,
        )
        ssl_cafile = resolve_string_value(
            field_name="ssl_cafile",
            direct=self.ssl_cafile,
            env_name=self.ssl_cafile_env,
        )
        ssl_certfile = resolve_string_value(
            field_name="ssl_certfile",
            direct=self.ssl_certfile,
            env_name=self.ssl_certfile_env,
        )
        ssl_keyfile = resolve_string_value(
            field_name="ssl_keyfile",
            direct=self.ssl_keyfile,
            env_name=self.ssl_keyfile_env,
        )
        ssl_password = resolve_secret_value(
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
            warn_if_insecure_plaintext(
                subject=type(self).__name__,
                security_protocol=self.security_protocol,
                bootstrap_servers=getattr(self, "bootstrap_servers", None),
                env=getattr(self, "env", None),
            )
            return None

        sasl = None
        if has_sasl:
            if self.sasl_mechanism is None:
                raise ValueError("Kafka SASL configuration requires sasl_mechanism.")
            if self.sasl_mechanism in OAUTH_SASL_MECHANISMS:
                raise ValueError(
                    "Kafka OAUTHBEARER SASL requires an oauth_token_provider object. "
                    "Pass KafkaSecurityConfig(..., sasl=KafkaSASLConfig(...)) instead "
                    "of env-style KafkaConfig fields."
                )
            if self.sasl_mechanism in PASSWORD_SASL_MECHANISMS and (
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
        if has_tls or self.security_protocol in SSL_PROTOCOLS:
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
        username = resolve_string_value(
            field_name="schema_registry_username",
            direct=self.schema_registry_username,
            env_name=self.schema_registry_username_env,
        )
        password = resolve_string_value(
            field_name="schema_registry_password",
            direct=self.schema_registry_password,
            env_name=self.schema_registry_password_env,
            file_path=self.schema_registry_password_file,
        )
        return username, password

    def schema_registry_tls(self):
        from agora_plugins.kafka.config import KafkaTLSConfig

        cafile = resolve_string_value(
            field_name="schema_registry_ssl_cafile",
            direct=self.schema_registry_ssl_cafile,
            env_name=self.schema_registry_ssl_cafile_env,
        )
        certfile = resolve_string_value(
            field_name="schema_registry_ssl_certfile",
            direct=self.schema_registry_ssl_certfile,
            env_name=self.schema_registry_ssl_certfile_env,
        )
        keyfile = resolve_string_value(
            field_name="schema_registry_ssl_keyfile",
            direct=self.schema_registry_ssl_keyfile,
            env_name=self.schema_registry_ssl_keyfile_env,
        )
        password = resolve_secret_value(
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
    ) -> object:
        if self.schema_registry_url is None:
            raise ValueError("schema_registry_url must be configured to build a registry client.")
        username, password = self.schema_registry_auth()
        if (username is None) != (password is None):
            raise ValueError("Schema registry auth requires both username and password together.")
        if self.schema_registry_transport not in SCHEMA_REGISTRY_TRANSPORTS:
            supported = ", ".join(sorted(SCHEMA_REGISTRY_TRANSPORTS))
            raise ValueError(
                f"Unsupported schema_registry_transport {self.schema_registry_transport!r}. "
                f"Supported: {supported}"
            )
        from agora_plugins.kafka.schema_registry import (
            ConfluentSchemaRegistryClient,
            PooledConfluentSchemaRegistryClient,
        )

        client_cls: type[object] = (
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


__all__ = ["KafkaConfigSurfaceMixin"]
