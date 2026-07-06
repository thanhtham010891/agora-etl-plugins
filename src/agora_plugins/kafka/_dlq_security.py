"""Security resolution helpers for Kafka DLQ sink/source surfaces."""

from __future__ import annotations

from pydantic import SecretStr

from agora_plugins.kafka.config import KafkaPluginConfig, KafkaSecurityConfig


def resolve_dlq_security(
    *,
    bootstrap_servers: str,
    topic: str | None,
    security_protocol: str,
    security: KafkaSecurityConfig | None,
    sasl_mechanism: str | None,
    sasl_username: str | None,
    sasl_username_env: str | None,
    sasl_password: str | None,
    sasl_password_env: str | None,
    sasl_password_file: str | None,
    sasl_kerberos_service_name: str | None,
    sasl_kerberos_domain_name: str | None,
    ssl_cafile: str | None,
    ssl_cafile_env: str | None,
    ssl_certfile: str | None,
    ssl_certfile_env: str | None,
    ssl_keyfile: str | None,
    ssl_keyfile_env: str | None,
    ssl_password: str | None,
    ssl_password_env: str | None,
    ssl_password_file: str | None,
    ssl_check_hostname: bool,
) -> KafkaSecurityConfig | None:
    if security is not None:
        if security.security_protocol != security_protocol:
            raise ValueError(
                "Kafka DLQ security_protocol must match security.security_protocol when both are set."
            )
        return security
    return KafkaPluginConfig(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_username_env=sasl_username_env,
        sasl_password=SecretStr(sasl_password) if sasl_password is not None else None,
        sasl_password_env=sasl_password_env,
        sasl_password_file=sasl_password_file,
        sasl_kerberos_service_name=sasl_kerberos_service_name,
        sasl_kerberos_domain_name=sasl_kerberos_domain_name,
        ssl_cafile=ssl_cafile,
        ssl_cafile_env=ssl_cafile_env,
        ssl_certfile=ssl_certfile,
        ssl_certfile_env=ssl_certfile_env,
        ssl_keyfile=ssl_keyfile,
        ssl_keyfile_env=ssl_keyfile_env,
        ssl_password=SecretStr(ssl_password) if ssl_password is not None else None,
        ssl_password_env=ssl_password_env,
        ssl_password_file=ssl_password_file,
        ssl_check_hostname=ssl_check_hostname,
    ).security()
