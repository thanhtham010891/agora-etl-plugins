"""Configuration models for the PostgreSQL plugin package."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from agora_plugins.postgres.connection import (
    PostgresAuthConfig,
    PostgresConnectionConfig,
    PostgresTLSConfig,
)


class PostgresConfig(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        extra="ignore",
        populate_by_name=True,
    )

    database_url: str | None = Field(
        default=None,
        alias="DATABASE_URL",
    )
    database_url_env: str | None = Field(default=None, alias="DATABASE_URL_ENV")
    username: str | None = None
    username_env: str | None = None
    password: SecretStr | None = None
    password_env: str | None = None
    password_file: str | None = None
    sslmode: str = "verify-full"
    sslrootcert: str | None = None
    sslrootcert_env: str | None = None
    sslcert: str | None = None
    sslcert_env: str | None = None
    sslkey: str | None = None
    sslkey_env: str | None = None
    sslpassword: SecretStr | None = None
    sslpassword_env: str | None = None
    sslpassword_file: str | None = None
    connect_timeout_s: Annotated[int | None, Field(gt=0)] = None
    application_name: str | None = None
    application_name_env: str | None = None
    target_session_attrs: str | None = None
    target_session_attrs_env: str | None = None
    pool_size: Annotated[int, Field(gt=0)] = 5
    sink_batch_size: Annotated[int, Field(gt=0)] = 100
    sink_max_rows_per_statement: Annotated[int | None, Field(gt=0)] = None
    sink_max_parameters_per_statement: Annotated[int, Field(gt=0)] = 32_000

    def connection(self) -> PostgresConnectionConfig:
        auth = PostgresAuthConfig(
            username=self.username,
            username_env=self.username_env,
            password=self.password,
            password_env=self.password_env,
            password_file=self.password_file,
        )
        tls = PostgresTLSConfig(
            sslmode=self.sslmode,
            root_cert_file=self.sslrootcert,
            root_cert_env=self.sslrootcert_env,
            cert_file=self.sslcert,
            cert_env=self.sslcert_env,
            key_file=self.sslkey,
            key_env=self.sslkey_env,
            key_password=self.sslpassword,
            key_password_env=self.sslpassword_env,
            key_password_file=self.sslpassword_file,
        )
        return PostgresConnectionConfig(
            dsn=self.database_url,
            dsn_env=self.database_url_env,
            auth=(None if auth == PostgresAuthConfig() else auth),
            tls=tls,
            connect_timeout_s=self.connect_timeout_s,
            application_name=self.application_name,
            application_name_env=self.application_name_env,
            target_session_attrs=self.target_session_attrs,
            target_session_attrs_env=self.target_session_attrs_env,
        )


class PostgresPluginConfig(BaseModel):
    """Shared plugin-level settings that can be embedded in app config."""

    dsn: str | None = Field(
        default=None,
        description="PostgreSQL DSN, for example postgresql://...",
    )
    dsn_env: str | None = Field(
        default=None, description="Env var name holding the PostgreSQL DSN."
    )
    username: str | None = Field(default=None)
    username_env: str | None = Field(default=None)
    password: SecretStr | None = Field(default=None)
    password_env: str | None = Field(default=None)
    password_file: str | None = Field(default=None)
    sslmode: str = Field(default="verify-full")
    sslrootcert: str | None = Field(default=None)
    sslrootcert_env: str | None = Field(default=None)
    sslcert: str | None = Field(default=None)
    sslcert_env: str | None = Field(default=None)
    sslkey: str | None = Field(default=None)
    sslkey_env: str | None = Field(default=None)
    sslpassword: SecretStr | None = Field(default=None)
    sslpassword_env: str | None = Field(default=None)
    sslpassword_file: str | None = Field(default=None)
    connect_timeout_s: Annotated[int | None, Field(gt=0)] = None
    application_name: str | None = Field(default=None)
    application_name_env: str | None = Field(default=None)
    target_session_attrs: str | None = Field(default=None)
    target_session_attrs_env: str | None = Field(default=None)
    table: str = Field(description="Target table or schema-qualified table name.")
    pool_size: Annotated[int, Field(gt=0)] = 1
    max_rows_per_statement: Annotated[int | None, Field(gt=0)] = None
    max_parameters_per_statement: Annotated[int, Field(gt=0)] = 32_000

    def connection(self) -> PostgresConnectionConfig:
        auth = PostgresAuthConfig(
            username=self.username,
            username_env=self.username_env,
            password=self.password,
            password_env=self.password_env,
            password_file=self.password_file,
        )
        tls = PostgresTLSConfig(
            sslmode=self.sslmode,
            root_cert_file=self.sslrootcert,
            root_cert_env=self.sslrootcert_env,
            cert_file=self.sslcert,
            cert_env=self.sslcert_env,
            key_file=self.sslkey,
            key_env=self.sslkey_env,
            key_password=self.sslpassword,
            key_password_env=self.sslpassword_env,
            key_password_file=self.sslpassword_file,
        )
        return PostgresConnectionConfig(
            dsn=self.dsn,
            dsn_env=self.dsn_env,
            auth=(None if auth == PostgresAuthConfig() else auth),
            tls=tls,
            connect_timeout_s=self.connect_timeout_s,
            application_name=self.application_name,
            application_name_env=self.application_name_env,
            target_session_attrs=self.target_session_attrs,
            target_session_attrs_env=self.target_session_attrs_env,
        )


__all__ = [
    "PostgresAuthConfig",
    "PostgresConfig",
    "PostgresConnectionConfig",
    "PostgresPluginConfig",
    "PostgresTLSConfig",
]
