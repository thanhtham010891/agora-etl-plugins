"""Configuration models for the PostgreSQL plugin package."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresConfig(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        extra="ignore",
        populate_by_name=True,
    )

    database_url: str = Field(
        default="postgresql://localhost:5432/agora",
        alias="DATABASE_URL",
    )
    pool_size: Annotated[int, Field(gt=0)] = 5
    sink_batch_size: Annotated[int, Field(gt=0)] = 100
    sink_max_rows_per_statement: Annotated[int | None, Field(gt=0)] = None
    sink_max_parameters_per_statement: Annotated[int, Field(gt=0)] = 32_000


class PostgresPluginConfig(BaseModel):
    """Shared plugin-level settings that can be embedded in app config."""

    dsn: str = Field(description="PostgreSQL DSN, for example postgresql://...")
    table: str = Field(description="Target table or schema-qualified table name.")
    pool_size: Annotated[int, Field(gt=0)] = 1
    max_rows_per_statement: Annotated[int | None, Field(gt=0)] = None
    max_parameters_per_statement: Annotated[int, Field(gt=0)] = 32_000
