"""Configuration models for the Kafka plugin package."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    schema_registry_password: str | None = None
    schema_registry_timeout_s: Annotated[float, Field(gt=0)] = 5.0

    def topic(self, name: str) -> str:
        return f"{self.env}.{name}"


class KafkaPluginConfig(BaseModel):
    """Shared plugin-level settings that can be embedded in app config."""

    bootstrap_servers: str = Field(description="Kafka bootstrap servers.")
    topic: str | None = Field(default=None, description="Default topic for source or sink wiring.")
    schema_registry_url: str | None = Field(
        default=None,
        description="Optional Confluent-compatible schema registry URL.",
    )
    schema_registry_username: str | None = Field(default=None)
    schema_registry_password: str | None = Field(default=None)
    schema_registry_timeout_s: Annotated[float, Field(gt=0)] = 5.0
