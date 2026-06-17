"""DistributedConfig — pydantic-settings for Agora worker coordination."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DistributedConfig(BaseSettings):
    """Configuration for distributed worker coordination.

    All fields are readable from environment variables prefixed with
    ``AGORA_DISTRIBUTED_``.

    Example::

        AGORA_DISTRIBUTED_REDIS_URL=redis://redis:6379
        AGORA_DISTRIBUTED_LEASE_TTL_SECONDS=300
        AGORA_DISTRIBUTED_HEARTBEAT_INTERVAL=30
        AGORA_DISTRIBUTED_REDLOCK_REDIS_URLS='["redis://r1:6379","redis://r2:6379","redis://r3:6379"]'
    """

    model_config = SettingsConfigDict(env_prefix="AGORA_DISTRIBUTED_")

    redis_url: str = Field(default="redis://localhost:6379")
    lease_ttl_seconds: int = Field(default=300, gt=0)
    heartbeat_interval: int = Field(default=30, gt=0)
    key_prefix: str = Field(default="agora:distributed:")
    fallback_to_local: bool = Field(default=False)
    redlock_redis_urls: list[str] | None = Field(default=None)
    redlock_clock_drift_factor: float = Field(default=0.01, ge=0)
