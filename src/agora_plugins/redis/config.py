"""Configuration models for the Redis plugin package."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RedisPluginConfig(BaseModel):
    """Shared plugin-level settings that can be embedded in app config."""

    url: str = Field(default="redis://localhost:6379", description="Redis connection URL.")
    prefix: str = Field(default="agora:", description="Key prefix for the plugin namespace.")
