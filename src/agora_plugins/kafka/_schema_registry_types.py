"""Shared Schema Registry contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

SchemaAutoRegisterMode = Literal["disabled", "missing_subject", "always"]


@dataclass(frozen=True, slots=True)
class RegisteredSchema:
    """Schema metadata returned by a registry."""

    schema_id: int
    schema: str
    schema_type: str = "AVRO"
    subject: str | None = None
    version: int | None = None


class SchemaRegistryClient(Protocol):
    """Backend-agnostic async schema registry client."""

    async def get_schema(self, schema_id: int) -> RegisteredSchema: ...

    async def get_latest_schema(self, subject: str) -> RegisteredSchema: ...

    async def register_schema(
        self,
        subject: str,
        schema: str,
        *,
        schema_type: str = "AVRO",
    ) -> RegisteredSchema: ...
