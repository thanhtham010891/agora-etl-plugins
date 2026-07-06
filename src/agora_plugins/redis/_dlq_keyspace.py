"""Shared keyspace helpers for Redis-backed DLQ sink/source."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    from collections.abc import Mapping

    from agora.core.dlq import DLQRecord


UPSERT_INDEX_PIPELINE_FIELD = "__pipeline_index_key"
UPSERT_INDEX_STAGE_FIELD = "__stage_index_key"
UPSERT_INDEX_PIPELINE_STAGE_FIELD = "__pipeline_stage_index_key"


def index_part(value: str) -> str:
    return quote(value, safe="")


class RedisDLQKeyspace:
    """Centralizes Redis DLQ key/index naming and storage-key resolution."""

    def __init__(self, *, key_prefix: str, storage_key_prefix: str) -> None:
        self._key_prefix = key_prefix
        self._storage_key_prefix = storage_key_prefix

    def index_key(self, *, key_prefix: str | None = None) -> str:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return f"{prefix}:__index__"

    def pipeline_index_key(self, pipeline_id: str, *, key_prefix: str | None = None) -> str:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return f"{prefix}:__index__:pipeline:{index_part(pipeline_id)}"

    def stage_index_key(self, stage: str, *, key_prefix: str | None = None) -> str:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return f"{prefix}:__index__:stage:{index_part(stage)}"

    def pipeline_stage_index_key(
        self,
        pipeline_id: str,
        stage: str,
        *,
        key_prefix: str | None = None,
    ) -> str:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return f"{prefix}:__index__:pipeline_stage:{index_part(pipeline_id)}:{index_part(stage)}"

    def secondary_index_keys(
        self,
        record: DLQRecord,
        *,
        key_prefix: str | None = None,
    ) -> set[str]:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return {
            self.pipeline_index_key(record.pipeline_id, key_prefix=prefix),
            self.stage_index_key(record.stage, key_prefix=prefix),
            self.pipeline_stage_index_key(record.pipeline_id, record.stage, key_prefix=prefix),
        }

    def secondary_index_keys_from_payload(
        self,
        payload: Mapping[str, str],
        *,
        key_prefix: str | None = None,
    ) -> set[str]:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        explicit_index_keys = {
            payload.get(UPSERT_INDEX_PIPELINE_FIELD) or "",
            payload.get(UPSERT_INDEX_STAGE_FIELD) or "",
            payload.get(UPSERT_INDEX_PIPELINE_STAGE_FIELD) or "",
        } - {""}
        if explicit_index_keys:
            return explicit_index_keys
        pipeline_id = payload.get("pipeline_id")
        stage = payload.get("stage")
        if not pipeline_id or not stage:
            return set()
        return {
            self.pipeline_index_key(pipeline_id, key_prefix=prefix),
            self.stage_index_key(stage, key_prefix=prefix),
            self.pipeline_stage_index_key(pipeline_id, stage, key_prefix=prefix),
        }

    def ordered_secondary_index_keys(
        self,
        record: DLQRecord,
        *,
        key_prefix: str | None = None,
    ) -> tuple[str, str, str]:
        prefix = self._storage_key_prefix if key_prefix is None else key_prefix
        return (
            self.pipeline_index_key(record.pipeline_id, key_prefix=prefix),
            self.stage_index_key(record.stage, key_prefix=prefix),
            self.pipeline_stage_index_key(record.pipeline_id, record.stage, key_prefix=prefix),
        )

    def index_prefix_for_record_key(self, record_key: str) -> str:
        if record_key.startswith(f"{self._storage_key_prefix}:"):
            return self._storage_key_prefix
        return self._key_prefix

    def record_key(self, record: DLQRecord) -> str:
        storage_id = record._storage_id
        if isinstance(storage_id, str) and storage_id:
            return storage_id
        return (
            f"{self._storage_key_prefix}:{record.pipeline_id}:{record.run_id}:"
            f"{record.stage}:{record.created_at.isoformat()}:{uuid.uuid4().hex}"
        )

    def existing_record_key(self, record: DLQRecord) -> str:
        storage_id = record._storage_id
        if isinstance(storage_id, str) and storage_id:
            return storage_id
        raise ValueError("RedisDLQSink replay/acknowledge requires a persisted storage key.")

    def preferred_index_key(
        self,
        *,
        key_prefix: str,
        pipeline_id: str | None = None,
        stage: str | None = None,
    ) -> str:
        if pipeline_id is not None and stage is not None:
            return self.pipeline_stage_index_key(pipeline_id, stage, key_prefix=key_prefix)
        if pipeline_id is not None:
            return self.pipeline_index_key(pipeline_id, key_prefix=key_prefix)
        if stage is not None:
            return self.stage_index_key(stage, key_prefix=key_prefix)
        return self.index_key(key_prefix=key_prefix)

    async def resolve_index_key(
        self,
        client: Any,
        *,
        pipeline_id: str | None = None,
        stage: str | None = None,
    ) -> str:
        preferred = self.preferred_index_key(
            key_prefix=self._storage_key_prefix,
            pipeline_id=pipeline_id,
            stage=stage,
        )
        if await client.exists(preferred):
            return preferred
        legacy_preferred = self.preferred_index_key(
            key_prefix=self._key_prefix,
            pipeline_id=pipeline_id,
            stage=stage,
        )
        if legacy_preferred != preferred and await client.exists(legacy_preferred):
            return legacy_preferred
        primary_index = self.index_key()
        if preferred == primary_index:
            return preferred
        if await client.exists(primary_index):
            return primary_index
        legacy_primary = self.index_key(key_prefix=self._key_prefix)
        if legacy_primary != primary_index and await client.exists(legacy_primary):
            return legacy_primary
        return primary_index
