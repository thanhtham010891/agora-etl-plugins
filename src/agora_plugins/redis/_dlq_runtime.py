"""Runtime seams for Redis DLQ keyspace and record mutation flows."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from agora_plugins.redis._dlq_keyspace import (
    UPSERT_INDEX_PIPELINE_FIELD,
    UPSERT_INDEX_PIPELINE_STAGE_FIELD,
    UPSERT_INDEX_STAGE_FIELD,
)
from agora_plugins.redis._dlq_payloads import record_to_hash as _record_to_hash

if TYPE_CHECKING:
    from agora.core.dlq import DLQRecord


class RedisDLQKeyspaceCompat:
    """Compatibility accessors over the shared DLQ keyspace collaborator."""

    @property
    def _index_key(self) -> str:
        return self._keyspace.index_key()

    def _pipeline_index_key(self, pipeline_id: str) -> str:
        return self._keyspace.pipeline_index_key(pipeline_id)

    def _stage_index_key(self, stage: str) -> str:
        return self._keyspace.stage_index_key(stage)

    def _pipeline_stage_index_key(self, pipeline_id: str, stage: str) -> str:
        return self._keyspace.pipeline_stage_index_key(pipeline_id, stage)

    def _index_key_for_prefix(self, key_prefix: str) -> str:
        return self._keyspace.index_key(key_prefix=key_prefix)

    def _pipeline_index_key_for_prefix(self, key_prefix: str, pipeline_id: str) -> str:
        return self._keyspace.pipeline_index_key(pipeline_id, key_prefix=key_prefix)

    def _stage_index_key_for_prefix(self, key_prefix: str, stage: str) -> str:
        return self._keyspace.stage_index_key(stage, key_prefix=key_prefix)

    def _pipeline_stage_index_key_for_prefix(
        self,
        key_prefix: str,
        pipeline_id: str,
        stage: str,
    ) -> str:
        return self._keyspace.pipeline_stage_index_key(
            pipeline_id,
            stage,
            key_prefix=key_prefix,
        )


class RedisDLQSinkRuntime(RedisDLQKeyspaceCompat):
    """Owns sink-side Redis hash/index mutation helpers."""

    def _secondary_index_keys(
        self, record: DLQRecord, *, key_prefix: str | None = None
    ) -> set[str]:
        return self._keyspace.secondary_index_keys(record, key_prefix=key_prefix)

    def _secondary_index_keys_from_payload(
        self,
        payload: dict[str, str],
        *,
        key_prefix: str | None = None,
    ) -> set[str]:
        return self._keyspace.secondary_index_keys_from_payload(
            payload,
            key_prefix=key_prefix,
        )

    def _record_key(self, record: DLQRecord) -> str:
        return self._keyspace.record_key(record)

    def _existing_record_key(self, record: DLQRecord) -> str:
        return self._keyspace.existing_record_key(record)

    @staticmethod
    async def _record_payload(client: Any, record_key: str) -> dict[str, str]:
        return cast("dict[str, str]", await client.hgetall(record_key))

    async def _write_record(self, client: Any, record: DLQRecord, record_key: str) -> bool:
        index_prefix = self._index_prefix_for_record_key(record_key)
        payload = self._record_hash(record, record_key, index_prefix=index_prefix)
        secondary_index_keys = self._secondary_index_keys(record, key_prefix=index_prefix)
        if self._upsert_script is not None and index_prefix == self._storage_key_prefix:
            ordered_secondary_keys = self._ordered_secondary_index_keys(record)
            inserted = await self._upsert_script(
                keys=[record_key, self._index_key, *ordered_secondary_keys],
                args=self._flatten_hash_mapping(payload),
            )
            return int(inserted or 0) == 1

        existing_payload = await self._record_payload(client, record_key)
        should_index = not existing_payload
        async with client.pipeline(transaction=not self._redis_cluster) as pipe:
            pipe.hset(record_key, mapping=payload)
            if should_index:
                pipe.rpush(self._index_key_for_prefix(index_prefix), record_key)
                for index_key in secondary_index_keys:
                    pipe.rpush(index_key, record_key)
            else:
                old_index_keys = self._secondary_index_keys_from_payload(existing_payload)
                for index_key in old_index_keys - secondary_index_keys:
                    pipe.lrem(index_key, 0, record_key)
                for index_key in secondary_index_keys:
                    pipe.lrem(index_key, 0, record_key)
                    pipe.rpush(index_key, record_key)
            await pipe.execute()
        return should_index

    async def _acknowledge_record(self, client: Any, record: DLQRecord, record_key: str) -> None:
        existing_payload = await self._record_payload(client, record_key)
        index_prefix = self._index_prefix_for_record_key(record_key)
        if (
            self._acknowledge_script is not None
            and index_prefix == self._storage_key_prefix
            and (not self._redis_cluster or bool(existing_payload.get(UPSERT_INDEX_PIPELINE_FIELD)))
        ):
            await self._acknowledge_script(
                keys=[record_key, self._index_key, *self._ordered_secondary_index_keys(record)],
                args=[],
            )
            return

        secondary_index_keys = (
            self._secondary_index_keys_from_payload(existing_payload, key_prefix=index_prefix)
            if existing_payload
            else self._secondary_index_keys(record, key_prefix=index_prefix)
        )
        async with client.pipeline(transaction=not self._redis_cluster) as pipe:
            pipe.delete(record_key)
            pipe.lrem(self._index_key_for_prefix(index_prefix), 0, record_key)
            for index_key in secondary_index_keys:
                pipe.lrem(index_key, 0, record_key)
            await pipe.execute()

    def _record_hash(
        self,
        record: DLQRecord,
        record_key: str,
        *,
        index_prefix: str,
    ) -> dict[str, str]:
        payload = _record_to_hash(record, payload_policy=self._payload_policy)
        payload["storage_key"] = record_key
        payload.update(self._index_metadata(record, index_prefix=index_prefix))
        return payload

    def _index_metadata(self, record: DLQRecord, *, index_prefix: str) -> dict[str, str]:
        pipeline_index_key, stage_index_key, pipeline_stage_index_key = (
            self._ordered_secondary_index_keys(record, key_prefix=index_prefix)
        )
        return {
            UPSERT_INDEX_PIPELINE_FIELD: pipeline_index_key,
            UPSERT_INDEX_STAGE_FIELD: stage_index_key,
            UPSERT_INDEX_PIPELINE_STAGE_FIELD: pipeline_stage_index_key,
        }

    def _ordered_secondary_index_keys(
        self,
        record: DLQRecord,
        *,
        key_prefix: str | None = None,
    ) -> tuple[str, str, str]:
        return self._keyspace.ordered_secondary_index_keys(record, key_prefix=key_prefix)

    def _index_prefix_for_record_key(self, record_key: str) -> str:
        return self._keyspace.index_prefix_for_record_key(record_key)

    @staticmethod
    def _flatten_hash_mapping(mapping: dict[str, str]) -> list[str]:
        flattened: list[str] = [str(len(mapping))]
        for key, value in mapping.items():
            flattened.extend((key, value))
        return flattened

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisDLQSink.open() was not called")
        return self._client


class RedisDLQSourceRuntime(RedisDLQKeyspaceCompat):
    """Owns source-side index resolution and client access helpers."""

    async def _resolve_index_key(self, client: Any) -> str:
        return await self._keyspace.resolve_index_key(
            client,
            pipeline_id=self._pipeline_id,
            stage=self._stage,
        )

    def _preferred_index_key(self, key_prefix: str) -> str:
        return self._keyspace.preferred_index_key(
            key_prefix=key_prefix,
            pipeline_id=self._pipeline_id,
            stage=self._stage,
        )

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("RedisDLQSource.open() was not called")
        return self._client
