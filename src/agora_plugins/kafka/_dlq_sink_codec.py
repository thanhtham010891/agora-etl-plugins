"""Envelope/header wiring for Kafka DLQ sink records."""

from __future__ import annotations

import json
from typing import Any, cast


class KafkaDLQSinkCodec:
    """Builds Kafka DLQ sink envelopes plus serializer/key/header functions."""

    def __init__(
        self,
        *,
        key_fn: Any,
        payload_policy: Any,
        encode_envelope: Any,
        record_storage_key: Any,
        record_headers: Any,
        payload_to_record: Any,
    ) -> None:
        self._key_fn = key_fn
        self._payload_policy = payload_policy
        self._encode_envelope = encode_envelope
        self._record_storage_key = record_storage_key
        self._record_headers = record_headers
        self._payload_to_record = payload_to_record

    def serialize(self, envelope: dict[str, Any]) -> bytes:
        return json.dumps(
            envelope,
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")

    def partition_key(self, envelope: dict[str, Any]) -> bytes:
        return str(envelope["storage_key"]).encode("utf-8")

    def headers(self, envelope: dict[str, Any]) -> list[tuple[str, bytes]]:
        record = (
            self._payload_to_record(cast("dict[str, Any]", envelope["payload"]))
            if envelope.get("payload") is not None
            else None
        )
        return self._record_headers(
            str(envelope["storage_key"]),
            record,
            operation=str(envelope["op"]),
        )

    def build_upsert_envelope(self, record: Any) -> dict[str, Any]:
        storage_key = self._record_storage_key(record, self._key_fn)
        object.__setattr__(record, "_storage_id", storage_key)
        return self._encode_envelope(
            storage_key,
            operation="put",
            record=record,
            payload_policy=self._payload_policy,
        )

    def build_delete_envelope(self, record: Any) -> dict[str, Any]:
        storage_key = self._record_storage_key(record, self._key_fn)
        object.__setattr__(record, "_storage_id", storage_key)
        return self._encode_envelope(storage_key, operation="delete")
