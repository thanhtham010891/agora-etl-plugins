from __future__ import annotations

import json
from typing import Any

import pytest

from agora_plugins.kafka.schema_registry import (
    AvroSchemaRegistryDeserializer,
    AvroSchemaRegistrySerializer,
    ConfluentSchemaRegistryClient,
    RegisteredSchema,
)


class _FakeRegistryClient:
    def __init__(self) -> None:
        self.register_calls: list[tuple[str, str, str]] = []
        self.get_schema_calls: list[int] = []
        self.get_latest_calls: list[str] = []
        self._schemas_by_id: dict[int, RegisteredSchema] = {}
        self._next_id = 1

    async def get_schema(self, schema_id: int) -> RegisteredSchema:
        self.get_schema_calls.append(schema_id)
        return self._schemas_by_id[schema_id]

    async def get_latest_schema(self, subject: str) -> RegisteredSchema:
        self.get_latest_calls.append(subject)
        return next(iter(self._schemas_by_id.values()))

    async def register_schema(
        self,
        subject: str,
        schema: str,
        *,
        schema_type: str = "AVRO",
    ) -> RegisteredSchema:
        self.register_calls.append((subject, schema, schema_type))
        registered = RegisteredSchema(
            schema_id=self._next_id,
            schema=schema,
            schema_type=schema_type,
            subject=subject,
        )
        self._schemas_by_id[self._next_id] = registered
        self._next_id += 1
        return registered


@pytest.mark.asyncio
async def test_avro_schema_registry_serializer_and_deserializer_round_trip() -> None:
    schema = {
        "type": "record",
        "name": "OrderCreated",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "int"},
        ],
    }
    registry = _FakeRegistryClient()
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-value",
        schema=schema,
    )
    deserializer = AvroSchemaRegistryDeserializer[dict[str, Any]](registry_client=registry)

    await serializer.open()
    await deserializer.open()
    payload = await serializer({"order_id": "o-1", "amount": 42})
    decoded = await deserializer(payload)

    assert payload[0] == 0
    assert decoded == {"order_id": "o-1", "amount": 42}
    assert registry.register_calls[0][0] == "orders-value"
    assert registry.get_schema_calls == [1]


@pytest.mark.asyncio
async def test_avro_deserializer_caches_writer_schema_by_id() -> None:
    schema = json.dumps(
        {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "slug", "type": "string"}],
        }
    )
    registry = _FakeRegistryClient()
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="events-value",
        schema=schema,
    )
    deserializer = AvroSchemaRegistryDeserializer[dict[str, Any]](registry_client=registry)

    await serializer.open()
    first_payload = await serializer({"slug": "a"})
    second_payload = await serializer({"slug": "b"})

    await deserializer(first_payload)
    await deserializer(second_payload)

    assert registry.get_schema_calls == [1]


@pytest.mark.asyncio
async def test_avro_serializer_validates_latest_schema_when_auto_register_is_disabled() -> None:
    registry = _FakeRegistryClient()
    registry._schemas_by_id[7] = RegisteredSchema(
        schema_id=7,
        subject="orders-value",
        schema=json.dumps({"type": "record", "name": "X", "fields": []}),
    )
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-value",
        schema={"type": "record", "name": "Y", "fields": []},
        auto_register=False,
    )

    with pytest.raises(ValueError, match="does not match"):
        await serializer.open()


@pytest.mark.asyncio
async def test_confluent_schema_registry_client_sends_expected_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests_seen: list[tuple[str, str, dict[str, str], bytes | None]] = []

    class _Response:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def _fake_urlopen(req, timeout: float):
        requests_seen.append((req.get_method(), req.full_url, dict(req.header_items()), req.data))
        assert timeout == 2.5
        if req.full_url.endswith("/versions/latest"):
            return _Response(
                {
                    "id": 7,
                    "subject": "orders-value",
                    "schema": '{"type":"record","name":"Event","fields":[]}',
                }
            )
        return _Response({"id": 9})

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    client = ConfluentSchemaRegistryClient(
        "http://registry:8081",
        username="user",
        password="pass",
        timeout_s=2.5,
        headers={"X-Test": "1"},
    )

    latest = await client.get_latest_schema("orders-value")
    registered = await client.register_schema(
        "orders-value",
        '{"type":"record","name":"Event","fields":[]}',
    )

    assert latest.schema_id == 7
    assert registered.schema_id == 9
    assert requests_seen[0][0] == "GET"
    assert requests_seen[0][1].endswith("/subjects/orders-value/versions/latest")
    assert requests_seen[0][2]["Authorization"].startswith("Basic ")
    assert requests_seen[1][0] == "POST"
    assert requests_seen[1][1].endswith("/subjects/orders-value/versions")
    assert b'"schemaType": "AVRO"' in requests_seen[1][3]


@pytest.mark.asyncio
async def test_schema_registry_client_url_encodes_subject_segments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests_seen: list[str] = []

    class _Response:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def _fake_urlopen(req, timeout: float):
        del timeout
        requests_seen.append(req.full_url)
        return _Response(
            {
                "id": 7,
                "subject": "orders/dev-value",
                "schema": '{"type":"record","name":"Event","fields":[]}',
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    client = ConfluentSchemaRegistryClient("http://registry:8081")
    await client.get_latest_schema("orders/dev-value")

    assert requests_seen == ["http://registry:8081/subjects/orders%2Fdev-value/versions/latest"]
