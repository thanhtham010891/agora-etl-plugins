from __future__ import annotations

import json
import sys
import types
from typing import Any, ClassVar
from urllib import error

import pytest

from agora_plugins.kafka.schema_registry import (
    AvroSchemaRegistryDeserializer,
    AvroSchemaRegistrySerializer,
    ConfluentSchemaRegistryClient,
    JsonSchemaRegistryDeserializer,
    JsonSchemaRegistrySerializer,
    PooledConfluentSchemaRegistryClient,
    ProtobufSchemaRegistryDeserializer,
    ProtobufSchemaRegistrySerializer,
    RegisteredSchema,
    _resolve_proto_message_full_name,
)


class _FakeRegistryClient:
    def __init__(self) -> None:
        self.register_calls: list[tuple[str, str, str]] = []
        self.get_schema_calls: list[int] = []
        self.get_latest_calls: list[str] = []
        self._schemas_by_id: dict[int, RegisteredSchema] = {}
        self._latest_by_subject: dict[str, RegisteredSchema] = {}
        self._missing_subjects: set[str] = set()
        self._next_id = 1

    async def get_schema(self, schema_id: int) -> RegisteredSchema:
        self.get_schema_calls.append(schema_id)
        return self._schemas_by_id[schema_id]

    async def get_latest_schema(self, subject: str) -> RegisteredSchema:
        self.get_latest_calls.append(subject)
        if subject in self._missing_subjects:
            raise _FakeSchemaRegistryNotFoundError(subject)
        if subject in self._latest_by_subject:
            return self._latest_by_subject[subject]
        if not self._schemas_by_id:
            raise _FakeSchemaRegistryNotFoundError(subject)
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
        self._latest_by_subject[subject] = registered
        self._next_id += 1
        return registered


class _FakeSchemaRegistryNotFoundError(RuntimeError):
    status_code = 404

    def __init__(self, subject: str) -> None:
        super().__init__(f"subject {subject!r} not found")


class _FakePooledResponse:
    def __init__(
        self,
        payload: Any,
        *,
        status_code: int = 200,
        reason_phrase: str = "OK",
        text: str | None = None,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.reason_phrase = reason_phrase
        self.text = text if text is not None else json.dumps(payload)

    def json(self) -> Any:
        return self._payload


class _FakePooledAsyncClient:
    instances: ClassVar[list[_FakePooledAsyncClient]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.requests: list[tuple[str, str, Any, dict[str, str]]] = []
        self.closed = False
        self.responses: list[_FakePooledResponse] = []
        self.instances.append(self)

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        headers: dict[str, str] | None = None,
    ) -> _FakePooledResponse:
        self.requests.append((method, path, json, dict(headers or {})))
        return self.responses.pop(0)

    async def aclose(self) -> None:
        self.closed = True


def _make_protobuf_message_types(
    schema_name: str = "order_created.proto",
) -> dict[str, type[Any]]:
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = schema_name
    file_proto.package = "agora.test"
    file_proto.syntax = "proto3"

    order_type = file_proto.message_type.add()
    order_type.name = "OrderCreated"

    order_id_field = order_type.field.add()
    order_id_field.name = "order_id"
    order_id_field.number = 1
    order_id_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    order_id_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    amount_field = order_type.field.add()
    amount_field.name = "amount"
    amount_field.number = 2
    amount_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    amount_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT32

    customer_type = file_proto.message_type.add()
    customer_type.name = "CustomerCreated"

    customer_id_field = customer_type.field.add()
    customer_id_field.name = "customer_id"
    customer_id_field.number = 1
    customer_id_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    customer_id_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    return {
        name: message_factory.GetMessageClass(pool.FindMessageTypeByName(f"agora.test.{name}"))
        for name in ("OrderCreated", "CustomerCreated")
    }


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
async def test_avro_deserializer_evicts_old_writer_schemas_when_cache_is_bounded() -> None:
    schema_one = json.dumps(
        {
            "type": "record",
            "name": "EventOne",
            "fields": [{"name": "slug", "type": "string"}],
        }
    )
    schema_two = json.dumps(
        {
            "type": "record",
            "name": "EventTwo",
            "fields": [{"name": "slug", "type": "string"}],
        }
    )
    registry = _FakeRegistryClient()
    serializer_one = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="events-one-value",
        schema=schema_one,
        auto_register="always",
    )
    serializer_two = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="events-two-value",
        schema=schema_two,
        auto_register="always",
    )
    deserializer = AvroSchemaRegistryDeserializer[dict[str, Any]](
        registry_client=registry,
        schema_cache_max_entries=1,
    )

    await serializer_one.open()
    await serializer_two.open()
    first_payload = await serializer_one({"slug": "a"})
    second_payload = await serializer_two({"slug": "b"})

    await deserializer(first_payload)
    await deserializer(second_payload)
    await deserializer(first_payload)

    assert registry.get_schema_calls == [1, 2, 1]
    assert list(deserializer._writer_schemas.keys()) == [1]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_schema_registry_default_auto_register_only_bootstraps_missing_subject() -> None:
    schema = {"type": "record", "name": "OrderCreated", "fields": []}
    schema_text = json.dumps(schema, separators=(",", ":"))
    registry = _FakeRegistryClient()
    registry._latest_by_subject["orders-value"] = RegisteredSchema(
        schema_id=7,
        subject="orders-value",
        schema=schema_text,
    )
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-value",
        schema=schema,
    )

    await serializer.open()

    assert registry.get_latest_calls == ["orders-value"]
    assert registry.register_calls == []


def test_schema_registry_deserializers_reject_non_positive_cache_limits() -> None:
    registry = _FakeRegistryClient()

    with pytest.raises(ValueError, match="schema_cache_max_entries"):
        AvroSchemaRegistryDeserializer[dict[str, Any]](
            registry_client=registry,
            schema_cache_max_entries=0,
        )

    with pytest.raises(ValueError, match="schema_cache_max_entries"):
        JsonSchemaRegistryDeserializer[dict[str, Any]](
            registry_client=registry,
            schema_cache_max_entries=0,
        )

    with pytest.raises(ValueError, match="schema_cache_max_entries"):
        ProtobufSchemaRegistryDeserializer[dict[str, Any]](
            registry_client=registry,
            message_type=dict,
            schema_cache_max_entries=0,
        )


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
        auto_register="disabled",
    )

    with pytest.raises(ValueError, match="does not match"):
        await serializer.open()


@pytest.mark.asyncio
async def test_avro_serializer_uses_parsing_canonical_form_for_schema_comparison() -> None:
    registry = _FakeRegistryClient()
    registry._schemas_by_id[7] = RegisteredSchema(
        schema_id=7,
        subject="orders-value",
        schema=json.dumps(
            {
                "type": "record",
                "name": "OrderCreated",
                "doc": "Human-readable docs must not affect Avro canonical identity.",
                "fields": [
                    {
                        "name": "order_id",
                        "type": "string",
                        "doc": "Docs on fields are also ignored by parsing canonical form.",
                    }
                ],
            }
        ),
    )
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-value",
        schema={
            "fields": [{"type": "string", "name": "order_id"}],
            "name": "OrderCreated",
            "type": "record",
        },
        auto_register="disabled",
    )

    await serializer.open()


@pytest.mark.asyncio
async def test_schema_registry_auto_register_bool_is_deprecated_but_supported() -> None:
    schema = {"type": "record", "name": "OrderCreated", "fields": []}
    schema_text = json.dumps(schema, separators=(",", ":"))
    registry = _FakeRegistryClient()
    registry._latest_by_subject["orders-value"] = RegisteredSchema(
        schema_id=7,
        subject="orders-value",
        schema=schema_text,
    )

    with pytest.warns(DeprecationWarning, match="bool auto_register is deprecated"):
        serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
            registry_client=registry,
            subject="orders-value",
            schema=schema,
            auto_register=False,
        )

    await serializer.open()

    assert registry.get_latest_calls == ["orders-value"]
    assert registry.register_calls == []


@pytest.mark.asyncio
async def test_schema_registry_missing_subject_mode_registers_only_when_subject_is_missing() -> (
    None
):
    schema = {"type": "record", "name": "OrderCreated", "fields": []}
    registry = _FakeRegistryClient()
    registry._missing_subjects.add("orders-value")
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-value",
        schema=schema,
        auto_register="missing_subject",
    )

    await serializer.open()

    assert registry.get_latest_calls == ["orders-value"]
    assert registry.register_calls[0][0] == "orders-value"
    assert registry.register_calls[0][2] == "AVRO"
    assert json.loads(registry.register_calls[0][1]) == schema


@pytest.mark.asyncio
async def test_schema_registry_missing_subject_mode_reuses_matching_subject() -> None:
    schema = {"type": "record", "name": "OrderCreated", "fields": []}
    schema_text = json.dumps(schema, separators=(",", ":"))
    registry = _FakeRegistryClient()
    registry._latest_by_subject["orders-value"] = RegisteredSchema(
        schema_id=7,
        subject="orders-value",
        schema=schema_text,
    )
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-value",
        schema=schema,
        auto_register="missing_subject",
    )

    await serializer.open()
    payload = await serializer({})

    assert payload[:5] == b"\x00\x00\x00\x00\x07"
    assert registry.get_latest_calls == ["orders-value"]
    assert registry.register_calls == []


@pytest.mark.asyncio
async def test_schema_registry_missing_subject_mode_rejects_existing_mismatch() -> None:
    registry = _FakeRegistryClient()
    registry._latest_by_subject["orders-value"] = RegisteredSchema(
        schema_id=7,
        subject="orders-value",
        schema=json.dumps({"type": "record", "name": "Existing", "fields": []}),
    )
    serializer = AvroSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-value",
        schema={"type": "record", "name": "New", "fields": []},
        auto_register="missing_subject",
    )

    with pytest.raises(ValueError, match="does not match"):
        await serializer.open()

    assert registry.register_calls == []


@pytest.mark.asyncio
async def test_json_schema_registry_missing_subject_mode_registers_json_schema() -> None:
    registry = _FakeRegistryClient()
    registry._missing_subjects.add("orders-json-value")
    serializer = JsonSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-json-value",
        schema={"type": "object"},
        auto_register="missing_subject",
        validate_payload=False,
    )

    await serializer.open()

    assert registry.register_calls == [("orders-json-value", '{"type":"object"}', "JSON")]


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

    def _fake_urlopen(req, timeout: float, context=None):
        requests_seen.append((req.get_method(), req.full_url, dict(req.header_items()), req.data))
        assert timeout == 2.5
        assert context is None
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

    def _fake_urlopen(req, timeout: float, context=None):
        del timeout
        assert context is None
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


@pytest.mark.asyncio
async def test_schema_registry_client_manages_subject_compatibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests_seen: list[tuple[str, str, bytes | None]] = []

    class _Response:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def _fake_urlopen(req, timeout: float, context=None):
        del timeout, context
        requests_seen.append((req.get_method(), req.full_url, req.data))
        if req.get_method() == "PUT":
            return _Response({"compatibility": "BACKWARD"})
        return _Response({"compatibilityLevel": "BACKWARD"})

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    client = ConfluentSchemaRegistryClient("http://registry:8081")

    assert await client.set_subject_compatibility("orders/dev-value", "BACKWARD") == "BACKWARD"
    assert await client.get_subject_compatibility("orders/dev-value") == "BACKWARD"
    assert requests_seen == [
        (
            "PUT",
            "http://registry:8081/config/orders%2Fdev-value",
            b'{"compatibility": "BACKWARD"}',
        ),
        ("GET", "http://registry:8081/config/orders%2Fdev-value", None),
    ]


@pytest.mark.asyncio
async def test_confluent_schema_registry_client_passes_ssl_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts_seen: list[object] = []

    class _Response:
        def read(self) -> bytes:
            return b'{"schema":"{}"}'

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    sentinel_context = object()

    def _fake_urlopen(req, timeout: float, context=None):
        del req, timeout
        contexts_seen.append(context)
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    client = ConfluentSchemaRegistryClient(
        "https://registry:8081",
        ssl_context=sentinel_context,  # type: ignore[arg-type]
    )

    await client.get_schema(1)

    assert contexts_seen == [sentinel_context]


@pytest.mark.asyncio
async def test_schema_registry_http_error_preserves_status_when_body_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BrokenHTTPError(error.HTTPError):
        def read(self, amt: int | None = None) -> bytes:
            del amt
            raise OSError("body stream closed")

    def _fake_urlopen(req, timeout: float, context=None):
        del timeout, context
        raise _BrokenHTTPError(
            req.full_url,
            503,
            "Service Unavailable",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    client = ConfluentSchemaRegistryClient("http://registry:8081")
    with pytest.raises(RuntimeError) as exc_info:
        await client.get_latest_schema("orders-value")

    message = str(exc_info.value)
    assert "503 Service Unavailable" in message
    assert "failed to read response body" in message


@pytest.mark.asyncio
async def test_pooled_schema_registry_client_reuses_client_and_closes() -> None:
    _FakePooledAsyncClient.instances.clear()
    responses = [
        _FakePooledResponse(
            {
                "id": 7,
                "subject": "orders/value",
                "schema": '{"type":"record","name":"Event","fields":[]}',
                "version": 3,
            }
        ),
        _FakePooledResponse({"id": 9}),
    ]

    def _client_factory(**kwargs: Any) -> _FakePooledAsyncClient:
        fake_client = _FakePooledAsyncClient(**kwargs)
        fake_client.responses.extend(responses)
        return fake_client

    client = PooledConfluentSchemaRegistryClient(
        "https://registry:8081/",
        username="user",
        password="pass",
        timeout_s=2.5,
        headers={"X-Test": "1"},
        client_factory=_client_factory,
    )

    latest = await client.get_latest_schema("orders/value")
    registered = await client.register_schema(
        "orders/value",
        '{"type":"record","name":"Event","fields":[]}',
    )
    fake_client = _FakePooledAsyncClient.instances[0]

    assert latest.schema_id == 7
    assert latest.version == 3
    assert registered.schema_id == 9
    assert _FakePooledAsyncClient.instances == [fake_client]
    assert fake_client.kwargs["base_url"] == "https://registry:8081"
    assert fake_client.kwargs["auth"] == ("user", "pass")
    assert fake_client.kwargs["timeout"] == 2.5
    assert fake_client.kwargs["verify"] is True
    assert fake_client.kwargs["headers"]["X-Test"] == "1"
    assert fake_client.kwargs["headers"]["Accept"].startswith("application/vnd.schemaregistry")
    assert fake_client.requests[0] == (
        "GET",
        "/subjects/orders%2Fvalue/versions/latest",
        None,
        {},
    )
    assert fake_client.requests[1][0:3] == (
        "POST",
        "/subjects/orders%2Fvalue/versions",
        {
            "schema": '{"type":"record","name":"Event","fields":[]}',
            "schemaType": "AVRO",
        },
    )
    assert fake_client.requests[1][3]["Content-Type"] == ("application/vnd.schemaregistry.v1+json")

    await client.close()

    assert fake_client.closed is True
    with pytest.raises(RuntimeError, match="closed"):
        await client.get_schema(1)


@pytest.mark.asyncio
async def test_pooled_schema_registry_client_async_context_closes() -> None:
    _FakePooledAsyncClient.instances.clear()

    async with PooledConfluentSchemaRegistryClient(
        "http://registry:8081",
        client_factory=_FakePooledAsyncClient,
    ):
        fake_client = _FakePooledAsyncClient.instances[0]

    assert fake_client.closed is True


@pytest.mark.asyncio
async def test_pooled_schema_registry_client_preserves_http_error_status() -> None:
    _FakePooledAsyncClient.instances.clear()
    response = _FakePooledResponse(
        {"error_code": 40403, "message": "Schema not found"},
        status_code=404,
        reason_phrase="Not Found",
        text='{"message":"Schema not found"}',
    )

    def _client_factory(**kwargs: Any) -> _FakePooledAsyncClient:
        fake_client = _FakePooledAsyncClient(**kwargs)
        fake_client.responses.append(response)
        return fake_client

    client = PooledConfluentSchemaRegistryClient(
        "http://registry:8081",
        client_factory=_client_factory,
    )

    with pytest.raises(RuntimeError) as exc_info:
        await client.get_schema(404)

    message = str(exc_info.value)
    assert "404 Not Found" in message
    assert "Schema not found" in message


@pytest.mark.asyncio
async def test_pooled_schema_registry_client_manages_subject_compatibility() -> None:
    _FakePooledAsyncClient.instances.clear()
    responses = [
        _FakePooledResponse({"compatibility": "BACKWARD"}),
        _FakePooledResponse({"compatibilityLevel": "BACKWARD"}),
    ]

    def _client_factory(**kwargs: Any) -> _FakePooledAsyncClient:
        fake_client = _FakePooledAsyncClient(**kwargs)
        fake_client.responses.extend(responses)
        return fake_client

    client = PooledConfluentSchemaRegistryClient(
        "http://registry:8081",
        client_factory=_client_factory,
    )

    assert await client.set_subject_compatibility("orders/value", "BACKWARD") == "BACKWARD"
    assert await client.get_subject_compatibility("orders/value") == "BACKWARD"
    fake_client = _FakePooledAsyncClient.instances[0]
    assert fake_client.requests == [
        (
            "PUT",
            "/config/orders%2Fvalue",
            {"compatibility": "BACKWARD"},
            {"Content-Type": "application/vnd.schemaregistry.v1+json"},
        ),
        ("GET", "/config/orders%2Fvalue", None, {}),
    ]


@pytest.mark.asyncio
async def test_pooled_schema_registry_client_requires_object_response() -> None:
    _FakePooledAsyncClient.instances.clear()

    def _client_factory(**kwargs: Any) -> _FakePooledAsyncClient:
        fake_client = _FakePooledAsyncClient(**kwargs)
        fake_client.responses.append(_FakePooledResponse([{"schema": "{}"}]))
        return fake_client

    client = PooledConfluentSchemaRegistryClient(
        "http://registry:8081",
        client_factory=_client_factory,
    )

    with pytest.raises(TypeError, match="JSON object"):
        await client.get_schema(1)


@pytest.mark.asyncio
async def test_json_schema_registry_serializer_and_deserializer_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation_calls: list[tuple[Any, Any]] = []
    jsonschema_module = types.ModuleType("jsonschema")

    def _validate(*, instance: Any, schema: Any) -> None:
        validation_calls.append((instance, schema))
        assert isinstance(schema, dict)

    jsonschema_module.validate = _validate  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "jsonschema", jsonschema_module)

    schema = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": "integer"},
        },
        "required": ["order_id", "amount"],
    }
    registry = _FakeRegistryClient()
    serializer = JsonSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-json-value",
        schema=schema,
    )
    deserializer = JsonSchemaRegistryDeserializer[dict[str, Any]](registry_client=registry)

    await serializer.open()
    await deserializer.open()
    payload = await serializer({"order_id": "o-1", "amount": 42})
    decoded = await deserializer(payload)

    assert payload[0] == 0
    assert decoded == {"order_id": "o-1", "amount": 42}
    assert registry.register_calls[0][2] == "JSON"
    assert len(validation_calls) == 2


@pytest.mark.asyncio
async def test_protobuf_schema_registry_serializer_and_deserializer_round_trip() -> None:
    pytest.importorskip("google.protobuf")
    from google.protobuf.json_format import MessageToDict

    message_types = _make_protobuf_message_types()
    registry = _FakeRegistryClient()
    serializer = ProtobufSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-proto-value",
        schema="""
            syntax = "proto3";
            package agora.test;

            message OrderCreated {
              string order_id = 1;
              int32 amount = 2;
            }

            message CustomerCreated {
              string customer_id = 1;
            }
        """,
        message_type=message_types["OrderCreated"],
        message_indexes=(0,),
    )
    deserializer = ProtobufSchemaRegistryDeserializer[dict[str, Any]](
        registry_client=registry,
        message_type=message_types["OrderCreated"],
        record_mapper=lambda message: MessageToDict(
            message,
            preserving_proto_field_name=True,
        ),
    )

    await serializer.open()
    await deserializer.open()
    payload = await serializer({"order_id": "o-1", "amount": 42})
    decoded = await deserializer(payload)

    assert payload[0] == 0
    assert decoded == {"amount": 42, "order_id": "o-1"}
    assert registry.register_calls[0][2] == "PROTOBUF"
    assert registry.get_schema_calls == [1]


@pytest.mark.asyncio
async def test_protobuf_serializer_missing_subject_mode_reuses_matching_subject_despite_indent() -> (
    None
):
    pytest.importorskip("google.protobuf")

    schema = """
        syntax = "proto3";
        package agora.test;

        message OrderCreated {
          string order_id = 1;
          int32 amount = 2;
        }

        message CustomerCreated {
          string customer_id = 1;
        }
    """
    registry = _FakeRegistryClient()
    registry._latest_by_subject["orders-proto-value"] = RegisteredSchema(
        schema_id=7,
        subject="orders-proto-value",
        schema=(
            'syntax = "proto3";\n'
            "package agora.test;\n\n"
            "message OrderCreated {\n"
            "  string order_id = 1;\n"
            "  int32 amount = 2;\n"
            "}\n"
            "message CustomerCreated {\n"
            "  string customer_id = 1;\n"
            "}"
        ),
        schema_type="PROTOBUF",
    )
    message_types = _make_protobuf_message_types("matching_subject.proto")
    serializer = ProtobufSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-proto-value",
        schema=schema,
        message_type=message_types["OrderCreated"],
        message_indexes=(0,),
    )

    await serializer.open()
    payload = await serializer({"order_id": "o-1", "amount": 42})

    assert payload[:5] == b"\x00\x00\x00\x00\x07"
    assert registry.get_latest_calls == ["orders-proto-value"]
    assert registry.register_calls == []


@pytest.mark.asyncio
async def test_protobuf_serializer_rejects_schema_message_index_mismatch() -> None:
    pytest.importorskip("google.protobuf")

    message_types = _make_protobuf_message_types("mismatch.proto")
    registry = _FakeRegistryClient()
    serializer = ProtobufSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-proto-value",
        schema="""
            syntax = "proto3";
            package agora.test;

            message OrderCreated {
              string order_id = 1;
              int32 amount = 2;
            }

            message CustomerCreated {
              string customer_id = 1;
            }
        """,
        message_type=message_types["OrderCreated"],
        message_indexes=(1,),
    )

    with pytest.raises(ValueError, match="binding mismatch"):
        await serializer.open()


def test_protobuf_parser_handles_nested_messages_and_non_message_blocks() -> None:
    schema = r"""
        syntax = "proto3";
        package agora.test;

        import "google/protobuf/timestamp.proto";

        // Comments with braces { should not affect parsing.
        message OrderCreated {
          /* Block comments with } should also be ignored. */
          enum Status {
            STATUS_UNSPECIFIED = 0;
            STATUS_PAID = 1;
          }

          message Address {
            string street = 1;
          }

          oneof destination {
            Address billing_address = 2;
            string pickup_code = 3;
          }

          map<string, Address> address_by_id = 4;
          repeated Status statuses = 5;
          string url = 6 [json_name = "https://example.invalid/orders"];
        }

        service OrderService {
          rpc Get (OrderCreated) returns (OrderCreated);
        }

        message CustomerCreated {
          string customer_id = 1;
        }
    """

    assert _resolve_proto_message_full_name(schema, (0,)) == "agora.test.OrderCreated"
    assert _resolve_proto_message_full_name(schema, (0, 0)) == "agora.test.OrderCreated.Address"
    assert _resolve_proto_message_full_name(schema, (1,)) == "agora.test.CustomerCreated"


def test_protobuf_parser_reports_malformed_schema() -> None:
    with pytest.raises(ValueError, match="unclosed block"):
        _resolve_proto_message_full_name("message Broken {", (0,))

    with pytest.raises(ValueError, match="no message definitions"):
        _resolve_proto_message_full_name('syntax = "proto3"; enum OnlyEnum { A = 0; }', (0,))


@pytest.mark.asyncio
async def test_protobuf_deserializer_rejects_registry_binding_mismatch() -> None:
    pytest.importorskip("google.protobuf")

    message_types = _make_protobuf_message_types("binding.proto")
    registry = _FakeRegistryClient()
    serializer = ProtobufSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-proto-value",
        schema="""
            syntax = "proto3";
            package agora.test;

            message OrderCreated {
              string order_id = 1;
              int32 amount = 2;
            }

            message CustomerCreated {
              string customer_id = 1;
            }
        """,
        message_type=message_types["OrderCreated"],
        message_indexes=(0,),
    )
    deserializer = ProtobufSchemaRegistryDeserializer[dict[str, Any]](
        registry_client=registry,
        message_type=message_types["CustomerCreated"],
        record_mapper=lambda message: message,
    )

    await serializer.open()
    payload = await serializer({"order_id": "o-1", "amount": 42})

    with pytest.raises(ValueError, match="binding mismatch"):
        await deserializer(payload)


@pytest.mark.asyncio
async def test_protobuf_deserializer_evicts_old_registered_schemas_when_cache_is_bounded() -> None:
    pytest.importorskip("google.protobuf")

    message_types = _make_protobuf_message_types("eviction.proto")
    schema = """
        syntax = "proto3";
        package agora.test;

        message OrderCreated {
          string order_id = 1;
          int32 amount = 2;
        }

        message CustomerCreated {
          string customer_id = 1;
        }
    """
    registry = _FakeRegistryClient()
    serializer_one = ProtobufSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-proto-one-value",
        schema=schema,
        message_type=message_types["OrderCreated"],
        auto_register="always",
        message_indexes=(0,),
    )
    serializer_two = ProtobufSchemaRegistrySerializer[dict[str, Any]](
        registry_client=registry,
        subject="orders-proto-two-value",
        schema=schema,
        message_type=message_types["OrderCreated"],
        auto_register="always",
        message_indexes=(0,),
    )
    deserializer = ProtobufSchemaRegistryDeserializer[dict[str, Any]](
        registry_client=registry,
        message_type=message_types["OrderCreated"],
        record_mapper=lambda message: message,
        schema_cache_max_entries=1,
    )

    await serializer_one.open()
    await serializer_two.open()
    first_payload = await serializer_one({"order_id": "o-1", "amount": 1})
    second_payload = await serializer_two({"order_id": "o-2", "amount": 2})

    await deserializer(first_payload)
    await deserializer(second_payload)
    await deserializer(first_payload)

    assert registry.get_schema_calls == [1, 2, 1]
    assert list(deserializer._registered_schemas.keys()) == [1]  # type: ignore[attr-defined]
    assert list(deserializer._validated_bindings.keys()) == [  # type: ignore[attr-defined]
        (1, (0,))
    ]
