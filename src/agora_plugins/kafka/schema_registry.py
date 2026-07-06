"""Schema Registry helpers for Kafka serializers/deserializers."""

from __future__ import annotations

import asyncio
import base64
import importlib
import inspect
import io
import json
import re
import struct
import textwrap
import warnings
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, Protocol, TypeVar, cast
from urllib import error, parse, request

if TYPE_CHECKING:
    import ssl
    from collections.abc import Callable, Sequence

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
SchemaAutoRegisterMode = Literal["disabled", "missing_subject", "always"]

_CONFLUENT_MAGIC_BYTE = 0
_SCHEMA_AUTO_REGISTER_MODES: set[str] = {"disabled", "missing_subject", "always"}
_PROTO_IDENTIFIER_RE = re.compile(r"[A-Za-z_][\w.]*")
_PROTO_BLOCK_KEYWORDS = {"message", "enum", "oneof", "service", "extend", "group"}
_DEFAULT_SCHEMA_CACHE_MAX_ENTRIES = 256


@dataclass(slots=True)
class _ProtoMessageNode:
    name: str
    children: list[_ProtoMessageNode] = field(default_factory=list)


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


class ConfluentSchemaRegistryClient:
    """Minimal Confluent-compatible schema registry client."""

    def __init__(
        self,
        base_url: str,
        *,
        username: str | None = None,
        password: str | None = None,
        headers: dict[str, str] | None = None,
        timeout_s: float = 5.0,
        tls: Any | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        if tls is not None and ssl_context is not None:
            raise ValueError("Pass either tls or ssl_context to schema registry client, not both.")
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._headers = dict(headers or {})
        self._timeout_s = timeout_s
        self._ssl_context = (
            ssl_context
            if ssl_context is not None
            else (tls.build_ssl_context() if tls is not None else None)
        )

    async def get_schema(self, schema_id: int) -> RegisteredSchema:
        payload = await self._request_json("GET", f"/schemas/ids/{schema_id}")
        return RegisteredSchema(
            schema_id=schema_id,
            schema=payload["schema"],
            schema_type=payload.get("schemaType", "AVRO"),
        )

    async def get_latest_schema(self, subject: str) -> RegisteredSchema:
        payload = await self._request_json(
            "GET", f"/subjects/{_quote_path_segment(subject)}/versions/latest"
        )
        return RegisteredSchema(
            schema_id=payload["id"],
            schema=payload["schema"],
            schema_type=payload.get("schemaType", "AVRO"),
            subject=payload.get("subject", subject),
            version=payload.get("version"),
        )

    async def register_schema(
        self,
        subject: str,
        schema: str,
        *,
        schema_type: str = "AVRO",
    ) -> RegisteredSchema:
        payload = await self._request_json(
            "POST",
            f"/subjects/{_quote_path_segment(subject)}/versions",
            body={
                "schema": schema,
                "schemaType": schema_type,
            },
        )
        return RegisteredSchema(
            schema_id=payload["id"],
            schema=schema,
            schema_type=schema_type,
            subject=subject,
        )

    async def get_subject_compatibility(self, subject: str) -> str:
        payload = await self._request_json("GET", f"/config/{_quote_path_segment(subject)}")
        compatibility = payload.get("compatibilityLevel", payload.get("compatibility"))
        if not isinstance(compatibility, str):
            raise TypeError("Schema registry compatibility response must include a level.")
        return compatibility

    async def set_subject_compatibility(self, subject: str, level: str) -> str:
        payload = await self._request_json(
            "PUT",
            f"/config/{_quote_path_segment(subject)}",
            body={"compatibility": level},
        )
        compatibility = payload.get("compatibility", payload.get("compatibilityLevel"))
        if not isinstance(compatibility, str):
            raise TypeError("Schema registry compatibility response must include a level.")
        return compatibility

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._request_json_sync, method, path, body)

    def _request_json_sync(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = None
        headers = {
            "Accept": "application/vnd.schemaregistry.v1+json, application/json",
            **self._headers,
        }
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"

        req = request.Request(
            f"{self._base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        if self._username is not None and self._password is not None:
            token = base64.b64encode(f"{self._username}:{self._password}".encode()).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")

        try:
            with request.urlopen(
                req,
                timeout=self._timeout_s,
                context=self._ssl_context,
            ) as response:
                payload = response.read().decode("utf-8")
        except (
            error.HTTPError
        ) as exc:  # pragma: no cover - exercised via unit tests through wrapper
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            except Exception as body_exc:
                detail = f"<failed to read response body: {body_exc}>"
            raise RuntimeError(
                f"Schema registry request failed with {exc.code} {exc.reason}: {detail}"
            ) from exc
        except error.URLError as exc:  # pragma: no cover - exercised via unit tests through wrapper
            raise RuntimeError(f"Schema registry request failed: {exc.reason}") from exc

        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise TypeError("Schema registry response must be a JSON object.")
        return decoded

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> ConfluentSchemaRegistryClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        await self.close()


class PooledConfluentSchemaRegistryClient(ConfluentSchemaRegistryClient):
    """Confluent-compatible schema registry client backed by a pooled async transport."""

    def __init__(
        self,
        base_url: str,
        *,
        username: str | None = None,
        password: str | None = None,
        headers: dict[str, str] | None = None,
        timeout_s: float = 5.0,
        tls: Any | None = None,
        ssl_context: ssl.SSLContext | None = None,
        client_factory: Callable[..., Any] | None = None,
    ) -> None:
        super().__init__(
            base_url,
            username=username,
            password=password,
            headers=headers,
            timeout_s=timeout_s,
            tls=tls,
            ssl_context=ssl_context,
        )
        self._client_factory = client_factory
        self._client: Any | None = None
        self._closed = False

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        client = self._get_client()
        headers: dict[str, str] = {}
        request_kwargs: dict[str, Any] = {"headers": headers}
        if body is not None:
            headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
            request_kwargs["json"] = body
        try:
            response = await client.request(
                method,
                path,
                **request_kwargs,
            )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Schema registry request failed: {exc}") from exc

        status_code = int(getattr(response, "status_code", 0))
        if status_code >= 400:
            reason = str(getattr(response, "reason_phrase", ""))
            detail = str(getattr(response, "text", ""))
            raise RuntimeError(
                f"Schema registry request failed with {status_code} {reason}: {detail}"
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError("Schema registry response must be a JSON object.")
        return payload

    async def close(self) -> None:
        client = self._client
        self._client = None
        self._closed = True
        if client is None:
            return
        close = getattr(client, "aclose", None)
        if close is None:
            close = getattr(client, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    async def __aenter__(self) -> PooledConfluentSchemaRegistryClient:
        self._get_client()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        await self.close()

    def _get_client(self) -> Any:
        if self._closed:
            raise RuntimeError("Schema registry pooled client is closed.")
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self) -> Any:
        headers = {
            "Accept": "application/vnd.schemaregistry.v1+json, application/json",
            **self._headers,
        }
        auth = (
            (self._username, self._password)
            if self._username is not None and self._password is not None
            else None
        )
        kwargs: dict[str, Any] = {
            "base_url": self._base_url,
            "headers": headers,
            "timeout": self._timeout_s,
            "auth": auth,
            "verify": self._ssl_context if self._ssl_context is not None else True,
        }
        if self._client_factory is not None:
            return self._client_factory(**kwargs)
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - depends on optional environment
            raise ImportError(
                "Pooled schema registry transport requires httpx. "
                "Install httpx or use schema_registry_transport='stdlib'."
            ) from exc
        return httpx.AsyncClient(**kwargs)


def _coerce_auto_register_mode(
    auto_register: bool | SchemaAutoRegisterMode,
) -> SchemaAutoRegisterMode:
    if isinstance(auto_register, bool):
        warnings.warn(
            "Passing bool auto_register is deprecated; pass one of "
            "'always', 'missing_subject', or 'disabled' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return "always" if auto_register else "disabled"
    if auto_register not in _SCHEMA_AUTO_REGISTER_MODES:
        raise ValueError("auto_register must be one of 'always', 'missing_subject', or 'disabled'.")
    return auto_register


async def _resolve_registered_schema(
    registry_client: SchemaRegistryClient,
    *,
    subject: str,
    schema_text: str,
    schema_type: str,
    auto_register: SchemaAutoRegisterMode,
    normalize_schema: Callable[[str], str],
) -> RegisteredSchema:
    normalized_schema_text = normalize_schema(schema_text)
    if auto_register == "always":
        return await registry_client.register_schema(
            subject,
            normalized_schema_text,
            schema_type=schema_type,
        )

    try:
        registered = await registry_client.get_latest_schema(subject)
    except Exception as exc:
        if auto_register == "missing_subject" and _schema_registry_subject_is_missing(exc):
            return await registry_client.register_schema(
                subject,
                normalized_schema_text,
                schema_type=schema_type,
            )
        raise

    if normalize_schema(registered.schema) != normalized_schema_text:
        raise ValueError(f"Latest schema for subject '{subject}' does not match serializer schema.")
    return registered


def _schema_registry_subject_is_missing(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code is None:
        status_code = getattr(exc, "code", None)
    if status_code is not None:
        try:
            if int(status_code) == 404:
                return True
        except (TypeError, ValueError):
            pass
    message = str(exc).lower()
    return "404" in message and (
        "schema registry request failed" in message
        or "subject" in message
        or "not found" in message
    )


def _coerce_schema_cache_max_entries(schema_cache_max_entries: int) -> int:
    if schema_cache_max_entries < 1:
        raise ValueError("schema_cache_max_entries must be >= 1.")
    return schema_cache_max_entries


def _lru_cache_get(cache: OrderedDict[K, V], key: K) -> V | None:
    value = cache.get(key)
    if value is None:
        return None
    cache.move_to_end(key)
    return value


def _lru_cache_put(
    cache: OrderedDict[K, V],
    key: K,
    value: V,
    *,
    max_entries: int,
) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_entries:
        cache.popitem(last=False)


class AvroSchemaRegistrySerializer(Generic[T]):
    """Encode records using Confluent wire format and registry-managed Avro schemas."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        subject: str,
        schema: dict[str, Any] | list[Any] | str,
        record_mapper: Callable[[T], dict[str, Any]] | None = None,
        auto_register: bool | SchemaAutoRegisterMode = "missing_subject",
    ) -> None:
        self._registry_client = registry_client
        self._subject = subject
        self._schema_text = _normalize_avro_schema_text(schema)
        self._record_mapper = record_mapper or _default_record_mapper
        self._auto_register = _coerce_auto_register_mode(auto_register)
        self._schema_id: int | None = None
        self._parsed_schema: Any = None

    async def open(self) -> None:
        from fastavro import parse_schema

        registered = await _resolve_registered_schema(
            self._registry_client,
            subject=self._subject,
            schema_text=self._schema_text,
            schema_type="AVRO",
            auto_register=self._auto_register,
            normalize_schema=_normalize_avro_schema_text,
        )
        self._schema_id = registered.schema_id
        self._parsed_schema = parse_schema(json.loads(self._schema_text))

    async def close(self) -> None:
        return None

    async def __call__(self, record: T) -> bytes:
        from fastavro import schemaless_writer

        if self._schema_id is None or self._parsed_schema is None:
            raise RuntimeError("AvroSchemaRegistrySerializer.open() must be called before use.")
        payload = self._record_mapper(record)
        buffer = io.BytesIO()
        buffer.write(_encode_confluent_prefix(self._schema_id))
        schemaless_writer(buffer, self._parsed_schema, payload)
        return buffer.getvalue()


class AvroSchemaRegistryDeserializer(Generic[T]):
    """Decode Confluent wire-format Avro payloads via a schema registry."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        record_mapper: Callable[[dict[str, Any]], T] | None = None,
        reader_schema: dict[str, Any] | list[Any] | str | None = None,
        schema_cache_max_entries: int = _DEFAULT_SCHEMA_CACHE_MAX_ENTRIES,
    ) -> None:
        self._registry_client = registry_client
        self._record_mapper = record_mapper
        self._reader_schema_text = (
            _normalize_avro_schema_text(reader_schema) if reader_schema else None
        )
        self._reader_schema: Any = None
        self._schema_cache_max_entries = _coerce_schema_cache_max_entries(schema_cache_max_entries)
        self._writer_schemas: OrderedDict[int, Any] = OrderedDict()

    async def open(self) -> None:
        if self._reader_schema_text is None:
            return
        from fastavro import parse_schema

        self._reader_schema = parse_schema(json.loads(self._reader_schema_text))

    async def close(self) -> None:
        return None

    async def __call__(self, value: bytes) -> T:
        from fastavro import parse_schema, schemaless_reader

        if len(value) < 5:
            raise ValueError("Schema-registry Avro payload must be at least 5 bytes long.")
        if value[0] != _CONFLUENT_MAGIC_BYTE:
            raise ValueError("Unsupported schema-registry payload magic byte.")

        schema_id, payload_offset, _ = _decode_confluent_prefix(value)
        writer_schema = _lru_cache_get(self._writer_schemas, schema_id)
        if writer_schema is None:
            registered = await self._registry_client.get_schema(schema_id)
            writer_schema = parse_schema(json.loads(_normalize_avro_schema_text(registered.schema)))
            _lru_cache_put(
                self._writer_schemas,
                schema_id,
                writer_schema,
                max_entries=self._schema_cache_max_entries,
            )

        record = schemaless_reader(
            io.BytesIO(value[payload_offset:]),
            writer_schema,
            self._reader_schema,
        )
        if self._record_mapper is None:
            return cast("T", record)
        if not isinstance(record, dict):
            raise TypeError(
                "Schema-registry Avro deserializer expected a record object for record_mapper."
            )
        return self._record_mapper(cast("dict[str, Any]", record))


class JsonSchemaRegistrySerializer(Generic[T]):
    """Encode JSON payloads using Confluent wire format and registry-managed schemas."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        subject: str,
        schema: dict[str, Any] | list[Any] | str,
        record_mapper: Callable[[T], Any] | None = None,
        auto_register: bool | SchemaAutoRegisterMode = "missing_subject",
        validate_payload: bool = True,
    ) -> None:
        self._registry_client = registry_client
        self._subject = subject
        self._schema_text = _normalize_schema_text(schema)
        self._record_mapper = record_mapper or _identity_record_mapper
        self._auto_register = _coerce_auto_register_mode(auto_register)
        self._validate_payload = validate_payload
        self._schema_id: int | None = None
        self._schema_object: Any = None

    async def open(self) -> None:
        registered = await _resolve_registered_schema(
            self._registry_client,
            subject=self._subject,
            schema_text=self._schema_text,
            schema_type="JSON",
            auto_register=self._auto_register,
            normalize_schema=_normalize_schema_text,
        )
        self._schema_id = registered.schema_id
        self._schema_object = json.loads(self._schema_text)

    async def close(self) -> None:
        return None

    async def __call__(self, record: T) -> bytes:
        if self._schema_id is None or self._schema_object is None:
            raise RuntimeError("JsonSchemaRegistrySerializer.open() must be called before use.")
        payload = self._record_mapper(record)
        if self._validate_payload:
            _jsonschema_validate(payload, self._schema_object)
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return _encode_confluent_prefix(self._schema_id) + encoded


class JsonSchemaRegistryDeserializer(Generic[T]):
    """Decode Confluent wire-format JSON payloads via a schema registry."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        record_mapper: Callable[[Any], T] | None = None,
        reader_schema: dict[str, Any] | list[Any] | str | None = None,
        validate_payload: bool = True,
        schema_cache_max_entries: int = _DEFAULT_SCHEMA_CACHE_MAX_ENTRIES,
    ) -> None:
        self._registry_client = registry_client
        self._record_mapper = record_mapper
        self._reader_schema_text = _normalize_schema_text(reader_schema) if reader_schema else None
        self._reader_schema: Any = None
        self._schema_cache_max_entries = _coerce_schema_cache_max_entries(schema_cache_max_entries)
        self._writer_schemas: OrderedDict[int, Any] = OrderedDict()
        self._validate_payload = validate_payload

    async def open(self) -> None:
        if self._reader_schema_text is None:
            return
        self._reader_schema = json.loads(self._reader_schema_text)

    async def close(self) -> None:
        return None

    async def __call__(self, value: bytes) -> T:
        schema_id, payload_offset, _ = _decode_confluent_prefix(value)
        writer_schema = _lru_cache_get(self._writer_schemas, schema_id)
        if writer_schema is None:
            registered = await self._registry_client.get_schema(schema_id)
            writer_schema = json.loads(_normalize_schema_text(registered.schema))
            _lru_cache_put(
                self._writer_schemas,
                schema_id,
                writer_schema,
                max_entries=self._schema_cache_max_entries,
            )
        payload = json.loads(value[payload_offset:].decode("utf-8"))
        if self._validate_payload:
            _jsonschema_validate(payload, self._reader_schema or writer_schema)
        if self._record_mapper is None:
            return cast("T", payload)
        return self._record_mapper(payload)


class ProtobufSchemaRegistrySerializer(Generic[T]):
    """Encode Protobuf payloads using Confluent wire format and registry-managed schemas."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        subject: str,
        schema: str,
        message_type: type[Any],
        record_mapper: Callable[[T], Any] | None = None,
        auto_register: bool | SchemaAutoRegisterMode = "missing_subject",
        message_indexes: Sequence[int] = (0,),
    ) -> None:
        self._registry_client = registry_client
        self._subject = subject
        self._schema_text = _normalize_proto_schema_text(schema)
        self._message_type = message_type
        self._record_mapper = record_mapper
        self._auto_register = _coerce_auto_register_mode(auto_register)
        self._schema_id: int | None = None
        self._message_indexes = tuple(int(index) for index in message_indexes)

    async def open(self) -> None:
        _validate_protobuf_schema_binding(
            self._schema_text,
            self._message_type,
            self._message_indexes,
        )
        registered = await _resolve_registered_schema(
            self._registry_client,
            subject=self._subject,
            schema_text=self._schema_text,
            schema_type="PROTOBUF",
            auto_register=self._auto_register,
            normalize_schema=_normalize_proto_schema_text,
        )
        if registered.schema_type != "PROTOBUF":
            raise ValueError(
                f"Subject '{self._subject}' is registered as {registered.schema_type!r}, not 'PROTOBUF'."
            )
        _validate_protobuf_schema_binding(
            registered.schema,
            self._message_type,
            self._message_indexes,
        )
        self._schema_id = registered.schema_id

    async def close(self) -> None:
        return None

    async def __call__(self, record: T) -> bytes:
        if self._schema_id is None:
            raise RuntimeError("ProtobufSchemaRegistrySerializer.open() must be called before use.")
        message = _coerce_protobuf_message(
            record if self._record_mapper is None else self._record_mapper(record),
            self._message_type,
        )
        prefix = _encode_confluent_prefix(
            self._schema_id,
            message_indexes=self._message_indexes,
        )
        return cast("bytes", prefix + message.SerializeToString())


class ProtobufSchemaRegistryDeserializer(Generic[T]):
    """Decode Confluent wire-format Protobuf payloads via a schema registry."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        message_type: type[Any],
        record_mapper: Callable[[Any], T] | None = None,
        schema_cache_max_entries: int = _DEFAULT_SCHEMA_CACHE_MAX_ENTRIES,
    ) -> None:
        self._registry_client = registry_client
        self._message_type = message_type
        self._record_mapper = record_mapper
        self._schema_cache_max_entries = _coerce_schema_cache_max_entries(schema_cache_max_entries)
        self._registered_schemas: OrderedDict[int, RegisteredSchema] = OrderedDict()
        self._validated_bindings: OrderedDict[tuple[int, tuple[int, ...]], None] = OrderedDict()

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def __call__(self, value: bytes) -> T:
        schema_id, payload_offset, message_indexes = _decode_confluent_prefix(
            value,
            expect_message_indexes=True,
        )
        binding_key = (schema_id, message_indexes or (0,))
        registered = _lru_cache_get(self._registered_schemas, schema_id)
        if registered is None:
            registered = await self._registry_client.get_schema(schema_id)
            _lru_cache_put(
                self._registered_schemas,
                schema_id,
                registered,
                max_entries=self._schema_cache_max_entries,
            )
        if registered.schema_type != "PROTOBUF":
            raise ValueError(
                f"Schema id {schema_id} is registered as {registered.schema_type!r}, not 'PROTOBUF'."
            )
        if _lru_cache_get(self._validated_bindings, binding_key) is None:
            _validate_protobuf_schema_binding(
                registered.schema,
                self._message_type,
                binding_key[1],
            )
            _lru_cache_put(
                self._validated_bindings,
                binding_key,
                None,
                max_entries=self._schema_cache_max_entries,
            )
        message = self._message_type()
        message.ParseFromString(value[payload_offset:])
        if self._record_mapper is None:
            return cast("T", message)
        return self._record_mapper(message)


def _normalize_schema_text(schema: dict[str, Any] | list[Any] | str) -> str:
    if isinstance(schema, str):
        return json.dumps(json.loads(schema), sort_keys=True, separators=(",", ":"))
    return json.dumps(schema, sort_keys=True, separators=(",", ":"))


def _normalize_avro_schema_text(schema: dict[str, Any] | list[Any] | str) -> str:
    schema_object = json.loads(schema) if isinstance(schema, str) else schema
    try:
        from fastavro.schema import to_parsing_canonical_form
    except ImportError:
        return json.dumps(schema_object, sort_keys=True, separators=(",", ":"))
    canonical = to_parsing_canonical_form(schema_object)
    if isinstance(canonical, str):
        return canonical
    return str(canonical)


def _normalize_proto_schema_text(schema: str) -> str:
    normalized = textwrap.dedent(schema).strip()
    return "\n".join(line.strip() for line in normalized.splitlines() if line.strip())


def _encode_confluent_prefix(
    schema_id: int,
    *,
    message_indexes: Sequence[int] | None = None,
) -> bytes:
    prefix = bytearray()
    prefix.append(_CONFLUENT_MAGIC_BYTE)
    prefix.extend(struct.pack(">I", int(schema_id)))
    if message_indexes is not None:
        prefix.extend(_encode_message_indexes(message_indexes))
    return bytes(prefix)


def _decode_confluent_prefix(
    value: bytes,
    *,
    expect_message_indexes: bool = False,
) -> tuple[int, int, tuple[int, ...] | None]:
    if len(value) < 5:
        raise ValueError("Schema-registry payload must be at least 5 bytes long.")
    if value[0] != _CONFLUENT_MAGIC_BYTE:
        raise ValueError("Unsupported schema-registry payload magic byte.")
    schema_id = struct.unpack(">I", value[1:5])[0]
    payload_offset = 5
    message_indexes: tuple[int, ...] | None = None
    if expect_message_indexes:
        message_indexes, payload_offset = _decode_message_indexes(value, payload_offset)
    return schema_id, payload_offset, message_indexes


def _encode_message_indexes(indexes: Sequence[int]) -> bytes:
    normalized = tuple(int(index) for index in indexes)
    if normalized == (0,):
        return b"\x00"
    encoded = bytearray()
    encoded.extend(_encode_zigzag_varint(len(normalized)))
    for index in normalized:
        if index < 0:
            raise ValueError("Protobuf message indexes must be >= 0.")
        encoded.extend(_encode_zigzag_varint(index))
    return bytes(encoded)


def _decode_message_indexes(value: bytes, offset: int) -> tuple[tuple[int, ...], int]:
    if offset >= len(value):
        raise ValueError("Schema-registry Protobuf payload is missing message indexes.")
    if value[offset] == 0:
        return (0,), offset + 1
    length, offset = _decode_zigzag_varint(value, offset)
    indexes: list[int] = []
    for _ in range(length):
        item, offset = _decode_zigzag_varint(value, offset)
        indexes.append(item)
    return tuple(indexes), offset


def _encode_zigzag_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("Varint value must be >= 0.")
    return _encode_unsigned_varint(value << 1)


def _decode_zigzag_varint(value: bytes, offset: int) -> tuple[int, int]:
    encoded, offset = _decode_unsigned_varint(value, offset)
    decoded = (encoded >> 1) ^ -(encoded & 1)
    if decoded < 0:
        raise ValueError("Decoded zigzag varint must be >= 0 for message indexes.")
    return decoded, offset


def _encode_unsigned_varint(value: int) -> bytes:
    encoded = bytearray()
    remaining = value
    while True:
        bits = remaining & 0x7F
        remaining >>= 7
        if remaining:
            encoded.append(bits | 0x80)
        else:
            encoded.append(bits)
            return bytes(encoded)


def _decode_unsigned_varint(value: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    decoded = 0
    cursor = offset
    while cursor < len(value):
        item = value[cursor]
        cursor += 1
        decoded |= (item & 0x7F) << shift
        if not item & 0x80:
            return decoded, cursor
        shift += 7
    raise ValueError("Unexpected end of schema-registry varint payload.")


def _jsonschema_validate(instance: Any, schema: Any) -> None:
    jsonschema = importlib.import_module("jsonschema")
    jsonschema.validate(instance=instance, schema=schema)


def _coerce_protobuf_message(value: Any, message_type: type[Any]) -> Any:
    if isinstance(value, message_type):
        return value
    if isinstance(value, dict):
        json_format = importlib.import_module("google.protobuf.json_format")
        message = message_type()
        json_format.ParseDict(value, message)
        return message
    if hasattr(value, "SerializeToString") and hasattr(value, "ParseFromString"):
        return value
    raise TypeError(
        "ProtobufSchemaRegistrySerializer requires a protobuf message instance or dict payload. "
        "Provide record_mapper=... for custom objects."
    )


def _validate_protobuf_schema_binding(
    schema_text: str,
    message_type: type[Any],
    message_indexes: Sequence[int],
) -> None:
    expected_full_name = _resolve_proto_message_full_name(schema_text, message_indexes)
    actual_full_name = _protobuf_message_full_name(message_type)
    if expected_full_name != actual_full_name:
        raise ValueError(
            "Protobuf schema-registry binding mismatch: "
            f"payload indexes {tuple(int(index) for index in message_indexes)!r} resolve to "
            f"{expected_full_name!r}, but local message_type is {actual_full_name!r}."
        )


def _protobuf_message_full_name(message_type: type[Any]) -> str:
    descriptor = getattr(message_type, "DESCRIPTOR", None)
    if descriptor is None:
        raise TypeError(
            "Protobuf schema-registry integration requires a protobuf message class with DESCRIPTOR."
        )
    full_name = getattr(descriptor, "full_name", None)
    if not isinstance(full_name, str) or not full_name:
        raise TypeError("Protobuf message class DESCRIPTOR.full_name must be a non-empty string.")
    return full_name


def _resolve_proto_message_full_name(schema_text: str, message_indexes: Sequence[int]) -> str:
    package_name, root_messages = _parse_proto_message_tree(schema_text)
    indexes = tuple(int(index) for index in message_indexes)
    if not indexes:
        raise ValueError("Protobuf schema-registry message indexes cannot be empty.")

    node: _ProtoMessageNode | None = None
    path: list[str] = []
    siblings = root_messages
    for index in indexes:
        if index < 0 or index >= len(siblings):
            raise ValueError(
                f"Protobuf schema-registry message index {index} is out of range for path {indexes!r}."
            )
        node = siblings[index]
        path.append(node.name)
        siblings = node.children
    if node is None:
        raise ValueError(f"Unable to resolve protobuf message indexes {indexes!r}.")
    return ".".join([package_name, *path]) if package_name else ".".join(path)


def _parse_proto_message_tree(schema_text: str) -> tuple[str, list[_ProtoMessageNode]]:
    package_name = ""
    token_stream = _tokenize_proto_schema(schema_text)

    root_messages: list[_ProtoMessageNode] = []
    message_stack: list[_ProtoMessageNode] = []
    brace_stack: list[str] = []
    pending_block_kind: str | None = None
    pending_message_name: str | None = None
    cursor = 0
    while cursor < len(token_stream):
        token = token_stream[cursor]
        if token == "package":
            if cursor + 1 < len(token_stream):
                package_name = token_stream[cursor + 1]
            cursor += 1
            while cursor < len(token_stream) and token_stream[cursor] != ";":
                cursor += 1
        elif token in _PROTO_BLOCK_KEYWORDS:
            pending_block_kind = token
            pending_message_name = (
                token_stream[cursor + 1] if cursor + 1 < len(token_stream) else None
            )
            cursor += 1
        elif token == "{":
            if pending_block_kind in {"message", "group"}:
                if pending_message_name is None:
                    raise ValueError("Malformed protobuf schema: message block is missing a name.")
                node = _ProtoMessageNode(name=pending_message_name)
                if message_stack:
                    message_stack[-1].children.append(node)
                else:
                    root_messages.append(node)
                message_stack.append(node)
                brace_stack.append(pending_block_kind)
            elif pending_block_kind is not None:
                brace_stack.append(pending_block_kind)
            else:
                brace_stack.append("block")
            pending_block_kind = None
            pending_message_name = None
        elif token == "}":
            if not brace_stack:
                raise ValueError("Malformed protobuf schema: unmatched closing brace.")
            kind = brace_stack.pop()
            if kind in {"message", "group"} and message_stack:
                message_stack.pop()
            pending_block_kind = None
            pending_message_name = None
        cursor += 1
    if brace_stack:
        raise ValueError("Malformed protobuf schema: unclosed block.")
    if not root_messages:
        raise ValueError("Malformed protobuf schema: no message definitions found.")
    return package_name, root_messages


def _tokenize_proto_schema(schema_text: str) -> list[str]:
    tokens: list[str] = []
    cursor = 0
    length = len(schema_text)
    while cursor < length:
        char = schema_text[cursor]
        if char.isspace():
            cursor += 1
            continue
        if schema_text.startswith("//", cursor):
            cursor = _skip_line_comment(schema_text, cursor + 2)
            continue
        if schema_text.startswith("/*", cursor):
            cursor = _skip_block_comment(schema_text, cursor + 2)
            continue
        if char in {'"', "'"}:
            cursor = _skip_quoted_string(schema_text, cursor)
            continue
        if char in "{};":
            tokens.append(char)
            cursor += 1
            continue
        match = _PROTO_IDENTIFIER_RE.match(schema_text, cursor)
        if match is not None:
            tokens.append(match.group(0))
            cursor = match.end()
            continue
        cursor += 1
    return tokens


def _skip_line_comment(schema_text: str, cursor: int) -> int:
    newline = schema_text.find("\n", cursor)
    return len(schema_text) if newline < 0 else newline + 1


def _skip_block_comment(schema_text: str, cursor: int) -> int:
    end = schema_text.find("*/", cursor)
    if end < 0:
        raise ValueError("Malformed protobuf schema: unclosed block comment.")
    return end + 2


def _skip_quoted_string(schema_text: str, cursor: int) -> int:
    quote = schema_text[cursor]
    cursor += 1
    while cursor < len(schema_text):
        char = schema_text[cursor]
        if char == "\\":
            cursor += 2
            continue
        if char == quote:
            return cursor + 1
        cursor += 1
    raise ValueError("Malformed protobuf schema: unclosed quoted string.")


def _quote_path_segment(value: str) -> str:
    return parse.quote(value, safe="")


def _default_record_mapper(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise TypeError(
            "AvroSchemaRegistrySerializer requires a mapping record by default. "
            "Provide record_mapper=... for custom objects."
        )
    return record


def _identity_record_mapper(record: T) -> T:
    return record


__all__ = [
    "AvroSchemaRegistryDeserializer",
    "AvroSchemaRegistrySerializer",
    "ConfluentSchemaRegistryClient",
    "JsonSchemaRegistryDeserializer",
    "JsonSchemaRegistrySerializer",
    "PooledConfluentSchemaRegistryClient",
    "ProtobufSchemaRegistryDeserializer",
    "ProtobufSchemaRegistrySerializer",
    "RegisteredSchema",
    "SchemaAutoRegisterMode",
    "SchemaRegistryClient",
]
