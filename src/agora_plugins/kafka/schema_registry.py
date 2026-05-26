"""Schema Registry helpers for Kafka serializers/deserializers."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar
from urllib import error, parse, request

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")

_CONFLUENT_MAGIC_BYTE = 0


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
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._headers = dict(headers or {})
        self._timeout_s = timeout_s

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
            with request.urlopen(req, timeout=self._timeout_s) as response:
                payload = response.read().decode("utf-8")
        except (
            error.HTTPError
        ) as exc:  # pragma: no cover - exercised via unit tests through wrapper
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Schema registry request failed with {exc.code} {exc.reason}: {detail}"
            ) from exc
        except error.URLError as exc:  # pragma: no cover - exercised via unit tests through wrapper
            raise RuntimeError(f"Schema registry request failed: {exc.reason}") from exc

        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise TypeError("Schema registry response must be a JSON object.")
        return decoded


class AvroSchemaRegistrySerializer(Generic[T]):
    """Encode records using Confluent wire format and registry-managed Avro schemas."""

    def __init__(
        self,
        *,
        registry_client: SchemaRegistryClient,
        subject: str,
        schema: dict[str, Any] | list[Any] | str,
        record_mapper: Callable[[T], dict[str, Any]] | None = None,
        auto_register: bool = True,
    ) -> None:
        self._registry_client = registry_client
        self._subject = subject
        self._schema_text = _normalize_schema_text(schema)
        self._record_mapper = record_mapper or _default_record_mapper
        self._auto_register = auto_register
        self._schema_id: int | None = None
        self._parsed_schema: Any = None

    async def open(self) -> None:
        from fastavro import parse_schema

        registered = (
            await self._registry_client.register_schema(self._subject, self._schema_text)
            if self._auto_register
            else await self._registry_client.get_latest_schema(self._subject)
        )
        if (
            not self._auto_register
            and _normalize_schema_text(registered.schema) != self._schema_text
        ):
            raise ValueError(
                f"Latest schema for subject '{self._subject}' does not match serializer schema."
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
        buffer.write(bytes([_CONFLUENT_MAGIC_BYTE]))
        buffer.write(struct.pack(">I", self._schema_id))
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
    ) -> None:
        self._registry_client = registry_client
        self._record_mapper = record_mapper
        self._reader_schema_text = _normalize_schema_text(reader_schema) if reader_schema else None
        self._reader_schema: Any = None
        self._writer_schemas: dict[int, Any] = {}

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

        schema_id = struct.unpack(">I", value[1:5])[0]
        writer_schema = self._writer_schemas.get(schema_id)
        if writer_schema is None:
            registered = await self._registry_client.get_schema(schema_id)
            writer_schema = parse_schema(json.loads(_normalize_schema_text(registered.schema)))
            self._writer_schemas[schema_id] = writer_schema

        record = schemaless_reader(io.BytesIO(value[5:]), writer_schema, self._reader_schema)
        if self._record_mapper is None:
            return record  # type: ignore[return-value]
        return self._record_mapper(record)


def _normalize_schema_text(schema: dict[str, Any] | list[Any] | str) -> str:
    if isinstance(schema, str):
        return json.dumps(json.loads(schema), sort_keys=True, separators=(",", ":"))
    return json.dumps(schema, sort_keys=True, separators=(",", ":"))


def _quote_path_segment(value: str) -> str:
    return parse.quote(value, safe="")


def _default_record_mapper(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise TypeError(
            "AvroSchemaRegistrySerializer requires a mapping record by default. "
            "Provide record_mapper=... for custom objects."
        )
    return record


__all__ = [
    "AvroSchemaRegistryDeserializer",
    "AvroSchemaRegistrySerializer",
    "ConfluentSchemaRegistryClient",
    "RegisteredSchema",
    "SchemaRegistryClient",
]
