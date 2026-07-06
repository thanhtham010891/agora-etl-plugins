"""Confluent-compatible Schema Registry clients."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
from typing import TYPE_CHECKING, Any
from urllib import error, parse, request

from agora_plugins.kafka._schema_registry_types import RegisteredSchema

if TYPE_CHECKING:
    import ssl
    from collections.abc import Callable


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
            "GET", f"/subjects/{quote_path_segment(subject)}/versions/latest"
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
            f"/subjects/{quote_path_segment(subject)}/versions",
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
        payload = await self._request_json("GET", f"/config/{quote_path_segment(subject)}")
        compatibility = payload.get("compatibilityLevel", payload.get("compatibility"))
        if not isinstance(compatibility, str):
            raise TypeError("Schema registry compatibility response must include a level.")
        return compatibility

    async def set_subject_compatibility(self, subject: str, level: str) -> str:
        payload = await self._request_json(
            "PUT",
            f"/config/{quote_path_segment(subject)}",
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


def quote_path_segment(value: str) -> str:
    return parse.quote(value, safe="")
