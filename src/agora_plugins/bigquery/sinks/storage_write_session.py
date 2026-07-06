"""Session lifecycle collaborator for BigQuery Storage Write sinks."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from agora_plugins.bigquery.config import build_bigquery_client, build_bigquery_write_client

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from agora_plugins.bigquery.config import BigQueryConnectionConfig


class BigQueryStorageWriteSession:
    """Public-facing session collaborator for Storage Write sink resources."""

    def __init__(
        self,
        *,
        table: str,
        table_schema: Sequence[object],
        connection: BigQueryConnectionConfig,
        validate_table_access: bool,
        client: object | None,
        write_client: object | None,
        stream: object | None,
        stream_name: str | None,
        serializer: object | None,
        stream_factory: Callable[[object, str, object], object],
        serializer_factory: Callable[[Sequence[object]], object],
        resolve_table_path: Callable[[str], tuple[str, str, str]],
    ) -> None:
        self._table = table
        self._table_schema = tuple(table_schema)
        self._connection = connection
        self._validate_table_access = validate_table_access
        self._client = client
        self._write_client = write_client
        self._stream = stream
        self._stream_name = stream_name
        self._serializer = serializer
        self._stream_factory = stream_factory
        self._serializer_factory = serializer_factory
        self._resolve_table_path = resolve_table_path

    @property
    def client(self) -> object | None:
        return self._client

    @client.setter
    def client(self, value: object | None) -> None:
        self._client = value

    @property
    def write_client(self) -> object | None:
        return self._write_client

    @write_client.setter
    def write_client(self, value: object | None) -> None:
        self._write_client = value

    @property
    def stream(self) -> object | None:
        return self._stream

    @stream.setter
    def stream(self, value: object | None) -> None:
        self._stream = value

    @property
    def stream_name(self) -> str | None:
        return self._stream_name

    @stream_name.setter
    def stream_name(self, value: str | None) -> None:
        self._stream_name = value

    @property
    def serializer(self) -> object | None:
        return self._serializer

    @serializer.setter
    def serializer(self, value: object | None) -> None:
        self._serializer = value

    def connection_ready(self) -> bool:
        return self._stream is not None and self._write_client is not None

    async def open(self) -> None:
        if self._write_client is None:
            self._write_client = build_bigquery_write_client(self._connection)
        table_metadata: object | None = None
        if self._validate_table_access:
            if self._client is None:
                self._client = build_bigquery_client(self._connection)
            assert self._client is not None
            table_metadata = await asyncio.to_thread(self._client.get_table, self._table)
        if self._serializer is None:
            schema_fields = self._table_schema
            if not schema_fields:
                if table_metadata is None:
                    if self._client is None:
                        self._client = build_bigquery_client(self._connection)
                    assert self._client is not None
                    table_metadata = await asyncio.to_thread(self._client.get_table, self._table)
                schema_fields = tuple(getattr(table_metadata, "schema", ()) or ())
            self._serializer = self._serializer_factory(schema_fields)
        if self._stream is not None:
            return
        if (
            self._client is None
            and self._connection.project is None
            and self._table.replace(":", ".", 1).count(".") == 1
        ):
            self._client = build_bigquery_client(self._connection)
        project_id, dataset_id, table_id = self._resolve_table_path(self._table)
        self._stream_name = (
            f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}/streams/_default"
        )
        self._stream = self._stream_factory(
            self._write_client,
            self._stream_name,
            self._serializer.descriptor_proto,
        )

    async def close(self, *, flush: Callable[[], Awaitable[None]]) -> None:
        try:
            await flush()
        finally:
            if self._stream is not None:
                close = getattr(self._stream, "close", None)
                if callable(close):
                    close()
                self._stream = None
            for client in (self._write_client, self._client):
                close = getattr(client, "close", None)
                if callable(close):
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result


__all__ = ["BigQueryStorageWriteSession"]
