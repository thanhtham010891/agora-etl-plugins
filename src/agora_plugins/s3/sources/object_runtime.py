"""Object scan and download runtime for S3 dataset sources."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.sources.file.csv import CsvSource
from agora.sources.file.jsonlines import JsonLinesSource
from agora.sources.file.parquet import ParquetSource

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Sequence
    from typing import Literal

    from agora.core.source import BaseSource
    from agora.core.types import SourceRecordFailurePolicy

T = TypeVar("T")
_DOWNLOAD_CHUNK_BYTES = 1024 * 1024


class S3SourceObjectRuntime(Generic[T]):
    """Public-facing object scan and download runtime for S3 sources."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        format: Literal["jsonl", "csv", "parquet"],
        row_mapper: Callable[[dict[str, Any]], T | None],
        delimiter: str,
        has_header: bool,
        fieldnames: Sequence[str],
        encoding: str,
        on_record_error: SourceRecordFailurePolicy,
        client_provider: Callable[[], Any | None],
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._format = format
        self._row_mapper = row_mapper
        self._delimiter = delimiter
        self._has_header = has_header
        self._fieldnames = list(fieldnames)
        self._encoding = encoding
        self._on_record_error = on_record_error
        self._client_provider = client_provider

    async def iter_object_keys(self) -> AsyncGenerator[str, None]:
        client = self._client_provider()
        assert client is not None
        paginator = await asyncio.to_thread(client.get_paginator, "list_objects_v2")
        pages = await asyncio.to_thread(
            lambda: iter(paginator.paginate(Bucket=self._bucket, Prefix=self._prefix))
        )
        while True:
            page = await asyncio.to_thread(lambda: next(pages, None))
            if page is None:
                break
            for item in page.get("Contents", []):
                yield str(item["Key"])

    async def download_to_tempfile(self, object_key: str) -> Path:
        return await asyncio.to_thread(self._download_to_tempfile, object_key)

    def build_local_source(self, temp_path: Path) -> BaseSource[T]:
        if self._format == "jsonl":
            return JsonLinesSource(
                temp_path,
                row_mapper=self._row_mapper,
                encoding=self._encoding,
                on_record_error=self._on_record_error,
            )
        if self._format == "csv":
            return CsvSource(
                temp_path,
                row_mapper=self._row_mapper,
                delimiter=self._delimiter,
                has_header=self._has_header,
                fieldnames=self._fieldnames or None,
                encoding=self._encoding,
                on_record_error=self._on_record_error,
            )
        return ParquetSource(
            temp_path,
            row_mapper=self._row_mapper,
            on_record_error=self._on_record_error,
        )

    def _download_to_tempfile(self, object_key: str) -> Path:
        client = self._client_provider()
        assert client is not None
        response = client.get_object(Bucket=self._bucket, Key=object_key)
        fd, raw_path = tempfile.mkstemp(suffix=f".{self._format}")
        path = Path(raw_path)
        try:
            with os.fdopen(fd, "wb") as target:
                body = response["Body"]
                while chunk := body.read(_DOWNLOAD_CHUNK_BYTES):
                    target.write(chunk)
        except Exception:
            path.unlink(missing_ok=True)
            raise
        return path


__all__ = ["S3SourceObjectRuntime"]
