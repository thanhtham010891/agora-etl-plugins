"""White-box compatibility accessors for BigQuery Storage Write sinks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from datetime import datetime

T = TypeVar("T")


class BigQueryStorageWriteCompatMixin(Generic[T]):
    """Keep legacy/private test accessors out of the public sink facade."""

    @property
    def _batch_size(self) -> int:
        if "_flush_runtime" not in self.__dict__:
            return self.__dict__["_batch_size"]
        return self._flush_runtime.batch_size

    @_batch_size.setter
    def _batch_size(self, value: int) -> None:
        if hasattr(self, "_flush_runtime"):
            self._flush_runtime._batch_size = value
            return
        self.__dict__["_batch_size"] = value

    @property
    def _max_request_bytes(self) -> int:
        if "_flush_runtime" not in self.__dict__:
            return self.__dict__["_max_request_bytes"]
        return self._flush_runtime.max_request_bytes

    @_max_request_bytes.setter
    def _max_request_bytes(self, value: int) -> None:
        if hasattr(self, "_flush_runtime"):
            self._flush_runtime.max_request_bytes = value
            return
        self.__dict__["_max_request_bytes"] = value

    @property
    def _buffer(self) -> list[T]:
        return self._flush_runtime.buffer

    @_buffer.setter
    def _buffer(self, value: list[T]) -> None:
        self._flush_runtime.buffer = value

    @property
    def _flush_count(self) -> int:
        return self._flush_runtime.flush_count

    @_flush_count.setter
    def _flush_count(self, value: int) -> None:
        self._flush_runtime.flush_count = value

    @property
    def _append_error_count(self) -> int:
        return self._flush_runtime.append_error_count

    @_append_error_count.setter
    def _append_error_count(self, value: int) -> None:
        self._flush_runtime.append_error_count = value

    @property
    def _submitted_row_count(self) -> int:
        return self._flush_runtime.submitted_row_count

    @_submitted_row_count.setter
    def _submitted_row_count(self, value: int) -> None:
        self._flush_runtime.submitted_row_count = value

    @property
    def _appended_row_count(self) -> int:
        return self._flush_runtime.appended_row_count

    @_appended_row_count.setter
    def _appended_row_count(self, value: int) -> None:
        self._flush_runtime.appended_row_count = value

    @property
    def _last_append_offset(self) -> int | None:
        return self._flush_runtime.last_append_offset

    @_last_append_offset.setter
    def _last_append_offset(self, value: int | None) -> None:
        self._flush_runtime.last_append_offset = value

    @property
    def _last_append_at(self) -> datetime | None:
        return self._flush_runtime.last_append_at

    @_last_append_at.setter
    def _last_append_at(self, value: datetime | None) -> None:
        self._flush_runtime.last_append_at = value

    @property
    def _last_append_succeeded(self) -> bool | None:
        return self._flush_runtime.last_append_succeeded

    @_last_append_succeeded.setter
    def _last_append_succeeded(self, value: bool | None) -> None:
        self._flush_runtime.last_append_succeeded = value

    @property
    def _last_error(self) -> str | None:
        return self._flush_runtime.last_error

    @_last_error.setter
    def _last_error(self, value: str | None) -> None:
        self._flush_runtime.last_error = value

    @property
    def _client(self) -> object | None:
        return self._session.client

    @_client.setter
    def _client(self, value: object | None) -> None:
        self._session.client = value

    @property
    def _write_client(self) -> object | None:
        return self._session.write_client

    @_write_client.setter
    def _write_client(self, value: object | None) -> None:
        self._session.write_client = value

    @property
    def _stream(self) -> object | None:
        return self._session.stream

    @_stream.setter
    def _stream(self, value: object | None) -> None:
        self._session.stream = value

    @property
    def _stream_name(self) -> str | None:
        return self._session.stream_name

    @_stream_name.setter
    def _stream_name(self, value: str | None) -> None:
        self._session.stream_name = value

    @property
    def _serializer(self) -> object | None:
        return self._session.serializer

    @_serializer.setter
    def _serializer(self, value: object | None) -> None:
        self._session.serializer = value


__all__ = ["BigQueryStorageWriteCompatMixin"]
