"""Serializer and schema coercion collaborator for BigQuery Storage Write sinks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, ClassVar

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

_PROTO_PACKAGE = "agora_plugins.bigquery.storagewrite"
_ROOT_MESSAGE_NAME = "AgoraBigQueryRow"
_EPOCH_DATE = date(1970, 1, 1)
_MICROS_PER_SECOND = 1_000_000


def _schema_field_name(field: Any) -> str:
    return str(field.name)


def _schema_field_type(field: Any) -> str:
    return str(field.field_type).upper()


def _schema_field_mode(field: Any) -> str:
    return str(getattr(field, "mode", "NULLABLE")).upper()


def _schema_field_children(field: Any) -> tuple[Any, ...]:
    return tuple(getattr(field, "fields", ()) or ())


def _message_class_from_descriptor(
    pool: descriptor_pool.DescriptorPool, full_name: str
) -> type[Any]:
    descriptor = pool.FindMessageTypeByName(full_name)
    get_message_class = getattr(message_factory, "GetMessageClass", None)
    if callable(get_message_class):
        return get_message_class(descriptor)
    factory = message_factory.MessageFactory(pool)
    return factory.GetPrototype(descriptor)


@dataclass(frozen=True, slots=True)
class _ProtoSerializerBuild:
    descriptor_proto: descriptor_pb2.DescriptorProto
    message_cls: type[Any]


class BigQueryStorageWriteRowSerializer:
    """Public-facing proto row serializer over the supported BigQuery schema subset."""

    _SIMPLE_FIELD_TYPES: ClassVar[dict[str, descriptor_pb2.FieldDescriptorProto.Type]] = {
        "STRING": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "BYTES": descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
        "BOOL": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
        "BOOLEAN": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
        "INT64": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "INTEGER": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "FLOAT64": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
        "FLOAT": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
        "DATE": descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
        "DATETIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "TIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "TIMESTAMP": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "NUMERIC": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "BIGNUMERIC": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "JSON": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "GEOGRAPHY": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    }

    def __init__(self, schema_fields: tuple[Any, ...] | list[Any]) -> None:
        self._schema_fields = tuple(schema_fields)
        build = self._build(self._schema_fields)
        self.descriptor_proto = build.descriptor_proto
        self._message_cls = build.message_cls

    def serialize_row(self, row: dict[str, Any]) -> bytes:
        message = self._message_cls()
        self._populate_message(message, self._schema_fields, row, path="")
        return message.SerializeToString()

    def _build(self, schema_fields: tuple[Any, ...]) -> _ProtoSerializerBuild:
        file_proto = descriptor_pb2.FileDescriptorProto()
        file_proto.name = "agora_plugins_bigquery_storage_write.proto"
        file_proto.package = _PROTO_PACKAGE
        file_proto.syntax = "proto2"
        root = file_proto.message_type.add()
        root.name = _ROOT_MESSAGE_NAME
        self._add_fields(root, schema_fields)
        pool = descriptor_pool.DescriptorPool()
        pool.Add(file_proto)
        return _ProtoSerializerBuild(
            descriptor_proto=descriptor_pb2.DescriptorProto.FromString(root.SerializeToString()),
            message_cls=_message_class_from_descriptor(
                pool, f"{_PROTO_PACKAGE}.{_ROOT_MESSAGE_NAME}"
            ),
        )

    def _add_fields(
        self,
        message_proto: descriptor_pb2.DescriptorProto,
        schema_fields: tuple[Any, ...],
    ) -> None:
        for index, field in enumerate(schema_fields, start=1):
            field_proto = message_proto.field.add()
            field_proto.name = _schema_field_name(field)
            field_proto.number = index
            field_proto.label = (
                descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
                if _schema_field_mode(field) == "REPEATED"
                else descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
            )
            field_type = _schema_field_type(field)
            if field_type == "RECORD":
                raise ValueError(
                    "BigQueryStorageWriteSink current GA boundary does not yet support "
                    f"RECORD fields. Unsupported BigQuery type: {field_type} "
                    f"for field {_schema_field_name(field)!r}. Use flat scalar columns "
                    "and REPEATED scalar arrays."
                )
            try:
                field_proto.type = self._SIMPLE_FIELD_TYPES[field_type]
            except KeyError as exc:
                raise ValueError(
                    "BigQueryStorageWriteSink phase 2 supports STRING, BYTES, "
                    "BOOL, INT64, FLOAT64, DATE, DATETIME, TIME, TIMESTAMP, "
                    "NUMERIC, BIGNUMERIC, JSON, GEOGRAPHY, and REPEATED variants "
                    "of those scalar types. Unsupported BigQuery type: "
                    f"{field_type} "
                    f"for field {_schema_field_name(field)!r}."
                ) from exc

    def _populate_message(
        self,
        message: Any,
        schema_fields: tuple[Any, ...],
        row: dict[str, Any],
        *,
        path: str,
    ) -> None:
        for field in schema_fields:
            name = _schema_field_name(field)
            current_path = f"{path}.{name}" if path else name
            if name not in row:
                if _schema_field_mode(field) == "REQUIRED":
                    raise ValueError(f"Missing required BigQuery field {current_path!r}.")
                continue
            value = row[name]
            if value is None:
                if _schema_field_mode(field) == "REQUIRED":
                    raise ValueError(f"BigQuery field {current_path!r} cannot be null.")
                continue
            if _schema_field_mode(field) == "REPEATED":
                if not isinstance(value, (list, tuple)):
                    raise TypeError(
                        f"BigQuery field {current_path!r} is REPEATED and expects list/tuple input."
                    )
                self._populate_repeated(message, field, tuple(value), path=current_path)
                continue
            if _schema_field_type(field) == "RECORD":
                if not isinstance(value, dict):
                    raise TypeError(
                        f"BigQuery RECORD field {current_path!r} expects dict input, "
                        f"got {type(value).__name__}."
                    )
                child = getattr(message, name)
                self._populate_message(
                    child, _schema_field_children(field), value, path=current_path
                )
                continue
            setattr(message, name, self._coerce_scalar(field, value, path=current_path))

    def _populate_repeated(
        self,
        message: Any,
        field: Any,
        values: tuple[Any, ...],
        *,
        path: str,
    ) -> None:
        target = getattr(message, _schema_field_name(field))
        if _schema_field_type(field) == "RECORD":
            for index, item in enumerate(values):
                if not isinstance(item, dict):
                    raise TypeError(
                        f"BigQuery RECORD field {path}[{index}] expects dict input, "
                        f"got {type(item).__name__}."
                    )
                child = target.add()
                self._populate_message(
                    child,
                    _schema_field_children(field),
                    item,
                    path=f"{path}[{index}]",
                )
            return
        target.extend(
            self._coerce_scalar(field, item, path=f"{path}[{index}]")
            for index, item in enumerate(values)
        )

    def _coerce_scalar(self, field: Any, value: Any, *, path: str) -> Any:
        field_type = _schema_field_type(field)
        if field_type == "STRING":
            return str(value)
        if field_type == "BYTES":
            if isinstance(value, bytes):
                return value
            if isinstance(value, (bytearray, memoryview)):
                return bytes(value)
            if isinstance(value, str):
                return value.encode("utf-8")
            raise TypeError(f"BigQuery BYTES field {path!r} expects bytes-like input.")
        if field_type in {"BOOL", "BOOLEAN"}:
            if not isinstance(value, bool):
                raise TypeError(f"BigQuery BOOL field {path!r} expects bool input.")
            return value
        if field_type in {"INT64", "INTEGER"}:
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"BigQuery INT64 field {path!r} expects int input.")
            return value
        if field_type in {"FLOAT64", "FLOAT"}:
            if isinstance(value, bool) or not isinstance(value, int | float | Decimal):
                raise TypeError(f"BigQuery FLOAT64 field {path!r} expects numeric input.")
            return float(value)
        if field_type == "DATE":
            return self._coerce_date(value, path=path)
        if field_type == "DATETIME":
            return self._coerce_datetime(value, path=path)
        if field_type == "TIME":
            return self._coerce_time(value, path=path)
        if field_type == "TIMESTAMP":
            return self._coerce_timestamp(value, path=path)
        if field_type in {"NUMERIC", "BIGNUMERIC"}:
            return self._coerce_decimal_string(value, path=path, field_type=field_type)
        if field_type == "JSON":
            return self._coerce_json(value, path=path)
        if field_type == "GEOGRAPHY":
            return self._coerce_geography(value, path=path)
        raise TypeError(
            f"Unsupported BigQuery phase-2 field type {_schema_field_type(field)!r} at {path!r}."
        )

    def _coerce_date(self, value: Any, *, path: str) -> int:
        if isinstance(value, datetime):
            raise TypeError(
                f"BigQuery DATE field {path!r} expects date/int/iso-date input, not datetime."
            )
        if isinstance(value, date):
            return (value - _EPOCH_DATE).days
        if isinstance(value, str):
            try:
                parsed = date.fromisoformat(value)
            except ValueError as exc:
                raise TypeError(
                    f"BigQuery DATE field {path!r} expects YYYY-MM-DD string input."
                ) from exc
            return (parsed - _EPOCH_DATE).days
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"BigQuery DATE field {path!r} expects date/int/iso-date input.")
        return value

    def _coerce_datetime(self, value: Any, *, path: str) -> str:
        if isinstance(value, datetime):
            normalized = value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
            return normalized.isoformat(sep=" ")
        if isinstance(value, str):
            return value
        raise TypeError(
            f"BigQuery DATETIME field {path!r} expects datetime or ISO-like string input."
        )

    def _coerce_time(self, value: Any, *, path: str) -> str:
        if isinstance(value, time):
            if value.tzinfo is not None:
                raise TypeError(
                    f"BigQuery TIME field {path!r} does not accept timezone-aware time input."
                )
            return value.isoformat()
        if isinstance(value, str):
            return value
        raise TypeError(f"BigQuery TIME field {path!r} expects time or HH:MM:SS string input.")

    def _coerce_timestamp(self, value: Any, *, path: str) -> int:
        if isinstance(value, datetime):
            normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
            return int(normalized.timestamp() * _MICROS_PER_SECOND)
        if isinstance(value, str):
            candidate = value.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError as exc:
                raise TypeError(
                    f"BigQuery TIMESTAMP field {path!r} expects datetime/int/ISO-8601 string input."
                ) from exc
            normalized = parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            return int(normalized.timestamp() * _MICROS_PER_SECOND)
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(
                f"BigQuery TIMESTAMP field {path!r} expects datetime/int/ISO-8601 string input."
            )
        return value

    def _coerce_decimal_string(self, value: Any, *, path: str, field_type: str) -> str:
        if isinstance(value, bool):
            raise TypeError(
                f"BigQuery {field_type} field {path!r} expects Decimal/int/string input."
            )
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, int):
            return str(value)
        if isinstance(value, str):
            try:
                Decimal(value)
            except Exception as exc:
                raise TypeError(
                    f"BigQuery {field_type} field {path!r} expects decimal-compatible string input."
                ) from exc
            return value
        raise TypeError(f"BigQuery {field_type} field {path!r} expects Decimal/int/string input.")

    def _coerce_json(self, value: Any, *, path: str) -> str:
        if isinstance(value, str):
            try:
                json.loads(value)
            except ValueError as exc:
                raise TypeError(
                    f"BigQuery JSON field {path!r} expects valid JSON string or JSON-serializable input."
                ) from exc
            return value
        try:
            return json.dumps(value, separators=(",", ":"), default=self._json_default)
        except TypeError as exc:
            raise TypeError(
                f"BigQuery JSON field {path!r} expects JSON-serializable input."
            ) from exc

    def _coerce_geography(self, value: Any, *, path: str) -> str:
        if isinstance(value, str):
            return value
        geo_interface = getattr(value, "__geo_interface__", None)
        if geo_interface is not None:
            return json.dumps(geo_interface, separators=(",", ":"), default=self._json_default)
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, separators=(",", ":"), default=self._json_default)
        raise TypeError(
            f"BigQuery GEOGRAPHY field {path!r} expects WKT/GeoJSON string, __geo_interface__, or GeoJSON mapping input."
        )

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


__all__ = ["BigQueryStorageWriteRowSerializer"]
