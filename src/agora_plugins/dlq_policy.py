"""Compatibility shim for the core-owned DLQ payload policy contract."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Iterable

try:
    from agora.core import DLQPayloadPolicy as DLQPayloadPolicy
except ImportError:
    _REDACTED_VALUE = "[REDACTED]"
    _COMMON_SENSITIVE_FIELDS = frozenset(
        {
            "access_token",
            "api_key",
            "authorization",
            "client_secret",
            "cookie",
            "password",
            "private_key",
            "refresh_token",
            "secret",
            "set-cookie",
            "token",
            "x-api-key",
            "x_api_key",
        }
    )
    _COMMON_SENSITIVE_HEADERS = frozenset(
        {
            "api-key",
            "authorization",
            "cookie",
            "proxy-authorization",
            "set-cookie",
            "x-api-key",
            "x-auth-token",
        }
    )

    @dataclass(frozen=True, slots=True)
    class DLQPayloadPolicy:
        """Controls how DLQ payloads are persisted."""

        mode: Literal["raw", "redacted", "encrypted"] = "raw"
        redact_fields: tuple[str, ...] = ()
        redact_headers: tuple[str, ...] = ()
        redacted_value: str = _REDACTED_VALUE
        include_common_sensitive_names: bool = True
        encryptor: Any | None = None
        decryptor: Any | None = None
        encryption_algorithm: str = "user"
        encryption_key_id: str | None = None

        def __post_init__(self) -> None:
            if self.mode not in {"raw", "redacted", "encrypted"}:
                raise ValueError("DLQPayloadPolicy mode must be 'raw', 'redacted', or 'encrypted'.")
            if self.mode == "encrypted" and self.encryptor is None:
                raise ValueError("DLQPayloadPolicy mode='encrypted' requires an encryptor.")
            object.__setattr__(
                self,
                "redact_fields",
                tuple(_normalize_sensitive_name(name) for name in self.redact_fields),
            )
            object.__setattr__(
                self,
                "redact_headers",
                tuple(_normalize_sensitive_name(name) for name in self.redact_headers),
            )

        @classmethod
        def raw(cls) -> DLQPayloadPolicy:
            return cls(mode="raw")

        @classmethod
        def redacted(
            cls,
            *,
            redact_fields: Iterable[str] = (),
            redact_headers: Iterable[str] = (),
            redacted_value: str = _REDACTED_VALUE,
            include_common_sensitive_names: bool = True,
        ) -> DLQPayloadPolicy:
            return cls(
                mode="redacted",
                redact_fields=tuple(redact_fields),
                redact_headers=tuple(redact_headers),
                redacted_value=redacted_value,
                include_common_sensitive_names=include_common_sensitive_names,
            )

        @classmethod
        def encrypted(
            cls,
            *,
            encryptor: Any,
            decryptor: Any | None = None,
            encryption_algorithm: str = "user",
            encryption_key_id: str | None = None,
        ) -> DLQPayloadPolicy:
            return cls(
                mode="encrypted",
                encryptor=encryptor,
                decryptor=decryptor,
                encryption_algorithm=encryption_algorithm,
                encryption_key_id=encryption_key_id,
            )

        def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
            if self.mode in {"raw", "encrypted"}:
                return payload
            field_names = set(self.redact_fields)
            header_names = set(self.redact_headers)
            if self.include_common_sensitive_names:
                field_names.update(_COMMON_SENSITIVE_FIELDS)
                header_names.update(_COMMON_SENSITIVE_HEADERS)
            return cast(
                "dict[str, Any]",
                _redact_value(
                    payload,
                    field_names=field_names,
                    header_names=header_names,
                    redacted_value=self.redacted_value,
                    parent_key=None,
                ),
            )

        def encrypt_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
            if self.mode != "encrypted":
                raise ValueError("DLQPayloadPolicy.encrypt_payload requires mode='encrypted'.")
            if self.encryptor is None:
                raise ValueError("DLQPayloadPolicy mode='encrypted' requires an encryptor.")
            plaintext = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode(
                "utf-8"
            )
            encrypted = self.encryptor.encrypt(plaintext)
            if isinstance(encrypted, str):
                encrypted = encrypted.encode("utf-8")
            if not isinstance(encrypted, bytes):
                raise TypeError("DLQ payload encryptor must return bytes or str.")
            envelope = {
                "payload_encoding": "encrypted",
                "payload_algorithm": self.encryption_algorithm,
                "payload_ciphertext": base64.b64encode(encrypted).decode("ascii"),
            }
            if self.encryption_key_id is not None:
                envelope["payload_key_id"] = self.encryption_key_id
            return envelope

        def decrypt_payload(self, encrypted_payload: dict[str, Any]) -> dict[str, Any]:
            if self.mode != "encrypted":
                raise ValueError("Encrypted DLQ payload requires mode='encrypted' policy.")
            decryptor = self.decryptor or self.encryptor
            if decryptor is None:
                raise ValueError("Encrypted DLQ payload requires a decryptor.")
            ciphertext = base64.b64decode(str(encrypted_payload["payload_ciphertext"]))
            plaintext = decryptor.decrypt(ciphertext)
            decoded = plaintext.decode("utf-8") if isinstance(plaintext, bytes) else str(plaintext)
            return cast("dict[str, Any]", json.loads(decoded))

    def _normalize_sensitive_name(name: str) -> str:
        return str(name).strip().lower()

    def _redacted_header_value(redacted_value: str) -> dict[str, str]:
        return {"encoding": "redacted", "data": redacted_value}

    def _redact_value(
        value: Any,
        *,
        field_names: set[str],
        header_names: set[str],
        redacted_value: str,
        parent_key: str | None,
    ) -> Any:
        if isinstance(value, dict):
            redacted: dict[str, Any] = {}
            for key, item in value.items():
                normalized = _normalize_sensitive_name(str(key))
                if normalized in field_names:
                    redacted[str(key)] = redacted_value
                    continue
                redacted[str(key)] = _redact_value(
                    item,
                    field_names=field_names,
                    header_names=header_names,
                    redacted_value=redacted_value,
                    parent_key=normalized,
                )
            return redacted
        if isinstance(value, list):
            if parent_key == "headers":
                return [
                    _redact_header(item, header_names=header_names, redacted_value=redacted_value)
                    for item in value
                ]
            return [
                _redact_value(
                    item,
                    field_names=field_names,
                    header_names=header_names,
                    redacted_value=redacted_value,
                    parent_key=parent_key,
                )
                for item in value
            ]
        return value

    def _redact_header(
        item: Any,
        *,
        header_names: set[str],
        redacted_value: str,
    ) -> Any:
        if not isinstance(item, dict):
            return item
        header_key = _normalize_sensitive_name(str(item.get("key", "")))
        if header_key not in header_names:
            return {
                str(key): _redact_value(
                    value,
                    field_names=set(),
                    header_names=header_names,
                    redacted_value=redacted_value,
                    parent_key=_normalize_sensitive_name(str(key)),
                )
                for key, value in item.items()
            }
        redacted = dict(item)
        redacted["value"] = _redacted_header_value(redacted_value)
        return redacted


__all__ = ["DLQPayloadPolicy"]
