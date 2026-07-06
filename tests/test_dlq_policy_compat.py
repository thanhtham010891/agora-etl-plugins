from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_DLQ_POLICY_PATH = Path(__file__).resolve().parents[1] / "src" / "agora_plugins" / "dlq_policy.py"


class _ReverseCipher:
    def encrypt(self, payload: bytes) -> bytes:
        return payload[::-1]

    def decrypt(self, payload: bytes) -> bytes:
        return payload[::-1]


def test_dlq_policy_shim_defines_fallback_when_core_lacks_export(monkeypatch) -> None:
    import agora.core as core

    monkeypatch.delattr(core, "DLQPayloadPolicy", raising=False)
    module_name = "tests._compat_dlq_policy"
    spec = importlib.util.spec_from_file_location(module_name, _DLQ_POLICY_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)

        redacted = module.DLQPayloadPolicy.redacted(
            redact_fields=("customer_secret",),
            redact_headers=("authorization",),
        )
        payload = redacted.apply(
            {
                "customer_secret": "top-secret",
                "headers": [{"key": "authorization", "value": {"token": "abc"}}],
            }
        )
        encrypted = module.DLQPayloadPolicy.encrypted(encryptor=_ReverseCipher())
        envelope = encrypted.encrypt_payload({"pipeline_id": "demo", "record": {"id": 1}})

        assert payload["customer_secret"] == "[REDACTED]"
        assert payload["headers"][0]["value"] == {"encoding": "redacted", "data": "[REDACTED]"}
        assert envelope["payload_encoding"] == "encrypted"
        assert encrypted.decrypt_payload(envelope)["record"] == {"id": 1}
    finally:
        sys.modules.pop(module_name, None)
