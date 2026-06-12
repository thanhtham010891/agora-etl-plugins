from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest
from agora.ai.providers.base import CompletionResponse
from pydantic import BaseModel

from agora_plugins.anthropic import MANIFEST, AnthropicProvider

_PACKAGE_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT_PATH = _PACKAGE_ROOT / "pyproject.toml"


class _StructuredReview(BaseModel):
    summary: str
    sentiment: str


def _install_fake_anthropic(
    monkeypatch: pytest.MonkeyPatch,
    *,
    response_text: str,
    input_tokens: int = 12,
    output_tokens: int = 7,
) -> tuple[list[dict[str, object]], list[str]]:
    calls: list[dict[str, object]] = []
    api_keys: list[str] = []

    class _FakeMessages:
        async def create(self, **kwargs: object) -> object:
            calls.append(kwargs)
            return SimpleNamespace(
                content=[SimpleNamespace(text=response_text)],
                usage=SimpleNamespace(
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                ),
            )

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str) -> None:
            api_keys.append(api_key)
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    return calls, api_keys


def test_manifest_version_matches_bundle_metadata() -> None:
    metadata = tomllib.loads(_PYPROJECT_PATH.read_text(encoding="utf-8"))

    assert metadata["project"]["name"] == MANIFEST.package
    assert MANIFEST.version in {metadata["project"]["version"], "0+unknown"}


def test_package_root_exports_manifest_and_provider() -> None:
    from agora_plugins.anthropic import __all__

    assert "MANIFEST" in __all__
    assert "AnthropicProvider" in __all__


def test_pyproject_registers_anthropic_extra_and_entrypoint() -> None:
    metadata = tomllib.loads(_PYPROJECT_PATH.read_text(encoding="utf-8"))

    assert "anthropic" in metadata["project"]["optional-dependencies"]
    assert metadata["project"]["entry-points"]["agora.ai.providers"]["anthropic"] == (
        "agora_plugins.anthropic:AnthropicProvider"
    )


@pytest.mark.asyncio
async def test_complete_returns_completion_response(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, api_keys = _install_fake_anthropic(
        monkeypatch,
        response_text='{"summary": "Great broth", "sentiment": "positive"}',
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

    provider = AnthropicProvider(model="claude-3-5-haiku-20241022")
    response = await provider.complete(
        "Summarize this review",
        system="Return concise JSON.",
        max_tokens=256,
    )

    assert api_keys == ["test-key"]
    assert isinstance(response, CompletionResponse)
    assert response.content == '{"summary": "Great broth", "sentiment": "positive"}'
    assert response.model == "claude-3-5-haiku-20241022"
    assert response.input_tokens == 12
    assert response.output_tokens == 7
    assert calls == [
        {
            "model": "claude-3-5-haiku-20241022",
            "max_tokens": 256,
            "temperature": 0.0,
            "messages": [{"role": "user", "content": "Summarize this review"}],
            "system": "Return concise JSON.",
        }
    ]


@pytest.mark.asyncio
async def test_complete_with_response_format_adds_schema_instruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls, _api_keys = _install_fake_anthropic(
        monkeypatch,
        response_text='{"summary": "Good coffee", "sentiment": "positive"}',
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "schema-key")

    provider = AnthropicProvider()
    response = await provider.complete(
        "Label this cafe review",
        system="Return JSON only.",
        response_format=_StructuredReview,
    )

    assert response.content == '{"summary": "Good coffee", "sentiment": "positive"}'
    system_text = str(calls[0]["system"])
    assert "Return JSON only." in system_text
    assert "respond with a valid json object matching this schema" in system_text.lower()
    assert "summary" in system_text
    assert "sentiment" in system_text


@pytest.mark.asyncio
async def test_complete_raises_clear_error_for_invalid_structured_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_anthropic(monkeypatch, response_text="not-json-at-all")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "bad-json-key")

    provider = AnthropicProvider()

    with pytest.raises(ValueError, match="did not match expected schema"):
        await provider.complete(
            "Label this cafe review",
            response_format=_StructuredReview,
        )


def test_provider_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_anthropic(monkeypatch, response_text='{"summary": "ok", "sentiment": "neutral"}')
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    with pytest.raises(ValueError, match="Anthropic API key is required"):
        AnthropicProvider()


@pytest.mark.asyncio
async def test_embedding_methods_raise_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_anthropic(monkeypatch, response_text='{"summary": "ok", "sentiment": "neutral"}')
    monkeypatch.setenv("ANTHROPIC_API_KEY", "embed-key")
    provider = AnthropicProvider()

    with pytest.raises(NotImplementedError, match="does not provide embedding models"):
        await provider.embed("hello world")

    with pytest.raises(NotImplementedError, match="does not provide embedding models"):
        await provider.embed_batch(["a", "b"])
