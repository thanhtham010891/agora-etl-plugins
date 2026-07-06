from __future__ import annotations

import asyncio
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
    cache_read_input_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
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
                    cache_read_input_tokens=cache_read_input_tokens,
                    cache_creation_input_tokens=cache_creation_input_tokens,
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


def _anthropic_message(
    text: str,
    *,
    input_tokens: int = 1,
    output_tokens: int = 1,
) -> SimpleNamespace:
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=text)],
        usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens),
    )


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

    provider = AnthropicProvider(model="claude-haiku-4-5-20251001")
    response = await provider.complete(
        "Summarize this review",
        system="Return concise JSON.",
        max_tokens=256,
    )

    assert api_keys == ["test-key"]
    assert isinstance(response, CompletionResponse)
    assert response.content == '{"summary": "Great broth", "sentiment": "positive"}'
    assert response.model == "claude-haiku-4-5-20251001"
    assert response.input_tokens == 12
    assert response.output_tokens == 7
    assert calls == [
        {
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 256,
            "temperature": 0.0,
            "messages": [{"role": "user", "content": "Summarize this review"}],
            "system": "Return concise JSON.",
        }
    ]


@pytest.mark.asyncio
async def test_complete_counts_cached_input_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    _calls, _api_keys = _install_fake_anthropic(
        monkeypatch,
        response_text="cached",
        input_tokens=10,
        cache_read_input_tokens=3,
        cache_creation_input_tokens=4,
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

    provider = AnthropicProvider(model="claude-haiku-4-5-20251001")
    response = await provider.complete("Summarize this review")

    assert response.input_tokens == 17


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
        use_tool_for_response_format=False,
    )

    assert response.content == '{"summary": "Good coffee", "sentiment": "positive"}'
    system_text = str(calls[0]["system"])
    assert "Return JSON only." in system_text
    assert "respond with a valid json object matching this schema" in system_text.lower()
    assert "summary" in system_text
    assert "sentiment" in system_text


@pytest.mark.asyncio
async def test_complete_with_response_format_returns_repaired_json_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_anthropic(
        monkeypatch,
        response_text='Here is the JSON:\n{"summary": "Good coffee", "sentiment": "positive"}\nDone.',
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "repair-key")

    provider = AnthropicProvider()
    response = await provider.complete(
        "Label this cafe review",
        response_format=_StructuredReview,
        use_tool_for_response_format=False,
        repair_invalid_json=True,
    )

    assert response.content == '{"summary": "Good coffee", "sentiment": "positive"}'
    assert _StructuredReview.model_validate_json(response.content).sentiment == "positive"


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
            use_tool_for_response_format=False,
        )


@pytest.mark.asyncio
async def test_complete_retries_retryable_anthropic_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    class _RateLimitError(RuntimeError):
        status_code = 429

    class _FakeMessages:
        async def create(self, **kwargs: object) -> object:
            calls.append(kwargs)
            if len(calls) == 1:
                raise _RateLimitError("rate limited")
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text="ok")],
                usage=SimpleNamespace(input_tokens=1, output_tokens=1),
            )

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic, RateLimitError=_RateLimitError),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "retry-key")

    provider = AnthropicProvider(max_retries=2, retry_initial_backoff_s=0)
    response = await provider.complete("hello")

    assert response.content == "ok"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_complete_retry_honors_retry_after_header(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []
    sleeps: list[float] = []

    class _RateLimitError(RuntimeError):
        status_code = 429

        def __init__(self, message: str) -> None:
            super().__init__(message)
            self.headers = {"Retry-After": "2.5"}

    class _FakeMessages:
        async def create(self, **kwargs: object) -> object:
            calls.append(kwargs)
            if len(calls) == 1:
                raise _RateLimitError("rate limited")
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text="ok")],
                usage=SimpleNamespace(input_tokens=1, output_tokens=1),
            )

    class _FakeAsyncAnthropic:
        def __init__(
            self, *, api_key: str, max_retries: int = 0, timeout: float | None = None
        ) -> None:
            del api_key, max_retries, timeout
            self.messages = _FakeMessages()

    async def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic, RateLimitError=_RateLimitError),
    )
    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "retry-after-key")

    provider = AnthropicProvider(max_retries=2, retry_initial_backoff_s=0)
    response = await provider.complete("hello")

    assert response.content == "ok"
    assert sleeps == [2.5]


def test_retryable_error_does_not_retry_permanent_501_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=lambda **_kwargs: object()),
    )
    provider = AnthropicProvider(api_key="test-key")

    class _NotImplementedError(RuntimeError):
        status_code = 501
        response = object()

    assert provider._is_retryable_error(_NotImplementedError("not implemented")) is False


def test_anthropic_retry_policy_uses_configured_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=lambda **_kwargs: object()),
    )

    provider = AnthropicProvider(
        api_key="test-key",
        retry_initial_backoff_s=1.0,
        retry_max_backoff_s=4.0,
        retry_jitter_ratio=0.25,
    )

    assert provider._retry_policy.jitter_ratio == 0.25  # type: ignore[attr-defined]
    assert provider._retry_policy.max_backoff_s == 4.0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_complete_concatenates_multiple_text_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeMessages:
        async def create(self, **kwargs: object) -> object:
            del kwargs
            return SimpleNamespace(
                content=[
                    SimpleNamespace(type="text", text="hello "),
                    SimpleNamespace(type="text", text="world"),
                ],
                usage=SimpleNamespace(input_tokens=1, output_tokens=2),
            )

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "blocks-key")

    provider = AnthropicProvider()
    response = await provider.complete("hello")

    assert response.content == "hello world"


@pytest.mark.asyncio
async def test_complete_raises_clear_error_when_response_is_truncated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeMessages:
        async def create(self, **kwargs: object) -> object:
            del kwargs
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text="{")],
                stop_reason="max_tokens",
                usage=SimpleNamespace(input_tokens=1, output_tokens=1),
            )

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "truncated-key")

    provider = AnthropicProvider()

    with pytest.raises(ValueError, match="max_tokens"):
        await provider.complete("hello", max_tokens=1)


@pytest.mark.asyncio
async def test_complete_can_use_prompt_cache_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, _api_keys = _install_fake_anthropic(monkeypatch, response_text="ok")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "cache-key")

    provider = AnthropicProvider()
    await provider.complete(
        "summarize this",
        system="stable system prompt",
        cache_system_prompt=True,
        cache_prompt=True,
    )

    assert calls[0]["system"] == [
        {
            "type": "text",
            "text": "stable system prompt",
            "cache_control": {"type": "ephemeral"},
        }
    ]
    assert calls[0]["messages"] == [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "summarize this",
                    "cache_control": {"type": "ephemeral"},
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_complete_uses_structured_tool_use_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class _FakeMessages:
        async def create(self, **kwargs: object) -> object:
            calls.append(kwargs)
            return SimpleNamespace(
                content=[
                    SimpleNamespace(
                        type="tool_use",
                        input={"summary": "Solid noodles", "sentiment": "positive"},
                    )
                ],
                usage=SimpleNamespace(input_tokens=3, output_tokens=4),
            )

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "tool-key")

    provider = AnthropicProvider()
    response = await provider.complete(
        "Label this review",
        response_format=_StructuredReview,
    )

    assert response.content == '{"summary":"Solid noodles","sentiment":"positive"}'
    assert calls[0]["tool_choice"] == {"type": "tool", "name": "agora__structuredreview_response"}
    assert calls[0]["tools"]


@pytest.mark.asyncio
async def test_complete_repairs_wrapped_json_for_structured_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_anthropic(
        monkeypatch,
        response_text='Here is the JSON:\n{"summary": "ok", "sentiment": "neutral"}\nDone.',
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "repair-key")

    provider = AnthropicProvider()
    response = await provider.complete(
        "Label",
        response_format=_StructuredReview,
        use_tool_for_response_format=False,
    )

    assert '"summary": "ok"' in response.content


@pytest.mark.asyncio
async def test_stream_complete_yields_text_deltas(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeStream:
        async def __aenter__(self) -> _FakeStream:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

        def __aiter__(self) -> _FakeStream:
            self._items = iter(
                [
                    SimpleNamespace(delta=SimpleNamespace(text="hel")),
                    SimpleNamespace(delta=SimpleNamespace(text="lo")),
                ]
            )
            return self

        async def __anext__(self) -> object:
            try:
                return next(self._items)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    class _FakeMessages:
        def stream(self, **kwargs: object) -> _FakeStream:
            del kwargs
            return _FakeStream()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "stream-key")

    provider = AnthropicProvider()
    chunks = [chunk async for chunk in provider.stream_complete("hello")]

    assert chunks == ["hel", "lo"]


@pytest.mark.asyncio
async def test_stream_complete_retries_retryable_error_before_first_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _RateLimitError(RuntimeError):
        status_code = 429

    class _FakeStream:
        async def __aenter__(self) -> _FakeStream:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

        def __aiter__(self) -> _FakeStream:
            self._items = iter([SimpleNamespace(delta=SimpleNamespace(text="ok"))])
            return self

        async def __anext__(self) -> object:
            try:
                return next(self._items)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    class _FakeMessages:
        def __init__(self) -> None:
            self.calls = 0

        def stream(self, **kwargs: object) -> _FakeStream:
            del kwargs
            self.calls += 1
            if self.calls == 1:
                raise _RateLimitError("rate limited")
            return _FakeStream()

    messages = _FakeMessages()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = messages

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic, RateLimitError=_RateLimitError),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "stream-retry-key")

    provider = AnthropicProvider(max_retries=2, retry_initial_backoff_s=0)
    chunks = [chunk async for chunk in provider.stream_complete("hello")]

    assert chunks == ["ok"]
    assert messages.calls == 2


@pytest.mark.asyncio
async def test_stream_complete_does_not_retry_after_yielding_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _RateLimitError(RuntimeError):
        status_code = 429

    class _FakeStream:
        async def __aenter__(self) -> _FakeStream:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

        def __aiter__(self) -> _FakeStream:
            self._sent = False
            return self

        async def __anext__(self) -> object:
            if not self._sent:
                self._sent = True
                return SimpleNamespace(delta=SimpleNamespace(text="partial"))
            raise _RateLimitError("late stream failure")

    class _FakeMessages:
        def __init__(self) -> None:
            self.calls = 0

        def stream(self, **kwargs: object) -> _FakeStream:
            del kwargs
            self.calls += 1
            return _FakeStream()

    messages = _FakeMessages()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = messages

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic, RateLimitError=_RateLimitError),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "stream-late-failure-key")

    provider = AnthropicProvider(max_retries=2, retry_initial_backoff_s=0)
    chunks: list[str] = []
    with pytest.raises(_RateLimitError, match="late stream failure"):
        async for chunk in provider.stream_complete("hello"):
            chunks.append(chunk)

    assert chunks == ["partial"]
    assert messages.calls == 1


@pytest.mark.asyncio
async def test_complete_batch_uses_concurrency_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    active = 0
    max_active = 0

    class _FakeMessages:
        async def create(self, **kwargs: object) -> object:
            nonlocal active, max_active
            del kwargs
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0)
            active -= 1
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text="ok")],
                usage=SimpleNamespace(input_tokens=1, output_tokens=1),
            )

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "batch-key")

    provider = AnthropicProvider(max_concurrency=1)
    responses = await provider.complete_batch(["a", "b", "c"])

    assert [response.content for response in responses] == ["ok", "ok", "ok"]
    assert max_active == 1


@pytest.mark.asyncio
async def test_create_message_batch_calls_anthropic_batches_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class _FakeBatches:
        async def create(self, **kwargs: object) -> object:
            calls.append(kwargs)
            return {"id": "batch-1"}

    class _FakeMessages:
        def __init__(self) -> None:
            self.batches = _FakeBatches()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "batch-api-key")

    provider = AnthropicProvider()
    result = await provider.create_message_batch(["a", "b"], system="shared")

    assert result == {"id": "batch-1"}
    assert [request["custom_id"] for request in calls[0]["requests"]] == ["agora-0", "agora-1"]
    assert calls[0]["requests"][0]["params"]["system"] == "shared"


@pytest.mark.asyncio
async def test_complete_batch_message_batches_api_polls_and_returns_results_in_prompt_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class _FakeBatches:
        def __init__(self) -> None:
            self._statuses = ["in_progress", "ended"]

        async def create(self, **kwargs: object) -> object:
            calls.append(("create", kwargs))
            return SimpleNamespace(id="batch-1", processing_status="in_progress")

        async def retrieve(self, batch_id: str) -> object:
            calls.append(("retrieve", batch_id))
            return SimpleNamespace(
                id=batch_id,
                processing_status=self._statuses.pop(0),
            )

        async def results(self, batch_id: str) -> list[object]:
            calls.append(("results", batch_id))
            return [
                SimpleNamespace(
                    custom_id="agora-1",
                    result=SimpleNamespace(
                        type="succeeded",
                        message=_anthropic_message("second", input_tokens=3, output_tokens=4),
                    ),
                ),
                SimpleNamespace(
                    custom_id="agora-0",
                    result=SimpleNamespace(
                        type="succeeded",
                        message=_anthropic_message("first", input_tokens=1, output_tokens=2),
                    ),
                ),
            ]

    class _FakeMessages:
        def __init__(self) -> None:
            self.batches = _FakeBatches()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "batch-completion-key")

    provider = AnthropicProvider()
    responses = await provider.complete_batch(
        ["a", "b"],
        system="shared",
        use_message_batches_api=True,
        message_batch_poll_interval_s=0,
        message_batch_timeout_s=1,
    )

    assert [response.content for response in responses] == ["first", "second"]
    assert [(response.input_tokens, response.output_tokens) for response in responses] == [
        (1, 2),
        (3, 4),
    ]
    assert [response.metadata["anthropic_custom_id"] for response in responses] == [
        "agora-0",
        "agora-1",
    ]
    assert [name for name, _payload in calls] == ["create", "retrieve", "retrieve", "results"]
    create_payload = calls[0][1]
    assert isinstance(create_payload, dict)
    assert create_payload["requests"][0]["params"]["system"] == "shared"


@pytest.mark.asyncio
async def test_complete_batch_message_batches_api_uses_structured_tool_use_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class _FakeBatches:
        async def create(self, **kwargs: object) -> object:
            calls.append(("create", kwargs))
            return SimpleNamespace(id="batch-structured", processing_status="in_progress")

        async def retrieve(self, batch_id: str) -> object:
            calls.append(("retrieve", batch_id))
            return SimpleNamespace(id=batch_id, processing_status="ended")

        async def results(self, batch_id: str) -> list[object]:
            calls.append(("results", batch_id))
            return [
                SimpleNamespace(
                    custom_id="agora-0",
                    result=SimpleNamespace(
                        type="succeeded",
                        message=SimpleNamespace(
                            content=[
                                SimpleNamespace(
                                    type="tool_use",
                                    input={"summary": "first", "sentiment": "positive"},
                                )
                            ],
                            usage=SimpleNamespace(input_tokens=1, output_tokens=2),
                        ),
                    ),
                )
            ]

    class _FakeMessages:
        def __init__(self) -> None:
            self.batches = _FakeBatches()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "batch-structured-key")

    provider = AnthropicProvider()
    responses = await provider.complete_batch(
        ["a"],
        response_format=_StructuredReview,
        use_message_batches_api=True,
        message_batch_poll_interval_s=0,
        message_batch_timeout_s=1,
    )

    assert [response.content for response in responses] == [
        '{"summary":"first","sentiment":"positive"}'
    ]
    create_payload = calls[0][1]
    assert isinstance(create_payload, dict)
    assert create_payload["requests"][0]["params"]["tool_choice"] == {
        "type": "tool",
        "name": "agora__structuredreview_response",
    }


@pytest.mark.asyncio
async def test_complete_batch_message_batches_api_raises_for_failed_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeBatches:
        async def create(self, **kwargs: object) -> object:
            del kwargs
            return {"id": "batch-2"}

        async def retrieve(self, batch_id: str) -> object:
            return {"id": batch_id, "processing_status": "ended"}

        async def results(self, batch_id: str) -> list[object]:
            del batch_id
            return [
                {
                    "custom_id": "agora-0",
                    "result": {
                        "type": "errored",
                        "error": {
                            "type": "invalid_request",
                            "message": "bad prompt",
                        },
                    },
                }
            ]

    class _FakeMessages:
        def __init__(self) -> None:
            self.batches = _FakeBatches()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "batch-failure-key")

    provider = AnthropicProvider()

    with pytest.raises(RuntimeError, match=r"agora-0.*invalid_request.*bad prompt"):
        await provider.complete_batch(
            ["a"],
            use_message_batches_api=True,
            message_batch_poll_interval_s=0,
            message_batch_timeout_s=1,
        )


@pytest.mark.asyncio
async def test_complete_batch_message_batches_api_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeBatches:
        async def create(self, **kwargs: object) -> object:
            del kwargs
            return SimpleNamespace(id="batch-3")

        async def retrieve(self, batch_id: str) -> object:
            return SimpleNamespace(id=batch_id, processing_status="in_progress")

        async def results(self, batch_id: str) -> list[object]:
            del batch_id
            return []

    class _FakeMessages:
        def __init__(self) -> None:
            self.batches = _FakeBatches()

    class _FakeAsyncAnthropic:
        def __init__(self, *, api_key: str, max_retries: int = 0) -> None:
            del api_key, max_retries
            self.messages = _FakeMessages()

    monkeypatch.setitem(
        sys.modules,
        "anthropic",
        SimpleNamespace(AsyncAnthropic=_FakeAsyncAnthropic),
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "batch-timeout-key")

    provider = AnthropicProvider()

    with pytest.raises(TimeoutError, match="batch-3"):
        await provider.complete_batch(
            ["a"],
            use_message_batches_api=True,
            message_batch_poll_interval_s=0,
            message_batch_timeout_s=0,
        )


def test_provider_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_anthropic(monkeypatch, response_text='{"summary": "ok", "sentiment": "neutral"}')
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    with pytest.raises(ValueError, match="Anthropic API key is required"):
        AnthropicProvider()


def test_provider_rejects_unknown_model_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_anthropic(monkeypatch, response_text="ok")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "model-key")

    with pytest.raises(ValueError, match="supported production model ids"):
        AnthropicProvider(model="claude-9-9-phantom-20990101")


def test_provider_can_opt_into_unknown_model(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_anthropic(monkeypatch, response_text="ok")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "model-key")

    provider = AnthropicProvider(
        model="claude-9-9-phantom-20990101",
        allow_unknown_models=True,
    )

    assert provider.model == "claude-9-9-phantom-20990101"


@pytest.mark.asyncio
async def test_embedding_methods_raise_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_anthropic(monkeypatch, response_text='{"summary": "ok", "sentiment": "neutral"}')
    monkeypatch.setenv("ANTHROPIC_API_KEY", "embed-key")
    provider = AnthropicProvider()

    with pytest.raises(NotImplementedError, match="does not provide embedding models"):
        await provider.embed("hello world")

    with pytest.raises(NotImplementedError, match="does not provide embedding models"):
        await provider.embed_batch(["a", "b"])
