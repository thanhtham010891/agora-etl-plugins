"""Anthropic Claude provider for the official Agora plugin bundle.

Supported models
----------------
- ``claude-haiku-4-5-20251001``  (default — fast, cheap, great for ETL)
- ``claude-sonnet-5``
- ``claude-opus-4-8``
- ``claude-fable-5``
- ``claude-sonnet-4-6``
- ``claude-sonnet-4-5-20250929``
- ``claude-opus-4-7``
- ``claude-opus-4-6``
- ``claude-opus-4-5-20251101``

Note: Anthropic does not provide native embedding models.
Calling ``embed()`` on this provider raises ``NotImplementedError``.
Use ``OpenAIProvider`` or ``GeminiProvider`` for embeddings.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any

import logstruct

from agora_plugins.anthropic.provider_bootstrap import AnthropicProviderBootstrap
from agora_plugins.anthropic.request_runtime import AnthropicRequestRuntime
from agora_plugins.anthropic.response_surface import AnthropicResponseSurface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from agora.ai.providers.base import CompletionResponse, EmbeddingResponse
    from pydantic import BaseModel

logger = logstruct.getLogger(__name__)

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_SUPPORTED_MODELS = frozenset(
    {
        "claude-fable-5",
        "claude-opus-4-8",
        "claude-opus-4-7",
        "claude-opus-4-6",
        "claude-opus-4-5-20251101",
        "claude-sonnet-5",
        "claude-sonnet-4-6",
        "claude-sonnet-4-5-20250929",
        "claude-haiku-4-5-20251001",
    }
)


class AnthropicProvider:
    """Anthropic Claude LLM provider."""

    supports_embeddings = False

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = _DEFAULT_MODEL,
        allow_unknown_models: bool = False,
        max_retries: int = 5,
        retry_initial_backoff_s: float = 0.5,
        retry_max_backoff_s: float = 8.0,
        retry_jitter_ratio: float = 0.2,
        max_concurrency: int | None = 16,
        request_timeout_s: float | None = 60.0,
    ) -> None:
        self._bootstrap = AnthropicProviderBootstrap(supported_models=_SUPPORTED_MODELS)
        self._anthropic_module = self._bootstrap.import_anthropic_module()
        resolved_key = self._bootstrap.resolve_api_key(api_key)
        self._model = self._bootstrap.resolve_model(
            model,
            allow_unknown_models=allow_unknown_models,
        )
        self._request_timeout_s = self._bootstrap.validate_request_timeout(request_timeout_s)
        validated_max_concurrency = self._bootstrap.validate_max_concurrency(max_concurrency)
        self._client = self._bootstrap.build_client(
            self._anthropic_module,
            api_key=resolved_key,
            request_timeout_s=self._request_timeout_s,
        )
        self._retry_policy = self._bootstrap.build_retry_policy(
            max_retries=max_retries,
            retry_initial_backoff_s=retry_initial_backoff_s,
            retry_max_backoff_s=retry_max_backoff_s,
            retry_jitter_ratio=retry_jitter_ratio,
            retry_if=self._is_retryable_error,
        )
        self._request_semaphore = self._bootstrap.build_request_semaphore(validated_max_concurrency)
        self._request_runtime = AnthropicRequestRuntime(
            model=self._model,
            request_timeout_s=self._request_timeout_s,
            retry_policy=self._retry_policy,
            retry_delay=self._retry_delay,
            throttle_factory=self._maybe_throttle,
            logger=logger,
        )
        self._response_surface = AnthropicResponseSurface(model=self._model, logger=logger)

    @property
    def model(self) -> str:
        return self._model

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format: type[BaseModel] | None = None,
        cache_system_prompt: bool = False,
        cache_prompt: bool = False,
        use_tool_for_response_format: bool = True,
        repair_invalid_json: bool = True,
    ) -> CompletionResponse:
        kwargs = self._request_runtime.build_completion_kwargs(
            prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
            cache_system_prompt=cache_system_prompt,
            cache_prompt=cache_prompt,
            use_tool_for_response_format=use_tool_for_response_format,
        )

        response = await self._request_runtime.create_with_retry(
            self._client.messages.create, kwargs
        )
        return self._response_surface.completion_response_from_message(
            response,
            response_format=response_format,
            use_tool_for_response_format=use_tool_for_response_format,
            repair_invalid_json=repair_invalid_json,
        )

    async def stream_complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        cache_system_prompt: bool = False,
        cache_prompt: bool = False,
    ) -> AsyncIterator[str]:
        """Yield text deltas from Anthropic's streaming Messages API."""

        stream = getattr(self._client.messages, "stream", None)
        if not callable(stream):
            raise NotImplementedError("Installed anthropic SDK does not expose messages.stream().")
        kwargs = self._request_runtime.build_completion_kwargs(
            prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=None,
            cache_system_prompt=cache_system_prompt,
            cache_prompt=cache_prompt,
            use_tool_for_response_format=False,
        )
        async for chunk in self._request_runtime.stream_text(stream, kwargs):
            yield chunk

    async def complete_batch(
        self,
        prompts: list[str],
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format: type[BaseModel] | None = None,
        cache_system_prompt: bool = False,
        cache_prompt: bool = False,
        use_tool_for_response_format: bool = True,
        use_message_batches_api: bool = False,
        message_batch_poll_interval_s: float = 5.0,
        message_batch_timeout_s: float | None = 24 * 60 * 60,
    ) -> list[CompletionResponse]:
        """Complete many prompts, optionally using Anthropic's Message Batches API when available."""

        if not prompts:
            return []
        if use_message_batches_api:
            batch = await self.create_message_batch(
                prompts,
                system=system,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format=response_format,
                cache_system_prompt=cache_system_prompt,
                cache_prompt=cache_prompt,
                use_tool_for_response_format=use_tool_for_response_format,
            )
            batch_id = self._response_surface.require_message_batch_id(batch)
            await self.wait_for_message_batch(
                batch_id,
                poll_interval_s=message_batch_poll_interval_s,
                timeout_s=message_batch_timeout_s,
            )
            return await self.retrieve_message_batch_results(
                batch_id,
                expected_custom_ids=[f"agora-{idx}" for idx in range(len(prompts))],
                response_format=response_format,
                use_tool_for_response_format=use_tool_for_response_format,
                repair_invalid_json=True,
            )
        return await asyncio.gather(
            *[
                self.complete(
                    prompt,
                    system=system,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    response_format=response_format,
                    cache_system_prompt=cache_system_prompt,
                    cache_prompt=cache_prompt,
                    use_tool_for_response_format=use_tool_for_response_format,
                )
                for prompt in prompts
            ]
        )

    async def create_message_batch(
        self,
        prompts: list[str],
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format: type[BaseModel] | None = None,
        cache_system_prompt: bool = False,
        cache_prompt: bool = False,
        use_tool_for_response_format: bool = True,
    ) -> Any:
        """Create an Anthropic Message Batch request and return the SDK response object."""

        batches = getattr(self._client.messages, "batches", None)
        create = getattr(batches, "create", None)
        if not callable(create):
            raise NotImplementedError(
                "Installed anthropic SDK does not expose messages.batches.create()."
            )
        requests = self._request_runtime.build_message_batch_requests(
            prompts,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
            cache_system_prompt=cache_system_prompt,
            cache_prompt=cache_prompt,
            use_tool_for_response_format=use_tool_for_response_format,
        )
        return await self._request_runtime.create_message_batch(create, requests=requests)

    async def retrieve_message_batch(self, batch_id: str) -> Any:
        """Retrieve the current Anthropic Message Batch status."""

        batches = getattr(self._client.messages, "batches", None)
        retrieve = getattr(batches, "retrieve", None)
        if not callable(retrieve):
            raise NotImplementedError(
                "Installed anthropic SDK does not expose messages.batches.retrieve()."
            )
        return await self._request_runtime.retrieve_message_batch(retrieve, batch_id)

    async def wait_for_message_batch(
        self,
        batch_id: str,
        *,
        poll_interval_s: float = 5.0,
        timeout_s: float | None = 24 * 60 * 60,
    ) -> Any:
        """Poll Anthropic until a Message Batch reaches the results-ready state."""

        return await self._request_runtime.wait_for_message_batch(
            batch_id=batch_id,
            retrieve_batch=self.retrieve_message_batch,
            poll_interval_s=poll_interval_s,
            timeout_s=timeout_s,
        )

    async def retrieve_message_batch_results(
        self,
        batch_id: str,
        *,
        expected_custom_ids: list[str],
        response_format: type[BaseModel] | None = None,
        use_tool_for_response_format: bool = True,
        repair_invalid_json: bool = True,
    ) -> list[CompletionResponse]:
        """Read Message Batch results and return completions in request order."""

        batches = getattr(self._client.messages, "batches", None)
        results = getattr(batches, "results", None)
        if not callable(results):
            raise NotImplementedError(
                "Installed anthropic SDK does not expose messages.batches.results()."
            )

        async with self._maybe_throttle():
            result_source = results(batch_id)
            if inspect.isawaitable(result_source):
                result_source = await result_source
            items = await self._response_surface.collect_message_batch_results(result_source)

        return self._response_surface.completion_responses_from_batch_results(
            batch_id=batch_id,
            items=items,
            expected_custom_ids=expected_custom_ids,
            response_format=response_format,
            use_tool_for_response_format=use_tool_for_response_format,
            repair_invalid_json=repair_invalid_json,
        )

    def _maybe_throttle(self) -> _RequestThrottle:
        return _RequestThrottle(self._request_semaphore)

    def _retry_delay(self, exc: Exception, *, attempt: int) -> float:
        retry_after = self._request_runtime.retry_after_delay_s(exc)
        fallback = self._retry_policy.backoff_for(attempt=attempt)
        return max(fallback, retry_after) if retry_after is not None else fallback

    def _is_retryable_error(self, exc: Exception) -> bool:
        if isinstance(exc, TimeoutError):
            return True
        status_code = getattr(exc, "status_code", None)
        if status_code in {408, 409, 429, 500, 502, 503, 504, 529}:
            return True
        for name in (
            "APIConnectionError",
            "APITimeoutError",
            "RateLimitError",
            "InternalServerError",
        ):
            error_type = getattr(self._anthropic_module, name, None)
            if error_type is not None and isinstance(exc, error_type):
                return True
        return False

    async def embed(self, text: str) -> EmbeddingResponse:
        raise NotImplementedError(
            "Anthropic does not provide embedding models. "
            "Use an embedding-capable provider for embedding workflows."
        )

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        raise NotImplementedError(
            "Anthropic does not provide embedding models. "
            "Use an embedding-capable provider for embedding workflows."
        )


class _RequestThrottle:
    def __init__(self, semaphore: asyncio.Semaphore | None) -> None:
        self._semaphore = semaphore

    async def __aenter__(self) -> None:
        if self._semaphore is not None:
            await self._semaphore.acquire()

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        if self._semaphore is not None:
            self._semaphore.release()
