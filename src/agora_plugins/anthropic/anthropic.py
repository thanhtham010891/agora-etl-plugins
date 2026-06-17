"""Anthropic Claude provider for the official Agora plugin bundle.

Supported models
----------------
- ``claude-3-5-haiku-20241022``  (default — fast, cheap, great for ETL)
- ``claude-3-5-sonnet-20241022``
- ``claude-3-opus-20240229``

Note: Anthropic does not provide native embedding models.
Calling ``embed()`` on this provider raises ``NotImplementedError``.
Use ``OpenAIProvider`` or ``GeminiProvider`` for embeddings.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any, cast

import logstruct
from agora.ai.providers.base import CompletionResponse, EmbeddingResponse
from agora.core.retry import RetryPolicy

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pydantic import BaseModel

logger = logstruct.getLogger(__name__)

_DEFAULT_MODEL = "claude-3-5-haiku-20241022"


class AnthropicProvider:
    """Anthropic Claude LLM provider."""

    supports_embeddings = False

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = _DEFAULT_MODEL,
        max_retries: int = 5,
        retry_initial_backoff_s: float = 0.5,
        retry_max_backoff_s: float = 8.0,
        retry_jitter_ratio: float = 0.2,
        max_concurrency: int | None = 16,
        request_timeout_s: float | None = 60.0,
    ) -> None:
        try:
            import anthropic
        except ImportError as exc:
            raise ImportError(
                "AnthropicProvider requires the 'anthropic' dependency. "
                'Install with: pip install "agora-etl-plugins[anthropic]"'
            ) from exc

        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Anthropic API key is required. Pass api_key= or set ANTHROPIC_API_KEY env var."
            )

        self._anthropic_module = anthropic
        if request_timeout_s is not None and request_timeout_s <= 0:
            raise ValueError("request_timeout_s must be greater than zero when provided.")
        try:
            self._client = anthropic.AsyncAnthropic(
                api_key=resolved_key,
                max_retries=0,
                timeout=request_timeout_s,
            )
        except TypeError:
            try:
                self._client = anthropic.AsyncAnthropic(api_key=resolved_key, max_retries=0)
            except TypeError:
                self._client = anthropic.AsyncAnthropic(api_key=resolved_key)
        self._model = model
        self._request_timeout_s = request_timeout_s
        self._retry_policy: RetryPolicy[Any] = RetryPolicy[Any](
            max_attempts=max(1, max_retries),
            initial_backoff_s=max(0.0, retry_initial_backoff_s),
            backoff_multiplier=2.0,
            max_backoff_s=max(max(0.0, retry_initial_backoff_s), retry_max_backoff_s),
            jitter_ratio=max(0.0, retry_jitter_ratio),
            retry_exceptions=(Exception,),
            retry_if=self._is_retryable_error,
        )
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than zero when provided.")
        self._request_semaphore = (
            asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None
        )

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
        use_tool_for_response_format: bool = False,
        repair_invalid_json: bool = True,
    ) -> CompletionResponse:
        kwargs = self._completion_kwargs(
            prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
            cache_system_prompt=cache_system_prompt,
            cache_prompt=cache_prompt,
            use_tool_for_response_format=use_tool_for_response_format,
        )

        response = await self._create_with_retry(kwargs)
        return self._completion_response_from_message(
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
        kwargs = self._completion_kwargs(
            prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=None,
            cache_system_prompt=cache_system_prompt,
            cache_prompt=cache_prompt,
            use_tool_for_response_format=False,
        )
        yielded_chunk = False
        attempt = 1
        while True:
            try:
                async with self._maybe_throttle():
                    manager = stream(**kwargs)
                    async with manager as events:
                        async for event in events:
                            delta = getattr(event, "delta", None)
                            text = getattr(delta, "text", None) or getattr(event, "text", None)
                            if isinstance(text, str) and text:
                                yielded_chunk = True
                                yield text
                return
            except Exception as exc:
                if yielded_chunk or not self._retry_policy.should_retry(exc, attempt=attempt):
                    raise
                delay = self._retry_delay(exc, attempt=attempt)
                logger.warning(
                    "anthropic_stream_retry",
                    model=self._model,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                attempt += 1

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
            )
            batch_id = self._require_message_batch_id(batch)
            await self.wait_for_message_batch(
                batch_id,
                poll_interval_s=message_batch_poll_interval_s,
                timeout_s=message_batch_timeout_s,
            )
            return await self.retrieve_message_batch_results(
                batch_id,
                expected_custom_ids=[f"agora-{idx}" for idx in range(len(prompts))],
                response_format=response_format,
                use_tool_for_response_format=False,
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
    ) -> Any:
        """Create an Anthropic Message Batch request and return the SDK response object."""

        batches = getattr(self._client.messages, "batches", None)
        create = getattr(batches, "create", None)
        if not callable(create):
            raise NotImplementedError(
                "Installed anthropic SDK does not expose messages.batches.create()."
            )
        requests = []
        for idx, prompt in enumerate(prompts):
            requests.append(
                {
                    "custom_id": f"agora-{idx}",
                    "params": self._completion_kwargs(
                        prompt,
                        system=system,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        response_format=response_format,
                        cache_system_prompt=cache_system_prompt,
                        cache_prompt=cache_prompt,
                        use_tool_for_response_format=False,
                    ),
                }
            )
        async with self._maybe_throttle():
            return await create(requests=requests)

    async def retrieve_message_batch(self, batch_id: str) -> Any:
        """Retrieve the current Anthropic Message Batch status."""

        batches = getattr(self._client.messages, "batches", None)
        retrieve = getattr(batches, "retrieve", None)
        if not callable(retrieve):
            raise NotImplementedError(
                "Installed anthropic SDK does not expose messages.batches.retrieve()."
            )
        async with self._maybe_throttle():
            return await retrieve(batch_id)

    async def wait_for_message_batch(
        self,
        batch_id: str,
        *,
        poll_interval_s: float = 5.0,
        timeout_s: float | None = 24 * 60 * 60,
    ) -> Any:
        """Poll Anthropic until a Message Batch reaches the results-ready state."""

        if poll_interval_s < 0:
            raise ValueError("poll_interval_s must be non-negative.")
        if timeout_s is not None and timeout_s < 0:
            raise ValueError("timeout_s must be non-negative when provided.")

        loop = asyncio.get_running_loop()
        deadline = None if timeout_s is None else loop.time() + timeout_s
        while True:
            batch = await self.retrieve_message_batch(batch_id)
            status = str(self._get_field(batch, "processing_status", "") or "")
            if status == "ended":
                return batch
            if status in {"failed", "canceled", "expired"}:
                raise RuntimeError(
                    f"AnthropicProvider: Message Batch {batch_id!r} ended with "
                    f"processing_status={status!r}."
                )

            if deadline is not None:
                remaining_s = deadline - loop.time()
                if remaining_s <= 0:
                    raise TimeoutError(
                        f"AnthropicProvider: Message Batch {batch_id!r} did not finish "
                        f"within {timeout_s:g}s."
                    )
                sleep_s = min(poll_interval_s, remaining_s)
            else:
                sleep_s = poll_interval_s
            await asyncio.sleep(sleep_s)

    async def retrieve_message_batch_results(
        self,
        batch_id: str,
        *,
        expected_custom_ids: list[str],
        response_format: type[BaseModel] | None = None,
        use_tool_for_response_format: bool = False,
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
            items = await self._collect_message_batch_results(result_source)

        by_custom_id: dict[str, CompletionResponse] = {}
        for item in items:
            custom_id = str(self._get_field(item, "custom_id", "") or "")
            if not custom_id:
                raise ValueError("AnthropicProvider: Message Batch result is missing custom_id.")
            if custom_id in by_custom_id:
                raise ValueError(
                    f"AnthropicProvider: duplicate Message Batch result custom_id={custom_id!r}."
                )

            result = self._get_field(item, "result")
            result_type = str(self._get_field(result, "type", "") or "")
            if result_type != "succeeded":
                raise RuntimeError(
                    "AnthropicProvider: Message Batch request "
                    f"{custom_id!r} failed with result_type={result_type!r}: "
                    f"{self._format_batch_result_error(result)}"
                )
            message = self._get_field(result, "message")
            if message is None:
                raise ValueError(
                    f"AnthropicProvider: succeeded Message Batch result {custom_id!r} "
                    "is missing message."
                )
            response = self._completion_response_from_message(
                message,
                response_format=response_format,
                use_tool_for_response_format=use_tool_for_response_format,
                repair_invalid_json=repair_invalid_json,
                metadata={
                    "anthropic_message_batch": True,
                    "anthropic_message_batch_id": batch_id,
                    "anthropic_custom_id": custom_id,
                },
            )
            by_custom_id[custom_id] = response

        missing = [custom_id for custom_id in expected_custom_ids if custom_id not in by_custom_id]
        if missing:
            raise RuntimeError(
                "AnthropicProvider: Message Batch results missing expected custom_ids: "
                f"{missing!r}."
            )
        return [by_custom_id[custom_id] for custom_id in expected_custom_ids]

    def _completion_kwargs(
        self,
        prompt: str,
        *,
        system: str | None,
        temperature: float,
        max_tokens: int,
        response_format: type[BaseModel] | None,
        cache_system_prompt: bool,
        cache_prompt: bool,
        use_tool_for_response_format: bool,
    ) -> dict[str, object]:
        user_content: object = prompt
        if cache_prompt:
            user_content = [
                {
                    "type": "text",
                    "text": prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        kwargs: dict[str, object] = {
            "model": self._model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": user_content}],
        }

        if system:
            kwargs["system"] = (
                [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
                if cache_system_prompt
                else system
            )

        if response_format is not None:
            schema = response_format.model_json_schema()
            if use_tool_for_response_format:
                tool_name = f"agora_{response_format.__name__.lower()}_response"
                kwargs["tools"] = [
                    {
                        "name": tool_name,
                        "description": "Return the structured response for Agora ETL.",
                        "input_schema": schema,
                    }
                ]
                kwargs["tool_choice"] = {"type": "tool", "name": tool_name}
            else:
                schema_hint = json.dumps(schema, indent=2)
                schema_instruction = f"You MUST respond with a valid JSON object matching this schema:\n{schema_hint}"
                if system:
                    if cache_system_prompt:
                        kwargs["system"] = [
                            *cast("list[dict[str, object]]", kwargs["system"]),
                            {"type": "text", "text": schema_instruction},
                        ]
                    else:
                        kwargs["system"] = f"{system}\n\n{schema_instruction}"
                else:
                    kwargs["system"] = schema_instruction
        return kwargs

    async def _create_with_retry(self, kwargs: dict[str, object]) -> Any:
        create = cast("Any", self._client.messages.create)
        attempt = 1
        while True:
            try:
                async with self._maybe_throttle():
                    request = create(**kwargs)
                    if self._request_timeout_s is None:
                        return await request
                    return await asyncio.wait_for(request, timeout=self._request_timeout_s)
            except Exception as exc:
                if not self._retry_policy.should_retry(exc, attempt=attempt):
                    raise
                delay = self._retry_delay(exc, attempt=attempt)
                logger.warning(
                    "anthropic_complete_retry",
                    model=self._model,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                attempt += 1

    def _maybe_throttle(self) -> _RequestThrottle:
        return _RequestThrottle(self._request_semaphore)

    def _retry_delay(self, exc: Exception, *, attempt: int) -> float:
        retry_after = self._retry_after_delay_s(exc)
        fallback = self._retry_policy.backoff_for(attempt=attempt)
        return max(fallback, retry_after) if retry_after is not None else fallback

    def _retry_after_delay_s(self, exc: Exception) -> float | None:
        headers = getattr(exc, "headers", None)
        if headers is None:
            response = getattr(exc, "response", None)
            headers = getattr(response, "headers", None)
        if headers is None:
            return None
        get = getattr(headers, "get", None)
        if not callable(get):
            return None
        value = get("retry-after") or get("Retry-After")
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("ascii", errors="ignore")
        value = str(value).strip()
        if not value:
            return None
        try:
            return max(float(value), 0.0)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max((retry_at - datetime.now(UTC)).total_seconds(), 0.0)

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

    def _completion_response_from_message(
        self,
        response: Any,
        *,
        response_format: type[BaseModel] | None,
        use_tool_for_response_format: bool,
        repair_invalid_json: bool,
        metadata: dict[str, Any] | None = None,
    ) -> CompletionResponse:
        stop_reason = self._get_field(response, "stop_reason")
        if stop_reason == "max_tokens":
            raise ValueError(
                "AnthropicProvider: response stopped at max_tokens before completion. "
                "Increase max_tokens or narrow the prompt/schema."
            )
        if response_format is not None and use_tool_for_response_format:
            content = self._extract_tool_use_json_content(response, response_format)
        else:
            content = self._extract_text_content(response)

        if response_format is not None:
            try:
                parsed, normalized_content = self._parse_structured_json(
                    content, repair_invalid_json=repair_invalid_json
                )
                response_format.model_validate(parsed)
                content = normalized_content
            except Exception as exc:
                raise ValueError(
                    f"AnthropicProvider: response did not match expected schema "
                    f"{response_format.__name__!r}: {exc}\nRaw response: {content!r}"
                ) from exc

        usage = self._get_field(response, "usage")
        input_tokens = self._get_field(usage, "input_tokens", 0) or 0
        input_tokens += self._get_field(usage, "cache_read_input_tokens", 0) or 0
        input_tokens += self._get_field(usage, "cache_creation_input_tokens", 0) or 0
        output_tokens = self._get_field(usage, "output_tokens", 0) or 0
        logger.debug(
            "anthropic_complete",
            model=self._model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        return CompletionResponse(
            content=content,
            model=self._model,
            input_tokens=int(input_tokens),
            output_tokens=int(output_tokens),
            metadata=metadata or {},
        )

    def _extract_text_content(self, response: Any) -> str:
        text_parts: list[str] = []
        non_text_types: list[str] = []
        for block in self._get_field(response, "content", []) or []:
            block_type = self._get_field(block, "type")
            text = self._get_field(block, "text")
            if isinstance(text, str):
                text_parts.append(text)
            elif block_type is not None:
                non_text_types.append(str(block_type))
        if text_parts:
            return "".join(text_parts)
        if non_text_types:
            raise ValueError(
                "AnthropicProvider: response contained no text blocks; "
                f"non_text_content={non_text_types!r}."
            )
        return ""

    def _extract_tool_use_json_content(
        self, response: Any, response_format: type[BaseModel]
    ) -> str:
        for block in self._get_field(response, "content", []) or []:
            if self._get_field(block, "type") != "tool_use":
                continue
            tool_input = self._get_field(block, "input")
            if isinstance(tool_input, dict):
                return json.dumps(tool_input, separators=(",", ":"))
        raise ValueError(
            "AnthropicProvider: response did not include the requested structured "
            f"tool call for {response_format.__name__!r}."
        )

    async def _collect_message_batch_results(self, result_source: Any) -> list[Any]:
        if hasattr(result_source, "__aiter__"):
            items = []
            async for item in result_source:
                items.append(item)
            return items
        data = self._get_field(result_source, "data")
        if data is not None:
            return list(data)
        if isinstance(result_source, list | tuple):
            return list(result_source)
        if hasattr(result_source, "__iter__"):
            return list(result_source)
        raise TypeError(
            "AnthropicProvider: messages.batches.results() returned a non-iterable "
            f"{type(result_source).__name__}."
        )

    def _require_message_batch_id(self, batch: Any) -> str:
        batch_id = self._get_field(batch, "id")
        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("AnthropicProvider: Message Batch create response is missing id.")
        return batch_id

    def _format_batch_result_error(self, result: Any) -> str:
        error = self._get_field(result, "error")
        if error is None:
            return "no error detail"
        if isinstance(error, str):
            return error
        message = self._get_field(error, "message")
        error_type = self._get_field(error, "type")
        if message is not None and error_type is not None:
            return f"{error_type}: {message}"
        return str(error)

    def _get_field(self, value: Any, name: str, default: Any = None) -> Any:
        if isinstance(value, dict):
            return value.get(name, default)
        return getattr(value, name, default)

    def _parse_structured_json(
        self, content: str, *, repair_invalid_json: bool
    ) -> tuple[object, str]:
        try:
            return json.loads(content), content
        except json.JSONDecodeError:
            if not repair_invalid_json:
                raise
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end <= start:
            raise ValueError("no JSON object found in response content")
        repaired_content = content[start : end + 1]
        return json.loads(repaired_content), repaired_content

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
