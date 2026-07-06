"""Request and batch runtime for Anthropic provider integrations."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from agora.core.retry import RetryPolicy
    from pydantic import BaseModel


class AnthropicRequestRuntime:
    """Public-facing request and batch runtime for Anthropic providers."""

    def __init__(
        self,
        *,
        model: str,
        request_timeout_s: float | None,
        retry_policy: RetryPolicy[Any],
        retry_delay: Callable[[Exception, int], float],
        throttle_factory: Callable[[], Any],
        logger: Any,
    ) -> None:
        self._model = model
        self._request_timeout_s = request_timeout_s
        self._retry_policy = retry_policy
        self._retry_delay = retry_delay
        self._throttle_factory = throttle_factory
        self._logger = logger

    def build_completion_kwargs(
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
                schema_instruction = (
                    "You MUST respond with a valid JSON object matching this schema:\n"
                    f"{schema_hint}"
                )
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

    def build_message_batch_requests(
        self,
        prompts: list[str],
        *,
        system: str | None,
        temperature: float,
        max_tokens: int,
        response_format: type[BaseModel] | None,
        cache_system_prompt: bool,
        cache_prompt: bool,
        use_tool_for_response_format: bool,
    ) -> list[dict[str, object]]:
        return [
            {
                "custom_id": f"agora-{idx}",
                "params": self.build_completion_kwargs(
                    prompt,
                    system=system,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    response_format=response_format,
                    cache_system_prompt=cache_system_prompt,
                    cache_prompt=cache_prompt,
                    use_tool_for_response_format=use_tool_for_response_format,
                ),
            }
            for idx, prompt in enumerate(prompts)
        ]

    async def create_with_retry(
        self,
        create: Callable[..., Awaitable[Any]],
        kwargs: dict[str, object],
    ) -> Any:
        attempt = 1
        while True:
            try:
                async with self._throttle_factory():
                    request = create(**kwargs)
                    if self._request_timeout_s is None:
                        return await request
                    return await asyncio.wait_for(request, timeout=self._request_timeout_s)
            except Exception as exc:
                if not self._retry_policy.should_retry(exc, attempt=attempt):
                    raise
                delay = self._retry_delay(exc, attempt=attempt)
                self._logger.warning(
                    "anthropic_complete_retry",
                    model=self._model,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                attempt += 1

    async def stream_text(
        self,
        stream: Callable[..., Any],
        kwargs: dict[str, object],
    ) -> AsyncIterator[str]:
        yielded_chunk = False
        attempt = 1
        while True:
            try:
                async with self._throttle_factory():
                    if self._request_timeout_s is None:
                        manager = stream(**kwargs)
                        async with manager as events:
                            async for text in self._iter_stream_events(events):
                                yielded_chunk = True
                                yield text
                    else:
                        async with asyncio.timeout(self._request_timeout_s):
                            manager = stream(**kwargs)
                            async with manager as events:
                                async for text in self._iter_stream_events(events):
                                    yielded_chunk = True
                                    yield text
                return
            except Exception as exc:
                if yielded_chunk or not self._retry_policy.should_retry(exc, attempt=attempt):
                    raise
                delay = self._retry_delay(exc, attempt=attempt)
                self._logger.warning(
                    "anthropic_stream_retry",
                    model=self._model,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                attempt += 1

    async def create_message_batch(
        self,
        create: Callable[..., Awaitable[Any]],
        *,
        requests: list[dict[str, object]],
    ) -> Any:
        async with self._throttle_factory():
            return await create(requests=requests)

    async def retrieve_message_batch(
        self,
        retrieve: Callable[[str], Awaitable[Any]],
        batch_id: str,
    ) -> Any:
        async with self._throttle_factory():
            return await retrieve(batch_id)

    async def wait_for_message_batch(
        self,
        *,
        batch_id: str,
        retrieve_batch: Callable[[str], Awaitable[Any]],
        poll_interval_s: float = 5.0,
        timeout_s: float | None = 24 * 60 * 60,
    ) -> Any:
        if poll_interval_s < 0:
            raise ValueError("poll_interval_s must be non-negative.")
        if timeout_s is not None and timeout_s < 0:
            raise ValueError("timeout_s must be non-negative when provided.")

        loop = asyncio.get_running_loop()
        deadline = None if timeout_s is None else loop.time() + timeout_s
        while True:
            batch = await retrieve_batch(batch_id)
            status = str(self.get_field(batch, "processing_status", "") or "")
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

    def retry_after_delay_s(self, exc: Exception) -> float | None:
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

    @staticmethod
    def get_field(value: Any, name: str, default: Any = None) -> Any:
        if isinstance(value, dict):
            return value.get(name, default)
        return getattr(value, name, default)

    async def _iter_stream_events(self, events: Any) -> AsyncIterator[str]:
        async for event in events:
            delta = getattr(event, "delta", None)
            text = getattr(delta, "text", None) or getattr(event, "text", None)
            if isinstance(text, str) and text:
                yield text


__all__ = ["AnthropicRequestRuntime"]
