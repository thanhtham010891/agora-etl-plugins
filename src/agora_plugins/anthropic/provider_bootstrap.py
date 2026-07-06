"""Bootstrap and config validation helpers for Anthropic providers."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from agora.core.retry import RetryPolicy


class AnthropicProviderBootstrap:
    """Public-facing bootstrap helper for Anthropic provider initialization."""

    def __init__(self, *, supported_models: frozenset[str]) -> None:
        self._supported_models = supported_models

    def import_anthropic_module(self) -> Any:
        try:
            import anthropic
        except ImportError as exc:
            raise ImportError(
                "AnthropicProvider requires the 'anthropic' dependency. "
                'Install with: pip install "agora-etl-plugins[anthropic]"'
            ) from exc
        return anthropic

    def resolve_api_key(self, api_key: str | None) -> str:
        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Anthropic API key is required. Pass api_key= or set ANTHROPIC_API_KEY env var."
            )
        return resolved_key

    def resolve_model(self, model: str, *, allow_unknown_models: bool) -> str:
        resolved_model = str(model).strip()
        if not resolved_model:
            raise ValueError("Anthropic model must be a non-empty string.")
        if not allow_unknown_models and resolved_model not in self._supported_models:
            supported = ", ".join(sorted(self._supported_models))
            raise ValueError(
                "Anthropic model must be one of the supported production model ids: "
                f"{supported}. Got {resolved_model!r}. "
                "Pass allow_unknown_models=True to opt into an unlisted model."
            )
        return resolved_model

    def validate_request_timeout(self, request_timeout_s: float | None) -> float | None:
        if request_timeout_s is not None and request_timeout_s <= 0:
            raise ValueError("request_timeout_s must be greater than zero when provided.")
        return request_timeout_s

    def validate_max_concurrency(self, max_concurrency: int | None) -> int | None:
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than zero when provided.")
        return max_concurrency

    def build_client(
        self,
        anthropic_module: Any,
        *,
        api_key: str,
        request_timeout_s: float | None,
    ) -> Any:
        try:
            return anthropic_module.AsyncAnthropic(
                api_key=api_key,
                max_retries=0,
                timeout=request_timeout_s,
            )
        except TypeError:
            try:
                return anthropic_module.AsyncAnthropic(api_key=api_key, max_retries=0)
            except TypeError:
                return anthropic_module.AsyncAnthropic(api_key=api_key)

    def build_retry_policy(
        self,
        *,
        max_retries: int,
        retry_initial_backoff_s: float,
        retry_max_backoff_s: float,
        retry_jitter_ratio: float,
        retry_if: Any,
    ) -> RetryPolicy[Any]:
        return RetryPolicy[Any](
            max_attempts=max(1, max_retries),
            initial_backoff_s=max(0.0, retry_initial_backoff_s),
            backoff_multiplier=2.0,
            max_backoff_s=max(max(0.0, retry_initial_backoff_s), retry_max_backoff_s),
            jitter_ratio=max(0.0, retry_jitter_ratio),
            retry_exceptions=(Exception,),
            retry_if=retry_if,
        )

    def build_request_semaphore(self, max_concurrency: int | None) -> asyncio.Semaphore | None:
        return asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None


__all__ = ["AnthropicProviderBootstrap"]
