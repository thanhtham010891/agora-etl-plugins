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

import json
import os
from typing import TYPE_CHECKING, Any, cast

import logstruct
from agora.ai.providers.base import CompletionResponse, EmbeddingResponse

if TYPE_CHECKING:
    from pydantic import BaseModel

logger = logstruct.getLogger(__name__)

_DEFAULT_MODEL = "claude-3-5-haiku-20241022"


class AnthropicProvider:
    """Anthropic Claude LLM provider."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = _DEFAULT_MODEL,
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

        self._client = anthropic.AsyncAnthropic(api_key=resolved_key)
        self._model = model

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
    ) -> CompletionResponse:
        kwargs: dict[str, object] = {
            "model": self._model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }

        if system:
            kwargs["system"] = system

        if response_format is not None:
            schema_hint = json.dumps(response_format.model_json_schema(), indent=2)
            schema_instruction = (
                f"You MUST respond with a valid JSON object matching this schema:\n{schema_hint}"
            )
            if system:
                kwargs["system"] = f"{system}\n\n{schema_instruction}"
            else:
                kwargs["system"] = schema_instruction

        create = cast("Any", self._client.messages.create)
        response = await create(**kwargs)
        content = response.content[0].text if response.content else ""

        if response_format is not None:
            try:
                parsed = json.loads(content)
                response_format.model_validate(parsed)
            except (json.JSONDecodeError, Exception) as exc:
                raise ValueError(
                    f"AnthropicProvider: response did not match expected schema "
                    f"{response_format.__name__!r}: {exc}\nRaw response: {content!r}"
                ) from exc

        usage = getattr(response, "usage", None)
        input_tokens = getattr(usage, "input_tokens", 0) or 0
        output_tokens = getattr(usage, "output_tokens", 0) or 0
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
        )

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
