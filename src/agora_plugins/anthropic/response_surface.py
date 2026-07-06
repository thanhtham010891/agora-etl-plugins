"""Response shaping surface for Anthropic provider integrations."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from agora.ai.providers.base import CompletionResponse

if TYPE_CHECKING:
    from pydantic import BaseModel


class AnthropicResponseSurface:
    """Public-facing response parser and shaper for Anthropic provider outputs."""

    def __init__(self, *, model: str, logger: Any) -> None:
        self._model = model
        self._logger = logger

    def completion_response_from_message(
        self,
        response: Any,
        *,
        response_format: type[BaseModel] | None,
        use_tool_for_response_format: bool,
        repair_invalid_json: bool,
        metadata: dict[str, Any] | None = None,
    ) -> CompletionResponse:
        stop_reason = self.get_field(response, "stop_reason")
        if stop_reason == "max_tokens":
            raise ValueError(
                "AnthropicProvider: response stopped at max_tokens before completion. "
                "Increase max_tokens or narrow the prompt/schema."
            )
        if response_format is not None and use_tool_for_response_format:
            content = self.extract_tool_use_json_content(response, response_format)
        else:
            content = self.extract_text_content(response)

        if response_format is not None:
            try:
                parsed, normalized_content = self.parse_structured_json(
                    content,
                    repair_invalid_json=repair_invalid_json,
                )
                response_format.model_validate(parsed)
                content = normalized_content
            except Exception as exc:
                raise ValueError(
                    f"AnthropicProvider: response did not match expected schema "
                    f"{response_format.__name__!r}: {exc}\nRaw response: {content!r}"
                ) from exc

        usage = self.get_field(response, "usage")
        input_tokens = self.get_field(usage, "input_tokens", 0) or 0
        input_tokens += self.get_field(usage, "cache_read_input_tokens", 0) or 0
        input_tokens += self.get_field(usage, "cache_creation_input_tokens", 0) or 0
        output_tokens = self.get_field(usage, "output_tokens", 0) or 0
        self._logger.debug(
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

    def extract_text_content(self, response: Any) -> str:
        text_parts: list[str] = []
        non_text_types: list[str] = []
        for block in self.get_field(response, "content", []) or []:
            block_type = self.get_field(block, "type")
            text = self.get_field(block, "text")
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

    def extract_tool_use_json_content(self, response: Any, response_format: type[BaseModel]) -> str:
        for block in self.get_field(response, "content", []) or []:
            if self.get_field(block, "type") != "tool_use":
                continue
            tool_input = self.get_field(block, "input")
            if isinstance(tool_input, dict):
                return json.dumps(tool_input, separators=(",", ":"))
        raise ValueError(
            "AnthropicProvider: response did not include the requested structured "
            f"tool call for {response_format.__name__!r}."
        )

    async def collect_message_batch_results(self, result_source: Any) -> list[Any]:
        if hasattr(result_source, "__aiter__"):
            items = []
            async for item in result_source:
                items.append(item)
            return items
        data = self.get_field(result_source, "data")
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

    def completion_responses_from_batch_results(
        self,
        *,
        batch_id: str,
        items: list[Any],
        expected_custom_ids: list[str],
        response_format: type[BaseModel] | None,
        use_tool_for_response_format: bool,
        repair_invalid_json: bool,
    ) -> list[CompletionResponse]:
        by_custom_id: dict[str, CompletionResponse] = {}
        for item in items:
            custom_id = str(self.get_field(item, "custom_id", "") or "")
            if not custom_id:
                raise ValueError("AnthropicProvider: Message Batch result is missing custom_id.")
            if custom_id in by_custom_id:
                raise ValueError(
                    f"AnthropicProvider: duplicate Message Batch result custom_id={custom_id!r}."
                )

            result = self.get_field(item, "result")
            result_type = str(self.get_field(result, "type", "") or "")
            if result_type != "succeeded":
                raise RuntimeError(
                    "AnthropicProvider: Message Batch request "
                    f"{custom_id!r} failed with result_type={result_type!r}: "
                    f"{self.format_batch_result_error(result)}"
                )
            message = self.get_field(result, "message")
            if message is None:
                raise ValueError(
                    f"AnthropicProvider: succeeded Message Batch result {custom_id!r} "
                    "is missing message."
                )
            by_custom_id[custom_id] = self.completion_response_from_message(
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

        missing = [custom_id for custom_id in expected_custom_ids if custom_id not in by_custom_id]
        if missing:
            raise RuntimeError(
                "AnthropicProvider: Message Batch results missing expected custom_ids: "
                f"{missing!r}."
            )
        return [by_custom_id[custom_id] for custom_id in expected_custom_ids]

    def require_message_batch_id(self, batch: Any) -> str:
        batch_id = self.get_field(batch, "id")
        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("AnthropicProvider: Message Batch create response is missing id.")
        return batch_id

    def format_batch_result_error(self, result: Any) -> str:
        error = self.get_field(result, "error")
        if error is None:
            return "no error detail"
        if isinstance(error, str):
            return error
        message = self.get_field(error, "message")
        error_type = self.get_field(error, "type")
        if message is not None and error_type is not None:
            return f"{error_type}: {message}"
        return str(error)

    def get_field(self, value: Any, name: str, default: Any = None) -> Any:
        if isinstance(value, dict):
            return value.get(name, default)
        return getattr(value, name, default)

    def parse_structured_json(
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


__all__ = ["AnthropicResponseSurface"]
