"""Kafka sink producer option validation and compatibility helpers."""

from __future__ import annotations

from functools import lru_cache
from inspect import Parameter, signature
from typing import Any

VALID_ACKS = frozenset({0, 1, -1, "all"})
PRODUCER_POSITIVE_INT_CONFIGS = frozenset(
    {
        "connections_max_idle_ms",
        "max_batch_size",
        "max_in_flight_requests_per_connection",
        "max_request_size",
        "metadata_max_age_ms",
        "request_timeout_ms",
    }
)
PRODUCER_NON_NEGATIVE_INT_CONFIGS = frozenset({"linger_ms", "retry_backoff_ms"})


def validate_producer_tuning(producer_kwargs: dict[str, Any]) -> None:
    for name in sorted(PRODUCER_POSITIVE_INT_CONFIGS):
        if name in producer_kwargs:
            _validate_int_config(name, producer_kwargs[name], minimum=1)
    for name in sorted(PRODUCER_NON_NEGATIVE_INT_CONFIGS):
        if name in producer_kwargs:
            _validate_int_config(name, producer_kwargs[name], minimum=0)
    if "acks" in producer_kwargs and producer_kwargs["acks"] not in VALID_ACKS:
        raise ValueError("KafkaSink producer option acks must be one of 0, 1, -1, or 'all'.")
    if "enable_idempotence" in producer_kwargs and not isinstance(
        producer_kwargs["enable_idempotence"],
        bool,
    ):
        raise TypeError("KafkaSink producer option enable_idempotence must be a bool.")


@lru_cache(maxsize=1)
def producer_supported_kwargs(producer_cls: Any) -> set[str] | None:
    try:
        parameters = signature(producer_cls.__init__).parameters
    except (TypeError, ValueError):  # pragma: no cover
        return None
    if any(parameter.kind is Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return None
    return set(parameters)


def _validate_int_config(
    name: str,
    value: object,
    *,
    minimum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"KafkaSink producer option {name} must be an integer >= {minimum}.")
    if value < minimum:
        raise ValueError(f"KafkaSink producer option {name} must be >= {minimum}.")
    return value
