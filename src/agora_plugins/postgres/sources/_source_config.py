"""Configuration validation helpers for PostgreSQL sources."""

from __future__ import annotations

import uuid
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Literal, cast

from agora.core.retry import RetryPolicy

if TYPE_CHECKING:
    from collections.abc import Callable

DEFAULT_SOURCE_RETRY_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    OSError,
    TimeoutError,
)
READ_ROUTING_TARGET_SESSION_ATTRS: dict[str, str | None] = {
    "dsn": None,
    "primary": "primary",
    "standby": "standby",
    "prefer_standby": "prefer-standby",
    "any": "any",
}


def validate_postgres_source_config(
    *,
    checkpoint_field: str | None,
    checkpoint_param: str | None,
    checkpoint_fields: list[str] | None,
    checkpoint_params: dict[str, str] | None,
    statement_timeout_ms: int | None,
    read_routing: str,
    max_replica_replay_lag_s: float | None,
    on_replica_stale: str,
    fetch_strategy: str,
    batch_size: int,
) -> None:
    singular_config = checkpoint_field is not None or checkpoint_param is not None
    composite_config = checkpoint_fields is not None or checkpoint_params is not None

    if singular_config and composite_config:
        raise ValueError(
            "Use either checkpoint_field/checkpoint_param or "
            "checkpoint_fields/checkpoint_params, not both."
        )
    if singular_config and (checkpoint_field is None or checkpoint_param is None):
        raise ValueError("checkpoint_field and checkpoint_param must be provided together.")
    if composite_config and (not checkpoint_fields or not checkpoint_params):
        raise ValueError("checkpoint_fields and checkpoint_params must be provided together.")
    if checkpoint_fields is not None and checkpoint_params is not None:
        missing = [field for field in checkpoint_fields if field not in checkpoint_params]
        if missing:
            raise ValueError(
                "checkpoint_params must provide a query parameter for every "
                f"checkpoint field. Missing: {missing!r}"
            )
    if statement_timeout_ms is not None and statement_timeout_ms < 1:
        raise ValueError("statement_timeout_ms must be >= 1 when provided.")
    if read_routing not in READ_ROUTING_TARGET_SESSION_ATTRS:
        raise ValueError("read_routing must be one of: dsn, primary, standby, prefer_standby, any.")
    if max_replica_replay_lag_s is not None and max_replica_replay_lag_s < 0:
        raise ValueError("max_replica_replay_lag_s must be >= 0 when provided.")
    if on_replica_stale not in {"fail_closed", "route_primary"}:
        raise ValueError("on_replica_stale must be 'fail_closed' or 'route_primary'.")
    if fetch_strategy not in {"client", "server_side"}:
        raise ValueError("fetch_strategy must be 'client' or 'server_side'.")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1.")


def default_source_retry_policy() -> RetryPolicy[object]:
    return RetryPolicy[object](
        max_attempts=3,
        initial_backoff_s=0.25,
        backoff_multiplier=2.0,
        max_backoff_s=2.0,
        retry_exceptions=DEFAULT_SOURCE_RETRY_EXCEPTIONS,
        retry_if=is_retriable_postgres_read_error,
    )


def callable_accepts_context(func: object) -> bool:
    try:
        parameters = signature(cast("Callable[..., object]", func)).parameters.values()
    except (TypeError, ValueError):
        return False

    positional = [
        parameter
        for parameter in parameters
        if parameter.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if any(parameter.kind is Parameter.VAR_POSITIONAL for parameter in parameters):
        return True
    return len(positional) >= 2


def is_retriable_postgres_read_error(exc: Exception) -> bool:
    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(sqlstate, str) and (
        sqlstate.startswith("08") or sqlstate in {"57P01", "57P02", "57P03", "40001", "40P01"}
    ):
        return True
    error_name = type(exc).__name__.lower()
    message = str(exc).lower()
    return any(
        marker in error_name or marker in message
        for marker in (
            "connection",
            "connect",
            "timeout",
            "temporar",
            "operational",
            "admin shutdown",
            "server closed the connection",
        )
    )


def sql_isolation_level(
    value: Literal["read_committed", "repeatable_read", "serializable"],
) -> str:
    return {
        "read_committed": "read committed",
        "repeatable_read": "repeatable read",
        "serializable": "serializable",
    }[value]


def generated_server_side_cursor_name() -> str:
    return f"agora_pg_source_{uuid.uuid4().hex}"
