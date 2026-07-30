"""Redis Streams -> PostgreSQL runtime helpers with explicit replay boundaries."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport

from agora_plugins.postgres.sinks.postgres import PostgresSink, QuotedIdentifier

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora_plugins.postgres.connection import PostgresConnectionConfig
    from agora_plugins.redis.sources.redis import RedisStreamSource

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RedisPostgresDeliveryConfig:
    """Delivery fields persisted by the certified Redis Streams profile."""

    key_field: str = "redis_delivery_key"
    metadata_field: str | None = None


@dataclass(frozen=True, slots=True)
class RedisPostgresAcceptanceThresholds:
    """Required invariants for replay-safe Redis Streams -> PostgreSQL delivery."""

    require_source_ready: bool = True
    require_sink_connection_ready: bool = True
    require_source_ack_on_success: bool = True
    require_sink_upsert: bool = True
    require_delivery_key_conflict: bool = True
    require_strict_sink_write_safety: bool = True
    require_sink_replay_safe: bool = True

    def to_dict(self) -> dict[str, bool]:
        return {
            "require_source_ready": self.require_source_ready,
            "require_sink_connection_ready": self.require_sink_connection_ready,
            "require_source_ack_on_success": self.require_source_ack_on_success,
            "require_sink_upsert": self.require_sink_upsert,
            "require_delivery_key_conflict": self.require_delivery_key_conflict,
            "require_strict_sink_write_safety": self.require_strict_sink_write_safety,
            "require_sink_replay_safe": self.require_sink_replay_safe,
        }


def with_redis_delivery_fields(
    row_mapper: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    delivery: RedisPostgresDeliveryConfig | None = None,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Preserve injected Redis delivery fields through a custom row mapper."""

    resolved_delivery = delivery or RedisPostgresDeliveryConfig()

    def _wrapped(record: dict[str, Any]) -> dict[str, Any]:
        row = dict(row_mapper(record))
        delivery_key = record.get(resolved_delivery.key_field)
        if delivery_key is not None and resolved_delivery.key_field not in row:
            row[resolved_delivery.key_field] = delivery_key

        metadata_field = resolved_delivery.metadata_field
        if metadata_field is not None:
            delivery_metadata = record.get(metadata_field)
            if delivery_metadata is not None and metadata_field not in row:
                row[metadata_field] = delivery_metadata
        return row

    return _wrapped


def build_redis_postgres_sink(
    *,
    dsn: str | None = None,
    table: str,
    row_mapper: Callable[[dict[str, Any]], dict[str, Any]],
    conflict_key: str | list[str] | None = None,
    batch_size: int = 100,
    upsert: bool = True,
    insert_mode: str = "sql",
    pool_size: int = 1,
    max_rows_per_statement: int | None = None,
    max_parameters_per_statement: int = 32_000,
    retry_policy: Any | None = None,
    delivery: RedisPostgresDeliveryConfig | None = None,
    connection: PostgresConnectionConfig | None = None,
) -> PostgresSink[dict[str, Any]]:
    """Build a Postgres sink whose default conflict key is a Redis message identity."""

    resolved_delivery = delivery or RedisPostgresDeliveryConfig()
    resolved_conflict_key = cast(
        "str | list[str | QuotedIdentifier]",
        conflict_key or resolved_delivery.key_field,
    )
    conflict_keys = (
        (resolved_conflict_key,)
        if isinstance(resolved_conflict_key, (str, QuotedIdentifier))
        else tuple(resolved_conflict_key)
    )
    if not upsert:
        raise ValueError("Redis Streams -> PostgreSQL requires upsert=True")
    if resolved_delivery.key_field not in conflict_keys:
        raise ValueError(
            "Redis Streams -> PostgreSQL conflict_key must include the configured delivery key"
        )
    return PostgresSink[dict[str, Any]](
        dsn=dsn,
        table=table,
        row_mapper=with_redis_delivery_fields(row_mapper, delivery=resolved_delivery),
        conflict_key=resolved_conflict_key,
        batch_size=batch_size,
        upsert=upsert,
        insert_mode=insert_mode,  # type: ignore[arg-type]
        pool_size=pool_size,
        max_rows_per_statement=max_rows_per_statement,
        max_parameters_per_statement=max_parameters_per_statement,
        retry_policy=retry_policy,
        replay_safe_key_contract=True,
        connection=connection,
    )


def build_redis_postgres_runtime(
    *,
    source: RedisStreamSource[T],
    dsn: str | None = None,
    table: str,
    transform: Callable[[T], dict[str, Any] | Awaitable[dict[str, Any]]] | None = None,
    row_mapper: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    conflict_key: str | list[str] | None = None,
    batch_size: int = 100,
    upsert: bool = True,
    insert_mode: str = "sql",
    pool_size: int = 1,
    max_rows_per_statement: int | None = None,
    max_parameters_per_statement: int = 32_000,
    retry_policy: Any | None = None,
    flush_each_record: bool = True,
    delivery: RedisPostgresDeliveryConfig | None = None,
    connection: PostgresConnectionConfig | None = None,
) -> RedisPostgresRuntime[T]:
    """Build the certified Redis Streams -> PostgreSQL runtime profile."""

    sink = build_redis_postgres_sink(
        dsn=dsn,
        table=table,
        row_mapper=(lambda row: row) if row_mapper is None else row_mapper,
        conflict_key=conflict_key,
        batch_size=batch_size,
        upsert=upsert,
        insert_mode=insert_mode,
        pool_size=pool_size,
        max_rows_per_statement=max_rows_per_statement,
        max_parameters_per_statement=max_parameters_per_statement,
        retry_policy=retry_policy,
        delivery=delivery,
        connection=connection,
    )
    return RedisPostgresRuntime(
        source,
        sink,
        transform=transform,
        flush_each_record=flush_each_record,
        delivery=delivery,
    )


class RedisPostgresRuntime:
    """Write and flush PostgreSQL before acknowledging each Redis Stream message."""

    def __init__(
        self,
        source: RedisStreamSource[T],
        sink: PostgresSink[dict[str, Any]],
        *,
        transform: Callable[[T], dict[str, Any] | Awaitable[dict[str, Any]]] | None = None,
        flush_each_record: bool = True,
        delivery: RedisPostgresDeliveryConfig | None = None,
    ) -> None:
        if not flush_each_record:
            raise ValueError(
                "RedisPostgresRuntime requires flush_each_record=True so XACK cannot precede "
                "a durable PostgreSQL write."
            )
        self.source = source
        self.sink = sink
        self.transform = transform
        self.flush_each_record = flush_each_record
        self.delivery = delivery or RedisPostgresDeliveryConfig()

    async def open(self) -> None:
        await self.sink.open()
        try:
            await self.source.open()
        except Exception:
            with contextlib.suppress(Exception):
                await self.sink.close()
            raise

    async def close(self) -> None:
        source_error: Exception | None = None
        try:
            await self.source.close()
        except Exception as exc:
            source_error = exc
        try:
            await self.sink.close()
        except Exception:
            if source_error is None:
                raise
        if source_error is not None:
            raise source_error

    async def deliver(self, record: T) -> dict[str, Any]:
        context = self.source.delivery_context()
        ack_hook = self.source.delivery_success_callback()
        if context is None or ack_hook is None:
            raise RuntimeError(
                "RedisPostgresRuntime.deliver() requires an active Redis Stream delivery context "
                "with ack_on_success=True. Consume from RedisStreamSource.stream() immediately "
                "before calling deliver(), or use drain()."
            )

        outbound = await self._transform_record(record)
        decorated = dict(outbound)
        decorated[self.delivery.key_field] = context.delivery_id
        if self.delivery.metadata_field is not None:
            decorated[self.delivery.metadata_field] = context.to_dict()

        await self.sink.write(decorated)
        await self.sink.flush()
        await ack_hook()
        await self.source.flush_delivery_acks()
        return decorated

    async def drain(self, *, max_records: int | None = None) -> list[T]:
        stream = self.source.stream()
        records: list[T] = []
        try:
            while max_records is None or len(records) < max_records:
                try:
                    record = await anext(stream)
                except StopAsyncIteration:
                    break
                records.append(record)
                await self.deliver(record)
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
        return records

    def acceptance_report(
        self,
        thresholds: RedisPostgresAcceptanceThresholds | None = None,
    ) -> AcceptanceReport:
        """Report whether this composed profile has all replay-safety invariants."""

        resolved = thresholds or RedisPostgresAcceptanceThresholds()
        findings: list[AcceptanceFinding] = []
        source = self.source.metrics_snapshot()
        sink = self.sink.metrics_snapshot()
        capability = self.sink.delivery_capability()

        if resolved.require_source_ready and not source.health.ready:
            findings.append(
                AcceptanceFinding(
                    metric="source.ready",
                    message="Redis Streams source is not ready.",
                    value=source.health.ready,
                    threshold=True,
                    component="redis_stream",
                )
            )
        if resolved.require_sink_connection_ready and not sink.connection_ready:
            findings.append(
                AcceptanceFinding(
                    metric="sink.connection_ready",
                    message="PostgreSQL sink connection is not ready.",
                    value=sink.connection_ready,
                    threshold=True,
                    component="postgres",
                )
            )
        if resolved.require_source_ack_on_success and not source.ack_on_success:
            findings.append(
                AcceptanceFinding(
                    metric="source.ack_on_success",
                    message="Redis Streams must acknowledge only after PostgreSQL handling succeeds.",
                    value=source.ack_on_success,
                    threshold=True,
                    component="redis_stream",
                )
            )
        if resolved.require_sink_upsert and not sink.upsert:
            findings.append(
                AcceptanceFinding(
                    metric="sink.upsert",
                    message="PostgreSQL upsert must be enabled for Redis replay safety.",
                    value=sink.upsert,
                    threshold=True,
                    component="postgres",
                )
            )
        if (
            resolved.require_delivery_key_conflict
            and self.delivery.key_field not in sink.conflict_keys
        ):
            findings.append(
                AcceptanceFinding(
                    metric="sink.delivery_key_conflict",
                    message="PostgreSQL conflict keys must include the Redis delivery key.",
                    value=list(sink.conflict_keys),
                    threshold=self.delivery.key_field,
                    component="postgres",
                )
            )
        if resolved.require_strict_sink_write_safety and sink.write_safety_policy != "strict":
            findings.append(
                AcceptanceFinding(
                    metric="sink.write_safety_policy",
                    message="Redis Streams acceptance requires strict PostgreSQL write safety.",
                    value=sink.write_safety_policy,
                    threshold="strict",
                    component="postgres",
                )
            )
        if resolved.require_sink_replay_safe and not capability.replay_safe:
            findings.append(
                AcceptanceFinding(
                    metric="sink.replay_safe",
                    message="The configured PostgreSQL sink does not declare replay safety.",
                    value=capability.replay_safe,
                    threshold=True,
                    component="postgres",
                )
            )
        return AcceptanceReport(
            passed=not findings,
            thresholds=resolved,
            findings=tuple(findings),
            component="redis_postgres",
        )

    async def ensure_ready(
        self,
        thresholds: RedisPostgresAcceptanceThresholds | None = None,
    ) -> AcceptanceReport:
        """Fail closed unless the opened profile satisfies its acceptance contract."""

        report = self.acceptance_report(thresholds)
        if report.passed:
            return report
        metrics = ", ".join(finding.metric for finding in report.findings)
        raise RuntimeError(f"Redis Streams -> PostgreSQL readiness failed: {metrics}")

    async def _transform_record(self, record: T) -> dict[str, Any]:
        if self.transform is None:
            if not isinstance(record, dict):
                raise TypeError(
                    "RedisPostgresRuntime requires mapping records when transform is not provided."
                )
            return dict(record)
        outbound = self.transform(record)
        if isawaitable(outbound):
            outbound = await outbound
        return outbound


__all__ = [
    "RedisPostgresAcceptanceThresholds",
    "RedisPostgresDeliveryConfig",
    "RedisPostgresRuntime",
    "build_redis_postgres_runtime",
    "build_redis_postgres_sink",
    "with_redis_delivery_fields",
]
