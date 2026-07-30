from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from agora_plugins.postgres import (
    PostgresSink,
    RedisPostgresAcceptanceThresholds,
    RedisPostgresDeliveryConfig,
    RedisPostgresRuntime,
    build_redis_postgres_runtime,
    build_redis_postgres_sink,
)
from agora_plugins.redis.sources import RedisStreamDeliveryContext, RedisStreamSource

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class _FakeRedisStreamSource:
    def __init__(self, record: dict[str, object], *, ack_enabled: bool = True) -> None:
        self._record = record
        self._context = RedisStreamDeliveryContext(
            stream="orders",
            group="orders-writers",
            consumer="worker-1",
            message_id="1710000000000-0",
        )
        self.ack_enabled = ack_enabled
        self.ack_count = 0
        self.ack_flush_count = 0

    def delivery_context(self) -> RedisStreamDeliveryContext:
        return self._context

    def delivery_success_callback(self):
        if not self.ack_enabled:
            return None

        async def _ack() -> None:
            self.ack_count += 1

        return _ack

    async def flush_delivery_acks(self) -> None:
        self.ack_flush_count += 1

    async def stream(self) -> AsyncGenerator[dict[str, object], None]:
        yield self._record


class _FakePostgresSink:
    def __init__(self, *, fail_write: bool = False) -> None:
        self.fail_write = fail_write
        self.writes: list[dict[str, object]] = []
        self.flush_count = 0

    async def write(self, record: dict[str, object]) -> None:
        if self.fail_write:
            raise RuntimeError("postgres write failed")
        self.writes.append(record)

    async def flush(self) -> None:
        self.flush_count += 1


class _SingleMessageRedisClient:
    def __init__(self) -> None:
        self._emitted = False
        self.xack_calls: list[tuple[str, str, tuple[str, ...]]] = []

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int,
        block: int,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        del group, consumer, streams, count, block
        if self._emitted:
            return []
        self._emitted = True
        return [("orders", [("1710000000000-0", {"order_id": "o-1"})])]

    async def xack(self, stream: str, group: str, *message_ids: str) -> None:
        self.xack_calls.append((stream, group, message_ids))


@pytest.mark.asyncio
async def test_redis_postgres_runtime_persists_delivery_identity_before_ack() -> None:
    source = _FakeRedisStreamSource({"order_id": "o-1"})
    sink = _FakePostgresSink()
    runtime = RedisPostgresRuntime(  # type: ignore[arg-type]
        source,
        sink,
        delivery=RedisPostgresDeliveryConfig(metadata_field="redis_metadata"),
    )

    records = await runtime.drain(max_records=1)

    assert records == [{"order_id": "o-1"}]
    assert sink.writes == [
        {
            "order_id": "o-1",
            "redis_delivery_key": "orders:1710000000000-0",
            "redis_metadata": {
                "stream": "orders",
                "group": "orders-writers",
                "consumer": "worker-1",
                "message_id": "1710000000000-0",
                "delivery_id": "orders:1710000000000-0",
            },
        }
    ]
    assert sink.flush_count == 1
    assert source.ack_count == 1
    assert source.ack_flush_count == 1


@pytest.mark.asyncio
async def test_redis_postgres_runtime_flushes_the_real_redis_ack_after_postgres() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="orders",
        group="orders-writers",
        consumer="worker-1",
    )
    client = _SingleMessageRedisClient()
    source._client = client  # type: ignore[attr-defined]
    sink = _FakePostgresSink()
    runtime = RedisPostgresRuntime(source, sink)  # type: ignore[arg-type]

    await runtime.drain(max_records=1)

    assert sink.writes[0]["redis_delivery_key"] == "orders:1710000000000-0"
    assert "redis_metadata" not in sink.writes[0]
    assert client.xack_calls == [("orders", "orders-writers", ("1710000000000-0",))]


@pytest.mark.asyncio
async def test_redis_postgres_runtime_does_not_ack_when_postgres_write_fails() -> None:
    source = _FakeRedisStreamSource({"order_id": "o-1"})
    sink = _FakePostgresSink(fail_write=True)
    runtime = RedisPostgresRuntime(source, sink)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="postgres write failed"):
        await runtime.drain(max_records=1)

    assert source.ack_count == 0
    assert source.ack_flush_count == 0


def test_redis_postgres_builder_defaults_to_replay_safe_delivery_key() -> None:
    sink = build_redis_postgres_sink(
        dsn="postgresql://localhost/test",
        table="events",
        row_mapper=lambda row: {"order_id": row["order_id"]},
    )

    assert sink._conflict_keys == ["redis_delivery_key"]  # type: ignore[attr-defined]
    assert sink.delivery_capability().replay_safe is True


def test_redis_postgres_builder_rejects_an_unsafe_delivery_recipe() -> None:
    with pytest.raises(ValueError, match="requires upsert=True"):
        build_redis_postgres_sink(
            dsn="postgresql://localhost/test",
            table="events",
            row_mapper=lambda row: row,
            upsert=False,
        )

    with pytest.raises(ValueError, match="must include the configured delivery key"):
        build_redis_postgres_sink(
            dsn="postgresql://localhost/test",
            table="events",
            row_mapper=lambda row: row,
            conflict_key="order_id",
        )


def test_redis_postgres_acceptance_rejects_an_unsafe_recipe() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="orders",
        group="orders-writers",
        consumer="worker-1",
        ack_on_success=False,
    )
    sink = PostgresSink(
        dsn="postgresql://localhost/test",
        table="events",
        row_mapper=lambda row: row,
        conflict_key="order_id",
    )
    runtime = RedisPostgresRuntime(source, sink)

    report = runtime.acceptance_report(RedisPostgresAcceptanceThresholds())

    assert report.passed is False
    assert {finding.metric for finding in report.findings} == {
        "source.ready",
        "sink.connection_ready",
        "source.ack_on_success",
        "sink.delivery_key_conflict",
        "sink.replay_safe",
    }


@pytest.mark.asyncio
async def test_redis_postgres_ensure_ready_fails_closed_before_components_open() -> None:
    source = RedisStreamSource(
        url="redis://localhost:6379",
        stream="orders",
        group="orders-writers",
        consumer="worker-1",
    )
    runtime = build_redis_postgres_runtime(
        source=source,
        dsn="postgresql://localhost/test",
        table="events",
    )

    with pytest.raises(RuntimeError, match=r"source\.ready, sink\.connection_ready"):
        await runtime.ensure_ready()


def test_redis_postgres_runtime_supports_custom_delivery_field_names() -> None:
    source = _FakeRedisStreamSource({"order_id": "o-1"})
    sink = _FakePostgresSink()
    runtime = RedisPostgresRuntime(
        source,  # type: ignore[arg-type]
        sink,  # type: ignore[arg-type]
        delivery=RedisPostgresDeliveryConfig(key_field="event_key", metadata_field=None),
    )

    assert runtime.delivery.key_field == "event_key"


def test_redis_postgres_runtime_rejects_ack_before_flush_configuration() -> None:
    with pytest.raises(ValueError, match="requires flush_each_record=True"):
        RedisPostgresRuntime(
            _FakeRedisStreamSource({"order_id": "o-1"}),  # type: ignore[arg-type]
            _FakePostgresSink(),  # type: ignore[arg-type]
            flush_each_record=False,
        )
