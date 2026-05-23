from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from agora.core.dlq import DLQRecord

from agora_plugins.redis.dlq import RedisDLQSink, RedisDLQSource


class _FakePipeline:
    def __init__(self, client: _FakeAsyncRedis) -> None:
        self._client = client
        self._commands: list[tuple[str, str]] = []

    def hset(self, key: str, *, mapping: dict[str, str]) -> None:
        self._client.hashes[key] = dict(mapping)

    def rpush(self, key: str, value: str) -> None:
        self._client.lists.setdefault(key, []).append(value)

    def hgetall(self, key: str) -> None:
        self._commands.append(("hgetall", key))

    async def execute(self) -> list[dict[str, str]]:
        results = []
        for cmd, key in self._commands:
            if cmd == "hgetall":
                results.append(dict(self._client.hashes.get(key, {})))
        return results

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeAsyncRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.closed = False

    async def hset(self, key: str, *, mapping: dict[str, str]) -> None:
        self.hashes.setdefault(key, {}).update(mapping)

    async def rpush(self, key: str, value: str) -> None:
        self.lists.setdefault(key, []).append(value)

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        values = self.lists.get(key, [])
        if end == -1:
            return values[start:]
        return values[start : end + 1]

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def delete(self, key: str) -> None:
        self.hashes.pop(key, None)

    async def lrem(self, key: str, count: int, value: str) -> None:
        del count
        self.lists[key] = [item for item in self.lists.get(key, []) if item != value]

    def pipeline(self, *, transaction: bool):
        assert transaction is False
        return _FakePipeline(self)

    async def aclose(self) -> None:
        self.closed = True


def _install_fake_async_redis(monkeypatch: pytest.MonkeyPatch, client: _FakeAsyncRedis) -> None:
    class _RedisFactory:
        @staticmethod
        def from_url(url: str, *, decode_responses: bool):
            del url, decode_responses
            return client

    fake_module = SimpleNamespace(from_url=_RedisFactory.from_url)
    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(asyncio=fake_module))
    monkeypatch.setitem(sys.modules, "redis.asyncio", fake_module)


def _make_record(**overrides) -> DLQRecord:
    defaults = {
        "pipeline_id": "orders",
        "run_id": "run-1",
        "stage": "sink_write",
        "error_type": "RuntimeError",
        "error_message": "sink exploded",
        "record": {"id": 1},
        "source": "orders_source",
        "checkpoint": {"offset": 10},
        "middleware": None,
        "sink": "redis",
        "created_at": datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
        "attempt": 0,
        "max_attempts": 5,
    }
    defaults.update(overrides)
    return DLQRecord(**defaults)


@pytest.mark.asyncio
async def test_redis_dlq_sink_writes_hash_and_index(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record()

    await sink.open()
    await sink.write(record)

    record_key = "agora:test:dlq:orders:run-1:sink_write:2026-05-21T12:00:00+00:00"
    assert client.hashes[record_key]["error_message"] == "sink exploded"
    assert client.lists["agora:test:dlq:__index__"] == [record_key]


@pytest.mark.asyncio
async def test_redis_dlq_sink_replay_updates_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record(attempt=1)

    await sink.open()
    await sink.write(record)
    updated = await sink.replay(record)

    record_key = "agora:test:dlq:orders:run-1:sink_write:2026-05-21T12:00:00+00:00"
    assert updated.attempt == 2
    assert client.hashes[record_key]["attempt"] == "2"


@pytest.mark.asyncio
async def test_redis_dlq_sink_acknowledge_removes_record(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record()

    await sink.open()
    await sink.write(record)
    await sink.acknowledge(record)

    record_key = "agora:test:dlq:orders:run-1:sink_write:2026-05-21T12:00:00+00:00"
    assert record_key not in client.hashes
    assert client.lists["agora:test:dlq:__index__"] == []


@pytest.mark.asyncio
async def test_redis_dlq_source_reads_filtered_records(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")

    await sink.open()
    await sink.write(_make_record())
    await sink.write(
        _make_record(pipeline_id="payments", created_at=datetime(2026, 5, 21, 12, 1, tzinfo=UTC))
    )

    source = RedisDLQSource(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        pipeline_id="orders",
        stage="sink_write",
        limit=10,
    )
    await source.open()
    records = [record async for record in source.stream()]

    assert len(records) == 1
    assert records[0].pipeline_id == "orders"
    assert records[0].record == {"id": 1}
    assert records[0].checkpoint == {"offset": 10}
