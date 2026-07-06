from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest
from agora.core.dlq import DLQRecord

from agora_plugins.dlq_policy import DLQPayloadPolicy
from agora_plugins.redis import (
    RedisDLQSinkEnterpriseAcceptanceThresholds,
    RedisDLQSourceEnterpriseAcceptanceThresholds,
)
from agora_plugins.redis.dlq import RedisDLQSink, RedisDLQSource


class _ReverseCipher:
    def encrypt(self, payload: bytes) -> bytes:
        return payload[::-1]

    def decrypt(self, payload: bytes) -> bytes:
        return payload[::-1]


class _FailingCipher:
    def encrypt(self, payload: bytes) -> bytes:
        del payload
        raise RuntimeError("kms unavailable")


class _FakePipeline:
    def __init__(self, client: _FakeAsyncRedis) -> None:
        self._client = client
        self._commands: list[tuple[str, str]] = []

    def hset(self, key: str, *, mapping: dict[str, str]) -> None:
        self._client.hashes[key] = dict(mapping)

    def rpush(self, key: str, value: str) -> None:
        self._client.lists.setdefault(key, []).append(value)

    def delete(self, key: str) -> None:
        self._client.hashes.pop(key, None)

    def lrem(self, key: str, count: int, value: str) -> None:
        del count
        self._client.lists[key] = [
            item for item in self._client.lists.get(key, []) if item != value
        ]

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
        self.lrange_calls: list[tuple[str, int, int]] = []
        self.exists_calls: list[str] = []
        self.upsert_script = _FakeDLQUpsertScript(self)
        self.acknowledge_script = _FakeDLQAcknowledgeScript(self)

    async def hset(self, key: str, *, mapping: dict[str, str]) -> None:
        self.hashes.setdefault(key, {}).update(mapping)

    async def rpush(self, key: str, value: str) -> None:
        self.lists.setdefault(key, []).append(value)

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        self.lrange_calls.append((key, start, end))
        values = self.lists.get(key, [])
        if end == -1:
            return values[start:]
        return values[start : end + 1]

    async def exists(self, key: str) -> int:
        self.exists_calls.append(key)
        return int(key in self.hashes or bool(self.lists.get(key)))

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def delete(self, key: str) -> None:
        self.hashes.pop(key, None)

    async def lrem(self, key: str, count: int, value: str) -> None:
        del count
        self.lists[key] = [item for item in self.lists.get(key, []) if item != value]

    def pipeline(self, *, transaction: bool):
        assert transaction in {False, True}
        return _FakePipeline(self)

    def register_script(self, script: str):
        if "REDIS_DLQ_UPSERT" in script:
            return self.upsert_script
        if "REDIS_DLQ_ACKNOWLEDGE" in script:
            return self.acknowledge_script
        raise AssertionError(f"unexpected script registration: {script[:40]!r}")

    async def aclose(self) -> None:
        self.closed = True


class _FakeDLQUpsertScript:
    def __init__(self, client: _FakeAsyncRedis) -> None:
        self._client = client
        self.calls: list[tuple[list[str], list[str]]] = []

    async def __call__(self, *, keys: list[str], args: list[str]) -> int:
        self.calls.append((keys, args))
        record_key, primary_index, pipeline_index, stage_index, pipeline_stage_index = keys
        field_count = int(args[0])
        mapping = {args[index]: args[index + 1] for index in range(1, field_count * 2 + 1, 2)}
        existing_payload = dict(self._client.hashes.get(record_key, {}))
        inserted = int(not existing_payload)
        self._client.hashes[record_key] = mapping
        if inserted:
            self._client.lists.setdefault(primary_index, []).append(record_key)
        else:
            old_secondary_keys = {
                existing_payload.get("__pipeline_index_key"),
                existing_payload.get("__stage_index_key"),
                existing_payload.get("__pipeline_stage_index_key"),
            } - {None}
            for index_key in old_secondary_keys | {
                pipeline_index,
                stage_index,
                pipeline_stage_index,
            }:
                self._client.lists[index_key] = [
                    item for item in self._client.lists.get(index_key, []) if item != record_key
                ]
        for index_key in (pipeline_index, stage_index, pipeline_stage_index):
            self._client.lists.setdefault(index_key, []).append(record_key)
        return inserted


class _FakeDLQAcknowledgeScript:
    def __init__(self, client: _FakeAsyncRedis) -> None:
        self._client = client
        self.calls: list[tuple[list[str], list[str]]] = []

    async def __call__(self, *, keys: list[str], args: list[str]) -> int:
        self.calls.append((keys, args))
        record_key, primary_index, pipeline_index, stage_index, pipeline_stage_index = keys
        payload = self._client.hashes.pop(record_key, {})
        for index_key in (
            primary_index,
            payload.get("__pipeline_index_key") or pipeline_index,
            payload.get("__stage_index_key") or stage_index,
            payload.get("__pipeline_stage_index_key") or pipeline_stage_index,
        ):
            self._client.lists[index_key] = [
                item for item in self._client.lists.get(index_key, []) if item != record_key
            ]
        return 1


def _install_fake_async_redis(monkeypatch: pytest.MonkeyPatch, client: _FakeAsyncRedis) -> None:
    class _RedisFactory:
        @staticmethod
        def from_url(url: str, **kwargs: object):
            del url, kwargs
            return client

    class _RedisClusterFactory:
        @staticmethod
        def from_url(url: str, **kwargs: object):
            del url, kwargs
            return client

    fake_module = SimpleNamespace(
        from_url=_RedisFactory.from_url,
        RedisCluster=_RedisClusterFactory,
    )
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

    record_key = client.lists["agora:test:dlq:__index__"][0]
    pipeline_index = "agora:test:dlq:__index__:pipeline:orders"
    stage_index = "agora:test:dlq:__index__:stage:sink_write"
    pipeline_stage_index = "agora:test:dlq:__index__:pipeline_stage:orders:sink_write"
    assert record_key.startswith(
        "agora:test:dlq:orders:run-1:sink_write:2026-05-21T12:00:00+00:00:"
    )
    assert client.hashes[record_key]["error_message"] == "sink exploded"
    assert client.hashes[record_key]["storage_key"] == record_key
    assert client.lists["agora:test:dlq:__index__"] == [record_key]
    assert client.lists[pipeline_index] == [record_key]
    assert client.lists[stage_index] == [record_key]
    assert client.lists[pipeline_stage_index] == [record_key]


@pytest.mark.asyncio
async def test_redis_dlq_sink_redacts_payload_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        payload_policy=DLQPayloadPolicy.redacted(redact_fields=("ssn",)),
    )
    record = _make_record(
        record={"id": 1, "password": "plain-secret"},
        original_record={"token": "raw-token"},
        processed_record={"ssn": "111-22-3333"},
        checkpoint={"offset": 10, "api_key": "secret-api-key"},
        details={"client_secret": "client-secret"},
    )

    await sink.open()
    await sink.write(record)

    record_key = client.lists["agora:test:dlq:__index__"][0]
    rendered_hash = "\n".join(client.hashes[record_key].values())
    assert "plain-secret" not in rendered_hash
    assert "raw-token" not in rendered_hash
    assert "111-22-3333" not in rendered_hash
    assert "secret-api-key" not in rendered_hash
    assert "client-secret" not in rendered_hash
    assert rendered_hash.count("[REDACTED]") >= 5


@pytest.mark.asyncio
async def test_redis_dlq_encrypted_payload_requires_policy_for_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    policy = DLQPayloadPolicy.encrypted(
        encryptor=_ReverseCipher(),
        encryption_algorithm="reverse-test",
        encryption_key_id="test-key",
    )
    sink = RedisDLQSink(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        payload_policy=policy,
    )
    record = _make_record(
        record={"id": 1, "password": "plain-secret"},
        original_record={"token": "raw-token"},
        checkpoint={"offset": 10, "api_key": "secret-api-key"},
        details={"client_secret": "client-secret"},
    )

    await sink.open()
    await sink.write(record)

    record_key = client.lists["agora:test:dlq:__index__"][0]
    rendered_hash = "\n".join(client.hashes[record_key].values())
    assert "plain-secret" not in rendered_hash
    assert "raw-token" not in rendered_hash
    assert "secret-api-key" not in rendered_hash
    assert "client-secret" not in rendered_hash
    assert '"payload_encoding": "encrypted"' in client.hashes[record_key]["record"]
    assert client.hashes[record_key]["original_record"] == ""
    assert client.hashes[record_key]["processed_record"] == ""
    assert client.hashes[record_key]["checkpoint"] == ""
    assert client.hashes[record_key]["details"] == ""

    source_without_policy = RedisDLQSource(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
    )
    await source_without_policy.open()
    with pytest.raises(ValueError, match="Encrypted Redis DLQ payload"):
        [item async for item in source_without_policy.stream()]

    source = RedisDLQSource(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        payload_policy=policy,
    )
    await source.open()
    records = [item async for item in source.stream()]

    assert len(records) == 1
    assert records[0].record == record.record
    assert records[0].original_record == record.original_record
    assert records[0].checkpoint == record.checkpoint
    assert records[0].details == record.details


@pytest.mark.asyncio
async def test_redis_dlq_encryption_failure_does_not_write_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        payload_policy=DLQPayloadPolicy.encrypted(encryptor=_FailingCipher()),
    )

    await sink.open()
    with pytest.raises(RuntimeError, match="kms unavailable"):
        await sink.write(_make_record(record={"id": 1, "password": "plain-secret"}))

    assert client.hashes == {}
    assert client.lists == {}


@pytest.mark.asyncio
async def test_redis_dlq_sink_replay_updates_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record(attempt=1)

    await sink.open()
    await sink.write(record)
    updated = await sink.replay(record)

    record_key = client.lists["agora:test:dlq:__index__"][0]
    assert updated.attempt == 2
    assert client.hashes[record_key]["attempt"] == "2"
    metrics = sink.metrics_snapshot()
    assert metrics.replay_count == 1
    assert metrics.replayed_record_count == 1
    assert metrics.last_replay_at is not None


@pytest.mark.asyncio
async def test_redis_dlq_sink_acknowledge_removes_record(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record()

    await sink.open()
    await sink.write(record)
    await sink.acknowledge(record)

    record_key = cast("str", record._storage_id)
    assert record_key not in client.hashes
    assert client.lists["agora:test:dlq:__index__"] == []
    assert client.lists["agora:test:dlq:__index__:pipeline:orders"] == []
    assert client.lists["agora:test:dlq:__index__:stage:sink_write"] == []
    assert client.lists["agora:test:dlq:__index__:pipeline_stage:orders:sink_write"] == []
    metrics = sink.metrics_snapshot()
    assert metrics.acknowledge_count == 1
    assert metrics.acknowledged_record_count == 1
    assert metrics.last_acknowledge_at is not None


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
    assert client.lrange_calls == [
        ("agora:test:dlq:__index__:pipeline_stage:orders:sink_write", 0, -1)
    ]
    metrics = source.metrics_snapshot()
    assert metrics.scan_count == 1
    assert metrics.emitted_record_count == 1
    assert metrics.last_scan_at is not None
    assert metrics.last_record_at is not None


@pytest.mark.asyncio
async def test_redis_dlq_source_scans_snapshot_and_stops_after_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")

    await sink.open()
    for minute in range(205):
        await sink.write(
            _make_record(
                created_at=datetime(2026, 5, 21, 12, minute % 60, tzinfo=UTC),
                run_id=f"run-{minute}",
            )
        )

    source = RedisDLQSource(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        limit=5,
    )
    await source.open()
    records = [record async for record in source.stream()]

    assert len(records) == 5
    assert client.lrange_calls == [("agora:test:dlq:__index__", 0, -1)]


@pytest.mark.asyncio
async def test_redis_dlq_source_falls_back_to_primary_index_for_legacy_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record()
    record_key = "agora:test:dlq:legacy-record"
    payload = {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": '{"id": 1}',
        "original_record": "null",
        "processed_record": "null",
        "source": record.source or "",
        "checkpoint": '{"offset": 10}',
        "details": "null",
        "middleware": "",
        "sink": record.sink or "",
        "created_at": record.created_at.isoformat(),
        "attempt": "0",
        "max_attempts": "5",
        "storage_key": record_key,
    }
    client.hashes[record_key] = payload
    client.lists[sink._index_key] = [record_key]

    source = RedisDLQSource(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        pipeline_id="orders",
        stage="sink_write",
    )
    await source.open()
    records = [record async for record in source.stream()]

    assert [record.pipeline_id for record in records] == ["orders"]
    assert client.exists_calls == [
        "agora:test:dlq:__index__:pipeline_stage:orders:sink_write",
        "agora:test:dlq:__index__",
    ]
    assert client.lrange_calls == [("agora:test:dlq:__index__", 0, -1)]


@pytest.mark.asyncio
async def test_redis_dlq_acknowledge_only_removes_target_record_when_identity_collides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    created_at = datetime(2026, 5, 21, 12, 0, tzinfo=UTC)
    first = _make_record(created_at=created_at, record={"id": 1})
    second = _make_record(created_at=created_at, record={"id": 2})

    await sink.open()
    await sink.write(first)
    await sink.write(second)

    assert len(client.lists["agora:test:dlq:__index__"]) == 2
    assert (
        client.lists["agora:test:dlq:__index__"][0] != client.lists["agora:test:dlq:__index__"][1]
    )

    await sink.acknowledge(first)

    remaining_key = client.lists["agora:test:dlq:__index__"][0]
    assert len(client.lists["agora:test:dlq:__index__"]) == 1
    assert client.hashes[remaining_key]["record"] == '{"id": 2}'


@pytest.mark.asyncio
async def test_redis_dlq_replay_requires_persisted_storage_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")

    await sink.open()
    with pytest.raises(ValueError, match="storage key"):
        await sink.replay(_make_record())

    assert client.hashes == {}
    metrics = sink.metrics_snapshot()
    assert metrics.replay_count == 0
    assert metrics.replayed_record_count == 0


@pytest.mark.asyncio
async def test_redis_dlq_rewrite_existing_storage_key_does_not_duplicate_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record()

    await sink.open()
    await sink.write(record)
    await sink.write(record)

    assert len(client.lists["agora:test:dlq:__index__"]) == 1
    assert len(client.lists["agora:test:dlq:__index__:pipeline:orders"]) == 1
    assert len(client.lists["agora:test:dlq:__index__:stage:sink_write"]) == 1
    assert len(client.lists["agora:test:dlq:__index__:pipeline_stage:orders:sink_write"]) == 1
    assert client.hashes[cast("str", record._storage_id)]["record"] == '{"id": 1}'
    metrics = sink.metrics_snapshot()
    assert metrics.inserted_record_count == 1
    assert metrics.upserted_record_count == 2
    assert metrics.updated_record_count == 1


@pytest.mark.asyncio
async def test_redis_dlq_rewrite_existing_storage_key_moves_secondary_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    record = _make_record()

    await sink.open()
    await sink.write(record)
    await sink.write(_make_record(stage="sink_retry", _storage_id=cast("str", record._storage_id)))

    record_key = cast("str", record._storage_id)
    assert client.lists["agora:test:dlq:__index__"] == [record_key]
    assert client.lists["agora:test:dlq:__index__:stage:sink_write"] == []
    assert client.lists["agora:test:dlq:__index__:stage:sink_retry"] == [record_key]


@pytest.mark.asyncio
async def test_redis_dlq_cluster_uses_tagged_keys_for_atomic_scripts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        redis_cluster=True,
    )
    source = RedisDLQSource(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        redis_cluster=True,
    )
    record = _make_record()

    await sink.open()
    await sink.write(record)

    record_key = cast("str", record._storage_id)
    tagged_prefix = "agora:test:dlq:{agora:test:dlq}"
    assert record_key.startswith(f"{tagged_prefix}:orders:run-1:sink_write:")
    assert client.lists[f"{tagged_prefix}:__index__"] == [record_key]
    assert client.upsert_script.calls

    await source.open()
    records = [item async for item in source.stream()]
    assert [item.record for item in records] == [{"id": 1}]

    await sink.acknowledge(record)
    assert client.acknowledge_script.calls
    assert client.lists[f"{tagged_prefix}:__index__"] == []


@pytest.mark.asyncio
async def test_redis_dlq_cluster_source_and_acknowledge_fallback_to_legacy_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        redis_cluster=True,
    )
    source = RedisDLQSource(
        url="redis://localhost:6379",
        key_prefix="agora:test:dlq",
        pipeline_id="orders",
        stage="sink_write",
        redis_cluster=True,
    )
    record = _make_record()
    record_key = "agora:test:dlq:legacy-record"
    payload = {
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "stage": record.stage,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "record": '{"id": 1}',
        "original_record": "null",
        "processed_record": "null",
        "source": record.source or "",
        "checkpoint": '{"offset": 10}',
        "details": "null",
        "middleware": "",
        "sink": record.sink or "",
        "created_at": record.created_at.isoformat(),
        "attempt": "0",
        "max_attempts": "5",
        "storage_key": record_key,
    }
    client.hashes[record_key] = payload
    client.lists["agora:test:dlq:__index__:pipeline_stage:orders:sink_write"] = [record_key]
    client.lists["agora:test:dlq:__index__"] = [record_key]
    client.lists["agora:test:dlq:__index__:pipeline:orders"] = [record_key]
    client.lists["agora:test:dlq:__index__:stage:sink_write"] = [record_key]

    await source.open()
    records = [item async for item in source.stream()]
    assert [item.record for item in records] == [{"id": 1}]

    await sink.open()
    await sink.acknowledge(records[0])
    assert record_key not in client.hashes
    assert client.lists["agora:test:dlq:__index__"] == []
    assert client.lists["agora:test:dlq:__index__:pipeline:orders"] == []
    assert client.lists["agora:test:dlq:__index__:stage:sink_write"] == []
    assert client.lists["agora:test:dlq:__index__:pipeline_stage:orders:sink_write"] == []


def test_redis_dlq_acceptance_and_prometheus_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeAsyncRedis()
    _install_fake_async_redis(monkeypatch, client)
    sink = RedisDLQSink(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    source = RedisDLQSource(url="redis://localhost:6379", key_prefix="agora:test:dlq")
    sink._client = client  # type: ignore[attr-defined]
    source._client = client  # type: ignore[attr-defined]

    sink_report = sink.acceptance_report(RedisDLQSinkEnterpriseAcceptanceThresholds())
    source_report = source.acceptance_report(RedisDLQSourceEnterpriseAcceptanceThresholds())
    sink_rendered = sink.render_prometheus_metrics(namespace="agora_test_redis")
    source_rendered = source.render_prometheus_metrics(namespace="agora_test_redis")

    assert sink_report.passed is True
    assert source_report.passed is True
    assert (
        'agora_test_redis_dlq_sink_state{key_prefix="agora:test:dlq",state="connection_ready"} 1'
        in sink_rendered
    )
    assert 'event="upserted_record"' in sink_rendered
    assert 'event="updated_record"' in sink_rendered
    assert (
        'agora_test_redis_dlq_source_state{key_prefix="agora:test:dlq",pipeline_id="",stage="",state="connection_ready"} 1'
        in source_rendered
    )
