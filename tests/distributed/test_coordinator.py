from __future__ import annotations

import asyncio
import json
import time

import pytest

from agora_plugins.distributed import (
    RedisCoordinatorLifecycle,
    RedisLeaseController,
    RedisLeaseManager,
    RedisLeaseOperations,
    RedisLeaseRuntime,
    RedisPrimaryLeaseStore,
    RedisRedlockQuorum,
    RedisWorkerRegistry,
    RedisWorkerSession,
)
from agora_plugins.distributed.coordinator import RedisWorkerCoordinator


class _FakeScript:
    def __init__(
        self,
        result: object = 1,
        *,
        delay_s: float = 0.0,
        redis: _FakeRedis | None = None,
        kind: str = "",
        match_field: str = "fencing_token",
    ) -> None:
        self.calls: list[tuple[list[str], list[str]]] = []
        self.result = result
        self.delay_s = delay_s
        self._redis = redis
        self._kind = kind
        self._match_field = match_field

    async def __call__(self, *, keys: list[str], args: list[str]) -> object:
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        self.calls.append((keys, args))
        if self._kind == "release" and self._redis is not None:
            raw = self._redis.worker_payloads.get(keys[0])
            match_value = args[1]
            if self._match_field == "fencing_token" and len(args) >= 3:
                match_value = args[2]
            if _payload_matches(raw, args[0], match_value, field=self._match_field):
                self._redis.worker_payloads.pop(keys[0], None)
        return self.result


class _FakeAcquireScript:
    def __init__(self, redis: _FakeRedis) -> None:
        self._redis = redis
        self.calls: list[tuple[list[str], list[str]]] = []

    async def __call__(self, *, keys: list[str], args: list[str]) -> list[object]:
        self.calls.append((keys, args))
        lease_key, fence_key = keys
        if lease_key in self._redis.worker_payloads:
            return [0, None]
        self._redis.counters[fence_key] = self._redis.counters.get(fence_key, 0) + 1
        token = self._redis.counters[fence_key]
        if int(args[5]) > 0:
            self._redis.expire_calls.append((fence_key, int(args[5])))
        self._redis.worker_payloads[lease_key] = json.dumps(
            {
                "worker_id": args[0],
                "acquired_at": args[1],
                "pipeline_id": args[2],
                "run_number": int(args[3]),
                "fencing_token": token,
            }
        )
        return [1, token]


class _FakeRedlockAcquireScript:
    def __init__(self, redis: _FakeRedis) -> None:
        self._redis = redis
        self.calls: list[tuple[list[str], list[str]]] = []

    async def __call__(self, *, keys: list[str], args: list[str]) -> list[object]:
        self.calls.append((keys, args))
        lease_key, _fence_key = keys
        if not self._redis.redlock_acquire or lease_key in self._redis.worker_payloads:
            return [0, None]
        token = int(args[5])
        self._redis.worker_payloads[lease_key] = json.dumps(
            {
                "worker_id": args[0],
                "acquired_at": args[1],
                "pipeline_id": args[2],
                "run_number": int(args[3]),
                "fencing_token": token,
            }
        )
        return [1, token]


class _FakeRedis:
    def __init__(
        self,
        *,
        ping_error: Exception | None = None,
        script_result: int = 1,
        redlock_acquire: bool = True,
    ) -> None:
        self._ping_error = ping_error
        self.set_calls: list[tuple[str, str, bool | None, int | None]] = []
        self.scan_calls = 0
        self.closed = False
        self.redlock_acquire = redlock_acquire
        self.mget_calls: list[tuple[str, ...]] = []
        self.release_script = _FakeScript(script_result, redis=self, kind="release")
        self.renew_script = _FakeScript(script_result)
        self.redlock_release_script = _FakeScript(
            script_result,
            redis=self,
            kind="release",
            match_field="fencing_token",
        )
        self.redlock_renew_script = _FakeScript(script_result)
        self.acquire_script = _FakeAcquireScript(self)
        self.redlock_acquire_script = _FakeRedlockAcquireScript(self)
        self.worker_payloads: dict[str, str] = {}
        self.counters: dict[str, int] = {}
        self.expire_calls: list[tuple[str, int]] = []

    async def ping(self) -> None:
        if self._ping_error is not None:
            raise self._ping_error

    def register_script(self, script: str) -> _FakeScript:
        if "REDLOCK_ACQUIRE" in script:
            return self.redlock_acquire_script  # type: ignore[return-value]
        if "REDLOCK_RELEASE" in script:
            return self.redlock_release_script
        if "REDLOCK_RENEW" in script:
            return self.redlock_renew_script
        if "INCR" in script and "cjson.encode" in script:
            return self.acquire_script  # type: ignore[return-value]
        if "EXPIRE" in script:
            return self.renew_script
        return self.release_script

    async def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool | None = None,
        ex: int | None = None,
    ) -> str | None:
        self.set_calls.append((key, value, nx, ex))
        if nx:
            return "OK"
        self.worker_payloads[key] = value
        return "OK"

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def expire(self, key: str, ttl: int) -> None:
        self.expire_calls.append((key, ttl))

    async def scan(
        self,
        cursor: int,
        *,
        match: str,
        count: int,
    ) -> tuple[int, list[str]]:
        del cursor, match, count
        self.scan_calls += 1
        return 0, list(self.worker_payloads)

    async def mget(self, *keys: str) -> list[str | None]:
        self.mget_calls.append(keys)
        return [self.worker_payloads.get(key) for key in keys]

    async def get(self, key: str) -> str | None:
        return self.worker_payloads.get(key)

    async def delete(self, key: str) -> None:
        self.worker_payloads.pop(key, None)

    async def aclose(self) -> None:
        self.closed = True


def _install_fake_aioredis(monkeypatch: pytest.MonkeyPatch, redis: _FakeRedis) -> None:
    class _Factory:
        @staticmethod
        def from_url(url: str, *, decode_responses: bool):
            del url, decode_responses
            return redis

    monkeypatch.setattr(
        "agora_plugins.distributed.coordinator.aioredis",
        _Factory,
    )


def _install_fake_aioredis_sequence(
    monkeypatch: pytest.MonkeyPatch,
    redis_nodes: list[_FakeRedis],
) -> None:
    calls: list[str] = []

    class _Factory:
        @staticmethod
        def from_url(url: str, *, decode_responses: bool):
            del decode_responses
            calls.append(url)
            return redis_nodes[len(calls) - 1]

    monkeypatch.setattr(
        "agora_plugins.distributed.coordinator.aioredis",
        _Factory,
    )


def _payload_matches(
    raw: str | None,
    worker_id: str,
    match_value: str,
    *,
    field: str = "fencing_token",
) -> bool:
    if raw is None:
        return False
    try:
        data = json.loads(raw)
    except Exception:
        return False
    return data.get("worker_id") == worker_id and str(data.get(field)) == str(match_value)


def test_distributed_exports_public_collaborators() -> None:
    assert RedisCoordinatorLifecycle.__name__ == "RedisCoordinatorLifecycle"
    assert RedisLeaseController.__name__ == "RedisLeaseController"
    assert RedisLeaseManager.__name__ == "RedisLeaseManager"
    assert RedisLeaseOperations.__name__ == "RedisLeaseOperations"
    assert RedisLeaseRuntime.__name__ == "RedisLeaseRuntime"
    assert RedisPrimaryLeaseStore.__name__ == "RedisPrimaryLeaseStore"
    assert RedisRedlockQuorum.__name__ == "RedisRedlockQuorum"
    assert RedisWorkerRegistry.__name__ == "RedisWorkerRegistry"
    assert RedisWorkerSession.__name__ == "RedisWorkerSession"


@pytest.mark.asyncio
async def test_coordinator_can_fallback_to_local_on_redis_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis(ping_error=RuntimeError("redis down"))
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator(fallback_to_local=True)
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 1) is True
    lease = coordinator.current_lease("pipe-a")
    assert lease is not None
    assert await coordinator.validate_lease("pipe-a", lease.fencing_token) is True
    assert await coordinator.renew_lease("pipe-a") is True
    await coordinator.release_lease("pipe-a")
    assert coordinator.current_lease("pipe-a") is None
    assert await coordinator.list_workers() == []


@pytest.mark.asyncio
async def test_coordinator_registers_worker_and_lists_it(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator()
    await coordinator.start("worker-a", ["pipe-a", "pipe-b"])

    workers = await coordinator.list_workers()

    assert len(workers) == 1
    worker = workers[0]
    assert worker.worker_id == "worker-a"
    assert worker.status == "running"
    assert worker.assigned_pipelines == ["pipe-a", "pipe-b"]

    await coordinator.stop()
    assert redis.closed is True


@pytest.mark.asyncio
async def test_coordinator_releases_lease_for_current_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator()
    await coordinator.start("worker-a", ["pipe-a"])

    acquired = await coordinator.try_acquire_lease("pipe-a", 42)
    assert acquired is True

    await coordinator.release_lease("pipe-a")

    assert redis.release_script.calls == [(["agora:distributed:lease:pipe-a"], ["worker-a", "1"])]

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_does_not_release_without_local_fencing_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator()
    await coordinator.start("worker-a", ["pipe-a"])

    await coordinator.release_lease("pipe-a")

    assert redis.release_script.calls == []

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_exposes_and_renews_fencing_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator()
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 42) is True
    lease = coordinator.current_lease("pipe-a")

    assert lease is not None
    assert lease.fencing_token == 1
    assert redis.acquire_script.calls[-1][0] == [
        "agora:distributed:lease:pipe-a",
        "agora:distributed:fence:pipe-a",
    ]
    assert redis.expire_calls == []
    assert await coordinator.renew_lease("pipe-a") is True
    assert redis.renew_script.calls[-1] == (
        ["agora:distributed:lease:pipe-a"],
        ["worker-a", "1", "300"],
    )
    assert redis.expire_calls == []

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_deduplicates_scan_results_when_listing_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator()
    await coordinator.start("worker-a", ["pipe-a"])
    worker_key = "agora:distributed:workers:worker-a"

    async def _duplicate_scan(
        cursor: int,
        *,
        match: str,
        count: int,
    ) -> tuple[int, list[str]]:
        del match, count
        if cursor == 0:
            return 1, [worker_key, worker_key]
        return 0, [worker_key]

    redis.scan = _duplicate_scan  # type: ignore[method-assign]

    workers = await coordinator.list_workers()

    assert len(workers) == 1
    assert workers[0].worker_id == "worker-a"

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_fires_lease_lost_callback_on_takeover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis(script_result=0)
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator()
    lost: list[str] = []

    async def _on_lost(pipeline_id: str) -> None:
        lost.append(pipeline_id)

    coordinator.set_lease_lost_callback(_on_lost)
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 1) is True
    assert await coordinator.renew_lease("pipe-a") is False
    assert lost == ["pipe-a"]
    assert coordinator.current_lease("pipe-a") is None

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_marks_leases_lost_when_renew_cycle_exceeds_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    redis.renew_script.delay_s = 0.05
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator(
        lease_ttl_seconds=3,
        heartbeat_interval=1,
        lease_renewal_deadline_seconds=0.001,
    )
    lost: list[str] = []

    async def _on_lost(pipeline_id: str) -> None:
        lost.append(pipeline_id)

    coordinator.set_lease_lost_callback(_on_lost)
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 1) is True
    assert coordinator.current_lease("pipe-a") is not None

    await coordinator._lease_controller.renew_leases_once(
        redis=coordinator._session.redis,
        worker_id=coordinator._session.worker_id,
    )

    assert coordinator.current_lease("pipe-a") is None
    assert lost == ["pipe-a"]

    await coordinator.stop()


def test_coordinator_rejects_invalid_renew_cycle_deadline() -> None:
    with pytest.raises(ValueError, match="lease_renewal_deadline_seconds"):
        RedisWorkerCoordinator(lease_renewal_deadline_seconds=0)


def test_coordinator_rejects_invalid_timing_values() -> None:
    with pytest.raises(ValueError, match="lease_ttl_seconds"):
        RedisWorkerCoordinator(lease_ttl_seconds=0)

    with pytest.raises(ValueError, match="heartbeat_interval"):
        RedisWorkerCoordinator(heartbeat_interval=0)

    with pytest.raises(ValueError, match="fencing_key_ttl_seconds"):
        RedisWorkerCoordinator(fencing_key_ttl_seconds=0)

    with pytest.raises(ValueError, match="worker_list_fetch_batch_size"):
        RedisWorkerCoordinator(worker_list_fetch_batch_size=0)


def test_coordinator_rejects_too_few_redlock_urls() -> None:
    with pytest.raises(ValueError, match="at least 3"):
        RedisWorkerCoordinator(redlock_redis_urls=["redis://r1", "redis://r2"])


@pytest.mark.asyncio
async def test_coordinator_does_not_increment_fencing_token_when_lease_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)

    first = RedisWorkerCoordinator()
    await first.start("worker-a", ["pipe-a"])
    assert await first.try_acquire_lease("pipe-a", 1) is True

    second = RedisWorkerCoordinator()
    await second.start("worker-b", ["pipe-a"])
    assert await second.try_acquire_lease("pipe-a", 2) is False

    assert redis.counters["agora:distributed:fence:pipe-a"] == 1

    await first.stop()
    await second.stop()


@pytest.mark.asyncio
async def test_coordinator_redlock_acquires_validates_and_releases_quorum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _FakeRedis()
    nodes = [_FakeRedis(), _FakeRedis(), _FakeRedis()]
    _install_fake_aioredis_sequence(monkeypatch, [primary, *nodes])

    coordinator = RedisWorkerCoordinator(
        redlock_redis_urls=["redis://r1", "redis://r2", "redis://r3"]
    )
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 42) is True
    lease = coordinator.current_lease("pipe-a")

    assert lease is not None
    assert lease.fencing_token == 1
    assert primary.counters["agora:distributed:fence:pipe-a"] == 1
    assert primary.expire_calls == []
    for node in nodes:
        assert node.counters == {}
        assert node.expire_calls == []
    assert await coordinator.validate_lease("pipe-a", 1) is True
    assert all("agora:distributed:lease:pipe-a" in node.worker_payloads for node in nodes)

    await coordinator.release_lease("pipe-a")

    assert coordinator.current_lease("pipe-a") is None
    assert all("agora:distributed:lease:pipe-a" not in node.worker_payloads for node in nodes)

    await coordinator.stop()
    assert primary.closed is True
    assert all(node.closed for node in nodes)


@pytest.mark.asyncio
async def test_coordinator_redlock_requires_quorum_and_releases_partial_acquire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _FakeRedis()
    nodes = [_FakeRedis(), _FakeRedis(redlock_acquire=False), _FakeRedis(redlock_acquire=False)]
    _install_fake_aioredis_sequence(monkeypatch, [primary, *nodes])

    coordinator = RedisWorkerCoordinator(
        redlock_redis_urls=["redis://r1", "redis://r2", "redis://r3"]
    )
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 1) is False
    assert coordinator.current_lease("pipe-a") is None
    assert "agora:distributed:lease:pipe-a" not in nodes[0].worker_payloads

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_redlock_marks_lease_lost_when_renew_quorum_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _FakeRedis()
    nodes = [_FakeRedis(), _FakeRedis(), _FakeRedis()]
    _install_fake_aioredis_sequence(monkeypatch, [primary, *nodes])

    coordinator = RedisWorkerCoordinator(
        redlock_redis_urls=["redis://r1", "redis://r2", "redis://r3"]
    )
    lost: list[str] = []

    async def _on_lost(pipeline_id: str) -> None:
        lost.append(pipeline_id)

    coordinator.set_lease_lost_callback(_on_lost)
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 1) is True
    nodes[1].redlock_renew_script.result = 0
    nodes[2].redlock_renew_script.result = 0

    assert await coordinator.renew_lease("pipe-a") is False
    assert coordinator.current_lease("pipe-a") is None
    assert lost == ["pipe-a"]

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_list_workers_fetches_in_bounded_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)
    worker_keys = [f"agora:distributed:workers:worker-{index}" for index in range(5)]
    for index, key in enumerate(worker_keys):
        redis.worker_payloads[key] = json.dumps(
            {
                "worker_id": f"worker-{index}",
                "hostname": "host",
                "pid": index,
                "status": "running",
                "assigned_pipelines": [f"pipe-{index}"],
                "last_heartbeat_at": "now",
            }
        )

    async def _paged_scan(
        cursor: int,
        *,
        match: str,
        count: int,
    ) -> tuple[int, list[str]]:
        del match, count
        if cursor == 0:
            return 1, worker_keys[:3]
        return 0, worker_keys[3:]

    redis.scan = _paged_scan  # type: ignore[method-assign]
    coordinator = RedisWorkerCoordinator(worker_list_fetch_batch_size=2)
    await coordinator.connect()

    workers = await coordinator.list_workers()

    assert [worker.worker_id for worker in workers] == [
        "worker-0",
        "worker-1",
        "worker-2",
        "worker-3",
        "worker-4",
    ]
    assert [len(call) for call in redis.mget_calls] == [2, 2, 1]

    await coordinator.close()


@pytest.mark.asyncio
async def test_coordinator_propagates_programming_errors_in_single_lease_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator()
    await coordinator.start("worker-a", ["pipe-a"])

    async def _broken_acquire(*, keys: list[str], args: list[str]) -> object:
        del keys, args
        raise AttributeError("broken acquire")

    coordinator._lease_manager._primary.acquire_script = _broken_acquire  # type: ignore[assignment]

    with pytest.raises(AttributeError, match="broken acquire"):
        await coordinator.try_acquire_lease("pipe-a", 1)

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_propagates_programming_errors_in_redlock_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _FakeRedis()
    nodes = [_FakeRedis(), _FakeRedis(), _FakeRedis()]
    _install_fake_aioredis_sequence(monkeypatch, [primary, *nodes])

    coordinator = RedisWorkerCoordinator(
        redlock_redis_urls=["redis://r1", "redis://r2", "redis://r3"]
    )
    await coordinator.start("worker-a", ["pipe-a"])
    assert await coordinator.try_acquire_lease("pipe-a", 1) is True

    async def _broken_get(key: str) -> str | None:
        del key
        raise AttributeError("broken validate")

    nodes[0].get = _broken_get  # type: ignore[method-assign]

    with pytest.raises(AttributeError, match="broken validate"):
        await coordinator.validate_lease("pipe-a", 1)

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_redlock_acquire_runs_nodes_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _FakeRedis()
    nodes = [_FakeRedis(), _FakeRedis(), _FakeRedis()]
    for node in nodes:
        node.redlock_acquire_script.delay_s = 0.05
    _install_fake_aioredis_sequence(monkeypatch, [primary, *nodes])

    coordinator = RedisWorkerCoordinator(
        redlock_redis_urls=["redis://r1", "redis://r2", "redis://r3"]
    )
    await coordinator.start("worker-a", ["pipe-a"])

    started = time.monotonic()
    assert await coordinator.try_acquire_lease("pipe-a", 1) is True
    elapsed = time.monotonic() - started

    assert elapsed < 0.12

    await coordinator.stop()


@pytest.mark.asyncio
async def test_coordinator_redlock_validate_runs_nodes_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _FakeRedis()
    nodes = [_FakeRedis(), _FakeRedis(), _FakeRedis()]
    _install_fake_aioredis_sequence(monkeypatch, [primary, *nodes])

    coordinator = RedisWorkerCoordinator(
        redlock_redis_urls=["redis://r1", "redis://r2", "redis://r3"]
    )
    await coordinator.start("worker-a", ["pipe-a"])
    assert await coordinator.try_acquire_lease("pipe-a", 1) is True
    for node in nodes:
        node.redlock_acquire_script.delay_s = 0.0
        original_get = node.get

        async def _slow_get(key: str, _original=original_get) -> str | None:
            await asyncio.sleep(0.05)
            return await _original(key)

        node.get = _slow_get  # type: ignore[method-assign]

    started = time.monotonic()
    assert await coordinator.validate_lease("pipe-a", 1) is True
    elapsed = time.monotonic() - started

    assert elapsed < 0.12

    await coordinator.stop()
