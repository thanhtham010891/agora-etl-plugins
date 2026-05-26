from __future__ import annotations

import pytest

from agora_plugins.distributed.coordinator import RedisWorkerCoordinator


class _FakeScript:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], list[str]]] = []

    async def __call__(self, *, keys: list[str], args: list[str]) -> int:
        self.calls.append((keys, args))
        return 1


class _FakeRedis:
    def __init__(self, *, ping_error: Exception | None = None) -> None:
        self._ping_error = ping_error
        self.set_calls: list[tuple[str, str, bool | None, int | None]] = []
        self.scan_calls = 0
        self.closed = False
        self.script = _FakeScript()
        self.worker_payloads: dict[str, str] = {}

    async def ping(self) -> None:
        if self._ping_error is not None:
            raise self._ping_error

    def register_script(self, script: str) -> _FakeScript:
        del script
        return self.script

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
        return [self.worker_payloads.get(key) for key in keys]

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


@pytest.mark.asyncio
async def test_coordinator_can_fallback_to_local_on_redis_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis(ping_error=RuntimeError("redis down"))
    _install_fake_aioredis(monkeypatch, redis)

    coordinator = RedisWorkerCoordinator(fallback_to_local=True)
    await coordinator.start("worker-a", ["pipe-a"])

    assert await coordinator.try_acquire_lease("pipe-a", 1) is True
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

    assert redis.script.calls == [(["agora:distributed:lease:pipe-a"], ["worker-a"])]

    await coordinator.stop()
