from __future__ import annotations

import pytest

from agora_plugins.distributed import RedisWorkerCoordinator

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_redis_worker_coordinator_exclusive_lease_and_fencing_against_real_redis(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    key_prefix = f"agora:distributed:it:{unique_suffix}:"
    pipeline_id = f"pipeline-{unique_suffix}"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    coordinators: list[RedisWorkerCoordinator] = []

    try:
        keys = list(client.scan_iter(match=f"{key_prefix}*"))
        if keys:
            client.delete(*keys)

        first = RedisWorkerCoordinator(
            redis_url=redis_url,
            lease_ttl_seconds=3,
            heartbeat_interval=1,
            key_prefix=key_prefix,
        )
        second = RedisWorkerCoordinator(
            redis_url=redis_url,
            lease_ttl_seconds=3,
            heartbeat_interval=1,
            key_prefix=key_prefix,
        )
        third = RedisWorkerCoordinator(
            redis_url=redis_url,
            lease_ttl_seconds=3,
            heartbeat_interval=1,
            key_prefix=key_prefix,
        )
        coordinators.extend([first, second, third])

        await first.start("worker-a", [pipeline_id])
        await second.start("worker-b", [pipeline_id])

        assert await first.try_acquire_lease(pipeline_id, 1) is True
        first_lease = first.current_lease(pipeline_id)
        assert first_lease is not None
        assert first_lease.fencing_token == 1
        assert await second.try_acquire_lease(pipeline_id, 2) is False
        assert await first.validate_lease(pipeline_id, first_lease.fencing_token) is True
        assert await second.validate_lease(pipeline_id, first_lease.fencing_token) is False

        await first.release_lease(pipeline_id)
        assert await second.try_acquire_lease(pipeline_id, 2) is True
        second_lease = second.current_lease(pipeline_id)
        assert second_lease is not None
        assert second_lease.fencing_token > first_lease.fencing_token

        await second.release_lease(pipeline_id)
        await second.stop()
        await third.start("worker-c", [pipeline_id])
        assert await third.try_acquire_lease(pipeline_id, 3) is True
        third_lease = third.current_lease(pipeline_id)
        assert third_lease is not None
        assert third_lease.fencing_token > second_lease.fencing_token
    finally:
        for coordinator in reversed(coordinators):
            await coordinator.stop()
        keys = list(client.scan_iter(match=f"{key_prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()


@pytest.mark.asyncio
async def test_redis_worker_coordinator_renew_detects_lost_lease_against_real_redis(
    redis_url: str,
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    key_prefix = f"agora:distributed:lost:{unique_suffix}:"
    pipeline_id = f"pipeline-{unique_suffix}"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    coordinator = RedisWorkerCoordinator(
        redis_url=redis_url,
        lease_ttl_seconds=3,
        heartbeat_interval=1,
        key_prefix=key_prefix,
    )
    lost: list[str] = []

    async def _on_lost(lost_pipeline_id: str) -> None:
        lost.append(lost_pipeline_id)

    try:
        keys = list(client.scan_iter(match=f"{key_prefix}*"))
        if keys:
            client.delete(*keys)

        await coordinator.start("worker-a", [pipeline_id])
        coordinator.set_lease_lost_callback(_on_lost)
        assert await coordinator.try_acquire_lease(pipeline_id, 1) is True
        lease = coordinator.current_lease(pipeline_id)
        assert lease is not None

        client.delete(f"{key_prefix}lease:{pipeline_id}")

        assert await coordinator.renew_lease(pipeline_id) is False
        assert coordinator.current_lease(pipeline_id) is None
        assert lost == [pipeline_id]
    finally:
        await coordinator.stop()
        keys = list(client.scan_iter(match=f"{key_prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()


@pytest.mark.asyncio
async def test_redis_worker_coordinator_redlock_quorum_against_independent_redis_masters(
    redis_redlock_urls: list[str],
    unique_suffix: str,
) -> None:
    redis = pytest.importorskip("redis")
    key_prefix = f"agora:distributed:redlock:{unique_suffix}:"
    pipeline_id = f"pipeline-{unique_suffix}"
    lease_key = f"{key_prefix}lease:{pipeline_id}"
    clients = [redis.Redis.from_url(url, decode_responses=True) for url in redis_redlock_urls]
    coordinators: list[RedisWorkerCoordinator] = []

    try:
        for client in clients:
            keys = list(client.scan_iter(match=f"{key_prefix}*"))
            if keys:
                client.delete(*keys)

        first = RedisWorkerCoordinator(
            redis_url=redis_redlock_urls[0],
            redlock_redis_urls=redis_redlock_urls,
            lease_ttl_seconds=5,
            heartbeat_interval=1,
            key_prefix=key_prefix,
        )
        second = RedisWorkerCoordinator(
            redis_url=redis_redlock_urls[0],
            redlock_redis_urls=redis_redlock_urls,
            lease_ttl_seconds=5,
            heartbeat_interval=1,
            key_prefix=key_prefix,
        )
        coordinators.extend([first, second])

        await first.start("worker-a", [pipeline_id])
        await second.start("worker-b", [pipeline_id])

        assert await first.try_acquire_lease(pipeline_id, 1) is True
        first_lease = first.current_lease(pipeline_id)
        assert first_lease is not None
        assert first_lease.fencing_token == 1
        assert all(client.get(lease_key) is not None for client in clients)
        assert await second.try_acquire_lease(pipeline_id, 2) is False

        clients[1].delete(lease_key)
        assert await first.validate_lease(pipeline_id, first_lease.fencing_token) is True

        clients[2].delete(lease_key)
        assert await first.validate_lease(pipeline_id, first_lease.fencing_token) is False
        assert first.current_lease(pipeline_id) is None

        for client in clients:
            client.delete(lease_key)

        assert await second.try_acquire_lease(pipeline_id, 2) is True
        second_lease = second.current_lease(pipeline_id)
        assert second_lease is not None
        assert second_lease.fencing_token > first_lease.fencing_token
    finally:
        for coordinator in reversed(coordinators):
            await coordinator.stop()
        for client in clients:
            keys = list(client.scan_iter(match=f"{key_prefix}*"))
            if keys:
                client.delete(*keys)
            client.close()
