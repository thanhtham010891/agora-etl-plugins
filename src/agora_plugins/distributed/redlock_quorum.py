from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import logstruct
from agora.runner import LeaseState

from agora_plugins.distributed._shared import (
    lease_payload_matches_redlock,
    raise_if_programming_error,
)
from agora_plugins.distributed.redlock_nodes import RedisRedlockNodeSet

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logstruct.getLogger(__name__)


class RedisRedlockQuorum:
    """Redlock quorum collaborator with node lifecycle and quorum behavior."""

    def __init__(
        self,
        *,
        redis_urls: list[str],
        key_prefix: str,
        lease_ttl: int,
        fencing_key_ttl_seconds: int,
        clock_drift_factor: float,
        now_fn: Callable[[], str],
    ) -> None:
        self._redis_urls = list(redis_urls)
        self._key_prefix = key_prefix
        self._lease_ttl = lease_ttl
        self._fencing_key_ttl_seconds = fencing_key_ttl_seconds
        self._clock_drift_factor = clock_drift_factor
        self._now = now_fn
        self._nodes = RedisRedlockNodeSet()
        self._lease_nodes: dict[str, set[int]] = {}

    @property
    def lease_nodes(self) -> dict[str, set[int]]:
        return self._lease_nodes

    @property
    def redis_nodes(self) -> list[Any]:
        return self._nodes.redis_nodes

    @property
    def acquire_scripts(self) -> list[Any]:
        return self._nodes.acquire_scripts

    @property
    def release_scripts(self) -> list[Any]:
        return self._nodes.release_scripts

    @property
    def renew_scripts(self) -> list[Any]:
        return self._nodes.renew_scripts

    def clear_lease_nodes(self) -> None:
        self._lease_nodes.clear()

    def enabled(self) -> bool:
        return bool(self._redis_urls)

    async def start_nodes(self, aioredis: Any) -> None:
        if not self._redis_urls:
            return
        await self._nodes.start(aioredis, self._redis_urls)

    async def close_nodes(self) -> None:
        await self._nodes.close()
        self._lease_nodes.clear()

    async def try_acquire(
        self,
        *,
        primary_redis: Any | None,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
        lease_tokens: dict[str, LeaseState],
        lease_key: str,
        fencing_key: str,
    ) -> bool:
        if not self.acquire_scripts or primary_redis is None:
            return False

        acquired_at = self._now()
        fencing_token = await self._reserve_fencing_token(primary_redis, fencing_key)
        started = time.monotonic()

        async def _acquire_on_node(index: int) -> object:
            script = self.acquire_scripts[index]
            try:
                return await script(
                    keys=[lease_key, fencing_key],
                    args=[
                        worker_id,
                        acquired_at,
                        pipeline_id,
                        str(run_number),
                        str(self._lease_ttl),
                        str(fencing_token),
                    ],
                )
            except Exception as exc:
                return exc

        acquire_results = await self._run_node_calls(
            indices=list(range(len(self.acquire_scripts))),
            call=_acquire_on_node,
            error_event="coordinator_redlock_node_acquire_error",
            pipeline_id=pipeline_id,
        )
        acquired_indices = {
            index
            for index, result in acquire_results.items()
            if isinstance(result, list) and int(result[0] or 0) == 1
        }

        elapsed = time.monotonic() - started
        if len(acquired_indices) >= self.quorum() and self.validity_s(elapsed) > 0:
            lease_tokens[pipeline_id] = LeaseState(
                pipeline_id=pipeline_id,
                run_number=run_number,
                worker_id=worker_id,
                fencing_token=fencing_token,
                acquired_at=acquired_at,
            )
            self._lease_nodes[pipeline_id] = acquired_indices
            return True

        await self.release_indices(
            lease_key=lease_key,
            pipeline_id=pipeline_id,
            worker_id=worker_id,
            acquired_at=acquired_at,
            fencing_token=fencing_token,
            indices=acquired_indices,
        )
        logger.debug(
            "coordinator_redlock_lease_skipped",
            pipeline_id=pipeline_id,
            run=run_number,
            acquired_nodes=len(acquired_indices),
            quorum=self.quorum(),
            validity_s=round(self.validity_s(elapsed), 6),
        )
        return False

    async def release_indices(
        self,
        *,
        lease_key: str,
        pipeline_id: str,
        worker_id: str,
        acquired_at: str,
        fencing_token: int,
        indices: range | set[int],
    ) -> None:
        async def _release_on_node(index: int) -> object:
            try:
                return await self.release_scripts[index](
                    keys=[lease_key],
                    args=[worker_id, acquired_at, str(fencing_token)],
                )
            except Exception as exc:
                return exc

        await self._run_node_calls(
            indices=list(indices),
            call=_release_on_node,
            error_event="coordinator_redlock_node_release_error",
            pipeline_id=pipeline_id,
        )

    async def release_all_nodes(
        self,
        *,
        lease_key: str,
        pipeline_id: str,
        worker_id: str,
        acquired_at: str,
        fencing_token: int,
    ) -> None:
        await self.release_indices(
            lease_key=lease_key,
            pipeline_id=pipeline_id,
            worker_id=worker_id,
            acquired_at=acquired_at,
            fencing_token=fencing_token,
            indices=range(len(self.release_scripts)),
        )

    async def validate(
        self,
        *,
        lease_key: str,
        worker_id: str,
        pipeline_id: str,
        fencing_token: int,
        lease_tokens: dict[str, LeaseState],
    ) -> bool:
        lease = lease_tokens.get(pipeline_id)
        if lease is None or lease.fencing_token != fencing_token:
            return False

        async def _validate_on_node(index: int) -> object:
            node = self.redis_nodes[index]
            try:
                return await node.get(lease_key)
            except Exception as exc:
                return exc

        validate_results = await self._run_node_calls(
            indices=list(range(len(self.redis_nodes))),
            call=_validate_on_node,
            error_event="coordinator_redlock_node_validate_error",
            pipeline_id=pipeline_id,
        )
        matching_nodes = 0
        for raw in validate_results.values():
            if lease_payload_matches_redlock(
                raw, worker_id, lease.acquired_at, lease.fencing_token
            ):
                matching_nodes += 1
        valid = matching_nodes >= self.quorum()
        if not valid:
            lease_tokens.pop(pipeline_id, None)
            self._lease_nodes.pop(pipeline_id, None)
        return valid

    async def renew(
        self,
        *,
        lease_key: str,
        worker_id: str,
        pipeline_id: str,
        lease_tokens: dict[str, LeaseState],
        mark_lease_lost: Callable[[str, LeaseState, str], Awaitable[None]],
    ) -> bool:
        lease = lease_tokens.get(pipeline_id)
        if lease is None:
            return False
        candidate_indices = self._lease_nodes.get(
            pipeline_id,
            set(range(len(self.renew_scripts))),
        )
        started = time.monotonic()

        async def _renew_on_node(index: int) -> object:
            try:
                return await self.renew_scripts[index](
                    keys=[lease_key],
                    args=[
                        worker_id,
                        lease.acquired_at,
                        str(lease.fencing_token),
                        str(self._lease_ttl),
                    ],
                )
            except Exception as exc:
                return exc

        renew_results = await self._run_node_calls(
            indices=list(candidate_indices),
            call=_renew_on_node,
            error_event="coordinator_redlock_node_renew_error",
            pipeline_id=pipeline_id,
        )
        renewed_indices = {
            index for index, renewed in renew_results.items() if int(renewed or 0) == 1
        }

        elapsed = time.monotonic() - started
        if len(renewed_indices) >= self.quorum() and self.validity_s(elapsed) > 0:
            self._lease_nodes[pipeline_id] = renewed_indices
            lease_tokens[pipeline_id] = LeaseState(
                pipeline_id=lease.pipeline_id,
                run_number=lease.run_number,
                worker_id=lease.worker_id,
                fencing_token=lease.fencing_token,
                acquired_at=lease.acquired_at,
                renewed_at=self._now(),
            )
            return True

        await self.release_indices(
            lease_key=lease_key,
            pipeline_id=pipeline_id,
            worker_id=worker_id,
            acquired_at=lease.acquired_at,
            fencing_token=lease.fencing_token,
            indices=renewed_indices,
        )
        await mark_lease_lost(pipeline_id, lease, "redlock_renew_quorum_failed")
        self._lease_nodes.pop(pipeline_id, None)
        return False

    def quorum(self) -> int:
        return (len(self.redis_nodes) // 2) + 1

    def validity_s(self, elapsed_s: float) -> float:
        drift_s = (self._lease_ttl * self._clock_drift_factor) + 0.002
        return self._lease_ttl - elapsed_s - drift_s

    async def _reserve_fencing_token(self, primary_redis: Any, fencing_key: str) -> int:
        token = int(await primary_redis.incr(fencing_key))
        if self._fencing_key_ttl_seconds > 0:
            await primary_redis.expire(fencing_key, self._fencing_key_ttl_seconds)
        return token

    async def _run_node_calls(
        self,
        *,
        indices: list[int],
        call: Any,
        error_event: str,
        pipeline_id: str,
    ) -> dict[int, object]:
        async def _invoke(index: int) -> tuple[int, object]:
            return index, await call(index)

        results = await asyncio.gather(
            *(_invoke(index) for index in indices), return_exceptions=True
        )
        successes: dict[int, object] = {}
        for result in results:
            if isinstance(result, Exception):
                continue
            index, value = result
            if isinstance(value, Exception):
                raise_if_programming_error(value)
                logger.warning(
                    error_event,
                    pipeline_id=pipeline_id,
                    node_index=index,
                    error=str(value),
                )
                continue
            successes[index] = value
        return successes


__all__ = ["RedisRedlockQuorum"]
