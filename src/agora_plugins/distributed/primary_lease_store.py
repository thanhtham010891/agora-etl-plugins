from __future__ import annotations

from typing import TYPE_CHECKING, Any

import logstruct
from agora.runner import LeaseState

from agora_plugins.distributed._shared import lease_payload_matches, raise_if_programming_error

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logstruct.getLogger(__name__)

_RELEASE_SCRIPT = """
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
local ok, data = pcall(cjson.decode, val)
if not ok then return 0 end
if data["worker_id"] == ARGV[1] and tostring(data["fencing_token"]) == ARGV[2] then
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
"""

_RENEW_SCRIPT = """
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
local ok, data = pcall(cjson.decode, val)
if not ok then return 0 end
if data["worker_id"] == ARGV[1] and tostring(data["fencing_token"]) == ARGV[2] then
    redis.call("EXPIRE", KEYS[1], tonumber(ARGV[3]))
    return 1
end
return 0
"""

_ACQUIRE_SCRIPT = """
local lease = redis.call("GET", KEYS[1])
if lease ~= false then return {0, false} end
local fencing_token = redis.call("INCR", KEYS[2])
if tonumber(ARGV[6]) > 0 then
    redis.call("EXPIRE", KEYS[2], tonumber(ARGV[6]))
end
local value = cjson.encode({
    worker_id = ARGV[1],
    acquired_at = ARGV[2],
    pipeline_id = ARGV[3],
    run_number = tonumber(ARGV[4]),
    fencing_token = fencing_token
})
local ok = redis.call("SET", KEYS[1], value, "NX", "EX", tonumber(ARGV[5]))
if ok then return {1, fencing_token} end
return {0, false}
"""


class RedisPrimaryLeaseStore:
    """Single-Redis lease ownership and local fallback collaborator."""

    def __init__(
        self,
        *,
        key_prefix: str,
        lease_ttl: int,
        fencing_key_ttl_seconds: int,
        now_fn: Callable[[], str],
    ) -> None:
        self._key_prefix = key_prefix
        self._lease_ttl = lease_ttl
        self._fencing_key_ttl_seconds = fencing_key_ttl_seconds
        self._now = now_fn
        self._lease_tokens: dict[str, LeaseState] = {}
        self._local_fencing_tokens: dict[str, int] = {}
        self._acquire_script: Any | None = None
        self._release_script: Any | None = None
        self._renew_script: Any | None = None

    @property
    def lease_tokens(self) -> dict[str, LeaseState]:
        return self._lease_tokens

    @property
    def local_fencing_tokens(self) -> dict[str, int]:
        return self._local_fencing_tokens

    @property
    def acquire_script(self) -> Any | None:
        return self._acquire_script

    @acquire_script.setter
    def acquire_script(self, script: Any | None) -> None:
        self._acquire_script = script

    @property
    def release_script(self) -> Any | None:
        return self._release_script

    @release_script.setter
    def release_script(self, script: Any | None) -> None:
        self._release_script = script

    @property
    def renew_script(self) -> Any | None:
        return self._renew_script

    @renew_script.setter
    def renew_script(self, script: Any | None) -> None:
        self._renew_script = script

    def lease_key(self, pipeline_id: str) -> str:
        return f"{self._key_prefix}lease:{pipeline_id}"

    def fencing_key(self, pipeline_id: str) -> str:
        return f"{self._key_prefix}fence:{pipeline_id}"

    def current_lease(self, pipeline_id: str) -> LeaseState | None:
        return self._lease_tokens.get(pipeline_id)

    def clear_active_leases(self) -> None:
        self._lease_tokens.clear()

    def pop_lease(self, pipeline_id: str) -> LeaseState | None:
        return self._lease_tokens.pop(pipeline_id, None)

    async def register_scripts(self, redis: Any) -> None:
        self._release_script = redis.register_script(_RELEASE_SCRIPT)
        self._renew_script = redis.register_script(_RENEW_SCRIPT)
        self._acquire_script = redis.register_script(_ACQUIRE_SCRIPT)

    async def try_acquire(
        self,
        *,
        redis: Any,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
    ) -> bool:
        try:
            if self._acquire_script is None:
                return False
            acquired_at = self._now()
            result = await self._acquire_script(
                keys=[self.lease_key(pipeline_id), self.fencing_key(pipeline_id)],
                args=[
                    worker_id,
                    acquired_at,
                    pipeline_id,
                    str(run_number),
                    str(self._lease_ttl),
                    str(self._fencing_key_ttl_seconds),
                ],
            )
            acquired = isinstance(result, list) and int(result[0] or 0) == 1
            if not acquired:
                logger.debug(
                    "coordinator_lease_skipped",
                    pipeline_id=pipeline_id,
                    run=run_number,
                )
                return False
            fencing_token = int(result[1])
            self._lease_tokens[pipeline_id] = LeaseState(
                pipeline_id=pipeline_id,
                run_number=run_number,
                worker_id=worker_id,
                fencing_token=fencing_token,
                acquired_at=acquired_at,
            )
            return True
        except Exception as exc:
            raise_if_programming_error(exc)
            logger.warning(
                "coordinator_lease_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )
            return False

    async def release(
        self,
        *,
        worker_id: str,
        pipeline_id: str,
        lease: LeaseState,
    ) -> None:
        if self._release_script is None:
            return
        try:
            await self._release_script(
                keys=[self.lease_key(pipeline_id)],
                args=[worker_id, str(lease.fencing_token)],
            )
        except Exception as exc:
            raise_if_programming_error(exc)
            logger.warning(
                "coordinator_release_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )

    async def validate(
        self,
        *,
        redis: Any,
        worker_id: str,
        pipeline_id: str,
        fencing_token: int,
    ) -> bool:
        try:
            raw = await redis.get(self.lease_key(pipeline_id))
        except Exception as exc:
            raise_if_programming_error(exc)
            logger.warning(
                "coordinator_lease_validate_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )
            return False
        if raw is None:
            self._lease_tokens.pop(pipeline_id, None)
            return False
        valid = lease_payload_matches(raw, worker_id, fencing_token)
        if not valid:
            self._lease_tokens.pop(pipeline_id, None)
        return valid

    async def renew(
        self,
        *,
        worker_id: str,
        pipeline_id: str,
        mark_lease_lost: Callable[[str, LeaseState, str], Awaitable[None]],
    ) -> bool:
        if self._renew_script is None:
            return False
        lease = self._lease_tokens.get(pipeline_id)
        if lease is None:
            return False
        try:
            renewed = await self._renew_script(
                keys=[self.lease_key(pipeline_id)],
                args=[worker_id, str(lease.fencing_token), str(self._lease_ttl)],
            )
        except Exception as exc:
            raise_if_programming_error(exc)
            logger.warning(
                "coordinator_lease_renew_error",
                pipeline_id=pipeline_id,
                error=str(exc),
            )
            return False
        if int(renewed or 0) != 1:
            await mark_lease_lost(pipeline_id, lease, "renew_failed")
            return False
        self._lease_tokens[pipeline_id] = LeaseState(
            pipeline_id=lease.pipeline_id,
            run_number=lease.run_number,
            worker_id=lease.worker_id,
            fencing_token=lease.fencing_token,
            acquired_at=lease.acquired_at,
            renewed_at=self._now(),
        )
        return True

    def acquire_local_fallback_lease(
        self,
        *,
        worker_id: str,
        pipeline_id: str,
        run_number: int,
        fallback_to_local: bool,
    ) -> bool:
        if not fallback_to_local or pipeline_id in self._lease_tokens:
            return False
        fencing_token = self._local_fencing_tokens.get(pipeline_id, 0) + 1
        self._local_fencing_tokens[pipeline_id] = fencing_token
        self._lease_tokens[pipeline_id] = LeaseState(
            pipeline_id=pipeline_id,
            run_number=run_number,
            worker_id=worker_id,
            fencing_token=fencing_token,
            acquired_at=self._now(),
        )
        return True

    def renew_local_fallback_lease(self, pipeline_id: str, *, fallback_to_local: bool) -> bool:
        if not fallback_to_local:
            return False
        lease = self._lease_tokens.get(pipeline_id)
        if lease is None:
            return False
        self._lease_tokens[pipeline_id] = LeaseState(
            pipeline_id=lease.pipeline_id,
            run_number=lease.run_number,
            worker_id=lease.worker_id,
            fencing_token=lease.fencing_token,
            acquired_at=lease.acquired_at,
            renewed_at=self._now(),
        )
        return True


__all__ = ["RedisPrimaryLeaseStore"]
