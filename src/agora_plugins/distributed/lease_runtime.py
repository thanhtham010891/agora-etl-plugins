from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.runner import LeaseState

    from agora_plugins.distributed.primary_lease_store import RedisPrimaryLeaseStore
    from agora_plugins.distributed.redlock_quorum import RedisRedlockQuorum


class RedisLeaseRuntime:
    """Public-facing runtime state collaborator for active distributed leases."""

    def __init__(
        self,
        *,
        primary: RedisPrimaryLeaseStore,
        redlock: RedisRedlockQuorum,
    ) -> None:
        self._primary = primary
        self._redlock = redlock

    @property
    def lease_tokens(self) -> dict[str, LeaseState]:
        return self._primary.lease_tokens

    @property
    def local_fencing_tokens(self) -> dict[str, int]:
        return self._primary.local_fencing_tokens

    @property
    def redlock_lease_nodes(self) -> dict[str, set[int]]:
        return self._redlock.lease_nodes

    def current_lease(self, pipeline_id: str) -> LeaseState | None:
        return self._primary.current_lease(pipeline_id)

    def active_pipeline_ids(self) -> list[str]:
        return list(self._primary.lease_tokens)

    def active_leases(self) -> list[tuple[str, LeaseState]]:
        return list(self._primary.lease_tokens.items())

    def lease_count(self) -> int:
        return len(self._primary.lease_tokens)

    def clear(self) -> None:
        self._primary.clear_active_leases()
        self._redlock.clear_lease_nodes()

    def discard_lease(self, pipeline_id: str) -> LeaseState | None:
        lease = self._primary.pop_lease(pipeline_id)
        self._redlock.lease_nodes.pop(pipeline_id, None)
        return lease

    def discard_redlock_nodes(self, pipeline_id: str) -> None:
        self._redlock.lease_nodes.pop(pipeline_id, None)


__all__ = ["RedisLeaseRuntime"]
