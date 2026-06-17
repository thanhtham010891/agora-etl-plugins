"""Consumer assignment normalization and rebalance-listener wiring."""

from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Iterable


class KafkaRebalanceOwner(Protocol):
    async def _handle_partitions_revoked(self, partitions: object) -> None: ...

    async def _handle_partitions_assigned(self, partitions: object) -> None: ...


def build_rebalance_listener(
    source: KafkaRebalanceOwner,
    delegate: object | None,
) -> object:
    try:
        from aiokafka.abc import ConsumerRebalanceListener
    except ImportError:

        class ConsumerRebalanceListener:  # type: ignore[no-redef]
            pass

    class _KafkaRebalanceListener(ConsumerRebalanceListener):  # type: ignore[misc]
        async def on_partitions_revoked(self, revoked: object) -> None:
            await source._handle_partitions_revoked(revoked)
            await call_rebalance_hook(delegate, "on_partitions_revoked", revoked)

        async def on_partitions_assigned(self, assigned: object) -> None:
            await source._handle_partitions_assigned(assigned)
            await call_rebalance_hook(delegate, "on_partitions_assigned", assigned)

    return _KafkaRebalanceListener()


async def call_rebalance_hook(
    delegate: object | None,
    method_name: str,
    partitions: object,
) -> None:
    if delegate is None:
        return
    method = getattr(delegate, method_name, None)
    if not callable(method):
        return
    result = method(partitions)
    if isawaitable(result):
        await result


def normalize_assignment_items(items: object) -> list[tuple[str, int]]:
    normalized: list[tuple[str, int]] = []
    for item in cast("Iterable[Any]", items or ()):
        topic = getattr(item, "topic", None)
        partition = getattr(item, "partition", None)
        if topic is None and isinstance(item, tuple) and len(item) >= 2:
            topic = item[0]
            partition = item[1]
        if topic is None or partition is None:
            continue
        normalized.append((str(topic), int(partition)))
    return normalized


def normalize_topic_partitions(
    items: Iterable[tuple[str, int]],
) -> list[tuple[str, int]]:
    return normalize_assignment_items(list(items))
