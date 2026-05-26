"""
agora_plugins.kafka.sources.kafka
=================================
Async Kafka source powered by ``aiokafka``.

Requires: ``pip install 'agora-etl-plugins[kafka]'``
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
from collections.abc import Iterable
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct
from agora.core.source import BaseSource, SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

from agora_plugins.kafka._lifecycle import call_lifecycle

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


class KafkaSource(BaseSource[T], Generic[T]):
    """Async Kafka consumer source."""

    source_name = "kafka"
    supports_checkpoint = True

    def __init__(
        self,
        topics: list[str],
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "agora-consumer",
        deserializer: Callable[[bytes], T | Awaitable[T]] | None = None,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        commit_every: int = 100,
        poll_timeout_ms: int = 1000,
        max_poll_records: int = 500,
        fetch_min_bytes: int = 1,
        fetch_max_wait_ms: int = 500,
        max_partition_fetch_bytes: int = 1_048_576,
        security_protocol: str = "PLAINTEXT",
        extra_config: dict[str, Any] | None = None,
        on_deserialize_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
    ) -> None:
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._deserializer: Callable[[bytes], T | Awaitable[T]] = (
            deserializer or (lambda b: b)  # type: ignore[assignment,return-value]
        )
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._commit_every = max(commit_every, 1)
        self._poll_timeout_ms = poll_timeout_ms
        self._max_poll_records = max_poll_records
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._security_protocol = security_protocol
        self._extra_config = extra_config or {}
        self._on_deserialize_error = on_deserialize_error
        self._consumer = None
        self._pending_commit_count = 0
        self._processed_offsets: dict[tuple[str, int], int] = {}
        self._last_seen: tuple[str, int, int] | None = None
        self._resume_offsets: dict[tuple[str, int], int] = {}
        self._resume_applied = False
        self._topic_partition_cls = None
        self._record_error_count = 0
        self._record_drop_count = 0
        self._checkpoint_cache: dict[str, Any] | None = None
        self._checkpoint_dirty = False

    async def open(self) -> None:
        try:
            await self._open_deserializer()
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            await self._close_deserializer()
            raise ImportError(
                "KafkaSource requires aiokafka. Install via: pip install 'agora-etl-plugins[kafka]'"
            ) from None
        except Exception:
            await self._close_deserializer()
            raise

        try:
            self._topic_partition_cls = getattr(
                importlib.import_module("aiokafka"), "TopicPartition", None
            )
            self._consumer = AIOKafkaConsumer(
                *self._topics,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset=self._auto_offset_reset,
                enable_auto_commit=self._enable_auto_commit,
                security_protocol=self._security_protocol,
                max_poll_records=self._max_poll_records,
                fetch_min_bytes=self._fetch_min_bytes,
                fetch_max_wait_ms=self._fetch_max_wait_ms,
                max_partition_fetch_bytes=self._max_partition_fetch_bytes,
                **self._extra_config,
            )
            await self._consumer.start()
            logger.info(
                "kafka_source_ready",
                topics=self._topics,
                group_id=self._group_id,
                bootstrap=self._bootstrap_servers,
            )
        except Exception:
            consumer = self._consumer
            self._consumer = None
            if consumer is not None:
                with contextlib.suppress(Exception):
                    await consumer.stop()
            await self._close_deserializer()
            raise

    async def close(self) -> None:
        if self._consumer is not None:
            consumer = self._consumer
            try:
                await self._commit_if_needed(force=True)
            except Exception:
                logger.exception("kafka_source_close_error")
            finally:
                with contextlib.suppress(Exception):
                    await consumer.stop()
                self._consumer = None
                logger.info("kafka_source_closed", group_id=self._group_id)
        await self._close_deserializer()

    async def _commit_if_needed(self, *, force: bool = False) -> None:
        if self._consumer is None or self._enable_auto_commit:
            return
        if self._pending_commit_count <= 0:
            return
        if not force and self._pending_commit_count < self._commit_every:
            return

        offsets = {
            self._build_topic_partition(topic, partition): offset + 1
            for (topic, partition), offset in self._processed_offsets.items()
        }
        if offsets:
            await self._consumer.commit(offsets=offsets)
        else:
            await self._consumer.commit()
        self._pending_commit_count = 0

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    async def stream(self) -> AsyncGenerator[T, None]:
        if self._consumer is None:
            raise RuntimeError(
                "KafkaSource must be used as an async context manager "
                "or call open() before stream()."
            )
        self._record_error_count = 0
        self._record_drop_count = 0
        self._pending_commit_count = 0
        self._processed_offsets = {}
        self._last_seen = None

        await self._apply_resume_checkpoint()
        consumer = self._consumer

        async def _iter_messages() -> AsyncGenerator[Any, None]:
            getmany = getattr(consumer, "getmany", None)
            if getmany is None:
                async for message in consumer:
                    yield message
                return

            while True:
                try:
                    batches = await getmany(
                        timeout_ms=self._poll_timeout_ms,
                        max_records=self._max_poll_records,
                    )
                except StopAsyncIteration:
                    return
                for messages in batches.values():
                    for message in messages:
                        yield message

        try:
            async for msg in _iter_messages():
                self._remember_processed_offset(msg.topic, msg.partition, msg.offset)
                try:
                    record = await self._deserialize(msg.value)
                except Exception as exc:
                    self._record_error_count += 1
                    logger.exception(
                        "kafka_deserialize_error",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                    )
                    if self._on_deserialize_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                        self._record_drop_count += 1
                        if not self._enable_auto_commit:
                            self._pending_commit_count += 1
                            await self._commit_if_needed()
                        continue
                    raise SourceRecordError(
                        exc,
                        record=msg.value,
                        checkpoint=self.current_checkpoint(),
                        source=self.source_name,
                    ) from exc

                yield record

                if not self._enable_auto_commit:
                    self._pending_commit_count += 1
                    await self._commit_if_needed()

        except asyncio.CancelledError:
            await self._commit_if_needed(force=True)
            logger.info("kafka_source_cancelled", group_id=self._group_id)
            raise
        except Exception:
            await self._commit_if_needed(force=True)
            logger.exception("kafka_source_stream_error")
            raise
        finally:
            await self._commit_if_needed(force=True)

    async def prepare_resume(self, checkpoint) -> None:
        self._resume_applied = False
        self._resume_offsets = {}
        if checkpoint is None or not isinstance(checkpoint.value, dict):
            return

        self._resume_offsets = self._normalize_checkpoint_offsets(checkpoint.value)

    async def _apply_resume_checkpoint(self) -> None:
        if self._consumer is None or self._resume_applied or not self._resume_offsets:
            return

        assignment = getattr(self._consumer, "assignment", None)
        if callable(assignment):
            assigned = assignment()
            if not self._assignment_contains_all(assigned, self._resume_offsets):
                getmany = getattr(self._consumer, "getmany", None)
                if callable(getmany):
                    with contextlib.suppress(Exception):
                        await getmany(timeout_ms=0, max_records=1)
                assigned = assignment()
        seek = getattr(self._consumer, "seek", None)
        if not callable(seek):
            logger.warning("kafka_resume_seek_unsupported", group_id=self._group_id)
            self._resume_applied = True
            return

        resume_count = 0
        for (topic, partition), offset in self._resume_offsets.items():
            if callable(assignment):
                assigned = assignment()
                if assigned and not self._assignment_contains(assigned, topic, partition):
                    logger.warning(
                        "kafka_resume_partition_unassigned",
                        topic=topic,
                        partition=partition,
                        group_id=self._group_id,
                    )
                    continue

            seek(self._build_topic_partition(topic, partition), offset + 1)
            resume_count += 1
        self._resume_applied = True
        logger.info(
            "kafka_resume_applied",
            partitions=resume_count,
            group_id=self._group_id,
        )

    @staticmethod
    def _assignment_contains(
        assignments: object,
        topic: str,
        partition: int,
    ) -> bool:
        if not assignments:
            return False
        for item in assignments:
            item_topic = getattr(item, "topic", None)
            item_partition = getattr(item, "partition", None)
            if item_topic is None and isinstance(item, tuple) and len(item) >= 2:
                item_topic = item[0]
                item_partition = item[1]
            if item_topic == topic and int(item_partition) == partition:
                return True
        return False

    @classmethod
    def _assignment_contains_all(
        cls,
        assignments: object,
        offsets: dict[tuple[str, int], int],
    ) -> bool:
        if not offsets:
            return True
        return all(
            cls._assignment_contains(assignments, topic, partition) for topic, partition in offsets
        )

    def _remember_processed_offset(self, topic: str, partition: int, offset: int) -> None:
        self._processed_offsets[(str(topic), int(partition))] = int(offset)
        self._last_seen = (str(topic), int(partition), int(offset))
        self._checkpoint_dirty = True

    def current_checkpoint(self) -> dict[str, Any] | None:
        if not self._processed_offsets or self._last_seen is None:
            return None
        if not self._checkpoint_dirty and self._checkpoint_cache is not None:
            return self._checkpoint_cache
        last_topic, last_partition, last_offset = self._last_seen
        self._checkpoint_cache = {
            "topic": last_topic,
            "partition": last_partition,
            "offset": last_offset,
            "offsets": [
                {"topic": t, "partition": p, "offset": o}
                for (t, p), o in sorted(self._processed_offsets.items())
            ],
        }
        self._checkpoint_dirty = False
        return self._checkpoint_cache

    def _build_topic_partition(self, topic: str, partition: int) -> object:
        if self._topic_partition_cls is not None:
            return self._topic_partition_cls(topic, partition)
        return (topic, partition)

    async def _deserialize(self, value: bytes) -> T:
        record = self._deserializer(value)
        if isawaitable(record):
            return await record
        return record

    async def _open_deserializer(self) -> None:
        await call_lifecycle(self._deserializer, "open")

    async def _close_deserializer(self) -> None:
        await call_lifecycle(self._deserializer, "close")

    @staticmethod
    def _normalize_checkpoint_offsets(value: dict[str, Any]) -> dict[tuple[str, int], int]:
        offsets: dict[tuple[str, int], int] = {}
        raw_offsets = value.get("offsets")
        if isinstance(raw_offsets, Iterable) and not isinstance(raw_offsets, (str, bytes, dict)):
            for item in raw_offsets:
                if not isinstance(item, dict):
                    continue
                if {"topic", "partition", "offset"} - set(item):
                    continue
                offsets[(str(item["topic"]), int(item["partition"]))] = int(item["offset"])

        if offsets:
            return offsets

        if {"topic", "partition", "offset"} - set(value):
            return {}
        return {
            (str(value["topic"]), int(value["partition"])): int(value["offset"]),
        }


__all__ = ["KafkaSource"]
