"""Consumer session lifecycle orchestration for Kafka sources."""

from __future__ import annotations

import contextlib
import importlib
from typing import TYPE_CHECKING, Any, cast

import logstruct
from agora.core.retry import RetryPolicy, retry_async

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora_plugins.kafka.sources._consumer_runtime import KafkaConsumerRuntime
    from agora_plugins.kafka.sources._deserializer_runtime import KafkaDeserializerRuntime
    from agora_plugins.kafka.sources._poison_controller import KafkaPoisonController

logger = logstruct.getLogger("agora_plugins.kafka.sources.kafka")


class KafkaConsumerSession:
    """Owns consumer open/close orchestration for ``KafkaSource``."""

    def __init__(
        self,
        *,
        topics: list[str],
        topic_pattern: str | None,
        assignments: list[tuple[str, int]],
        bootstrap_servers: str,
        group_id: str,
        auto_offset_reset: str,
        enable_auto_commit: bool,
        max_poll_records: int,
        fetch_min_bytes: int,
        fetch_max_wait_ms: int,
        max_partition_fetch_bytes: int,
        extra_config: dict[str, Any],
        rebalance_owner: Any,
        consumer_runtime: KafkaConsumerRuntime,
        poison_controller: KafkaPoisonController,
        deserializer_runtime: KafkaDeserializerRuntime[Any],
        security_kwargs: Callable[[], dict[str, Any]],
        current_consumer: Callable[[], Any | None],
        set_consumer: Callable[[Any | None], None],
        set_topic_partition_cls: Callable[[Any | None], None],
        commit_if_needed: Callable[..., Awaitable[None]],
        on_consumer_closed: Callable[[], None],
        retry_policy: RetryPolicy[Any],
    ) -> None:
        self._topics = list(topics)
        self._topic_pattern = topic_pattern
        self._assignments = list(assignments)
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._max_poll_records = max_poll_records
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._extra_config = dict(extra_config)
        self._rebalance_owner = rebalance_owner
        self._consumer_runtime = consumer_runtime
        self._poison_controller = poison_controller
        self._deserializer_runtime = deserializer_runtime
        self._security_kwargs = security_kwargs
        self._current_consumer = current_consumer
        self._set_consumer = set_consumer
        self._set_topic_partition_cls = set_topic_partition_cls
        self._commit_if_needed = commit_if_needed
        self._on_consumer_closed = on_consumer_closed
        self._retry_policy = retry_policy

    async def open(self) -> None:
        try:
            await self._poison_controller.open()
            await self._deserializer_runtime.open()
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            await self._poison_controller.close()
            await self._deserializer_runtime.close()
            raise ImportError(
                "KafkaSource requires aiokafka. Install via: pip install 'agora-etl-plugins[kafka]'"
            ) from None
        except Exception:
            await self._poison_controller.close()
            await self._deserializer_runtime.close()
            raise

        try:
            self._set_topic_partition_cls(
                getattr(importlib.import_module("aiokafka"), "TopicPartition", None)
            )
            consumer_args = (
                self._topics if self._topic_pattern is None and not self._assignments else []
            )
            consumer = AIOKafkaConsumer(
                *consumer_args,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset=self._auto_offset_reset,
                enable_auto_commit=self._enable_auto_commit,
                max_poll_records=self._max_poll_records,
                fetch_min_bytes=self._fetch_min_bytes,
                fetch_max_wait_ms=self._fetch_max_wait_ms,
                max_partition_fetch_bytes=self._max_partition_fetch_bytes,
                **self._security_kwargs(),
                **self._extra_config,
            )
            self._set_consumer(consumer)
            consumer = cast("Any", consumer)
            self._consumer_runtime.subscribe_consumer(self._rebalance_owner, consumer)
            await retry_async(consumer.start, policy=self._retry_policy)
            self._consumer_runtime.bind_consumer(consumer)
            logger.info(
                "kafka_source_ready",
                topics=self._topics,
                topic_pattern=self._topic_pattern,
                assignments=self._assignments,
                group_id=self._group_id,
                bootstrap=self._bootstrap_servers,
            )
        except Exception:
            consumer = self._current_consumer()
            self._set_consumer(None)
            if consumer is not None:
                with contextlib.suppress(Exception):
                    await consumer.stop()
            await self._poison_controller.close()
            await self._deserializer_runtime.close()
            raise

    async def close(self) -> None:
        consumer = self._current_consumer()
        if consumer is not None:
            try:
                await self._commit_if_needed(force=True)
            except Exception:
                logger.exception("kafka_source_close_error")
            finally:
                with contextlib.suppress(Exception):
                    await consumer.stop()
                self._set_consumer(None)
                self._on_consumer_closed()
                logger.info("kafka_source_closed", group_id=self._group_id)
        await self._poison_controller.close()
        await self._deserializer_runtime.close()
