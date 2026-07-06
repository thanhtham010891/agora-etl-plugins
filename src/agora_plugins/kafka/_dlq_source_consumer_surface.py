"""Consumer bootstrap and lifecycle helpers for Kafka DLQ sources."""

from __future__ import annotations

import contextlib
from typing import Any


class KafkaDLQSourceConsumerSurface:
    """Owns consumer creation, subscription wiring, and lifecycle shutdown."""

    def __init__(self, source: Any) -> None:
        self._source = source

    async def open(self) -> None:
        try:
            import aiokafka
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            raise ImportError(
                "KafkaDLQSource requires aiokafka. Install via: pip install 'agora-etl-plugins[kafka]'"
            ) from None

        self._source._topic_partition_cls = getattr(aiokafka, "TopicPartition", None)
        consumer_args: list[str] = []
        self._source._consumer = AIOKafkaConsumer(
            *consumer_args,
            bootstrap_servers=self._source._bootstrap_servers,
            group_id=self._source._group_id,
            auto_offset_reset=self._source._auto_offset_reset,
            enable_auto_commit=self._source._enable_auto_commit,
            **self.security_kwargs(),
            **self._source._extra_config,
        )
        consumer = self.require_consumer()
        self._apply_subscription(consumer)
        try:
            await consumer.start()
        except Exception:
            with contextlib.suppress(Exception):
                await consumer.stop()
            self._source._consumer = None
            raise

    async def close(self) -> None:
        if self._source._consumer is not None:
            consumer = self._source._consumer
            self._source._consumer = None
            await consumer.stop()

    def require_consumer(self) -> Any:
        if self._source._consumer is None:
            raise RuntimeError("KafkaDLQSource.open() was not called")
        return self._source._consumer

    def build_topic_partition(self, topic: str, partition: int) -> object:
        if self._source._topic_partition_cls is not None:
            return self._source._topic_partition_cls(topic, partition)
        return (topic, partition)

    def security_kwargs(self) -> dict[str, Any]:
        if self._source._security is None:
            return {"security_protocol": self._source._security_protocol}
        return self._source._security.to_aiokafka_client_kwargs()

    def _apply_subscription(self, consumer: Any) -> None:
        if self._source._assignments:
            consumer.assign(
                [
                    self.build_topic_partition(topic, partition)
                    for topic, partition in self._source._assignments
                ]
            )
            return
        if self._source._topic_pattern is not None:
            consumer.subscribe(pattern=self._source._topic_pattern)
            return
        consumer.subscribe(topics=self._source._topics)
