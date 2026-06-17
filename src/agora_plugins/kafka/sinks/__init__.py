"""Kafka sinks exposed by the official Agora plugin package."""

from agora_plugins.kafka.sinks.kafka import KafkaSink, KafkaSinkMessage

__all__ = ["KafkaSink", "KafkaSinkMessage"]
