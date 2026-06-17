"""Compatibility checks for the decomposed Kafka source modules."""

from agora_plugins.kafka.sources import (
    KafkaDeliveryContext,
    KafkaPartitionHealth,
    KafkaPoisonRecordInfo,
    KafkaPoisonRecordPolicy,
    KafkaSourceHealthSnapshot,
    KafkaSourceOperationalMetrics,
)
from agora_plugins.kafka.sources._models import (
    KafkaDeliveryContext as InternalKafkaDeliveryContext,
)
from agora_plugins.kafka.sources._models import (
    KafkaPartitionHealth as InternalKafkaPartitionHealth,
)
from agora_plugins.kafka.sources._models import (
    KafkaPoisonRecordInfo as InternalKafkaPoisonRecordInfo,
)
from agora_plugins.kafka.sources._models import (
    KafkaPoisonRecordPolicy as InternalKafkaPoisonRecordPolicy,
)
from agora_plugins.kafka.sources._models import (
    KafkaSourceHealthSnapshot as InternalKafkaSourceHealthSnapshot,
)
from agora_plugins.kafka.sources._models import (
    KafkaSourceOperationalMetrics as InternalKafkaSourceOperationalMetrics,
)


def test_kafka_source_models_keep_their_existing_public_import_identity() -> None:
    assert KafkaDeliveryContext is InternalKafkaDeliveryContext
    assert KafkaPartitionHealth is InternalKafkaPartitionHealth
    assert KafkaPoisonRecordInfo is InternalKafkaPoisonRecordInfo
    assert KafkaPoisonRecordPolicy is InternalKafkaPoisonRecordPolicy
    assert KafkaSourceHealthSnapshot is InternalKafkaSourceHealthSnapshot
    assert KafkaSourceOperationalMetrics is InternalKafkaSourceOperationalMetrics
