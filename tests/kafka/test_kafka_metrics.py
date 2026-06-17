from __future__ import annotations

from agora.core.health import ComponentHealthSnapshot
from agora.core.source import SourceRuntimeMetrics

from agora_plugins.kafka.metrics import KafkaSourceMetricsSnapshot, KafkaSourcePrometheusExporter
from agora_plugins.kafka.sources.kafka import (
    KafkaPartitionHealth,
    KafkaSourceHealthSnapshot,
    KafkaSourceOperationalMetrics,
)


def test_kafka_prometheus_exporter_renders_health_operational_and_partition_metrics() -> None:
    snapshot = KafkaSourceMetricsSnapshot(
        health=KafkaSourceHealthSnapshot(
            ready=True,
            stalled=False,
            consumer_group="orders-consumer",
            bootstrap_servers="kafka-1:9092",
            subscription_mode="manual_assign",
            assignment_count=2,
            paused_partition_count=1,
            pending_commit_count=3,
            rebalance_count=4,
            idle_poll_count=5,
            record_error_count=6,
            record_drop_count=7,
            last_poll_age_ms=11.5,
            last_message_age_ms=12.5,
            last_commit_age_ms=13.5,
            last_rebalance_age_ms=14.5,
            total_lag=21,
            lagging_partition_count=2,
            max_lag=16,
            total_commit_lag=24,
            max_commit_lag=17,
            partitions=(
                KafkaPartitionHealth(
                    topic="orders",
                    partition=0,
                    current_offset=100,
                    committed_offset=99,
                    processed_offset=100,
                    committable_offset=99,
                    end_offset=105,
                    lag=5,
                    commit_lag=6,
                    delivery_gap=1,
                    commit_gap=1,
                    paused=False,
                ),
                KafkaPartitionHealth(
                    topic="orders",
                    partition=1,
                    current_offset=200,
                    committed_offset=199,
                    processed_offset=200,
                    committable_offset=199,
                    end_offset=216,
                    lag=16,
                    commit_lag=17,
                    delivery_gap=1,
                    commit_gap=1,
                    paused=True,
                ),
            ),
        ),
        operational=KafkaSourceOperationalMetrics(
            rebalance_count=4,
            batch_deserialize_error_count=8,
            manual_assign_partition_count=2,
            paused_partition_count=1,
            poison_record_dlq_write_count=9,
            poison_record_dlq_write_failure_count=3,
            poison_record_log_only_count=10,
            poison_record_fail_closed_count=11,
            poison_record_deserialization_count=12,
            poison_record_schema_evolution_count=13,
            poison_record_schema_validation_count=14,
            poison_record_schema_registry_binding_mismatch_count=15,
            poison_record_unknown_count=16,
        ),
        runtime=SourceRuntimeMetrics(record_error_count=6, record_drop_count=7),
    )

    rendered = KafkaSourcePrometheusExporter(namespace="agora_kafka").render(snapshot)

    assert isinstance(snapshot.health, ComponentHealthSnapshot)
    assert "# HELP agora_kafka_source_state Kafka source readiness and lifecycle state" in rendered
    assert (
        'agora_kafka_source_state{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",state="ready"} 1'
    ) in rendered
    assert (
        'agora_kafka_source_gauge{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",gauge="total_lag"} 21'
    ) in rendered
    assert (
        'agora_kafka_source_gauge{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",gauge="total_commit_lag"} 24'
    ) in rendered
    assert 'gauge="poison_record_dlq_write_count"' not in rendered
    assert 'gauge="record_error_count"' not in rendered
    assert 'gauge="record_drop_count"' not in rendered
    assert (
        'agora_kafka_source_events_total{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",event="batch_deserialize_error"} 8'
    ) in rendered
    assert (
        'agora_kafka_source_events_total{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",event="poison_dlq_write"} 9'
    ) in rendered
    assert (
        'agora_kafka_source_events_total{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",event="poison_dlq_write_failure"} 3'
    ) in rendered
    assert (
        'agora_kafka_source_events_total{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",event="poison_classification_schema_registry_binding_mismatch"} 15'
    ) in rendered
    assert (
        'agora_kafka_source_age_ms{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",activity="commit"} 13.500000'
    ) in rendered
    assert (
        'agora_kafka_partition_gauge{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",topic="orders",partition="1",gauge="paused"} 1'
    ) in rendered
    assert (
        'agora_kafka_partition_gauge{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",topic="orders",partition="0",gauge="lag"} 5'
    ) in rendered
    assert (
        'agora_kafka_partition_gauge{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",topic="orders",partition="1",gauge="commit_lag"} 17'
    ) in rendered
    assert (
        'agora_kafka_partition_gauge{consumer_group="orders-consumer",bootstrap_servers="kafka-1:9092",'
        'subscription_mode="manual_assign",topic="orders",partition="0",gauge="committed_offset"} 99'
    ) in rendered
