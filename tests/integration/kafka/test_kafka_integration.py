from __future__ import annotations

import asyncio
import json

import pytest
from agora import Pipeline
from agora.core.source import IterableSource

from agora_plugins.kafka import KafkaSink, KafkaSource

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 30.0


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    async def open(self) -> None:
        return None

    async def write(self, record: dict[str, object]) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


async def _ensure_topic_exists(bootstrap_servers: str, topic: str) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        try:
            await admin.create_topics(
                [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
            )
        except TopicAlreadyExistsError:
            return
    finally:
        await admin.close()


@pytest.mark.asyncio
async def test_kafka_source_and_sink_round_trip_against_real_broker(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    pytest.importorskip("aiokafka")

    topic = f"agora-it-{unique_suffix}"
    await asyncio.wait_for(_ensure_topic_exists(kafka_bootstrap, topic), timeout=10.0)
    producer_summary = await asyncio.wait_for(
        (
            Pipeline(
                IterableSource(
                    [
                        {"id": 1, "name": "alpha"},
                        {"id": 2, "name": "bravo"},
                        {"id": 3, "name": "charlie"},
                    ]
                )
            )
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=kafka_bootstrap,
                    serializer=lambda record: json.dumps(record).encode("utf-8"),
                )
            )
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    await asyncio.sleep(1.0)

    collected = _CollectSink()
    try:
        consumer_summary = await asyncio.wait_for(
            (
                Pipeline(
                    KafkaSource(
                        topics=[topic],
                        bootstrap_servers=kafka_bootstrap,
                        group_id=f"agora-it-{unique_suffix}",
                        deserializer=lambda value: json.loads(value.decode("utf-8")),
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        commit_every=2,
                    )
                )
                .build(collected)  # type: ignore[arg-type]
                .run(max_records=3)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    except TimeoutError:
        pytest.fail(
            "Kafka integration test timed out while waiting for produced records. "
            "Check `docker compose ps`, `docker compose logs kafka`, and rerun "
            "with `pytest -vv -s -k kafka -m integration`."
        )

    assert producer_summary.records_written == 3
    assert consumer_summary.records_consumed == 3
    assert collected.records == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "bravo"},
        {"id": 3, "name": "charlie"},
    ]
