from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from agora.core.dlq import DLQRecord

from agora_plugins.kafka.dlq import (
    DLQPayloadPolicy,
    KafkaDLQPrometheusExporter,
    KafkaDLQSink,
    KafkaDLQSource,
    _decode_dlq_envelope,
    _encode_dlq_envelope,
    _payload_to_record,
    _record_to_payload,
)


def _make_record(**overrides) -> DLQRecord:
    defaults = {
        "pipeline_id": "orders",
        "run_id": "run-1",
        "stage": "middleware",
        "error_type": "ValueError",
        "error_message": "bad payload",
        "record": {"id": 1},
        "source": "kafka",
        "checkpoint": {"offset": 9},
        "middleware": "normalize",
        "sink": "postgres",
        "created_at": datetime(2026, 6, 18, 12, 0, tzinfo=UTC),
        "attempt": 1,
        "max_attempts": 5,
        "original_record": {"id": 1, "raw": True},
        "processed_record": {"id": 1, "ok": False},
    }
    defaults.update(overrides)
    return DLQRecord(**defaults)


class _FakeEnvelopeSink:
    def __init__(self) -> None:
        self.writes: list[dict[str, object]] = []
        self.batch_writes: list[list[dict[str, object]]] = []

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def write(self, envelope: dict[str, object]) -> None:
        self.writes.append(envelope)

    async def write_batch(self, envelopes: list[dict[str, object]]) -> None:
        self.batch_writes.append(envelopes)


class _FakeMessage:
    def __init__(self, value: bytes) -> None:
        self.value = value


class _FakeConsumer:
    def __init__(
        self,
        batches: list[dict[object, list[_FakeMessage]]],
        *,
        highwater_offsets: dict[object, int] | None = None,
        assignment_after_polls: int = 0,
    ) -> None:
        self._batches = list(batches)
        self.seek_calls: list[tuple[object, int]] = []
        self.highwater_offsets = highwater_offsets or {}
        self.getmany_calls = 0
        self.assignment_after_polls = assignment_after_polls

    def assignment(self) -> set[object]:
        if self.getmany_calls < self.assignment_after_polls:
            return set()
        return {("dlq", 0)}

    async def getmany(self, timeout_ms: int = 0):
        del timeout_ms
        self.getmany_calls += 1
        if self._batches:
            return self._batches.pop(0)
        return {}

    def seek(self, partition: object, offset: int) -> None:
        self.seek_calls.append((partition, offset))

    async def end_offsets(self, partitions: list[object]) -> dict[object, int]:
        return {
            partition: self.highwater_offsets[partition]
            for partition in partitions
            if partition in self.highwater_offsets
        }

    async def position(self, partition: object) -> int:
        return self.highwater_offsets.get(partition, 0)


def test_kafka_dlq_source_rejects_invalid_limit() -> None:
    with pytest.raises(ValueError, match="limit"):
        KafkaDLQSource(
            assignments=[("dlq", 0)],
            bootstrap_servers="localhost:9092",
            limit=0,
        )


class _ReverseCipher:
    def encrypt(self, plaintext: bytes) -> bytes:
        return plaintext[::-1]

    def decrypt(self, ciphertext: bytes) -> bytes:
        return ciphertext[::-1]


class _FailingCipher:
    def encrypt(self, plaintext: bytes) -> bytes:
        del plaintext
        raise RuntimeError("encrypt failed")


def test_kafka_dlq_payload_round_trip_preserves_record_fields() -> None:
    record = _make_record()

    payload = _record_to_payload(record)
    restored = _payload_to_record(payload)

    assert restored == record


def test_kafka_dlq_payload_policy_redacts_sensitive_fields_and_headers() -> None:
    record = _make_record(
        record={
            "id": 1,
            "password": "plain-secret",
            "headers": [
                {"key": "authorization", "value": {"encoding": "utf-8", "data": "Bearer abc"}},
                {"key": "tenant", "value": {"encoding": "utf-8", "data": "acme"}},
                {"key": "x-private", "value": {"encoding": "utf-8", "data": "private"}},
            ],
        },
        original_record={"token": "raw-token", "public": "kept"},
        processed_record={"profile": {"ssn": "111-22-3333", "name": "Ada"}},
        details={"client_secret": "client-secret", "safe": True},
    )
    policy = DLQPayloadPolicy.redacted(
        redact_fields=("ssn",),
        redact_headers=("x-private",),
    )

    payload = _record_to_payload(record, payload_policy=policy)
    rendered = json.dumps(payload, sort_keys=True)

    assert payload["record"]["password"] == "[REDACTED]"
    assert payload["record"]["headers"][0]["value"] == {
        "encoding": "redacted",
        "data": "[REDACTED]",
    }
    assert payload["record"]["headers"][1]["value"] == {"encoding": "utf-8", "data": "acme"}
    assert payload["record"]["headers"][2]["value"] == {
        "encoding": "redacted",
        "data": "[REDACTED]",
    }
    assert payload["original_record"]["token"] == "[REDACTED]"
    assert payload["original_record"]["public"] == "kept"
    assert payload["processed_record"]["profile"]["ssn"] == "[REDACTED]"
    assert payload["processed_record"]["profile"]["name"] == "Ada"
    assert payload["details"]["client_secret"] == "[REDACTED]"
    assert "plain-secret" not in rendered
    assert "Bearer abc" not in rendered
    assert "raw-token" not in rendered
    assert "111-22-3333" not in rendered


def test_kafka_dlq_payload_policy_encrypts_envelope_and_requires_decryptor() -> None:
    record = _make_record(record={"id": 1, "password": "plain-secret"})
    policy = DLQPayloadPolicy.encrypted(
        encryptor=_ReverseCipher(),
        encryption_algorithm="reverse",
        encryption_key_id="local-test",
    )

    envelope = _encode_dlq_envelope(
        "orders-1",
        operation="put",
        record=record,
        payload_policy=policy,
    )
    rendered = json.dumps(envelope, sort_keys=True)

    assert envelope["payload_encoding"] == "encrypted"
    assert envelope["payload_algorithm"] == "reverse"
    assert envelope["payload_key_id"] == "local-test"
    assert "payload" not in envelope
    assert "plain-secret" not in rendered
    with pytest.raises(ValueError, match="Encrypted Kafka DLQ payload requires"):
        _decode_dlq_envelope(json.dumps(envelope).encode("utf-8"))

    operation, storage_key, restored = _decode_dlq_envelope(
        json.dumps(envelope).encode("utf-8"),
        payload_policy=policy,
    )

    assert operation == "put"
    assert storage_key == "orders-1"
    assert restored is not None
    assert restored.record == {"id": 1, "password": "plain-secret"}


def test_kafka_dlq_envelope_round_trip_preserves_storage_key() -> None:
    record = _make_record()
    object.__setattr__(record, "_storage_id", "orders-1")

    encoded = json.dumps(
        _encode_dlq_envelope("orders-1", operation="put", record=record),
        sort_keys=True,
    ).encode("utf-8")
    operation, storage_key, restored = _decode_dlq_envelope(encoded)

    assert operation == "put"
    assert storage_key == "orders-1"
    assert restored == record
    assert restored is not None
    assert restored._storage_id == "orders-1"


def test_kafka_dlq_legacy_payload_decode_uses_stable_storage_key() -> None:
    payload = json.dumps(_record_to_payload(_make_record()), sort_keys=True).encode("utf-8")

    first_operation, first_storage_key, first_record = _decode_dlq_envelope(payload)
    second_operation, second_storage_key, second_record = _decode_dlq_envelope(payload)

    assert first_operation == second_operation == "put"
    assert first_storage_key == second_storage_key
    assert first_record is not None
    assert second_record is not None
    assert first_record._storage_id == first_storage_key
    assert second_record._storage_id == second_storage_key


@pytest.mark.asyncio
async def test_kafka_dlq_sink_applies_payload_policy_to_persisted_envelope() -> None:
    sink = KafkaDLQSink(
        topic="dlq",
        bootstrap_servers="localhost:9092",
        payload_policy=DLQPayloadPolicy.redacted(redact_fields=("secret_note",)),
    )
    fake_sink = _FakeEnvelopeSink()
    sink._sink = fake_sink  # type: ignore[attr-defined]
    record = _make_record(record={"id": 1, "secret_note": "hide-me"})

    await sink.write(record)

    assert fake_sink.writes[0]["payload"]["record"] == {
        "id": 1,
        "secret_note": "[REDACTED]",
    }


@pytest.mark.asyncio
async def test_kafka_dlq_sink_fails_closed_when_payload_encryption_fails() -> None:
    sink = KafkaDLQSink(
        topic="dlq",
        bootstrap_servers="localhost:9092",
        payload_policy=DLQPayloadPolicy.encrypted(encryptor=_FailingCipher()),
    )
    fake_sink = _FakeEnvelopeSink()
    sink._sink = fake_sink  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="encrypt failed"):
        await sink.write(_make_record(record={"id": 1, "password": "plain-secret"}))

    assert fake_sink.writes == []


@pytest.mark.asyncio
async def test_kafka_dlq_sink_replay_and_acknowledge_emit_stateful_envelopes() -> None:
    sink = KafkaDLQSink(
        topic="dlq",
        bootstrap_servers="localhost:9092",
    )
    fake_sink = _FakeEnvelopeSink()
    sink._sink = fake_sink  # type: ignore[attr-defined]

    record = _make_record(attempt=0)

    await sink.write(record)
    replayed = await sink.replay(record)
    await sink.acknowledge(replayed)

    assert len(fake_sink.writes) == 3
    first, second, third = fake_sink.writes
    assert first["op"] == "put"
    assert second["op"] == "put"
    assert third["op"] == "delete"
    assert first["storage_key"] == second["storage_key"] == third["storage_key"]
    assert first["payload"]["attempt"] == 0
    assert second["payload"]["attempt"] == 1
    assert replayed.attempt == 1

    metrics = sink.metrics_snapshot().to_dict()
    assert metrics["write_count"] == 1
    assert metrics["write_batch_count"] == 0
    assert metrics["replay_count"] == 1
    assert metrics["acknowledge_count"] == 1
    assert metrics["upsert_count"] == 2
    assert metrics["delete_count"] == 1
    assert metrics["last_write_at"] is not None
    assert metrics["last_replay_at"] is not None
    assert metrics["last_acknowledge_at"] is not None

    rendered = sink.render_prometheus_metrics(namespace="agora_dlq")
    assert (
        'agora_dlq_sink_events_total{topic="dlq",bootstrap_servers="localhost:9092",event="upsert"} 2'
        in rendered
    )
    assert (
        'agora_dlq_sink_events_total{topic="dlq",bootstrap_servers="localhost:9092",event="delete"} 1'
        in rendered
    )


@pytest.mark.asyncio
async def test_kafka_dlq_source_reduces_latest_state_and_filters_records() -> None:
    record_one = _make_record(attempt=0)
    record_one_replayed = _make_record(attempt=1)
    record_two = _make_record(
        run_id="run-2",
        stage="sink_write",
        processed_record={"id": 2, "ok": False},
    )
    object.__setattr__(record_one, "_storage_id", "record-1")
    object.__setattr__(record_one_replayed, "_storage_id", "record-1")
    object.__setattr__(record_two, "_storage_id", "record-2")

    source = KafkaDLQSource(
        assignments=[("dlq", 0)],
        bootstrap_servers="localhost:9092",
        pipeline_id="orders",
        stage="middleware",
        limit=1,
        start_offsets={("dlq", 0): 4},
    )
    source._consumer = _FakeConsumer(  # type: ignore[attr-defined]
        [
            {
                ("dlq", 0): [
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-1", operation="put", record=record_one),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-2", operation="put", record=record_two),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope(
                                "record-1",
                                operation="put",
                                record=record_one_replayed,
                            ),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-2", operation="delete"),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                ]
            },
            {},
            {},
        ]
    )
    source._topic_partition_cls = lambda topic, partition: (topic, partition)  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert len(records) == 1
    assert records[0].pipeline_id == "orders"
    assert records[0].stage == "middleware"
    assert records[0].attempt == 1
    assert records[0]._storage_id == "record-1"
    assert source._consumer.seek_calls == [(("dlq", 0), 4)]  # type: ignore[attr-defined]

    metrics = source.metrics_snapshot().to_dict()
    assert metrics["subscription_mode"] == "manual_assign"
    assert metrics["scan_count"] == 1
    assert metrics["scanned_message_count"] == 4
    assert metrics["upsert_event_count"] == 3
    assert metrics["delete_event_count"] == 1
    assert metrics["start_offset_seek_count"] == 1
    assert metrics["live_record_count"] == 1
    assert metrics["matched_record_count"] == 1
    assert metrics["replayable_record_count"] == 1
    assert metrics["retry_filtered_count"] == 0
    assert metrics["last_scan_completed_at"] is not None
    assert metrics["last_record_seen_at"] is not None

    rendered = source.render_prometheus_metrics(namespace="agora_dlq")
    assert (
        'agora_dlq_source_backlog{consumer_group="orders-kafka-dlq-replay",bootstrap_servers="localhost:9092",subscription_mode="manual_assign",gauge="replayable_record_count"} 1'
        in rendered
    )
    assert (
        'agora_dlq_source_events_total{consumer_group="orders-kafka-dlq-replay",bootstrap_servers="localhost:9092",subscription_mode="manual_assign",event="delete"} 1'
        in rendered
    )


@pytest.mark.asyncio
async def test_kafka_dlq_source_preserves_batch_polled_while_joining_group() -> None:
    record = _make_record(attempt=0)
    object.__setattr__(record, "_storage_id", "record-1")

    source = KafkaDLQSource(
        topics=["dlq"],
        bootstrap_servers="localhost:9092",
    )
    source._consumer = _FakeConsumer(  # type: ignore[attr-defined]
        [
            {
                ("dlq", 0): [
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-1", operation="put", record=record),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                ]
            },
            {},
            {},
        ],
        assignment_after_polls=1,
    )

    records = [record async for record in source.stream()]

    assert records == [record]
    assert source.metrics_snapshot().scanned_message_count == 1


@pytest.mark.asyncio
async def test_kafka_dlq_source_spills_compaction_state_when_threshold_is_reached() -> None:
    record_one = _make_record(attempt=0)
    record_one_replayed = _make_record(attempt=2)
    record_two = _make_record(run_id="run-2")
    object.__setattr__(record_one, "_storage_id", "record-1")
    object.__setattr__(record_one_replayed, "_storage_id", "record-1")
    object.__setattr__(record_two, "_storage_id", "record-2")

    source = KafkaDLQSource(
        assignments=[("dlq", 0)],
        bootstrap_servers="localhost:9092",
        compaction_spill_threshold=1,
    )
    source._consumer = _FakeConsumer(  # type: ignore[attr-defined]
        [
            {
                ("dlq", 0): [
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-1", operation="put", record=record_one),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-2", operation="put", record=record_two),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope(
                                "record-1",
                                operation="put",
                                record=record_one_replayed,
                            ),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-2", operation="delete"),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                ]
            },
            {},
            {},
        ]
    )

    records = [record async for record in source.stream()]

    assert [(record._storage_id, record.attempt) for record in records] == [("record-1", 2)]
    assert source.metrics_snapshot().live_record_count == 1
    assert source.metrics_snapshot().scanned_message_count == 4


@pytest.mark.asyncio
async def test_kafka_dlq_source_redacts_records_persisted_to_spill_storage() -> None:
    record_one = _make_record(record={"id": 1, "password": "first-secret"})
    record_two = _make_record(run_id="run-2", record={"id": 2, "api_key": "second-secret"})
    object.__setattr__(record_one, "_storage_id", "record-1")
    object.__setattr__(record_two, "_storage_id", "record-2")

    source = KafkaDLQSource(
        assignments=[("dlq", 0)],
        bootstrap_servers="localhost:9092",
        compaction_spill_threshold=1,
        payload_policy=DLQPayloadPolicy.redacted(),
    )
    source._consumer = _FakeConsumer(  # type: ignore[attr-defined]
        [
            {
                ("dlq", 0): [
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-1", operation="put", record=record_one),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-2", operation="put", record=record_two),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                ]
            },
            {},
            {},
        ]
    )

    records = [record async for record in source.stream()]
    rendered = json.dumps([record.record for record in records], sort_keys=True)

    assert [record._storage_id for record in records] == ["record-1", "record-2"]
    assert records[0].record == {"id": 1, "password": "[REDACTED]"}
    assert records[1].record == {"id": 2, "api_key": "[REDACTED]"}
    assert "first-secret" not in rendered
    assert "second-secret" not in rendered


@pytest.mark.asyncio
async def test_kafka_dlq_source_decrypts_records_persisted_to_spill_storage() -> None:
    policy = DLQPayloadPolicy.encrypted(encryptor=_ReverseCipher())
    record_one = _make_record(record={"id": 1, "password": "first-secret"})
    record_two = _make_record(run_id="run-2", record={"id": 2, "api_key": "second-secret"})
    object.__setattr__(record_one, "_storage_id", "record-1")
    object.__setattr__(record_two, "_storage_id", "record-2")

    source = KafkaDLQSource(
        assignments=[("dlq", 0)],
        bootstrap_servers="localhost:9092",
        compaction_spill_threshold=1,
        payload_policy=policy,
    )
    source._consumer = _FakeConsumer(  # type: ignore[attr-defined]
        [
            {
                ("dlq", 0): [
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope(
                                "record-1",
                                operation="put",
                                record=record_one,
                                payload_policy=policy,
                            ),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope(
                                "record-2",
                                operation="put",
                                record=record_two,
                                payload_policy=policy,
                            ),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                ]
            },
            {},
            {},
        ]
    )

    records = [record async for record in source.stream()]

    assert [record._storage_id for record in records] == ["record-1", "record-2"]
    assert records[0].record == {"id": 1, "password": "first-secret"}
    assert records[1].record == {"id": 2, "api_key": "second-secret"}


@pytest.mark.asyncio
async def test_kafka_dlq_source_metrics_track_retry_filtered_records() -> None:
    exhausted = _make_record(attempt=5, max_attempts=5)
    replayable = _make_record(run_id="run-2", attempt=2, max_attempts=5)
    object.__setattr__(exhausted, "_storage_id", "record-exhausted")
    object.__setattr__(replayable, "_storage_id", "record-replayable")

    source = KafkaDLQSource(
        assignments=[("dlq", 0)],
        bootstrap_servers="localhost:9092",
        start_offsets={("dlq", 0): 2},
    )
    source._consumer = _FakeConsumer(  # type: ignore[attr-defined]
        [
            {
                ("dlq", 0): [
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope(
                                "record-exhausted",
                                operation="put",
                                record=exhausted,
                            ),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope(
                                "record-replayable",
                                operation="put",
                                record=replayable,
                            ),
                            sort_keys=True,
                        ).encode("utf-8")
                    ),
                ]
            },
            {},
            {},
        ]
    )
    source._topic_partition_cls = lambda topic, partition: (topic, partition)  # type: ignore[attr-defined]

    records = [record async for record in source.stream()]

    assert [record.run_id for record in records] == ["run-2"]
    metrics = source.backlog_snapshot().to_dict()
    assert metrics["scan_count"] == 1
    assert metrics["scanned_message_count"] == 2
    assert metrics["live_record_count"] == 2
    assert metrics["matched_record_count"] == 2
    assert metrics["replayable_record_count"] == 1
    assert metrics["retry_filtered_count"] == 1


@pytest.mark.asyncio
async def test_kafka_dlq_source_stops_scan_at_highwater_without_idle_polls() -> None:
    record = _make_record(attempt=0)
    object.__setattr__(record, "_storage_id", "record-1")

    source = KafkaDLQSource(
        assignments=[("dlq", 0)],
        bootstrap_servers="localhost:9092",
        scan_idle_polls=10,
    )
    source._consumer = _FakeConsumer(  # type: ignore[attr-defined]
        [
            {
                ("dlq", 0): [
                    _FakeMessage(
                        json.dumps(
                            _encode_dlq_envelope("record-1", operation="put", record=record),
                            sort_keys=True,
                        ).encode("utf-8")
                    )
                ]
            },
            {},
        ],
        highwater_offsets={("dlq", 0): 1},
    )

    records = [item async for item in source.stream()]

    assert [item._storage_id for item in records] == ["record-1"]
    assert source._consumer.getmany_calls == 1  # type: ignore[attr-defined]
    assert source.metrics_snapshot().to_dict()["highwater_stop_count"] == 1


def test_kafka_dlq_prometheus_exporter_renders_explicit_snapshots() -> None:
    exporter = KafkaDLQPrometheusExporter(namespace="agora_dlq")

    sink_metrics = KafkaDLQSink(
        topic="dlq",
        bootstrap_servers="localhost:9092",
    ).metrics_snapshot()
    source_metrics = KafkaDLQSource(
        topic="dlq",
        bootstrap_servers="localhost:9092",
    ).metrics_snapshot()

    rendered_sink = exporter.render_sink(sink_metrics)
    rendered_source = exporter.render_source(source_metrics)

    assert "agora_dlq_sink_events_total" in rendered_sink
    assert "agora_dlq_source_backlog" in rendered_source


def test_kafka_dlq_source_uses_client_security_kwargs_for_tls_protocols() -> None:
    source = KafkaDLQSource(
        topic="dlq",
        bootstrap_servers="localhost:9092",
        security_protocol="SSL",
        ssl_check_hostname=False,
    )

    kwargs = source._security_kwargs()

    assert kwargs["security_protocol"] == "SSL"
    assert "ssl_context" in kwargs
    assert kwargs["ssl_context"].check_hostname is False


def test_kafka_dlq_source_uses_gssapi_security_kwargs() -> None:
    source = KafkaDLQSource(
        topic="dlq",
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="GSSAPI",
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name="EXAMPLE.COM",
    )

    kwargs = source._security_kwargs()

    assert kwargs == {
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "GSSAPI",
        "sasl_kerberos_service_name": "kafka",
        "sasl_kerberos_domain_name": "EXAMPLE.COM",
    }
