from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from agora.core.retry import RetryPolicy

from agora_plugins.kafka import (
    KafkaOpenTelemetryTracing,
    KafkaSASLConfig,
    KafkaSecurityConfig,
    KafkaSink,
    KafkaSinkMessage,
    KafkaTLSConfig,
)
from agora_plugins.kafka.sinks import kafka as kafka_module


class _FakeSpan:
    def __init__(self, tracer: _FakeTracer, name: str, kwargs: dict[str, Any]) -> None:
        self._tracer = tracer
        self._name = name
        self._kwargs = kwargs

    def __enter__(self) -> _FakeSpan:
        self._tracer.entered.append((self._name, self._kwargs))
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tracer.exited.append(self._name)


class _FakeTracer:
    def __init__(self) -> None:
        self.entered: list[tuple[str, dict[str, Any]]] = []
        self.exited: list[str] = []

    def start_as_current_span(self, name: str, **kwargs: Any) -> _FakeSpan:
        return _FakeSpan(self, name, kwargs)


class _FakePropagator:
    def __init__(self) -> None:
        self.injected: list[dict[str, str]] = []

    def inject(self, carrier: dict[str, str]) -> None:
        carrier["traceparent"] = "00-test-trace"
        self.injected.append(dict(carrier))


def test_kafka_sink_warns_on_plaintext_non_local_bootstrap() -> None:
    with pytest.warns(UserWarning, match="bootstrap_servers='broker.prod.example.com:9092'"):
        KafkaSink(
            topic="test-topic",
            bootstrap_servers="broker.prod.example.com:9092",
            serializer=lambda r: r.encode("utf-8"),
        )


@pytest.mark.asyncio
async def test_kafka_sink_write_sends_serialized_value() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write("test-record")

    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args
    assert call_args[0][0] == "test-topic"
    assert call_args[1]["value"] == b"test-record"


@pytest.mark.asyncio
async def test_kafka_sink_key_fn_applied() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        key_fn=lambda r: r["key"].encode("utf-8"),
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write({"key": "partition-1", "value": "data"})

    call_args = mock_producer.send.call_args
    assert call_args[1]["key"] == b"partition-1"


@pytest.mark.asyncio
async def test_kafka_sink_topic_fn_applied() -> None:
    sink = KafkaSink(
        topic="fallback-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        topic_fn=lambda r: r["topic"],
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write({"topic": "orders.cleaned", "value": "data"})

    call_args = mock_producer.send.call_args
    assert call_args[0][0] == "orders.cleaned"


@pytest.mark.asyncio
async def test_kafka_sink_partition_fn_applied() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        partition_fn=lambda r: r["partition"],
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write({"partition": 3, "value": "data"})

    call_args = mock_producer.send.call_args
    assert call_args[1]["partition"] == 3


@pytest.mark.asyncio
async def test_kafka_sink_timestamp_ms_fn_applied() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        timestamp_ms_fn=lambda r: r["timestamp_ms"],
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write({"timestamp_ms": 1_717_000_000_000, "value": "data"})

    call_args = mock_producer.send.call_args
    assert call_args[1]["timestamp_ms"] == 1_717_000_000_000


@pytest.mark.asyncio
async def test_kafka_sink_headers_fn_applied() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        headers_fn=lambda r: [
            ("tenant", r["tenant"].encode("utf-8")),
            ("event_type", r["event_type"].encode("utf-8")),
        ],
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write({"tenant": "acme", "event_type": "order.created", "value": "data"})

    call_args = mock_producer.send.call_args
    assert call_args[1]["headers"] == [
        ("tenant", b"acme"),
        ("event_type", b"order.created"),
    ]


@pytest.mark.asyncio
async def test_kafka_sink_tracing_injects_traceparent_header() -> None:
    tracer = _FakeTracer()
    propagator = _FakePropagator()
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        headers_fn=lambda r: [("tenant", r["tenant"].encode("utf-8"))],
        tracing=KafkaOpenTelemetryTracing(
            enabled=True,
            tracer=tracer,
            propagator=propagator,
            producer_span_kind="producer",
        ),
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write({"tenant": "acme", "value": "data"})

    call_args = mock_producer.send.call_args
    assert call_args.kwargs["headers"] == [
        ("tenant", b"acme"),
        ("traceparent", b"00-test-trace"),
    ]
    assert tracer.entered[0][0] == "kafka.produce"
    assert tracer.entered[0][1]["kind"] == "producer"
    assert tracer.entered[0][1]["attributes"]["messaging.destination.name"] == "test-topic"


@pytest.mark.asyncio
async def test_kafka_sink_backpressure_limits_pending_acks() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        max_pending_acks=2,
    )

    mock_producer = MagicMock()
    futures = [asyncio.Future() for _ in range(10)]
    for future in futures:
        future.set_result(None)

    mock_producer.send = AsyncMock(side_effect=futures)
    sink._producer = mock_producer  # type: ignore[attr-defined]

    for index in range(10):
        await sink.write(f"record{index}")

    assert len(sink._pending_acks) <= 2  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_sink_flush_drains_pending_acks() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
    )

    mock_producer = MagicMock()
    future1 = asyncio.Future()
    future1.set_result(None)
    future2 = asyncio.Future()
    future2.set_result(None)
    mock_producer.send = AsyncMock(side_effect=[future1, future2])
    mock_producer.flush = AsyncMock()
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write("record1")
    await sink.write("record2")

    assert len(sink._pending_acks) == 2  # type: ignore[attr-defined]

    await sink.flush()

    assert len(sink._pending_acks) == 0  # type: ignore[attr-defined]
    mock_producer.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_sink_write_batch_passes_headers() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        headers_fn=lambda r: [("tenant", r["tenant"].encode("utf-8"))],
        max_pending_acks=2,
    )

    mock_producer = MagicMock()
    future1 = asyncio.Future()
    future1.set_result(None)
    future2 = asyncio.Future()
    future2.set_result(None)
    mock_producer.send = AsyncMock(side_effect=[future1, future2])
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write_batch(
        [
            {"tenant": "acme", "value": "one"},
            {"tenant": "globex", "value": "two"},
        ]
    )

    calls = mock_producer.send.call_args_list
    assert calls[0].kwargs["headers"] == [("tenant", b"acme")]
    assert calls[1].kwargs["headers"] == [("tenant", b"globex")]


@pytest.mark.asyncio
async def test_kafka_sink_message_fn_can_publish_full_envelope() -> None:
    sink = KafkaSink(
        topic="fallback-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        message_fn=lambda r: KafkaSinkMessage(
            topic=r["topic"],
            value=r["payload"].encode("utf-8"),
            key=r["key"].encode("utf-8"),
            partition=r["partition"],
            headers=[("tenant", r["tenant"].encode("utf-8"))],
            timestamp_ms=r["timestamp_ms"],
        ),
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write(
        {
            "topic": "orders.created",
            "payload": "body",
            "key": "order-1",
            "partition": 4,
            "tenant": "acme",
            "timestamp_ms": 1_717_000_000_000,
            "value": "ignored",
        }
    )

    call_args = mock_producer.send.call_args
    assert call_args.args[0] == "orders.created"
    assert call_args.kwargs == {
        "value": b"body",
        "key": b"order-1",
        "partition": 4,
        "headers": [("tenant", b"acme")],
        "timestamp_ms": 1_717_000_000_000,
    }


@pytest.mark.asyncio
async def test_kafka_sink_message_fn_can_override_subset_and_fall_back_to_defaults() -> None:
    sink = KafkaSink(
        topic="fallback-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        key_fn=lambda r: r["key"].encode("utf-8"),
        headers_fn=lambda r: [("event_type", r["event_type"].encode("utf-8"))],
        message_fn=lambda r: KafkaSinkMessage(topic=r["topic"]),
    )

    mock_producer = MagicMock()
    mock_producer.send = AsyncMock(return_value=MagicMock())
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write(
        {
            "topic": "orders.updated",
            "value": "body",
            "key": "order-1",
            "event_type": "order.updated",
        }
    )

    call_args = mock_producer.send.call_args
    assert call_args.args[0] == "orders.updated"
    assert call_args.kwargs["value"] == b"body"
    assert call_args.kwargs["key"] == b"order-1"
    assert call_args.kwargs["headers"] == [("event_type", b"order.updated")]


@pytest.mark.asyncio
async def test_kafka_sink_write_batch_passes_topic_and_partition() -> None:
    sink = KafkaSink(
        topic="fallback-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        topic_fn=lambda r: r["topic"],
        partition_fn=lambda r: r["partition"],
        max_pending_acks=2,
    )

    mock_producer = MagicMock()
    future1 = asyncio.Future()
    future1.set_result(None)
    future2 = asyncio.Future()
    future2.set_result(None)
    mock_producer.send = AsyncMock(side_effect=[future1, future2])
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write_batch(
        [
            {"topic": "orders.created", "partition": 1, "value": "one"},
            {"topic": "orders.updated", "partition": 2, "value": "two"},
        ]
    )

    calls = mock_producer.send.call_args_list
    assert calls[0].args[0] == "orders.created"
    assert calls[0].kwargs["partition"] == 1
    assert calls[1].args[0] == "orders.updated"
    assert calls[1].kwargs["partition"] == 2


@pytest.mark.asyncio
async def test_kafka_sink_write_batch_supports_message_fn() -> None:
    sink = KafkaSink(
        topic="fallback-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r["value"].encode("utf-8"),
        message_fn=lambda r: KafkaSinkMessage(
            topic=r["topic"],
            value=r["value"].encode("utf-8"),
            timestamp_ms=r["timestamp_ms"],
        ),
        max_pending_acks=2,
    )

    mock_producer = MagicMock()
    future1 = asyncio.Future()
    future1.set_result(None)
    future2 = asyncio.Future()
    future2.set_result(None)
    mock_producer.send = AsyncMock(side_effect=[future1, future2])
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write_batch(
        [
            {"topic": "orders.created", "value": "one", "timestamp_ms": 1},
            {"topic": "orders.updated", "value": "two", "timestamp_ms": 2},
        ]
    )

    calls = mock_producer.send.call_args_list
    assert calls[0].args[0] == "orders.created"
    assert calls[0].kwargs["timestamp_ms"] == 1
    assert calls[1].args[0] == "orders.updated"
    assert calls[1].kwargs["timestamp_ms"] == 2


@pytest.mark.asyncio
async def test_kafka_sink_write_requires_open() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
    )

    with pytest.raises(RuntimeError, match=r"open.*was not called"):
        await sink.write("record")


@pytest.mark.asyncio
async def test_kafka_sink_close_stops_producer() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
    )

    mock_producer = MagicMock()
    mock_producer.flush = AsyncMock()
    mock_producer.stop = AsyncMock()
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.close()

    mock_producer.stop.assert_awaited_once()
    assert sink._producer is None  # type: ignore[attr-defined]


def test_kafka_sink_enables_idempotence_and_safe_defaults() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
    )

    assert sink._producer_kwargs["enable_idempotence"] is True  # type: ignore[attr-defined]
    assert sink._producer_kwargs["acks"] == "all"  # type: ignore[attr-defined]
    assert sink._producer_kwargs["compression_type"] == "gzip"  # type: ignore[attr-defined]
    supported = kafka_module._producer_supported_kwargs()
    if supported is None or "max_in_flight_requests_per_connection" in supported:
        assert sink._producer_kwargs["max_in_flight_requests_per_connection"] == 5  # type: ignore[attr-defined]
    else:
        assert "max_in_flight_requests_per_connection" not in sink._producer_kwargs  # type: ignore[attr-defined]


def test_kafka_sink_transactional_id_enables_transactional_producer_kwargs() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        transactional_id="orders-etl-1",
    )

    assert sink._producer_kwargs["transactional_id"] == "orders-etl-1"  # type: ignore[attr-defined]
    assert sink._producer_kwargs["enable_idempotence"] is True  # type: ignore[attr-defined]
    assert sink._producer_kwargs["acks"] == "all"  # type: ignore[attr-defined]


def test_kafka_sink_rejects_transaction_per_batch_without_transactional_id() -> None:
    with pytest.raises(ValueError, match="transactional_id"):
        KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            serializer=lambda r: r.encode("utf-8"),
            transaction_per_batch=True,
        )


@pytest.mark.asyncio
async def test_kafka_sink_transactional_id_does_not_implicitly_wrap_single_write() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        transactional_id="orders-etl-1",
    )

    mock_producer = MagicMock()
    future = asyncio.Future()
    future.set_result(None)
    mock_producer.begin_transaction = AsyncMock()
    mock_producer.commit_transaction = AsyncMock()
    mock_producer.abort_transaction = AsyncMock()
    mock_producer.send = AsyncMock(return_value=future)
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write("one")

    mock_producer.begin_transaction.assert_not_awaited()
    mock_producer.send.assert_awaited_once()
    mock_producer.commit_transaction.assert_not_awaited()
    mock_producer.abort_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_kafka_sink_transaction_per_batch_commits_transaction() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        transactional_id="orders-etl-1",
        transaction_per_batch=True,
    )

    mock_producer = MagicMock()
    future1 = asyncio.Future()
    future1.set_result(None)
    future2 = asyncio.Future()
    future2.set_result(None)
    mock_producer.begin_transaction = AsyncMock()
    mock_producer.commit_transaction = AsyncMock()
    mock_producer.abort_transaction = AsyncMock()
    mock_producer.send = AsyncMock(side_effect=[future1, future2])
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write_batch(["one", "two"])

    mock_producer.begin_transaction.assert_awaited_once()
    mock_producer.commit_transaction.assert_awaited_once()
    mock_producer.abort_transaction.assert_not_awaited()
    assert len(sink._pending_acks) == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_sink_transactional_id_does_not_wrap_batch_without_transaction_per_batch() -> (
    None
):
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        transactional_id="orders-etl-1",
    )

    mock_producer = MagicMock()
    future1 = asyncio.Future()
    future1.set_result(None)
    future2 = asyncio.Future()
    future2.set_result(None)
    mock_producer.begin_transaction = AsyncMock()
    mock_producer.commit_transaction = AsyncMock()
    mock_producer.abort_transaction = AsyncMock()
    mock_producer.send = AsyncMock(side_effect=[future1, future2])
    sink._producer = mock_producer  # type: ignore[attr-defined]

    await sink.write_batch(["one", "two"])

    mock_producer.begin_transaction.assert_not_awaited()
    mock_producer.commit_transaction.assert_not_awaited()
    mock_producer.abort_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_kafka_sink_transaction_can_send_offsets_to_transaction() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        transactional_id="orders-etl-1",
    )

    mock_producer = MagicMock()
    mock_producer.begin_transaction = AsyncMock()
    mock_producer.commit_transaction = AsyncMock()
    mock_producer.send_offsets_to_transaction = AsyncMock()
    sink._producer = mock_producer  # type: ignore[attr-defined]

    async with sink.transaction():
        await sink.send_offsets_to_transaction({"tp": 12}, "orders-group")

    mock_producer.begin_transaction.assert_awaited_once()
    mock_producer.send_offsets_to_transaction.assert_awaited_once_with(
        {"tp": 12},
        "orders-group",
    )
    mock_producer.commit_transaction.assert_awaited_once()


def test_kafka_sink_rejects_unsafe_acks_with_idempotence() -> None:
    with pytest.raises(ValueError, match="acks='all'"):
        KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            serializer=lambda r: r.encode("utf-8"),
            acks=1,
        )


@pytest.mark.parametrize(
    ("producer_kwargs", "exception_type", "message"),
    [
        ({"linger_ms": -1}, ValueError, "linger_ms"),
        ({"request_timeout_ms": 0}, ValueError, "request_timeout_ms"),
        ({"retry_backoff_ms": -1}, ValueError, "retry_backoff_ms"),
        ({"max_batch_size": 0}, ValueError, "max_batch_size"),
        ({"max_request_size": 0}, ValueError, "max_request_size"),
        ({"metadata_max_age_ms": False}, TypeError, "metadata_max_age_ms"),
        ({"enable_idempotence": "yes"}, TypeError, "enable_idempotence"),
        ({"enable_idempotence": False, "acks": "yes"}, ValueError, "acks"),
    ],
)
def test_kafka_sink_rejects_invalid_producer_tuning(
    producer_kwargs: dict[str, object],
    exception_type: type[Exception],
    message: str,
) -> None:
    with pytest.raises(exception_type, match=message):
        KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            serializer=lambda r: r.encode("utf-8"),
            **producer_kwargs,
        )


def test_kafka_sink_rejects_empty_transactional_id() -> None:
    with pytest.raises(ValueError, match="transactional_id"):
        KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            serializer=lambda r: r.encode("utf-8"),
            transactional_id="",
        )


def test_kafka_tls_config_rejects_password_without_keypair() -> None:
    with pytest.raises(ValueError, match="password"):
        KafkaTLSConfig(password="secret")


@pytest.mark.asyncio
async def test_kafka_sink_open_passes_stronger_defaults_to_producer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    kafka_module._producer_supported_kwargs.cache_clear()
    kwargs_seen: dict[str, Any] = {}

    class FakeProducer:
        def __init__(self, **kwargs: Any) -> None:
            kwargs_seen.update(kwargs)

        async def start(self) -> None:
            return None

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", FakeProducer)

    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
    )

    await sink.open()

    assert kwargs_seen["bootstrap_servers"] == "localhost:9092"
    assert kwargs_seen["enable_idempotence"] is True
    assert kwargs_seen["acks"] == "all"
    assert kwargs_seen["compression_type"] == "gzip"
    supported = kafka_module._producer_supported_kwargs()
    if supported is None or "max_in_flight_requests_per_connection" in supported:
        assert kwargs_seen["max_in_flight_requests_per_connection"] == 5
    else:
        assert "max_in_flight_requests_per_connection" not in kwargs_seen


@pytest.mark.asyncio
async def test_kafka_sink_open_filters_unsupported_producer_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    kafka_module._producer_supported_kwargs.cache_clear()
    kwargs_seen: dict[str, Any] = {}

    class FakeProducer:
        def __init__(
            self,
            *,
            bootstrap_servers: str,
            security_protocol: str,
            enable_idempotence: bool,
            acks: str,
            compression_type: str,
            linger_ms: int,
        ) -> None:
            kwargs_seen.update(
                {
                    "bootstrap_servers": bootstrap_servers,
                    "security_protocol": security_protocol,
                    "enable_idempotence": enable_idempotence,
                    "acks": acks,
                    "compression_type": compression_type,
                    "linger_ms": linger_ms,
                }
            )

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def flush(self) -> None:
            return None

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", FakeProducer)

    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
    )

    await sink.open()
    await sink.close()

    assert kwargs_seen["bootstrap_servers"] == "localhost:9092"
    assert kwargs_seen["enable_idempotence"] is True
    assert "max_in_flight_requests_per_connection" not in kwargs_seen


@pytest.mark.asyncio
async def test_kafka_sink_open_closes_serializer_if_producer_start_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    lifecycle: list[str] = []

    class _Serializer:
        def open(self) -> None:
            lifecycle.append("open")

        def close(self) -> None:
            lifecycle.append("close")

        def __call__(self, record: str) -> bytes:
            return record.encode()

    class FakeProducer:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs

        async def start(self) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", FakeProducer)

    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=_Serializer(),
    )

    with pytest.raises(RuntimeError, match="boom"):
        await sink.open()

    assert lifecycle == ["open", "close"]
    assert sink._producer is None  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_sink_open_stops_producer_if_start_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    stop_calls = 0

    class _Serializer:
        def open(self) -> None:
            return None

        def close(self) -> None:
            return None

        def __call__(self, record: str) -> bytes:
            return record.encode()

    class FakeProducer:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs

        async def start(self) -> None:
            raise RuntimeError("boom")

        async def stop(self) -> None:
            nonlocal stop_calls
            stop_calls += 1

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", FakeProducer)

    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=_Serializer(),
    )

    with pytest.raises(RuntimeError, match="boom"):
        await sink.open()

    assert stop_calls == 1


@pytest.mark.asyncio
async def test_kafka_sink_write_uses_bounded_send_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
        key_fn=lambda record: f"key-{record}".encode(),
        max_pending_acks=2,
    )

    class FakeProducer:
        def __init__(self) -> None:
            self.calls: list[tuple[str, bytes, bytes | None]] = []
            self.flush_calls = 0

        async def send(
            self,
            topic: str,
            *,
            value: bytes | None = None,
            key: bytes | None = None,
            headers: list[tuple[str, bytes]] | None = None,
        ):
            del headers
            self.calls.append((topic, value or b"", key))
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

        async def flush(self) -> None:
            self.flush_calls += 1

    producer = FakeProducer()
    sink._producer = producer  # type: ignore[attr-defined]

    await sink.write("hello")
    await sink.write_batch(["a", "b"])
    await sink.flush()

    assert producer.calls == [
        ("events", b"hello", b"key-hello"),
        ("events", b"a", b"key-a"),
        ("events", b"b", b"key-b"),
    ]
    assert producer.flush_calls == 1
    assert len(sink._pending_acks) == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_sink_awaits_oldest_ack_when_window_is_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
        max_pending_acks=2,
    )

    awaited: list[str] = []

    class _Delivery:
        def __init__(self, payload: str) -> None:
            self._payload = payload

        def __await__(self):
            async def _wait():
                awaited.append(self._payload)

            return _wait().__await__()

    class FakeProducer:
        async def send(
            self,
            topic: str,
            *,
            value: bytes | None = None,
            key: bytes | None = None,
            headers: list[tuple[str, bytes]] | None = None,
        ):
            del topic, key, headers
            payload = (value or b"").decode()
            return _Delivery(payload)

        async def flush(self) -> None:
            return None

    sink._producer = FakeProducer()  # type: ignore[attr-defined]

    await sink.write("first")
    assert awaited == []
    await sink.write("second")
    assert awaited == ["first"]
    await sink.write("third")
    assert awaited == ["first", "second"]
    await sink.flush()
    assert awaited == ["first", "second", "third"]


@pytest.mark.asyncio
async def test_kafka_sink_retries_transient_send_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
        retry_policy=RetryPolicy[Any](
            max_attempts=2,
            initial_backoff_s=0.0,
            retry_exceptions=(RuntimeError,),
        ),
        max_pending_acks=1,
    )

    class FakeProducer:
        def __init__(self) -> None:
            self.send_calls = 0

        async def send(
            self,
            topic: str,
            *,
            value: bytes | None = None,
            key: bytes | None = None,
            headers: list[tuple[str, bytes]] | None = None,
        ):
            del topic, value, key, headers
            self.send_calls += 1
            if self.send_calls == 1:
                raise RuntimeError("broker busy")
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

        async def flush(self) -> None:
            return None

    producer = FakeProducer()
    sink._producer = producer  # type: ignore[attr-defined]

    await sink.write("hello")

    assert producer.send_calls == 2


@pytest.mark.asyncio
async def test_kafka_sink_does_not_retry_non_retryable_delivery_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
        retry_policy=RetryPolicy[Any](
            max_attempts=2,
            initial_backoff_s=0.0,
            retry_exceptions=(RuntimeError,),
        ),
        max_pending_acks=1,
    )

    class FakeProducer:
        def __init__(self) -> None:
            self.send_calls = 0

        async def send(self, topic: str, **kwargs: object) -> asyncio.Future[None]:
            del topic, kwargs
            self.send_calls += 1
            delivery: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            delivery.set_exception(ValueError("invalid record"))
            return delivery

        async def flush(self) -> None:
            return None

    producer = FakeProducer()
    sink._producer = producer  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="invalid record"):
        await sink.write("hello")

    assert producer.send_calls == 1


@pytest.mark.asyncio
async def test_kafka_sink_close_stops_even_when_flush_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
    )

    class FakeProducer:
        def __init__(self) -> None:
            self.flush_calls = 0
            self.stop_calls = 0

        async def send(self, topic: str, *, value: bytes | None = None, key: bytes | None = None):
            del topic, value, key
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

        async def flush(self) -> None:
            self.flush_calls += 1
            raise RuntimeError("flush failed")

        async def stop(self) -> None:
            self.stop_calls += 1

    producer = FakeProducer()
    sink._producer = producer  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="flush failed"):
        await sink.close()

    assert producer.flush_calls == 1
    assert producer.stop_calls == 1
    assert sink._producer is None


@pytest.mark.asyncio
async def test_kafka_sink_close_clears_pending_acks_after_delivery_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
    )

    class FakeProducer:
        def __init__(self) -> None:
            self.stop_calls = 0

        async def flush(self) -> None:
            return None

        async def stop(self) -> None:
            self.stop_calls += 1

    failed = asyncio.Future()
    failed.set_exception(RuntimeError("delivery failed"))
    pending = asyncio.Future()
    pending.set_result(None)
    producer = FakeProducer()
    sink._producer = producer  # type: ignore[attr-defined]
    sink._pending_acks.extend([failed, pending])  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="delivery failed"):
        await sink.close()

    assert producer.stop_calls == 1
    assert len(sink._pending_acks) == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_sink_close_aborts_open_transaction() -> None:
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode(),
        transactional_id="orders-etl-1",
    )

    class FakeProducer:
        def __init__(self) -> None:
            self.abort_calls = 0
            self.flush_calls = 0
            self.stop_calls = 0

        async def abort_transaction(self) -> None:
            self.abort_calls += 1

        async def flush(self) -> None:
            self.flush_calls += 1

        async def stop(self) -> None:
            self.stop_calls += 1

    producer = FakeProducer()
    sink._producer = producer  # type: ignore[attr-defined]
    sink._in_transaction = True  # type: ignore[attr-defined]

    await sink.close()

    assert producer.abort_calls == 1
    assert producer.flush_calls == 0
    assert producer.stop_calls == 1
    assert sink._in_transaction is False  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kafka_sink_serializer_close_is_idempotent_after_open_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
    lifecycle: list[str] = []

    class Serializer:
        async def open(self) -> None:
            lifecycle.append("open")

        async def close(self) -> None:
            lifecycle.append("close")

        def __call__(self, record: str) -> bytes:
            return record.encode()

    class FakeProducer:
        async def start(self) -> None:
            raise RuntimeError("start failed")

        async def stop(self) -> None:
            return None

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", lambda **_kwargs: FakeProducer())
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=Serializer(),
    )

    with pytest.raises(RuntimeError, match="start failed"):
        await sink.open()
    await sink.close()

    assert lifecycle == ["open", "close"]


@pytest.mark.asyncio
async def test_kafka_sink_supports_async_serializer_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)

    class AsyncSerializer:
        def __init__(self) -> None:
            self.open_calls = 0
            self.close_calls = 0

        async def open(self) -> None:
            self.open_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

        async def __call__(self, record: str) -> bytes:
            return record.upper().encode("utf-8")

    class FakeProducer:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs
            self.sent: list[bytes] = []

        async def start(self) -> None:
            return None

        async def send(
            self,
            topic: str,
            *,
            value: bytes | None = None,
            key: bytes | None = None,
            headers: list[tuple[str, bytes]] | None = None,
        ):
            del topic, key, headers
            self.sent.append(value or b"")
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

        async def flush(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", FakeProducer)

    serializer = AsyncSerializer()
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=serializer,
    )

    await sink.open()
    await sink.write("hello")
    producer = sink._producer
    await sink.close()

    assert serializer.open_calls == 1
    assert serializer.close_calls == 1
    assert producer is not None
    assert producer.sent == [b"HELLO"]


@pytest.mark.asyncio
async def test_kafka_sink_supports_async_message_fn_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)

    class AsyncMessageBuilder:
        def __init__(self) -> None:
            self.open_calls = 0
            self.close_calls = 0

        async def open(self) -> None:
            self.open_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

        async def __call__(self, record: dict[str, object]) -> KafkaSinkMessage:
            return KafkaSinkMessage(
                topic=record["topic"],
                value=str(record["value"]).upper().encode("utf-8"),
                timestamp_ms=int(record["timestamp_ms"]),
            )

    class FakeProducer:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs
            self.calls: list[dict[str, Any]] = []

        async def start(self) -> None:
            return None

        async def send(self, topic: str, **kwargs: Any):
            self.calls.append({"topic": topic, **kwargs})
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

        async def flush(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", FakeProducer)

    message_builder = AsyncMessageBuilder()
    sink = KafkaSink(
        topic="events",
        bootstrap_servers="localhost:9092",
        serializer=lambda record: str(record).encode("utf-8"),
        message_fn=message_builder,
    )

    await sink.open()
    await sink.write({"topic": "orders.created", "value": "hello", "timestamp_ms": 7})
    producer = sink._producer
    await sink.close()

    assert message_builder.open_calls == 1
    assert message_builder.close_calls == 1
    assert producer is not None
    assert producer.calls == [
        {
            "topic": "orders.created",
            "value": b"HELLO",
            "key": None,
            "headers": None,
            "timestamp_ms": 7,
        }
    ]


def test_kafka_sink_includes_first_class_security_kwargs() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        security_protocol="SASL_PLAINTEXT",
        security=KafkaSecurityConfig(
            security_protocol="SASL_PLAINTEXT",
            sasl=KafkaSASLConfig(
                mechanism="PLAIN",
                username="svc",
                password="secret",
            ),
        ),
    )

    kwargs = sink._security_kwargs()  # type: ignore[attr-defined]

    assert kwargs == {
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "svc",
        "sasl_plain_password": "secret",
    }


def test_kafka_sink_includes_gssapi_security_kwargs() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        security_protocol="SASL_PLAINTEXT",
        security=KafkaSecurityConfig(
            security_protocol="SASL_PLAINTEXT",
            sasl=KafkaSASLConfig(
                mechanism="GSSAPI",
                kerberos_service_name="kafka",
                kerberos_domain_name="EXAMPLE.COM",
            ),
        ),
    )

    kwargs = sink._security_kwargs()  # type: ignore[attr-defined]

    assert kwargs == {
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "GSSAPI",
        "sasl_kerberos_service_name": "kafka",
        "sasl_kerberos_domain_name": "EXAMPLE.COM",
    }


def test_kafka_sink_includes_ssl_context_for_tls_protocols() -> None:
    sink = KafkaSink(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        serializer=lambda r: r.encode("utf-8"),
        security_protocol="SSL",
        security=KafkaSecurityConfig(
            security_protocol="SSL",
            tls={},
        ),
    )

    kwargs = sink._security_kwargs()  # type: ignore[attr-defined]

    assert kwargs["security_protocol"] == "SSL"
    assert "ssl_context" in kwargs


def test_kafka_sink_rejects_conflicting_security_protocol() -> None:
    with pytest.raises(ValueError, match="must match"):
        KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            serializer=lambda r: r.encode("utf-8"),
            security_protocol="PLAINTEXT",
            security=KafkaSecurityConfig(
                security_protocol="SSL",
                tls={"cafile": "/tmp/ca.pem"},
            ),
        )
