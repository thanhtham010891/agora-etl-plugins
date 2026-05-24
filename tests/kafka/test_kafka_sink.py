from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from agora.core.retry import RetryPolicy

from agora_plugins.kafka import KafkaSink
from agora_plugins.kafka.sinks import kafka as kafka_module


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
    assert sink._producer_kwargs["max_in_flight_requests_per_connection"] == 5  # type: ignore[attr-defined]


def test_kafka_sink_rejects_unsafe_acks_with_idempotence() -> None:
    with pytest.raises(ValueError, match="acks='all'"):
        KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            serializer=lambda r: r.encode("utf-8"),
            acks=1,
        )


@pytest.mark.asyncio
async def test_kafka_sink_open_passes_stronger_defaults_to_producer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
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
    assert kwargs_seen["max_in_flight_requests_per_connection"] == 5


@pytest.mark.asyncio
async def test_kafka_sink_open_filters_unsupported_producer_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_module, "_AIOKAFKA_AVAILABLE", True)
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
        ):
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
        async def send(self, topic: str, *, value: bytes | None = None, key: bytes | None = None):
            del topic, key
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
    )

    class FakeProducer:
        def __init__(self) -> None:
            self.send_calls = 0

        async def send(self, topic: str, *, value: bytes | None = None, key: bytes | None = None):
            del topic, value, key
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
    await sink.flush()

    assert producer.send_calls == 2


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

        async def send(self, topic: str, *, value: bytes | None = None, key: bytes | None = None):
            del topic, key
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
