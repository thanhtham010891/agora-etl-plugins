from __future__ import annotations

import pytest

from agora_plugins.redis import RedisSink


class _FakePipeline:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
        self.executed = False

    def set(self, key: str, value: object, **kwargs: object) -> None:
        self.calls.append(("set", (key, value), kwargs))

    def lpush(self, key: str, value: object) -> None:
        self.calls.append(("lpush", (key, value), {}))

    def rpush(self, key: str, value: object) -> None:
        self.calls.append(("rpush", (key, value), {}))

    def xadd(self, key: str, value: dict[str, object], **kwargs: object) -> None:
        self.calls.append(("xadd", (key, value), kwargs))

    async def execute(self) -> None:
        self.executed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
        self.pipeline_obj = _FakePipeline()

    async def set(self, key: str, value: object, **kwargs: object) -> None:
        self.calls.append(("set", (key, value), kwargs))

    async def lpush(self, key: str, value: object) -> None:
        self.calls.append(("lpush", (key, value), {}))

    async def rpush(self, key: str, value: object) -> None:
        self.calls.append(("rpush", (key, value), {}))

    async def xadd(self, key: str, value: dict[str, object], **kwargs: object) -> None:
        self.calls.append(("xadd", (key, value), kwargs))

    async def mset(self, mapping: dict[str, object]) -> None:
        self.calls.append(("mset", (mapping,), {}))

    def pipeline(self, *, transaction: bool):
        assert transaction is False
        return self.pipeline_obj


def test_redis_sink_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError, match="invalid mode"):
        RedisSink(
            url="redis://localhost:6379",
            key_fn=lambda record: str(record),
            mode="bad",
        )


@pytest.mark.asyncio
async def test_redis_sink_write_uses_set_with_ttl() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["id"],
        serializer=lambda record: record["value"],
        ttl_seconds=30,
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write({"id": "alpha", "value": "hello"})

    assert client.calls == [("set", ("alpha", "hello"), {"ex": 30})]


@pytest.mark.asyncio
async def test_redis_sink_write_batch_uses_mset_for_set_without_ttl() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["id"],
        serializer=lambda record: record["value"],
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write_batch(
        [
            {"id": "a", "value": "alpha"},
            {"id": "b", "value": "beta"},
        ]
    )

    assert client.calls == [("mset", ({"a": "alpha", "b": "beta"},), {})]
    assert client.pipeline_obj.executed is False


@pytest.mark.asyncio
async def test_redis_sink_write_batch_uses_pipeline_for_xadd() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["stream"],
        serializer=lambda record: {"value": record["value"]},
        mode="xadd",
        maxlen=100,
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write_batch(
        [
            {"stream": "events", "value": "a"},
            {"stream": "events", "value": "b"},
        ]
    )

    assert client.pipeline_obj.executed is True
    assert client.pipeline_obj.calls == [
        ("xadd", ("events", {"value": "a"}), {"maxlen": 100, "approximate": True}),
        ("xadd", ("events", {"value": "b"}), {"maxlen": 100, "approximate": True}),
    ]


@pytest.mark.asyncio
async def test_redis_sink_xadd_requires_dict_serializer() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: "events",
        serializer=lambda record: "not-a-dict",
        mode="xadd",
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    with pytest.raises(TypeError, match="serializer to return a dict"):
        await sink.write({"value": "a"})
