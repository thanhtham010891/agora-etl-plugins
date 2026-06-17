from __future__ import annotations

import pytest
from agora.core.retry import RetryPolicy

from agora_plugins.redis import (
    RedisPrometheusExporter,
    RedisSink,
    RedisSinkEnterpriseAcceptanceThresholds,
)
from agora_plugins.redis.sinks.redis import RedisSinkMetricsSnapshot


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

    def ltrim(self, key: str, start: int, stop: int) -> None:
        self.calls.append(("ltrim", (key, start, stop), {}))

    def eval(self, script: str, numkeys: int, *args: object) -> None:
        self.calls.append(("eval", (script, numkeys, *args), {}))

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

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        self.calls.append(("ltrim", (key, start, stop), {}))

    async def eval(self, script: str, numkeys: int, *args: object) -> None:
        self.calls.append(("eval", (script, numkeys, *args), {}))

    async def xadd(self, key: str, value: dict[str, object], **kwargs: object) -> None:
        self.calls.append(("xadd", (key, value), kwargs))

    async def mset(self, mapping: dict[str, object]) -> None:
        self.calls.append(("mset", (mapping,), {}))

    def pipeline(self, *, transaction: bool):
        assert transaction is False
        return self.pipeline_obj


class _FailOnceSetClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.failures_remaining = 1

    async def set(self, key: str, value: object, **kwargs: object) -> None:
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise ConnectionError("temporary redis outage")
        await super().set(key, value, **kwargs)


class _FailAfterListScriptClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.failures_remaining = 1
        self.lists: dict[str, list[object]] = {}
        self.markers: set[object] = set()

    async def eval(self, script: str, numkeys: int, *args: object) -> None:
        await super().eval(script, numkeys, *args)
        del script
        assert numkeys == 2
        key = str(args[0])
        marker_key = args[1]
        mode = str(args[2])
        value = args[3]
        maxlen = int(args[4])
        if marker_key not in self.markers:
            self.markers.add(marker_key)
            values = self.lists.setdefault(key, [])
            if mode == "lpush":
                values.insert(0, value)
                if maxlen > 0:
                    del values[maxlen:]
            elif mode == "rpush":
                values.append(value)
                if maxlen > 0:
                    del values[:-maxlen]
            else:
                raise AssertionError(f"Unexpected list mode {mode!r}")
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise ConnectionError("response lost after redis script applied")


def test_redis_sink_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError, match="invalid mode"):
        RedisSink(
            url="redis://localhost:6379",
            key_fn=lambda record: str(record),
            mode="bad",
        )


def test_redis_sink_rejects_invalid_ttl_and_maxlen() -> None:
    with pytest.raises(ValueError, match="ttl_seconds"):
        RedisSink(
            url="redis://localhost:6379",
            key_fn=lambda record: str(record),
            ttl_seconds=0,
        )

    with pytest.raises(ValueError, match="maxlen"):
        RedisSink(
            url="redis://localhost:6379",
            key_fn=lambda record: str(record),
            mode="lpush",
            maxlen=0,
        )


def test_redis_sink_prometheus_omits_empty_optional_metric_families() -> None:
    snapshot = RedisSinkMetricsSnapshot(
        target="orders",
        mode="set",
        ttl_seconds=None,
        maxlen=None,
        connection_ready=True,
    )

    rendered = RedisPrometheusExporter(namespace="agora_test_redis").render_sink(snapshot)

    assert "# TYPE agora_test_redis_sink_gauge gauge" not in rendered
    assert "# TYPE agora_test_redis_sink_age_ms gauge" not in rendered
    assert (
        'agora_test_redis_sink_state{target="orders",mode="set",state="connection_ready"} 1'
        in rendered
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
    assert sink.metrics_snapshot().to_dict() == {
        "target": "localhost:6379",
        "mode": "set",
        "ttl_seconds": 30,
        "maxlen": None,
        "connection_ready": True,
        "write_call_count": 1,
        "write_batch_call_count": 0,
        "direct_write_count": 1,
        "mset_batch_count": 0,
        "pipeline_execute_count": 0,
        "written_record_count": 1,
        "accepted_record_count": 1,
        "redis_mutation_count": 1,
        "last_write_at": sink.metrics_snapshot().to_dict()["last_write_at"],
    }
    assert sink.metrics_snapshot().last_write_at is not None


@pytest.mark.asyncio
async def test_redis_sink_retries_transient_write_errors() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["id"],
        serializer=lambda record: record["value"],
        retry_policy=RetryPolicy[object](
            max_attempts=2,
            initial_backoff_s=0,
            retry_exceptions=(ConnectionError,),
        ),
    )
    client = _FailOnceSetClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write({"id": "alpha", "value": "hello"})

    assert client.calls == [("set", ("alpha", "hello"), {})]
    metrics = sink.metrics_snapshot()
    assert metrics.write_call_count == 1
    assert metrics.direct_write_count == 1
    assert metrics.written_record_count == 1
    assert metrics.accepted_record_count == 1
    assert metrics.redis_mutation_count == 1


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
    metrics = sink.metrics_snapshot()
    assert metrics.write_call_count == 0
    assert metrics.write_batch_call_count == 1
    assert metrics.direct_write_count == 0
    assert metrics.mset_batch_count == 1
    assert metrics.pipeline_execute_count == 0
    assert metrics.written_record_count == 2
    assert metrics.accepted_record_count == 2
    assert metrics.redis_mutation_count == 2
    assert metrics.last_write_at is not None


@pytest.mark.asyncio
async def test_redis_sink_mset_metrics_count_duplicate_records() -> None:
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
            {"id": "a", "value": "beta"},
        ]
    )

    assert client.calls == [("mset", ({"a": "beta"},), {})]
    metrics = sink.metrics_snapshot()
    assert metrics.mset_batch_count == 1
    assert metrics.written_record_count == 2
    assert metrics.accepted_record_count == 2
    assert metrics.redis_mutation_count == 1


@pytest.mark.asyncio
async def test_redis_sink_cluster_set_batch_uses_pipeline_instead_of_mset() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["id"],
        serializer=lambda record: record["value"],
        redis_cluster=True,
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write_batch(
        [
            {"id": "a", "value": "alpha"},
            {"id": "b", "value": "beta"},
        ]
    )

    assert client.calls == []
    assert client.pipeline_obj.executed is True
    assert client.pipeline_obj.calls == [
        ("set", ("a", "alpha"), {}),
        ("set", ("b", "beta"), {}),
    ]
    metrics = sink.metrics_snapshot()
    assert metrics.mset_batch_count == 0
    assert metrics.pipeline_execute_count == 1


@pytest.mark.asyncio
async def test_redis_sink_lpush_maxlen_trims_newest_records() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["key"],
        serializer=lambda record: record["value"],
        mode="lpush",
        maxlen=2,
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write({"key": "queue", "value": "a"})

    assert len(client.calls) == 1
    command, args, kwargs = client.calls[0]
    assert command == "eval"
    assert kwargs == {}
    assert args[1] == 2
    assert args[2] == "queue"
    assert str(args[3]).startswith("{queue}:agora:list-write:")
    assert args[4:] == ("lpush", "a", 2, 24 * 60 * 60)


@pytest.mark.asyncio
async def test_redis_sink_list_write_retry_does_not_duplicate_after_script_applied() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["key"],
        serializer=lambda record: record["value"],
        mode="lpush",
        maxlen=5,
        retry_policy=RetryPolicy[object](
            max_attempts=2,
            initial_backoff_s=0,
            retry_exceptions=(ConnectionError,),
        ),
    )
    client = _FailAfterListScriptClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write({"key": "queue", "value": "a"})

    assert [call[0] for call in client.calls] == ["eval", "eval"]
    assert client.lists == {"queue": ["a"]}
    metrics = sink.metrics_snapshot()
    assert metrics.write_call_count == 1
    assert metrics.direct_write_count == 1
    assert metrics.written_record_count == 1
    assert metrics.accepted_record_count == 1
    assert metrics.redis_mutation_count == 1


@pytest.mark.asyncio
async def test_redis_sink_rpush_batch_maxlen_trims_newest_records() -> None:
    sink = RedisSink(
        url="redis://localhost:6379",
        key_fn=lambda record: record["key"],
        serializer=lambda record: record["value"],
        mode="rpush",
        maxlen=2,
    )
    client = _FakeClient()
    sink._client = client  # type: ignore[attr-defined]

    await sink.write_batch(
        [
            {"key": "queue", "value": "a"},
            {"key": "queue", "value": "b"},
        ]
    )

    assert [call[0] for call in client.pipeline_obj.calls] == ["eval", "eval"]
    first_args = client.pipeline_obj.calls[0][1]
    second_args = client.pipeline_obj.calls[1][1]
    assert first_args[1] == 2
    assert first_args[2] == "queue"
    assert str(first_args[3]).startswith("{queue}:agora:list-write:")
    assert first_args[4:] == ("rpush", "a", 2, 24 * 60 * 60)
    assert second_args[1] == 2
    assert second_args[2] == "queue"
    assert str(second_args[3]).startswith("{queue}:agora:list-write:")
    assert second_args[4:] == ("rpush", "b", 2, 24 * 60 * 60)


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
    metrics = sink.metrics_snapshot()
    assert metrics.mode == "xadd"
    assert metrics.maxlen == 100
    assert metrics.write_batch_call_count == 1
    assert metrics.pipeline_execute_count == 1
    assert metrics.written_record_count == 2
    assert metrics.accepted_record_count == 2
    assert metrics.redis_mutation_count == 2


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


def test_redis_sink_acceptance_and_prometheus_surface() -> None:
    sink = RedisSink(
        url="redis://localhost:6379/0",
        key_fn=lambda record: str(record),
        mode="set",
    )
    sink._client = object()  # type: ignore[attr-defined]

    report = sink.acceptance_report(RedisSinkEnterpriseAcceptanceThresholds())
    rendered = sink.render_prometheus_metrics(namespace="agora_test_redis")

    assert report.passed is True
    assert (
        'agora_test_redis_sink_state{target="localhost:6379/0",mode="set",state="connection_ready"} 1'
        in rendered
    )
    assert 'event="accepted_record"' in rendered
    assert 'event="redis_mutation"' in rendered
