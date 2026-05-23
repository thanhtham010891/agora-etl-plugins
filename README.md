# Agora ETL Plugins

**Official plugin collection for [agora-etl](https://pypi.org/project/agora-etl/) — Redis, cron scheduling, and distributed coordination.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl-plugins)](https://pypi.org/project/agora-etl-plugins/)

---

## Overview

`agora-etl-plugins` extends [agora-etl](https://pypi.org/project/agora-etl/) with production-ready integrations. Plugins are auto-discovered via Python entry-points — install the package and they register themselves automatically, no manual wiring needed.

```python
from agora import Pipeline
from agora_plugins.redis.sources import RedisStreamSource
from agora_plugins.redis.sinks import RedisSink

summary = await (
    Pipeline(RedisStreamSource(url="redis://localhost:6379", stream="events", group="my-group", consumer="worker-1"))
    .build(RedisSink(url="redis://localhost:6379", key_fn=lambda r: r["id"]))
    .run()
)
print(f"written={summary.records_written}  errors={summary.records_errored}")
```

---

## Install

```bash
pip install "agora-etl-plugins[redis]"        # Redis source, sink, state, DLQ, dedup, AI cache
pip install "agora-etl-plugins[cron]"         # Cron schedule support for ScheduledPipeline
pip install "agora-etl-plugins[distributed]"  # Redis-backed distributed worker coordination
pip install "agora-etl-plugins[all]"          # Everything
```

---

## Available plugins

### Redis `[redis]`

Full Redis integration — streaming ingestion, writes, dead-letter queue, state, deduplication, and LLM response caching.

| Component | Type | Description |
|---|---|---|
| `RedisStreamSource` | Source | Consume records from a Redis Stream via XREADGROUP |
| `RedisSink` | Sink | Write records to Redis (SET / LPUSH / RPUSH / XADD) |
| `RedisDLQSink` | Sink | Route failed records to a Redis-backed dead-letter queue |
| `RedisDLQSource` | Source | Replay failed records from the Redis DLQ |
| `RedisBackend` | State | Redis-backed state backend with TTL and membership support |
| `RedisStore` | Dedup | Exact-match deduplication via Redis SET NX |
| `RedisEmbeddingStore` | Dedup | Semantic deduplication using cosine similarity (up to ~10k entries) |
| `RedisLLMCache` | AI Cache | Distributed LLM response cache backed by Redis |

```python
from agora_plugins.redis.sources import RedisStreamSource
from agora_plugins.redis.sinks import RedisSink
from agora_plugins.redis.dlq import RedisDLQSink, RedisDLQSource
from agora_plugins.redis.state import RedisBackend
from agora_plugins.redis.dedup.stores import RedisStore, RedisEmbeddingStore
from agora_plugins.redis.ai.cache import RedisLLMCache
```

#### RedisStreamSource

At-least-once delivery via consumer groups. Acknowledges messages only after successful downstream write.

```python
source = RedisStreamSource(
    url="redis://localhost:6379",
    stream="agora:events",
    group="pipeline-1",
    consumer="worker-1",
    deserializer=lambda fields: MyRecord(**fields),
    batch_size=100,
    block_ms=2000,
    reclaim_idle_ms=60_000,   # reclaim stale pending messages from dead consumers
)
```

#### RedisSink

Supports four write modes: `set`, `lpush`, `rpush`, `xadd`.

```python
# Write as Redis Stream entries
sink = RedisSink(
    url="redis://localhost:6379",
    key_fn=lambda r: "agora:processed",
    serializer=lambda r: {"id": r["id"], "value": r["value"]},
    mode="xadd",
    maxlen=10_000,
)

# Write as key-value pairs with TTL
sink = RedisSink(
    url="redis://localhost:6379",
    key_fn=lambda r: f"cache:{r['id']}",
    serializer=lambda r: json.dumps(r),
    mode="set",
    ttl_seconds=3600,
)
```

#### Dead-letter queue

```python
from agora_plugins.redis.dlq import RedisDLQSink, RedisDLQSource

# Route failures to DLQ
summary = await (
    Pipeline(source)
    .build(sink, dlq=RedisDLQSink(url="redis://localhost:6379"))
    .run()
)

# Replay failed records
dlq_source = RedisDLQSource(
    url="redis://localhost:6379",
    pipeline_id="my-pipeline",
    stage="sink_write",
)
async for record in dlq_source.stream():
    print(record.error_message)
```

---

### Cron `[cron]`

Adds cron expression support to `ScheduledPipeline`. Without this plugin, only interval-based scheduling is available.

```python
from agora.runner import ScheduledPipeline

pipeline = ScheduledPipeline(
    factory=lambda: my_pipeline,
    schedule="0 9 * * 1-5",  # weekdays at 9am
)
await pipeline.run()
```

Supported expression format: standard 5-field cron (`minute hour day month weekday`).

---

### Distributed `[distributed]`

Redis-backed distributed worker coordination. Prevents duplicate pipeline runs when multiple worker instances are deployed.

Each worker acquires a per-pipeline lease before each run and releases it atomically via a Lua script. Workers register heartbeats so the fleet is visible via `agora plugins list`.

```python
from agora_plugins.distributed import RedisWorkerCoordinator, DistributedConfig
from agora.runner import WorkerPool

config = DistributedConfig()  # reads AGORA_DISTRIBUTED_* env vars
coordinator = RedisWorkerCoordinator(
    redis_url=config.redis_url,
    lease_ttl_seconds=config.lease_ttl_seconds,
    heartbeat_interval=config.heartbeat_interval,
)

pool = WorkerPool(pipelines=[my_pipeline], coordinator=coordinator)
await pool.run()
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `AGORA_DISTRIBUTED_REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `AGORA_DISTRIBUTED_LEASE_TTL_SECONDS` | `300` | Lease duration — must exceed longest pipeline run |
| `AGORA_DISTRIBUTED_HEARTBEAT_INTERVAL` | `30` | Heartbeat interval in seconds |
| `AGORA_DISTRIBUTED_KEY_PREFIX` | `agora:distributed:` | Redis key namespace |
| `AGORA_DISTRIBUTED_FALLBACK_TO_LOCAL` | `false` | If `true`, continue without coordination when Redis is unavailable (risks duplicate runs) |

---

## Plugin auto-discovery

All plugins register themselves via Python entry-points. After installing, run:

```bash
agora plugins list
```

to see all registered plugins and their capabilities.

---

## Requirements

- Python 3.11+
- `agora-etl >= 0.1.0`

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
