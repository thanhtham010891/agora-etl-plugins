# Agora ETL Plugins

**Official plugin collection for [agora-etl](https://pypi.org/project/agora-etl/) — Redis, cron scheduling, distributed coordination, Kafka, PostgreSQL, and Anthropic AI support.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl-plugins)](https://pypi.org/project/agora-etl-plugins/)

---

## Overview

`agora-etl-plugins` extends [agora-etl](https://pypi.org/project/agora-etl/) with production-ready integrations. Plugins are auto-discovered via Python entry-points — install the package and they register themselves automatically, no manual wiring needed.

Canonical ecosystem docs live in the public Agora docs surface:

- Plugin ecosystem overview:
  <https://github.com/thanhtham010891/agora-etl/blob/main/packages/agora/docs/plugins/index.md>
- Production readiness, compatibility, and release gates:
  <https://github.com/thanhtham010891/agora-etl/blob/main/packages/agora/docs/plugins/production-readiness.md>
- Core docs home:
  <https://github.com/thanhtham010891/agora-etl/tree/main/packages/agora/docs>

This README stays focused on package-specific quickstart information.

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.redis.sources import RedisStreamSource
from agora_plugins.redis.sinks import RedisSink

summary = await (
    Pipeline(RedisStreamSource(url="redis://localhost:6379", stream="events", group="my-group", consumer="worker-1"))
    .build(
        RedisSink(url="redis://localhost:6379", key_fn=lambda r: r["id"]),
        config=DeliveryConfig(batch_size=100),
    )
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
pip install "agora-etl-plugins[kafka]"        # Kafka source and sink
pip install "agora-etl-plugins[postgres]"     # PostgreSQL source, sink, DLQ, schema adapter
pip install "agora-etl-plugins[anthropic]"    # Anthropic completion and structured-output provider
pip install "agora-etl-plugins[all]"          # Everything in one install
```

This bundle now tracks the `agora-etl 0.4.x` compatibility line.
Current floor: `agora-etl>=0.4.1,<1`.
Supported Python versions: `3.11`, `3.12`, and `3.13`.

Important current bundle shape:

- flagship backend families: Redis, Kafka, and PostgreSQL
- focused official extensions: cron scheduling, distributed coordination, and
  Anthropic completion / structured output
- runtime planning, `agora doctor`, and public data-plane contracts now come
  from the `agora-etl 0.4.x` core line

Anthropic is now part of the official bundle through the `anthropic` extra,
with a completion and structured-output support story that stays explicit about
the lack of embeddings.

If your pipelines checkpoint frequently, you can also enable the Rust checkpoint hot path
from the core package:

```bash
pip install "agora-etl[rs]" "agora-etl-plugins[redis]"
```

## Local integration testing

The repository includes a local Docker stack and real-backend integration
tests for Redis, Kafka, and PostgreSQL.

Default local endpoints:

- `AGORA_TEST_REDIS_URL=redis://127.0.0.1:16379/0`
- `AGORA_TEST_KAFKA_BOOTSTRAP=127.0.0.1:19092`
- `AGORA_TEST_POSTGRES_DSN=postgresql://agora:agora@127.0.0.1:15432/agora_test`

Typical flow:

```bash
make integration-up
make integration-ps
make test-integration
make integration-down
```

`make test-integration` sets `AGORA_RUN_INTEGRATION=1` automatically and runs
only `tests/integration`, using those local Docker endpoints by default.

---

## Available plugins

### Anthropic `[anthropic]`

Official Anthropic AI provider support for completion-driven workflows and
structured JSON output.

| Component | Type | Description |
|---|---|---|
| `AnthropicProvider` | AI Provider | Claude-backed completion provider for enrichment, extraction, validation, translation, and classification in LLM mode |

Example:

```python
from agora.middlewares.ai.enrich import AIEnrichMiddleware
from agora_plugins.anthropic import AnthropicProvider

provider = AnthropicProvider(model="claude-3-5-haiku-20241022")
middleware = AIEnrichMiddleware(
    provider=provider,
    prompt_template="Review: {review_text}\nReturn JSON with keys summary and sentiment.",
    output_fields=["summary", "sentiment"],
)
```

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

Checkpoint resume for `RedisStreamSource` uses Redis `XGROUP SETID`, which
rewinds the consumer group cursor. It is intentionally guarded to single-consumer
groups; multi-consumer groups should resume through a dedicated group or an
operator-managed reset.

### Kafka `[kafka]`

Kafka source, sink, and dead-letter queue support for event-backed pipelines.

| Component | Type | Description |
|---|---|---|
| `KafkaSource` | Source | Consume Kafka records with manual offset control, poison-record policies, and Schema Registry-aware deserialization hooks |
| `KafkaSink` | Sink | Publish records to Kafka with idempotent-producer defaults and optional serializers |
| `KafkaDLQSink` | Sink | Persist failed records to Kafka for replay workflows |
| `KafkaDLQSource` | Source | Replay records from a Kafka-backed DLQ topic |

### PostgreSQL `[postgres]`

PostgreSQL extraction, loading, schema adaptation, and SQL-native DLQ support.

| Component | Type | Description |
|---|---|---|
| `PostgresSource` | Source | Stream query results with checkpoint-aware extraction and optional replica-read controls |
| `PostgresSink` | Sink | Write rows with SQL, COPY, or COPY-merge modes, upsert support, and schema-drift safety policies |
| `PostgresSchemaAdapter` | Sink wrapper | Apply inferred Agora schemas to PostgreSQL tables before writes |
| `PostgresDLQSink` | Sink | Persist failed records to a PostgreSQL DLQ table |
| `PostgresDLQSource` | Source | Replay records from a PostgreSQL DLQ table |

### Cron `[cron]`

Cron expression support for Agora scheduled pipelines.

| Component | Type | Description |
|---|---|---|
| `validate_cron_expression` | Helper | Validate cron expressions using `croniter` |
| `seconds_until_next_run` | Helper | Compute the next run delay from an epoch timestamp or timezone-aware `datetime` |

### Distributed `[distributed]`

Redis-backed worker coordination for multi-worker scheduled pipeline deployments.

| Component | Type | Description |
|---|---|---|
| `RedisWorkerCoordinator` | Worker coordinator | Register workers, acquire per-pipeline leases, renew held leases, expose fencing tokens, and release leases atomically |
| `DistributedConfig` | Config | Environment-backed settings for Redis URL, lease TTL, heartbeat, key prefix, and fail-safe local fallback behavior |

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
