# Agora ETL Plugins

**Official plugin collection for [agora-etl](https://pypi.org/project/agora-etl/) — Redis, cron scheduling, distributed coordination, Kafka, PostgreSQL, and Anthropic AI support.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl-plugins)](https://pypi.org/project/agora-etl-plugins/)

---

## Overview

`agora-etl-plugins` extends [agora-etl](https://pypi.org/project/agora-etl/) with production-ready integrations. Plugins are auto-discovered via Python entry-points — install the package and they register themselves automatically, no manual wiring needed.

Canonical ecosystem docs live in the Agora docs site:

- Plugin ecosystem overview: `https://agora.my-working.com/plugins/`
- Core docs home: `https://agora.my-working.com/`

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

This package now targets `agora-etl>=0.3.3`.

For plugin sources such as Redis Streams, Kafka, and PostgreSQL, `agora-etl 0.3.3`
adds three release-cycle improvements worth knowing about:

- config-driven workers can now build `WorkerPool` instances directly from
  `agora/v1` TOML
- OpenTelemetry tracing can be enabled from config with auto-wiring through the
  existing tracing path
- AI provider contracts now distinguish completion-only and embedding-capable
  providers more honestly

Anthropic is now part of the official bundle through the `anthropic` extra,
with a completion and structured-output support story that stays explicit about
the lack of embeddings.

If your pipelines checkpoint frequently, you can also enable the Rust checkpoint hot path
from the core package:

```bash
pip install "agora-etl[rs]" "agora-etl-plugins[redis]"
```

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

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
