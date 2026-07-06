# Agora ETL Plugins

**Official plugin collection for [agora-etl](https://pypi.org/project/agora-etl/) — Redis, Kafka, PostgreSQL, BigQuery, S3, cron scheduling, distributed coordination, and Anthropic AI support.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl-plugins)](https://pypi.org/project/agora-etl-plugins/)

---

## Overview

`agora-etl-plugins` extends [agora-etl](https://pypi.org/project/agora-etl/) with production-ready integrations. Plugins are auto-discovered via Python entry-points — install the package and they register themselves automatically, no manual wiring needed.

This package owns backend depth, not runtime semantics:

- `agora-etl` owns pipeline behavior, recovery contracts, CLI diagnostics, and
  public extension boundaries
- `agora-etl-plugins` owns first-party Redis, Kafka, PostgreSQL, BigQuery, S3,
  cron, distributed coordination, and Anthropic integrations
- `agora-etl-rs` stays optional and accelerates the runtime without changing
  the plugin contract

If a question is about delivery guarantees, checkpoint semantics, lane
selection, or replay contracts, the source of truth is still the core docs.
If a question is about backend maturity, backend runbooks, or integration
extras, this package is the right boundary.

Canonical ecosystem docs live in the public Agora docs surface:

- Plugin ecosystem overview:
  <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/index.md>
- Production readiness, compatibility, and local validation guidance:
  <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/production-readiness.md>
- Source-of-truth map:
  <https://github.com/thanhtham010891/agora-etl/blob/main/docs/source-of-truth.md>
- Core docs home:
  <https://github.com/thanhtham010891/agora-etl/tree/main/docs>

This README stays focused on bundle quickstart information.

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
pip install "agora-etl-plugins[bigquery]"     # BigQuery table/query source and batch table sink
pip install "agora-etl-plugins[s3]"           # S3 prefix source and partitioned dataset sink
pip install "agora-etl-plugins[anthropic]"    # Anthropic completion and structured-output provider
pip install "agora-etl-plugins[all]"          # Everything in one install
```

This package tracks the `agora-etl 0.4.x` compatibility line.
Current floor: `agora-etl>=0.4.5,<1`.
Supported Python versions: `3.11`, `3.12`, and `3.13`.

The bundle focuses on a small set of official backend families and helpers:
Redis, Kafka, PostgreSQL, BigQuery, S3, cron scheduling, distributed
coordination, and Anthropic. Runtime semantics, `agora doctor`, and public
data-plane contracts still come from the `agora-etl` core line.

Anthropic ships as an official first-party extra through `anthropic`, with a
completion and structured-output support story that stays explicit about the
lack of embeddings.

If your pipelines checkpoint frequently, you can also enable the Rust checkpoint hot path
from the core package:

```bash
pip install "agora-etl[rs]" "agora-etl-plugins[redis]"
```

## Local integration testing

The repository includes a local Docker stack and integration coverage for
Redis, Kafka, PostgreSQL, S3-compatible object storage, and an opt-in live
BigQuery validation slice.

Typical flow:

```bash
make catalog
make topology ACTION=up TOPOLOGY=base
make topology ACTION=status TOPOLOGY=base
make integration SUITE=integration_full
make topology ACTION=down TOPOLOGY=base
```

`make integration SUITE=integration_full` sets `AGORA_RUN_INTEGRATION=1`
through the declarative testkit and runs only `tests/integration`. Use
`make catalog` for all supported topologies, suites, gates, and matrices.

Backend-specific validation details live in the canonical docs:

- plugin readiness and release gates:
  <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/production-readiness.md>
- BigQuery family boundary and verification notes:
  <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/bigquery.md>
- S3 family boundary and verification notes:
  <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/s3.md>

Use the family docs above for BigQuery and S3-specific local validation flows,
including the live BigQuery gate and the MinIO-backed S3 dataset checks.

---

## Documentation map

This README does not duplicate the family docs. For backend boundaries,
validation notes, and operator guidance, prefer:

- <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/index.md>
- <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/production-readiness.md>
- <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/contract.md>

| Family | Install extra | Start here |
|---|---|---|
| Redis | `redis` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/redis.md> |
| Kafka | `kafka` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/kafka.md> |
| PostgreSQL | `postgres` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/postgresql.md> |
| BigQuery | `bigquery` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/bigquery.md> |
| S3 | `s3` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/s3.md> |
| Cron | `cron` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/scheduling.md> |
| Distributed | `distributed` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/distributed.md> |
| Anthropic | `anthropic` | <https://github.com/thanhtham010891/agora-etl/blob/main/docs/plugins/anthropic.md> |

For a backend capability matrix or production-boundary claim, trust the
canonical docs above over this README summary.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
