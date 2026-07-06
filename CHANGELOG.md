# Changelog

## 0.4.1 (July 6, 2026)

- Hardened Redis stream poison-record handling so reclaimed deserialize
  failures are acknowledged once `LOG_AND_CONTINUE` or DLQ routing has handled
  them, preventing pending-message poison loops and keeping Redis stream
  recovery state consistent.
- Reworked Redis DLQ index maintenance to use atomic Lua upsert and
  acknowledge flows, including Redis Cluster-safe key tagging and legacy index
  fallback during reads and acknowledgements.
- Hardened `RedisEmbeddingStore` distributed marking so long-running semantic
  scans renew the cross-worker lock and fail closed if the lock can no longer
  be renewed before writing dedup markers.
- Made `fallback_to_local` lease handling internally consistent in
  `RedisWorkerCoordinator`, including local acquire, validate, renew, and
  release behavior when Redis coordination is unavailable.
- Tightened Redlock fencing so one authoritative token is reserved on the
  primary Redis connection and the same token is validated across quorum-node
  acquire, renew, release, and lease-check flows.
- Made PostgreSQL sink batch writes and `ALIGN_TO_TARGET` flushes atomic across
  SQL, COPY, and COPY-merge paths so late write failures roll back the full
  buffered flush instead of partially committing aligned rows.
- Hardened PostgreSQL source replica-staleness guards so primary-only fallback
  rejects non-primary connections and active standby streams fail closed if
  replay lag exceeds the configured budget mid-stream.
- Changed Kafka poison-record handling so `DLQ_AND_CONTINUE` keeps progress on
  handled records even when the DLQ write itself fails, while
  `DLQ_AND_FAIL_CLOSED` still blocks advancement.
- Bounded Kafka Schema Registry deserializer caches, normalized schema
  comparisons more strictly, and changed default auto-registration to only
  bootstrap missing subjects instead of always registering new versions.
- Hardened the Anthropic provider around supported-model validation,
  structured-output repair, truncated responses, retry-after aware retry
  behavior, and refreshed default/allowlisted Claude model ids away from
  retired 3.x-era snapshots.
- Refreshed package README and security wording so the public narrative stays
  aligned with `agora-etl-plugins` as the official first-party plugin package.

## 0.4.0 (June 17, 2026)

- Moved `agora-etl-plugins` onto the `agora-etl 0.4.x` compatibility line with
  a floor of `agora-etl>=0.4.1,<1`
- Added first-class Kafka, PostgreSQL, and Redis connection security settings,
  secret-file/env resolution, and fail-closed validation for unsupported
  combinations
- Added Kafka Schema Registry support for Avro, JSON Schema, and Protobuf,
  transactional delivery hooks, source health/metrics, tracing, poison-record
  policies, and Kafka-backed DLQ replay
- Added PostgreSQL pooled writes, SQL/COPY/COPY-merge modes, target-schema
  safety policies, HA/replica routing controls, schema-apply locking,
  observability, and SQL-backed DLQ replay
- Added Redis Sentinel, Cluster, TLS/ACL, Redis Stack, and quorum coordination
  support together with stream reclaim/fairness and state atomicity controls
- Added backend-real release gates for Kafka failover/replay, PostgreSQL HA,
  Redis deployment topologies, secure transports, and cross-backend delivery
  wedges
- Added installed-wheel contract tests that verify package metadata, optional
  extras, public imports, and plugin entry points outside the source tree
- Removed the redundant plugin-side `pytest-cov` development constraint so the
  `dev` extra resolves consistently with the `agora-etl 0.4.1` toolchain
- Split Kafka source models, poison routing, rebalance helpers, and offset
  normalization into focused internal modules while preserving public imports
- Split PostgreSQL identifier safety, metric snapshots, connection pooling,
  SQL write planning, and write strategies into focused internal modules while
  preserving public imports
- Replaced placeholder package metadata URLs with real public GitHub package
  and docs links
- Rewrote the package README to reflect the current official bundle story
  instead of the old `0.3.3` compatibility era
- Added a first-class local integration workflow with `make integration-up`,
  `make integration-ps`, `make test-integration`, and `make integration-down`
- Fixed integration-suite expectations so Redis and PostgreSQL DLQ behavior is
  validated against current core semantics

## 0.3.2 (June 12, 2026)

- Promoted Anthropic support into the official `agora-etl-plugins` bundle through the `anthropic` extra
- Added the `agora.ai.providers` entry-point for `AnthropicProvider` under the `agora_plugins.anthropic` namespace
- Raised the core compatibility floor to `agora-etl>=0.3.3`
- Updated bundle docs and package quickstart guidance to position Anthropic as an official completion and structured-output integration
- Added regression coverage for the official Anthropic provider path, including structured-output and unsupported-embedding behavior

## 0.3.1 (June 3, 2026)

- Raised the core compatibility floor to `agora-etl>=0.2.2`
- Updated package quickstart guidance to call out `DeliveryConfig(batch_flush_interval_ms=...)` for long-lived Redis, Kafka, and PostgreSQL source pipelines
- Refreshed release metadata so the plugin bundle tracks the new long-lived worker observability and timed batch flush behavior in the core runtime

## 0.3.0 (June 2, 2026)

- Raised the core compatibility floor to `agora-etl>=0.2.1`
- Updated plugin docs to highlight `DeliveryConfig(batch_size=100)` for Redis, Kafka, and PostgreSQL source pipelines on the linear lane
- Documented that `BatchMiddleware` now works correctly with plugin sources that emit one record at a time, matching the `agora-etl 0.2.1` runtime fix
- Added regression coverage for running a plugin source through `BatchMapMiddleware` on the linear lane

## 0.2.2 (May 27, 2026)

- Reduced Kafka producer batch overhead by serializing synchronous batches eagerly and skipping the retry wrapper on the success path
- Restored Kafka send retries for custom retry policies that handle non-`KafkaError` transient failures
- Fixed async callable serializer detection so serializer objects with lifecycle hooks still serialize to bytes correctly
- Reduced Kafka source checkpoint churn by caching checkpoint payloads until processed offsets advance
- Improved Redis `set` batch throughput by switching TTL-free batch writes to `MSET`
- Added Redis stream consumer tuning knobs for batched acknowledgements and optional raw response handling
- Batched Redis stream acknowledgements and normalized byte-backed error payloads so shutdown, cancel, and fail-closed paths preserve cleaner replay metadata
- Reduced repeated `xadd(maxlen=..., approximate=True)` argument rebuilding in Redis stream sink batches
- Expanded Redis regression coverage for `MSET` batch writes and batched stream acknowledgements

## 0.2.1 (May 26, 2026)

- Fixed PostgreSQL source install guidance to point to `agora-etl-plugins[postgres]`
- Hardened Redis DLQ iteration so bounded reads stop early and avoid loading the full DLQ index at once
- Improved Redis state TTL precision with millisecond-expiry writes and safer handling of already-expired keys
- Reduced Redis semantic dedup scan pressure by switching embedding lookups from full-set reads to incremental `SSCAN` batches
- Fixed PostgreSQL schema-adapter introspection for schema-qualified tables such as `analytics.users`
- Hardened Kafka source and sink startup/shutdown recovery so serializer or deserializer lifecycle is rolled back on startup failure and best-effort cleanup still runs during teardown errors
- Fixed Confluent schema registry requests to URL-encode subject path segments safely
- Expanded regression coverage for distributed coordination, Kafka recovery paths, Redis DLQ iteration, Redis TTL semantics, Redis embedding dedup scans, and PostgreSQL schema-adapter introspection

## 0.2.0 (May 24, 2026)

- Added Kafka and PostgreSQL plugins to the official `agora-etl-plugins` distribution
- Fixed cron integration with `agora.runner.Schedule.cron(...)`
- Updated examples and package metadata to use the `agora_plugins.*` namespace consistently
- Hardened plugin manifests and source imports for local development and release packaging

## 0.1.0 (May 24, 2026)

- Initial release — package structure and entry-point scaffolding.
