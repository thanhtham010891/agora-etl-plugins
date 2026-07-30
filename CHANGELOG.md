# Changelog

## 0.4.3 (July 25, 2026)

- Raised the core compatibility floor to `agora-etl>=0.4.6,<1`. This is a
  fail-closed packaging correction: the official backend bundle now requires
  the core release that exports `SourceIdentity`, source-identity mismatch
  policy, and delivery-policy preflight contracts it uses at import time.
- Added release evidence for the first-party backend bundle: full local quality
  verification, wheel smoke against the matching core artifact, and the
  declarative local-backend integration matrix. Live BigQuery validation
  remains explicitly credential- and dataset-gated.
- Hardened delivery declarations for generic PostgreSQL and Redis writes:
  application callback keys are not inferred replay-safe; an explicit boolean
  contract is accepted only for PostgreSQL upserts and Redis `SET` mode.
- Applied the shared Kafka failure classifier to source startup and polling
  retries, so broker connectivity failures retain a consistent retry decision.
- Added the certified Redis Streams → PostgreSQL profile. It derives a stable
  `stream:message_id` delivery key, requires PostgreSQL flush before `XACK`,
  and reports fail-closed acceptance findings for unsafe acknowledgement,
  upsert, conflict-key, write-safety, replay-capability, or readiness
  configuration. A backend-real Redis/PostgreSQL integration test proves the
  upsert-then-ack path.

## 0.4.2 (July 16, 2026)

- Hardened production delivery and recovery contracts: BigQuery checkpointing
  now requires an explicitly unique cursor or a composite cursor; S3 dataset
  keys are run-scoped and conditionally created; Kafka transaction and retry
  options now match their public contract; and Redis fencing counters persist
  by default across idle periods.
- Raised the core compatibility floor to `agora-etl>=0.4.5,<1` so the plugin
  bundle tracks the new supportability and doctor-contract surfaces released on
  the current `0.4.x` core line.
- Added a defensive `DLQPayloadPolicy` compatibility shim in
  `agora_plugins.dlq_policy` so redaction and encryption behavior stays stable
  during mixed-version local development and wheel smoke validation.
- Added Kafka poison-policy compatibility handling so the published wheel can
  continue exposing stable poison-record metadata even when a local environment
  lags behind the newest core failure exports.
- Added secure-by-default Kafka plaintext posture warnings for obviously
  non-local brokers or non-dev environments, helping operators catch unsafe
  transport assumptions before shared or production deployment.
- Suppressed upstream redis-py cluster driver deprecation noise in the async
  Redis cluster connection path until the dependency exposes a non-deprecated
  constructor path.
- Expanded CI and release hardening with diagnostics artifact collection,
  a reusable Postgres release-gate wrapper, a lane-selectable nightly periodic
  matrix, and a stronger release workflow that verifies non-integration and
  base-gate slices before building distributions.

## 0.4.1 (July 6, 2026)

- Added a dedicated local `test-release-gate-bigquery-ga` target plus stronger
  live BigQuery coverage for denied-dataset query isolation, multi-page query
  batching, and sink acceptance-report behavior after failure and recovery.
- Added BigQuery source/sink readiness hooks with machine-readable
  `health_snapshot()` and `acceptance_report(...)` surfaces, plus bounded
  query-result batching so explicit query mode no longer materializes the full
  result set before emitting rows.
- Expanded the experimental `BigQueryStorageWriteSink` into a phase-2 surface
  for append-only default-stream writes, including typed schema support for
  `DATE`/`DATETIME`/`TIME`/`TIMESTAMP`/`NUMERIC`/`BIGNUMERIC`, logical-flush
  chunking under the request-size guard, stronger partial-failure unit
  coverage, and a separate local live BigQuery Storage Write verification
  slice.
- Promoted `BigQueryStorageWriteSink` into the public BigQuery support
  boundary by wiring it into the local validation gate, periodic integration
  matrix, wheel smoke coverage, and production-readiness docs while keeping
  its append-only `_default`-stream boundary explicit.
- Added official BigQuery source/sink coverage for dataset-style
  table/query extraction and batch-oriented table loads, including
  checkpoint-aware table mode, explicit full-rerun query mode semantics, and
  an opt-in live GCP verification suite for local release validation.
- Added official S3 source/sink coverage for lexically ordered
  dataset prefixes, JSONL/CSV/Parquet file formats, object-boundary replay,
  deterministic partitioned file naming, and a MinIO-backed local integration
  slice.
- Expanded the public BigQuery and S3 dataset support story with
  installed-wheel smoke coverage, package CI evidence, and stronger local
  dataset validation for both surfaces.
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
  aligned with `agora-etl-plugins` as the official plugin bundle.

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
- Added backend-real validation gates for Kafka failover/replay, PostgreSQL HA,
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
- Added regression coverage for the public Anthropic provider path, including structured-output and unsupported-embedding behavior

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

- Added Kafka and PostgreSQL plugins to the official `agora-etl-plugins` bundle
- Fixed cron integration with `agora.runner.Schedule.cron(...)`
- Updated examples and package metadata to use the `agora_plugins.*` namespace consistently
- Hardened plugin manifests and source imports for local development and release packaging

## 0.1.0 (May 24, 2026)

- Initial release — package structure and entry-point scaffolding.
