# Changelog

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
