# Changelog

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
