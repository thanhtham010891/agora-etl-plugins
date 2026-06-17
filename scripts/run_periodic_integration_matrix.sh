#!/usr/bin/env bash
set -euo pipefail

cleanup_all() {
  make integration-down >/dev/null 2>&1 || true
  make integration-down-postgres-ha >/dev/null 2>&1 || true
  make integration-down-kafka-secure >/dev/null 2>&1 || true
  make integration-down-kafka-cluster >/dev/null 2>&1 || true
  make integration-down-redis-secure >/dev/null 2>&1 || true
  make integration-down-redis-sentinel >/dev/null 2>&1 || true
  make integration-down-redis-cluster >/dev/null 2>&1 || true
  make integration-down-redis-stack >/dev/null 2>&1 || true
  make integration-down-redis-redlock >/dev/null 2>&1 || true
}

trap cleanup_all EXIT

echo "==> Wheel/import consistency gate"
make test-release-gate-wheel

echo "==> Base Kafka/Redis/Postgres stack gate"
make integration-down
make integration-up
make test-release-gate-base

echo "==> Postgres HA + Kafka/Postgres wedge gate"
make integration-up-postgres-ha
make test-release-gate-postgres
make integration-down-postgres-ha
make integration-down

echo "==> Secure Kafka/schema-registry gate"
make integration-up-kafka-secure
make test-release-gate-kafka-secure
make integration-down-kafka-secure

echo "==> Kafka cluster failover/replay gate"
make integration-up-kafka-cluster
make test-release-gate-kafka-cluster
make integration-down-kafka-cluster

echo "==> Redis secure gate"
make integration-up-redis-secure
make test-integration-redis-secure-matrix
make integration-down-redis-secure

echo "==> Redis enterprise topology gate"
make test-integration-redis-enterprise-matrix

echo "==> Redis Redlock quorum gate"
make test-integration-redis-redlock

echo "Periodic integration matrix completed."
