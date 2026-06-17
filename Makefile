# =============================================================================
# agora-etl-plugins — Makefile
# =============================================================================

VENV   := .venv
WHEEL_VENV := .wheel-smoke-venv
BOOTSTRAP_PYTHON ?= $(or $(shell command -v python 2>/dev/null),$(shell command -v python3.11 2>/dev/null),$(shell command -v python3 2>/dev/null))
PYTHON := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip
RUFF   := $(VENV)/bin/ruff
PYTEST := $(VENV)/bin/pytest
MYPY   := $(VENV)/bin/mypy
WHEEL_PYTHON := $(WHEEL_VENV)/bin/python
WHEEL_PIP := $(WHEEL_VENV)/bin/pip
WHEEL_PYTEST := $(WHEEL_VENV)/bin/pytest
WHEEL_PIP_INDEX_URL ?= https://pypi.org/simple
WHEEL_CORE_REQUIREMENT ?= agora-etl==0.4.1
DIST_WHEEL = $(firstword $(wildcard dist/*.whl))
COMPOSE := docker compose -f docker-compose.yaml
COMPOSE_KAFKA_CLUSTER := docker compose -f docker-compose.kafka-cluster.yaml
COMPOSE_SECURE_KAFKA := docker compose -f docker-compose.kafka-secure.yaml
COMPOSE_POSTGRES_HA := docker compose -f docker-compose.postgres-ha.yaml
COMPOSE_REDIS_SENTINEL := docker compose -f docker-compose.redis-sentinel.yaml
COMPOSE_REDIS_SECURE := docker compose -f docker-compose.redis-secure.yaml
COMPOSE_REDIS_CLUSTER := docker compose -f docker-compose.redis-cluster.yaml
COMPOSE_REDIS_STACK := docker compose -f docker-compose.redis-stack.yaml
COMPOSE_REDIS_REDLOCK := docker compose -f docker-compose.redis-redlock.yaml
KAFKA_SECURE_ASSET_DIR := $(CURDIR)/.docker/kafka-secure
REDIS_SECURE_ASSET_DIR := $(CURDIR)/.docker/redis-secure
INTEGRATION_REDIS_URL ?= redis://127.0.0.1:16379/0
INTEGRATION_REDIS_SENTINEL_URL ?= redis://127.0.0.1:16383/0
INTEGRATION_REDIS_CLUSTER_URL ?= redis://127.0.0.1:16385/0
INTEGRATION_REDIS_STACK_URL ?= redis://127.0.0.1:16388/0
INTEGRATION_REDIS_REDLOCK_URLS ?= redis://127.0.0.1:16389/0,redis://127.0.0.1:16390/0,redis://127.0.0.1:16391/0
INTEGRATION_REDIS_SENTINEL_SOAK_CYCLES ?= 3
INTEGRATION_KAFKA_BOOTSTRAP ?= 127.0.0.1:19092
INTEGRATION_KAFKA_CLUSTER_BOOTSTRAP ?= 127.0.0.1:19192,127.0.0.1:19193,127.0.0.1:19194
INTEGRATION_KAFKA_CLUSTER_SOAK_CYCLES ?= 5
INTEGRATION_POSTGRES_HA_SOAK_CYCLES ?= 2
INTEGRATION_KAFKA_SCRAM_BOOTSTRAP ?= 127.0.0.1:19093
INTEGRATION_KAFKA_MTLS_BOOTSTRAP ?= 127.0.0.1:19094
INTEGRATION_POSTGRES_DSN ?= postgresql://agora:agora@127.0.0.1:15432/agora_test
INTEGRATION_KAFKA_CLUSTER_POSTGRES_DSN ?= postgresql://agora:agora@127.0.0.1:15433/agora_test
INTEGRATION_POSTGRES_HA_DSN ?= postgresql://agora:agora@127.0.0.1:15435,127.0.0.1:15436/agora_test?target_session_attrs=read-write
INTEGRATION_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_REDIS_URL=$(INTEGRATION_REDIS_URL) \
	AGORA_TEST_KAFKA_BOOTSTRAP=$(INTEGRATION_KAFKA_BOOTSTRAP) \
	AGORA_TEST_POSTGRES_DSN=$(INTEGRATION_POSTGRES_DSN) \
	PYTHONPATH=src
INTEGRATION_CLUSTER_KAFKA_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_ALLOW_DOCKER_CONTROL=1 \
	AGORA_TEST_KAFKA_CLUSTER_BOOTSTRAP=$(INTEGRATION_KAFKA_CLUSTER_BOOTSTRAP) \
	AGORA_TEST_POSTGRES_DSN=$(INTEGRATION_KAFKA_CLUSTER_POSTGRES_DSN) \
	PYTHONPATH=src
INTEGRATION_CLUSTER_KAFKA_SOAK_ENV = $(INTEGRATION_CLUSTER_KAFKA_ENV) \
	AGORA_TEST_KAFKA_CLUSTER_SOAK_CYCLES=$(INTEGRATION_KAFKA_CLUSTER_SOAK_CYCLES)
INTEGRATION_POSTGRES_HA_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_ALLOW_DOCKER_CONTROL=1 \
	AGORA_TEST_POSTGRES_HA_DSN=$(INTEGRATION_POSTGRES_HA_DSN) \
	AGORA_TEST_POSTGRES_HA_SOAK_CYCLES=$(INTEGRATION_POSTGRES_HA_SOAK_CYCLES) \
	PYTHONPATH=src
INTEGRATION_KAFKA_POSTGRES_HA_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_ALLOW_DOCKER_CONTROL=1 \
	AGORA_TEST_KAFKA_BOOTSTRAP=$(INTEGRATION_KAFKA_BOOTSTRAP) \
	AGORA_TEST_POSTGRES_DSN=$(INTEGRATION_POSTGRES_HA_DSN) \
	AGORA_TEST_POSTGRES_HA_DSN=$(INTEGRATION_POSTGRES_HA_DSN) \
	AGORA_TEST_POSTGRES_HA_SOAK_CYCLES=$(INTEGRATION_POSTGRES_HA_SOAK_CYCLES) \
	PYTHONPATH=src
INTEGRATION_SECURE_KAFKA_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_KAFKA_SCRAM_BOOTSTRAP=$(INTEGRATION_KAFKA_SCRAM_BOOTSTRAP) \
	AGORA_TEST_KAFKA_MTLS_BOOTSTRAP=$(INTEGRATION_KAFKA_MTLS_BOOTSTRAP) \
	AGORA_TEST_KAFKA_SCRAM_USERNAME=agora \
	AGORA_TEST_KAFKA_SCRAM_PASSWORD_FILE=$(KAFKA_SECURE_ASSET_DIR)/scram-password.txt \
	AGORA_TEST_KAFKA_CA_FILE=$(KAFKA_SECURE_ASSET_DIR)/ca.crt \
	AGORA_TEST_KAFKA_CLIENT_CERT_FILE=$(KAFKA_SECURE_ASSET_DIR)/client.crt \
	AGORA_TEST_KAFKA_CLIENT_KEY_FILE=$(KAFKA_SECURE_ASSET_DIR)/client.key \
	AGORA_TEST_SCHEMA_REGISTRY_URL=https://127.0.0.1:18081 \
	AGORA_TEST_SCHEMA_REGISTRY_MTLS_URL=https://127.0.0.1:18443 \
	AGORA_TEST_SCHEMA_REGISTRY_USERNAME=agora \
	AGORA_TEST_SCHEMA_REGISTRY_PASSWORD_FILE=$(KAFKA_SECURE_ASSET_DIR)/schema-registry-password.txt \
	PYTHONPATH=src
INTEGRATION_REDIS_SENTINEL_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_ALLOW_DOCKER_CONTROL=1 \
	AGORA_TEST_REDIS_SENTINEL_URL=$(INTEGRATION_REDIS_SENTINEL_URL) \
	PYTHONPATH=src
INTEGRATION_REDIS_SENTINEL_SOAK_ENV = $(INTEGRATION_REDIS_SENTINEL_ENV) \
	AGORA_TEST_REDIS_SENTINEL_FAILOVER_CYCLES=$(INTEGRATION_REDIS_SENTINEL_SOAK_CYCLES)
INTEGRATION_REDIS_ENTERPRISE_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_ALLOW_DOCKER_CONTROL=1 \
	AGORA_TEST_REDIS_SENTINEL_URL=$(INTEGRATION_REDIS_SENTINEL_URL) \
	AGORA_TEST_REDIS_CLUSTER_URL=$(INTEGRATION_REDIS_CLUSTER_URL) \
	AGORA_TEST_REDIS_STACK_URL=$(INTEGRATION_REDIS_STACK_URL) \
	PYTHONPATH=src
INTEGRATION_REDIS_REDLOCK_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_REDIS_REDLOCK_URLS=$(INTEGRATION_REDIS_REDLOCK_URLS) \
	PYTHONPATH=src
INTEGRATION_SECURE_REDIS_ENV = AGORA_RUN_INTEGRATION=1 \
	AGORA_FAIL_ON_INTEGRATION_SKIP=1 \
	AGORA_TEST_REDIS_SECURE_HOST=127.0.0.1 \
	AGORA_TEST_REDIS_SECURE_PORT=16384 \
	AGORA_TEST_REDIS_SECURE_USERNAME=agora \
	AGORA_TEST_REDIS_SECURE_PASSWORD_FILE=$(REDIS_SECURE_ASSET_DIR)/password.txt \
	AGORA_TEST_REDIS_SECURE_CA_FILE=$(REDIS_SECURE_ASSET_DIR)/ca.crt \
	AGORA_TEST_REDIS_SECURE_ROGUE_CA_FILE=$(REDIS_SECURE_ASSET_DIR)/rogue-ca.crt \
	PYTHONPATH=src

.PHONY: help setup install lint format fix check test test-all test-integration \
	ci integration-up integration-down integration-ps integration-up-kafka-cluster \
	integration-down-kafka-cluster integration-ps-kafka-cluster integration-up-kafka-secure \
	integration-down-kafka-secure integration-ps-kafka-secure \
	integration-up-postgres-ha integration-down-postgres-ha integration-ps-postgres-ha \
	integration-up-redis-sentinel integration-down-redis-sentinel integration-ps-redis-sentinel \
	integration-up-redis-secure integration-down-redis-secure integration-ps-redis-secure \
	integration-up-redis-cluster integration-down-redis-cluster integration-ps-redis-cluster \
	integration-up-redis-stack integration-down-redis-stack integration-ps-redis-stack \
	integration-up-redis-redlock integration-down-redis-redlock integration-ps-redis-redlock \
	test-integration-kafka-redis-wedge \
	test-integration-periodic-matrix \
	test-integration-redis-sentinel test-integration-redis-sentinel-crash \
	test-integration-redis-sentinel-graceful test-integration-redis-sentinel-soak \
	test-integration-redis-sentinel-crash-soak test-integration-redis-sentinel-graceful-soak \
	test-integration-redis-secure-matrix test-integration-redis-enterprise-matrix \
	test-integration-redis-redlock \
	test-integration-postgres-ha test-integration-postgres-ha-replica-routing \
	test-integration-kafka-postgres-ha-failover \
	test-integration-kafka-cluster-failover test-integration-kafka-cluster-enterprise \
	test-integration-kafka-cluster-enterprise-soak test-integration-kafka-secure \
	test-integration-kafka-secure-enterprise-matrix \
	test-integration-kafka-secure-schema-failure-matrix build-dist test-installed-package-smoke \
	test-installed-wheel-contracts test-release-gate-wheel clean

.PHONY: test-release-gate-base test-release-gate-postgres \
	test-release-gate-kafka-secure test-release-gate-kafka-cluster test-release-gate-redis \
	test-release-gate-wheel

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup:  ## Create .venv and install all dependencies
	$(BOOTSTRAP_PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[all,dev]"

install:  ## Install/sync dependencies into existing .venv
	$(PIP) install -e ".[all,dev]"

lint:  ## Lint code (ruff check)
	$(RUFF) check .

format:  ## Format code (ruff format)
	$(RUFF) format .

fix:  ## Auto-fix lint + format issues
	$(RUFF) check --fix .
	$(RUFF) format .

check:  ## Lint + format check (no auto-fix, for CI)
	$(RUFF) check .
	$(RUFF) format --check .

test:  ## Run all tests (excluding integration)
	PYTHONPATH=src $(PYTEST) tests/ --ignore=tests/integration --cov --cov-report=term-missing -q

test-integration:  ## Run integration tests against Redis, Kafka, and Postgres
	$(INTEGRATION_ENV) $(PYTEST) tests/integration -q

test-integration-kafka-redis-wedge:  ## Run the real Kafka -> Redis wedge slice
	$(INTEGRATION_ENV) $(PYTEST) tests/integration/test_kafka_redis_wedge.py -q -vv -rs -s

test-integration-kafka-secure:  ## Run secure Kafka + schema registry integration tests
	$(INTEGRATION_SECURE_KAFKA_ENV) $(PYTEST) tests/integration/kafka/test_kafka_secure_integration.py -q

test-integration-kafka-secure-schema-failure-matrix:  ## Run secure schema failure-path matrix only
	$(INTEGRATION_SECURE_KAFKA_ENV) $(PYTEST) tests/integration/kafka/test_kafka_secure_integration.py \
		-k "json_schema_failure_path or protobuf_failure_path" -q -vv -rs -s

test-integration-periodic-matrix:  ## Run the scheduled enterprise integration matrix with cleanup
	bash ./scripts/run_periodic_integration_matrix.sh

test-integration-postgres-ha:  ## Run the Postgres HA failover reconnect/resume slice
	$(INTEGRATION_POSTGRES_HA_ENV) $(PYTEST) tests/integration/postgres/test_postgres_integration.py \
		-k "ha_failover" -q -vv -rs -s

test-integration-postgres-ha-replica-routing:  ## Run the Postgres HA replica-routing and staleness guard slice
	$(INTEGRATION_POSTGRES_HA_ENV) $(PYTEST) tests/integration/postgres/test_postgres_integration.py \
		-k "standby_route_fail_closes_when_replica_is_stale or prefer_standby_falls_back_to_primary_across_multi_cycle_failover" \
		-q -vv -rs -s

test-integration-kafka-postgres-ha-failover:  ## Run the Kafka -> Postgres HA failover wedge slice
	$(INTEGRATION_KAFKA_POSTGRES_HA_ENV) $(PYTEST) tests/integration/test_kafka_postgres_wedge.py \
		-k "postgres_failover" -q -vv -rs -s

test-all:  ## Run unit tests first, then integration tests
	$(MAKE) test
	$(MAKE) test-integration

ci:  ## Full CI check: lint + format + tests
	$(MAKE) check
	$(MYPY) src/agora_plugins
	$(MAKE) test

integration-up:  ## Start local Redis, Kafka, and Postgres for integration tests
	$(COMPOSE) up -d --wait

integration-up-kafka-cluster:  ## Start local Redis, Postgres, and a 3-broker Kafka cluster
	$(COMPOSE_KAFKA_CLUSTER) up -d --wait

integration-up-kafka-secure:  ## Start a secure Kafka broker for enterprise integration tests
	sh ./scripts/generate_kafka_secure_assets.sh
	$(COMPOSE_SECURE_KAFKA) up -d --wait --force-recreate kafka-secure schema-registry schema-registry-secure
	$(COMPOSE_SECURE_KAFKA) run --rm kafka-secure-init

integration-up-postgres-ha:  ## Start the Postgres HA failover topology
	$(COMPOSE_POSTGRES_HA) up -d --wait

integration-up-redis-sentinel:  ## Start the Redis Sentinel failover topology plus stable proxy
	$(COMPOSE_REDIS_SENTINEL) up -d --wait

integration-up-redis-secure:  ## Start the secure Redis TLS + ACL stack
	sh ./scripts/generate_redis_secure_assets.sh
	$(COMPOSE_REDIS_SECURE) up -d --wait

integration-up-redis-cluster:  ## Start the Redis Cluster topology
	$(COMPOSE_REDIS_CLUSTER) up -d --wait

integration-up-redis-stack:  ## Start Redis Stack with RediSearch
	$(COMPOSE_REDIS_STACK) up -d --wait

integration-up-redis-redlock:  ## Start three independent Redis masters for Redlock tests
	$(COMPOSE_REDIS_REDLOCK) up -d --wait

integration-down:  ## Stop the local integration stack and remove volumes
	$(COMPOSE) down -v --remove-orphans

integration-down-kafka-cluster:  ## Stop the 3-broker Kafka cluster stack and remove volumes
	$(COMPOSE_KAFKA_CLUSTER) down -v --remove-orphans

integration-down-kafka-secure:  ## Stop the secure Kafka stack and remove volumes
	$(COMPOSE_SECURE_KAFKA) down -v --remove-orphans

integration-down-postgres-ha:  ## Stop the Postgres HA topology and remove volumes
	$(COMPOSE_POSTGRES_HA) down -v --remove-orphans

integration-down-redis-sentinel:  ## Stop the Redis Sentinel topology and remove volumes
	$(COMPOSE_REDIS_SENTINEL) down -v

integration-down-redis-secure:  ## Stop the secure Redis stack and remove volumes
	$(COMPOSE_REDIS_SECURE) down -v

integration-down-redis-cluster:  ## Stop the Redis Cluster topology and remove volumes
	$(COMPOSE_REDIS_CLUSTER) down -v

integration-down-redis-stack:  ## Stop Redis Stack and remove volumes
	$(COMPOSE_REDIS_STACK) down -v

integration-down-redis-redlock:  ## Stop the Redlock Redis masters and remove volumes
	$(COMPOSE_REDIS_REDLOCK) down -v

integration-ps:  ## Show status of the local integration stack
	$(COMPOSE) ps

integration-ps-kafka-cluster:  ## Show status of the 3-broker Kafka cluster stack
	$(COMPOSE_KAFKA_CLUSTER) ps

integration-ps-kafka-secure:  ## Show status of the secure Kafka stack
	$(COMPOSE_SECURE_KAFKA) ps

integration-ps-postgres-ha:  ## Show status of the Postgres HA topology
	$(COMPOSE_POSTGRES_HA) ps

integration-ps-redis-sentinel:  ## Show status of the Redis Sentinel topology
	$(COMPOSE_REDIS_SENTINEL) ps

integration-ps-redis-secure:  ## Show status of the secure Redis stack
	$(COMPOSE_REDIS_SECURE) ps

integration-ps-redis-cluster:  ## Show status of the Redis Cluster topology
	$(COMPOSE_REDIS_CLUSTER) ps

integration-ps-redis-stack:  ## Show status of Redis Stack
	$(COMPOSE_REDIS_STACK) ps

integration-ps-redis-redlock:  ## Show status of Redlock Redis masters
	$(COMPOSE_REDIS_REDLOCK) ps

test-integration-redis-sentinel:  ## Run Redis Sentinel failover semantics
	$(INTEGRATION_REDIS_SENTINEL_ENV) $(PYTEST) tests/integration/redis/test_redis_sentinel_integration.py \
		-k "not multi_cycle" -q -vv -rs -s

test-integration-redis-sentinel-crash:  ## Run Redis Sentinel crash-failover reconnect semantics
	$(INTEGRATION_REDIS_SENTINEL_ENV) $(PYTEST) tests/integration/redis/test_redis_sentinel_integration.py \
		-k "crash and not multi_cycle" -q -vv -rs -s

test-integration-redis-sentinel-graceful:  ## Run Redis Sentinel graceful-failover reclaim semantics
	$(INTEGRATION_REDIS_SENTINEL_ENV) $(PYTEST) tests/integration/redis/test_redis_sentinel_integration.py \
		-k "graceful and not multi_cycle" -q -vv -rs -s

test-integration-redis-sentinel-soak:  ## Run the Redis Sentinel multi-cycle failover soak slice
	$(MAKE) integration-down-redis-sentinel
	$(MAKE) integration-up-redis-sentinel
	$(INTEGRATION_REDIS_SENTINEL_SOAK_ENV) $(PYTEST) tests/integration/redis/test_redis_sentinel_integration.py \
		-k "multi_cycle" -q -vv -rs -s

test-integration-redis-sentinel-crash-soak:  ## Run the Redis Sentinel multi-cycle crash-failover soak slice
	$(MAKE) integration-down-redis-sentinel
	$(MAKE) integration-up-redis-sentinel
	$(INTEGRATION_REDIS_SENTINEL_SOAK_ENV) $(PYTEST) tests/integration/redis/test_redis_sentinel_integration.py \
		-k "crash and multi_cycle" -q -vv -rs -s

test-integration-redis-sentinel-graceful-soak:  ## Run the Redis Sentinel multi-cycle graceful-failover soak slice
	$(MAKE) integration-down-redis-sentinel
	$(MAKE) integration-up-redis-sentinel
	$(INTEGRATION_REDIS_SENTINEL_SOAK_ENV) $(PYTEST) tests/integration/redis/test_redis_sentinel_integration.py \
		-k "graceful and multi_cycle" -q -vv -rs -s

test-integration-redis-secure-matrix:  ## Run Redis TLS/ACL auth-failure matrix
	$(INTEGRATION_SECURE_REDIS_ENV) $(PYTEST) tests/integration/redis/test_redis_secure_integration.py -q -vv -rs -s

test-integration-redis-enterprise-matrix:  ## Run Redis Sentinel/Cluster/RediSearch enterprise matrix
	$(MAKE) integration-down-redis-stack
	$(MAKE) integration-down-redis-cluster
	$(MAKE) integration-down-redis-sentinel
	$(MAKE) integration-up-redis-sentinel
	$(MAKE) integration-up-redis-cluster
	$(MAKE) integration-up-redis-stack
	$(INTEGRATION_REDIS_ENTERPRISE_ENV) $(PYTEST) tests/integration/redis/test_redis_enterprise_matrix.py -q -vv -rs -s

test-integration-redis-redlock:  ## Run distributed coordinator Redlock quorum topology tests
	$(MAKE) integration-down-redis-redlock
	$(MAKE) integration-up-redis-redlock
	$(INTEGRATION_REDIS_REDLOCK_ENV) $(PYTEST) tests/integration/test_distributed_integration.py \
		-k "redlock" -q -vv -rs -s

test-integration-kafka-cluster-failover:  ## Run the Kafka multi-broker leader-failover wedge test
	$(INTEGRATION_CLUSTER_KAFKA_ENV) $(PYTEST) tests/integration/test_kafka_postgres_wedge.py \
		-k "leader_failover or rolling_restart or coordinator_failover" -q -vv -rs -s

test-integration-kafka-cluster-enterprise:  ## Run the Kafka enterprise cluster regression slice
	$(INTEGRATION_CLUSTER_KAFKA_ENV) $(PYTEST) tests/integration/test_kafka_postgres_wedge.py \
		-k "leader_failover or rolling_restart or coordinator_failover or crash_windows_survive_multi_broker_rolling_restart or crash_windows_survive_coordinator_failover or seek_to_offsets_partial_replay_survives_multi_broker_rolling_restart or live_tail_after_seek_to_end_survives_multi_broker_rolling_restart" \
		-q -vv -rs -s

test-integration-kafka-cluster-enterprise-soak:  ## Run the Kafka enterprise cluster soak slice with repeated restart cycles
	$(INTEGRATION_CLUSTER_KAFKA_SOAK_ENV) $(PYTEST) tests/integration/test_kafka_postgres_wedge.py \
		-k "rolling_restart or seek_to_offsets_partial_replay_survives_multi_broker_rolling_restart or live_tail_after_seek_to_end_survives_multi_broker_rolling_restart" \
		-q -vv -rs -s

test-integration-kafka-secure-enterprise-matrix:  ## Run the secure Kafka/schema-registry compatibility matrix
	$(INTEGRATION_SECURE_KAFKA_ENV) $(PYTEST) tests/integration/kafka/test_kafka_secure_integration.py \
		-k "avro_round_trip or avro_round_trip_over_secure_schema_registry_mtls or json_schema_round_trip or json_schema_failure_path or protobuf_round_trip or protobuf_failure_path or schema_registry_mtls_rejects_client_without_certificate or schema_registry_mtls_rejects_client_with_untrusted_certificate or schema_registry_auth_failures_are_rejected_clearly" \
		-q -vv -rs -s

test-release-gate-base:  ## Narrow release gate for the base Redis/Kafka wedge
	$(MAKE) test-integration-kafka-redis-wedge

test-release-gate-postgres:  ## Narrow release gate for Postgres HA and the Kafka -> Postgres wedge
	$(MAKE) test-integration-postgres-ha
	$(MAKE) test-integration-postgres-ha-replica-routing
	$(MAKE) test-integration-kafka-postgres-ha-failover

test-release-gate-kafka-secure:  ## Narrow release gate for secure Kafka/schema-registry paths
	$(MAKE) test-integration-kafka-secure-enterprise-matrix

test-release-gate-kafka-cluster:  ## Narrow release gate for Kafka cluster failover/replay semantics
	$(MAKE) test-integration-kafka-cluster-enterprise

test-release-gate-redis:  ## Narrow release gate for Redis secure and enterprise topology semantics
	$(MAKE) test-integration-redis-secure-matrix
	$(MAKE) test-integration-redis-enterprise-matrix
	$(MAKE) test-integration-redis-redlock

build-dist:  ## Build wheel and sdist release artifacts
	rm -rf dist
	$(VENV)/bin/hatch build

test-installed-package-smoke: build-dist  ## Install the built wheel and verify public entry points
	rm -rf $(WHEEL_VENV)
	$(BOOTSTRAP_PYTHON) -m venv $(WHEEL_VENV)
	$(WHEEL_PIP) install --index-url "$(WHEEL_PIP_INDEX_URL)" "$(WHEEL_CORE_REQUIREMENT)"
	$(WHEEL_PIP) install --index-url "$(WHEEL_PIP_INDEX_URL)" "$(DIST_WHEEL)[all]" pytest==9.0.3 pytest-asyncio==1.4.0
	PYTHONPATH= $(WHEEL_PYTHON) scripts/smoke_installed_package.py

test-installed-wheel-contracts: test-installed-package-smoke  ## Run contract tests against the installed wheel, not src
	PYTHONPATH= $(WHEEL_PYTEST) --import-mode=importlib -q \
		tests/cron/test_cron_support.py \
		tests/distributed/test_coordinator.py \
		tests/kafka/test_kafka_dlq.py \
		tests/kafka/test_kafka_sink.py \
		tests/kafka/test_kafka_source.py \
		tests/postgres/test_postgres_dlq.py \
		tests/postgres/test_postgres_observability.py \
		tests/redis/test_redis_dlq.py \
		tests/redis/test_redis_sink.py

test-release-gate-wheel: test-installed-wheel-contracts  ## Release gate for wheel/import consistency

clean:  ## Remove build artifacts and caches
	rm -rf $(WHEEL_VENV)
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist"  -not -path "*/.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "build" -not -path "*/.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
