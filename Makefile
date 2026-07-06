# =============================================================================
# agora-etl-plugins developer commands
#
# The Makefile is deliberately a small facade. Test topology, suite, gate, and
# matrix definitions live only in qa/testkit.toml and are executed by
# scripts/testkit.py. Run `make catalog` to discover their current names.
# =============================================================================

VENV := .venv
WHEEL_VENV := .wheel-smoke-venv
BOOTSTRAP_PYTHON ?= $(or $(shell command -v python3.11 2>/dev/null),$(shell command -v python3 2>/dev/null),$(shell command -v python 2>/dev/null))
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
RUFF := $(VENV)/bin/ruff
PYTEST := $(VENV)/bin/pytest
MYPY := $(VENV)/bin/mypy
WHEEL_PYTHON := $(WHEEL_VENV)/bin/python
WHEEL_PIP := $(WHEEL_VENV)/bin/pip
WHEEL_PYTEST := $(WHEEL_VENV)/bin/pytest
WHEEL_PIP_INDEX_URL ?= https://pypi.org/simple
WHEEL_CORE_REQUIREMENT ?= agora-etl==0.4.5
WHEEL_CORE_SOURCE ?=
WHEEL_CORE_INSTALL := $(if $(strip $(WHEEL_CORE_SOURCE)),$(WHEEL_CORE_SOURCE),$(WHEEL_CORE_REQUIREMENT))
DIST_WHEEL = $(firstword $(wildcard dist/*.whl))

TESTKIT := $(BOOTSTRAP_PYTHON) scripts/testkit.py
TESTKIT_FLAGS ?=
DIAGNOSTICS_DIR ?= .artifacts/integration-diagnostics

.PHONY: help catalog setup install lint format fix check test-unit verify \
	topology integration gate matrix diagnostics \
	package package-check verify-package clean \
	ci gate-run collect-integration-diagnostics build-dist \
	test-installed-package-smoke test-installed-wheel-contracts test-release-gate-wheel

define require_var
	@test -n "$($1)" || (echo "Set $1=..."; exit 1)
endef

help:  ## Show the small, supported developer command surface
	@printf "\nAgora ETL Plugins developer commands\n\n"
	@printf "  Bootstrap and quality\n"
	@printf "    make setup                         Create .venv and install all dependencies\n"
	@printf "    make verify                        Lint, type-check, and run non-integration tests\n"
	@printf "    make test-unit                     Run non-integration tests with coverage\n\n"
	@printf '%s\n' '  Declarative integration runner (see make catalog)'
	@printf "    make topology ACTION=up TOPOLOGY=s3\n"
	@printf "    make integration SUITE=kafka_secure\n"
	@printf "    make gate GATE=release_redis\n"
	@printf "    make matrix MATRIX=periodic LANES=base,postgres\n\n"
	@printf "  Packaging\n"
	@printf "    make package                       Build wheel and sdist\n"
	@printf "    make package-check                 Verify the wheel already in dist/\n"
	@printf "    make verify-package                Build, then verify that exact wheel\n\n"
	@printf "  Maintenance\n"
	@printf "    make diagnostics [DIAGNOSTICS_DIR=...]\n"
	@printf "    make clean\n\n"

catalog:  ## List the named topologies, suites, gates, and matrices
	$(TESTKIT) catalog

setup:  ## Create .venv and install all dependencies
	$(BOOTSTRAP_PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[all,dev]"

install:  ## Sync dependencies into an existing .venv
	@test -x "$(PYTHON)" || (echo "Run 'make setup' first."; exit 1)
	$(PIP) install -e ".[all,dev]"

lint:  ## Lint code with Ruff
	$(RUFF) check .

format:  ## Format code with Ruff
	$(RUFF) format .

fix:  ## Apply Ruff fixes and formatting
	$(RUFF) check --fix .
	$(RUFF) format .

check:  ## Check lint and formatting without modifying files
	$(RUFF) check .
	$(RUFF) format --check .

test-unit:  ## Run all non-integration tests with coverage
	PYTHONPATH=src $(PYTEST) tests/ --ignore=tests/integration --cov --cov-report=term-missing -q

verify:  ## Run lint, formatting, type checks, and non-integration tests
	$(MAKE) check
	$(MYPY) src/agora_plugins
	$(MAKE) test-unit

topology:  ## Manage a topology: make topology ACTION=up|down|status TOPOLOGY=name
	$(call require_var,ACTION)
	$(call require_var,TOPOLOGY)
	@case "$(ACTION)" in up|down|status) ;; *) echo "ACTION must be up, down, or status."; exit 1 ;; esac
	$(TESTKIT) $(TESTKIT_FLAGS) topology "$(ACTION)" "$(TOPOLOGY)"

integration:  ## Run a named integration suite: make integration SUITE=name
	$(call require_var,SUITE)
	$(TESTKIT) $(TESTKIT_FLAGS) suite run "$(SUITE)"

gate:  ## Run a named release gate: make gate GATE=name
	$(call require_var,GATE)
	$(TESTKIT) $(TESTKIT_FLAGS) gate run "$(GATE)"

matrix:  ## Run a named matrix: make matrix MATRIX=name [LANES=a,b] [ARTIFACTS_DIR=path]
	$(call require_var,MATRIX)
	$(TESTKIT) $(TESTKIT_FLAGS) matrix run "$(MATRIX)" \
		$(if $(strip $(LANES)),--lanes "$(LANES)",) \
		$(if $(strip $(ARTIFACTS_DIR)),--artifacts-dir "$(ARTIFACTS_DIR)",)

diagnostics:  ## Collect Docker and test diagnostics into DIAGNOSTICS_DIR
	$(BOOTSTRAP_PYTHON) scripts/diagnostics/collect.py "$(DIAGNOSTICS_DIR)"

package:  ## Build wheel and sdist release artifacts
	rm -rf dist
	$(VENV)/bin/hatch build

package-check: test-installed-wheel-contracts  ## Verify public contracts against the wheel already in dist/

verify-package:  ## Build once, then verify that exact wheel
	$(MAKE) package
	$(MAKE) package-check

test-installed-package-smoke:
	@test -n "$(DIST_WHEEL)" && test -f "$(DIST_WHEEL)" || (echo "Build or download a wheel into dist/ before running this target."; exit 1)
	rm -rf $(WHEEL_VENV)
	$(BOOTSTRAP_PYTHON) -m venv $(WHEEL_VENV)
	$(WHEEL_PIP) install --index-url "$(WHEEL_PIP_INDEX_URL)" "$(WHEEL_CORE_INSTALL)"
	$(WHEEL_PIP) install --index-url "$(WHEEL_PIP_INDEX_URL)" "$(DIST_WHEEL)[all]" pytest==9.0.3 pytest-asyncio==1.4.0
	PYTHONPATH= $(WHEEL_PYTHON) scripts/packaging/smoke_installed_package.py

test-installed-wheel-contracts: test-installed-package-smoke
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

clean:  ## Remove generated local artifacts and caches
	rm -rf $(WHEEL_VENV) dist build .pytest_cache .ruff_cache .mypy_cache .coverage src/*.egg-info
	find src tests scripts -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
	find src tests scripts -type f -name "*.pyc" -delete 2>/dev/null || true

# Compatibility bridges for existing automation. They intentionally stay out
# of `make help`; migrate callers to the supported commands above.
ci: verify
gate-run: gate
collect-integration-diagnostics: diagnostics
build-dist: package
test-release-gate-wheel: verify-package
