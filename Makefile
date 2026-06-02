# =============================================================================
# agora-etl-plugins — Makefile
# =============================================================================

VENV   := .venv
PYTHON := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip
RUFF   := $(VENV)/bin/ruff
PYTEST := $(VENV)/bin/pytest
PYTEST := $(VENV)/bin/pytest
MYPY   := $(VENV)/bin/mypy

.PHONY: help setup install lint format fix check test ci clean

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup:  ## Create .venv and install all dependencies
	python3.11 -m venv $(VENV)
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
	$(PYTEST) tests/ --ignore=tests/integration -q

test-all:  ## Run all tests including integration
	$(PYTEST) tests/ -q

ci:  ## Full CI check: lint + format + tests
	$(MAKE) check
	$(MYPY) src/agora_plugins
	$(MAKE) test

clean:  ## Remove build artifacts and caches
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist"  -not -path "*/.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "build" -not -path "*/.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
