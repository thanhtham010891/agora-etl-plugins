from __future__ import annotations

import os
import socket
import time
import uuid
from urllib.parse import urlparse

import pytest


def _require_integration_enabled() -> None:
    if os.getenv("AGORA_RUN_INTEGRATION") != "1":
        pytest.skip("Set AGORA_RUN_INTEGRATION=1 to run integration tests.")


def _wait_for_tcp_endpoint(host: str, port: int, *, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.25)
    pytest.skip(f"Service {host}:{port} is not reachable.")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: requires external services")


@pytest.fixture(autouse=True)
def _integration_guard(request: pytest.FixtureRequest) -> None:
    if request.node.get_closest_marker("integration") is not None:
        _require_integration_enabled()


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    pytest.importorskip("psycopg")
    _require_integration_enabled()

    dsn = os.getenv(
        "AGORA_TEST_POSTGRES_DSN",
        "postgresql://agora:agora@127.0.0.1:5432/agora_test",
    )
    parsed = urlparse(dsn)
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 5432)
    return dsn


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    pytest.importorskip("aiokafka")
    _require_integration_enabled()

    bootstrap = os.getenv("AGORA_TEST_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
    host, port = bootstrap.rsplit(":", 1)
    _wait_for_tcp_endpoint(host, int(port))
    return bootstrap


@pytest.fixture(scope="session")
def redis_url() -> str:
    pytest.importorskip("redis")
    _require_integration_enabled()

    url = os.getenv("AGORA_TEST_REDIS_URL", "redis://127.0.0.1:6379/0")
    parsed = urlparse(url)
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 6379)
    return url


@pytest.fixture
def unique_suffix() -> str:
    return uuid.uuid4().hex[:8]
