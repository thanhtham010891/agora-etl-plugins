from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pytest

from agora_plugins.kafka import KafkaPluginConfig

_FAIL_ON_INTEGRATION_SKIP_ENV = "AGORA_FAIL_ON_INTEGRATION_SKIP"
_RELEASE_GATE_SKIP_REPORTS: list[str] = []
_POSTGRES_CONNECT_TIMEOUT_S = 2


def _fail_on_integration_skip_enabled() -> bool:
    return os.getenv(_FAIL_ON_INTEGRATION_SKIP_ENV) == "1"


def _skip_reason(report: pytest.TestReport) -> str:
    longrepr_text = getattr(report, "longreprtext", "")
    if longrepr_text:
        return str(longrepr_text)
    return str(report.longrepr)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if not _fail_on_integration_skip_enabled() or not report.skipped:
        return
    _RELEASE_GATE_SKIP_REPORTS.append(f"{report.nodeid}: {_skip_reason(report)}")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if not _fail_on_integration_skip_enabled() or not _RELEASE_GATE_SKIP_REPORTS:
        return
    terminal_reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if terminal_reporter is not None:
        terminal_reporter.write_sep(
            "=",
            "release gate rejected skipped integration tests",
            red=True,
        )
        for skipped_report in _RELEASE_GATE_SKIP_REPORTS:
            terminal_reporter.write_line(skipped_report, red=True)
    session.exitstatus = pytest.ExitCode.TESTS_FAILED


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


def _tcp_endpoint_ready(host: str, port: int, *, timeout_s: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


def _require_tcp_endpoint(host: str, port: int, *, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"Service {host}:{port} is not reachable: {last_error}")


def _wait_for_bootstrap_servers(
    bootstrap_servers: str,
    *,
    timeout_s: float = 30.0,
) -> None:
    for endpoint in bootstrap_servers.split(","):
        host, port = endpoint.strip().rsplit(":", 1)
        _wait_for_tcp_endpoint(host, int(port), timeout_s=timeout_s)


def _require_env_var(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        pytest.skip(f"Set {name}=... to enable secure Kafka integration tests.")
    return value


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value == "":
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _docker_control_enabled() -> bool:
    return os.getenv("AGORA_TEST_ALLOW_DOCKER_CONTROL") == "1"


def _require_docker_control_enabled() -> None:
    if not _docker_control_enabled():
        pytest.skip("Set AGORA_TEST_ALLOW_DOCKER_CONTROL=1 to run broker flap integration tests.")


def _plugin_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.yaml"),
        )
    )


def _secure_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_SECURE_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.kafka-secure.yaml"),
        )
    )


def _cluster_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_KAFKA_CLUSTER_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.kafka-cluster.yaml"),
        )
    )


def _postgres_ha_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_POSTGRES_HA_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.postgres-ha.yaml"),
        )
    )


def _redis_sentinel_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_REDIS_SENTINEL_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.redis-sentinel.yaml"),
        )
    )


def _redis_secure_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_REDIS_SECURE_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.redis-secure.yaml"),
        )
    )


def _redis_cluster_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_REDIS_CLUSTER_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.redis-cluster.yaml"),
        )
    )


def _redis_stack_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_REDIS_STACK_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.redis-stack.yaml"),
        )
    )


def _redis_redlock_docker_compose_file() -> Path:
    return Path(
        os.getenv(
            "AGORA_TEST_REDIS_REDLOCK_DOCKER_COMPOSE_FILE",
            str(_plugin_root() / "docker-compose.redis-redlock.yaml"),
        )
    )


def _run_docker_compose(
    *args: str,
    compose_file: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> None:
    compose_file = compose_file or _docker_compose_file()
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "docker compose command failed: "
            f"{' '.join(args)}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )


def _run_docker_compose_output(
    *args: str,
    compose_file: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> str:
    compose_file = compose_file or _docker_compose_file()
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "docker compose command failed: "
            f"{' '.join(args)}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed.stdout


def _parse_redis_info(raw: str) -> dict[str, str]:
    info: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        info[key] = value
    return info


def _restart_local_broker(
    *,
    compose_file: Path,
    service: str,
    host: str,
    port: int,
) -> None:
    _require_docker_control_enabled()
    _run_docker_compose("stop", service, compose_file=compose_file)
    _run_docker_compose("up", "-d", "--wait", service, compose_file=compose_file)
    _wait_for_tcp_endpoint(host, port, timeout_s=30.0)


def _stop_local_broker(
    *,
    compose_file: Path,
    service: str,
) -> None:
    _require_docker_control_enabled()
    _run_docker_compose("stop", service, compose_file=compose_file)


def _start_local_broker(
    *,
    compose_file: Path,
    service: str,
    host: str,
    port: int,
) -> None:
    _require_docker_control_enabled()
    _run_docker_compose("up", "-d", "--wait", service, compose_file=compose_file)
    _wait_for_tcp_endpoint(host, port, timeout_s=30.0)


def restart_local_kafka_broker() -> None:
    _restart_local_broker(
        compose_file=_docker_compose_file(),
        service="kafka",
        host="127.0.0.1",
        port=19092,
    )


def restart_local_secure_kafka_broker() -> None:
    _restart_local_broker(
        compose_file=_secure_docker_compose_file(),
        service="kafka-secure",
        host="127.0.0.1",
        port=19093,
    )


def restart_local_postgres() -> None:
    _restart_local_broker(
        compose_file=_docker_compose_file(),
        service="postgres",
        host="127.0.0.1",
        port=15432,
    )


def restart_local_redis() -> None:
    _restart_local_broker(
        compose_file=_docker_compose_file(),
        service="redis",
        host="127.0.0.1",
        port=16379,
    )


def _postgres_is_primary(dsn: str) -> bool:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT NOT pg_is_in_recovery()")
        row = cur.fetchone()
    return bool(row[0]) if row else False


def _dsn_with_query_params(dsn: str, /, **params: str | int) -> str:
    parsed = urlparse(dsn)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({key: str(value) for key, value in params.items()})
    return urlunparse(parsed._replace(query=urlencode(query)))


def _wait_for_postgres_dsn(
    dsn: str,
    *,
    timeout_s: float = 60.0,
    require_primary: bool | None = None,
    stable_polls: int = 1,
) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    stable_matches = 0
    while time.monotonic() < deadline:
        try:
            is_primary = _postgres_is_primary(dsn)
            if require_primary is None or is_primary is require_primary:
                stable_matches += 1
                if stable_matches >= max(stable_polls, 1):
                    return
            else:
                stable_matches = 0
        except Exception as exc:  # pragma: no cover - best effort poller
            last_error = exc
            stable_matches = 0
        time.sleep(0.5)
    raise RuntimeError(f"Postgres endpoint {dsn} did not become ready: {last_error}")


@dataclass(frozen=True, slots=True)
class KafkaClusterBroker:
    broker_id: int
    service: str
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class KafkaClusterControl:
    compose_file: Path
    bootstrap_servers: str
    brokers: dict[int, KafkaClusterBroker]

    def stop_broker(self, broker_id: int) -> None:
        broker = self.brokers[broker_id]
        _stop_local_broker(
            compose_file=self.compose_file,
            service=broker.service,
        )

    def start_broker(self, broker_id: int) -> None:
        broker = self.brokers[broker_id]
        _start_local_broker(
            compose_file=self.compose_file,
            service=broker.service,
            host=broker.host,
            port=broker.port,
        )

    def restart_broker(self, broker_id: int) -> None:
        broker = self.brokers[broker_id]
        _restart_local_broker(
            compose_file=self.compose_file,
            service=broker.service,
            host=broker.host,
            port=broker.port,
        )

    def rolling_restart(self) -> None:
        for broker_id in sorted(self.brokers):
            self.restart_broker(broker_id)


@dataclass(frozen=True, slots=True)
class PostgresHaNode:
    service: str
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class PostgresHaControl:
    compose_file: Path
    dsn: str
    nodes: dict[str, PostgresHaNode]
    database: str
    username: str
    password: str
    reset_timeout_s: float = 90.0
    teardown_timeout_s: float = 20.0

    def node_dsn(self, node_name: str) -> str:
        node = self.nodes[node_name]
        return _dsn_with_query_params(
            f"postgresql://{self.username}:{self.password}@{node.host}:{node.port}/{self.database}",
            connect_timeout=_POSTGRES_CONNECT_TIMEOUT_S,
        )

    def admin_node_dsn(self, node_name: str) -> str:
        node = self.nodes[node_name]
        return _dsn_with_query_params(
            f"postgresql://postgres:postgres@{node.host}:{node.port}/postgres",
            connect_timeout=_POSTGRES_CONNECT_TIMEOUT_S,
        )

    def stop_node(self, node_name: str) -> None:
        node = self.nodes[node_name]
        _stop_local_broker(compose_file=self.compose_file, service=node.service)

    def _repmgr_primary_host_env(self, node_name: str, primary_host: str | None) -> dict[str, str]:
        if primary_host is None:
            return {}
        if node_name == "postgres-primary":
            return {"AGORA_TEST_POSTGRES_PRIMARY_HOST": primary_host}
        if node_name == "postgres-standby":
            return {"AGORA_TEST_POSTGRES_STANDBY_PRIMARY_HOST": primary_host}
        return {}

    def _start_node_service(self, node_name: str, *, primary_host: str | None = None) -> None:
        node = self.nodes[node_name]
        _run_docker_compose(
            "up",
            "-d",
            "--no-deps",
            node.service,
            compose_file=self.compose_file,
            extra_env=self._repmgr_primary_host_env(node_name, primary_host),
        )

    def _recreate_node_service(self, node_name: str, *, primary_host: str | None = None) -> None:
        node = self.nodes[node_name]
        _run_docker_compose("rm", "-f", "-s", "-v", node.service, compose_file=self.compose_file)
        _run_docker_compose(
            "up",
            "-d",
            "--wait",
            "--no-deps",
            node.service,
            compose_file=self.compose_file,
            extra_env=self._repmgr_primary_host_env(node_name, primary_host),
        )

    def start_node(self, node_name: str) -> None:
        node = self.nodes[node_name]
        self._start_node_service(node_name)
        _wait_for_tcp_endpoint(node.host, node.port, timeout_s=30.0)

    def restart_node(self, node_name: str) -> None:
        node = self.nodes[node_name]
        _restart_local_broker(
            compose_file=self.compose_file,
            service=node.service,
            host=node.host,
            port=node.port,
        )

    def wait_for_node_role(
        self,
        node_name: str,
        *,
        primary: bool,
        timeout_s: float = 60.0,
        stable_polls: int = 1,
    ) -> None:
        _wait_for_postgres_dsn(
            self.node_dsn(node_name),
            timeout_s=timeout_s,
            require_primary=primary,
            stable_polls=stable_polls,
        )

    def wait_for_client_route_ready(
        self,
        *,
        timeout_s: float = 60.0,
        stable_polls: int = 1,
    ) -> None:
        _wait_for_postgres_dsn(
            _dsn_with_query_params(
                self.dsn,
                connect_timeout=_POSTGRES_CONNECT_TIMEOUT_S,
            ),
            timeout_s=timeout_s,
            stable_polls=stable_polls,
        )

    def current_primary(self, *, timeout_s: float = 60.0) -> str:
        deadline = time.monotonic() + timeout_s
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            for node_name in self.nodes:
                try:
                    if _postgres_is_primary(self.node_dsn(node_name)):
                        return node_name
                except Exception as exc:  # pragma: no cover - best effort poller
                    last_error = exc
            time.sleep(0.5)
        raise RuntimeError(f"Could not discover current Postgres primary: {last_error}")

    def current_standby(self, *, timeout_s: float = 60.0) -> str:
        primary = self.current_primary(timeout_s=timeout_s)
        for node_name in self.nodes:
            if node_name != primary:
                self.wait_for_node_role(node_name, primary=False, timeout_s=timeout_s)
                return node_name
        raise RuntimeError("Could not discover current Postgres standby.")

    def wait_for_table_row_count(
        self,
        node_name: str,
        table: str,
        *,
        expected_count: int,
        timeout_s: float = 30.0,
    ) -> None:
        import psycopg

        deadline = time.monotonic() + timeout_s
        query = f'SELECT COUNT(*) FROM "{table}"'
        last_count: int | None = None
        target_lsn: str | None = None
        if node_name != self.current_primary(timeout_s=timeout_s):
            primary_node = self.current_primary(timeout_s=timeout_s)
            with (
                psycopg.connect(self.node_dsn(primary_node), autocommit=True) as conn,
                conn.cursor() as cur,
            ):
                cur.execute("SELECT pg_current_wal_lsn()::text")
                row = cur.fetchone()
                target_lsn = str(row[0]) if row else None

        while time.monotonic() < deadline:
            try:
                with (
                    psycopg.connect(self.node_dsn(node_name), autocommit=True) as conn,
                    conn.cursor() as cur,
                ):
                    if target_lsn is not None:
                        cur.execute(
                            "SELECT COALESCE(pg_wal_lsn_diff(pg_last_wal_replay_lsn(), %s::pg_lsn), -1)",
                            (target_lsn,),
                        )
                        replay_row = cur.fetchone()
                        replay_delta = float(replay_row[0]) if replay_row else -1.0
                        if replay_delta < 0:
                            time.sleep(0.5)
                            continue
                    cur.execute(query)
                    row = cur.fetchone()
                    last_count = int(row[0]) if row else None
                    if last_count == expected_count:
                        return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            f"Timed out waiting for {node_name} to reach {expected_count} rows "
            f"for table {table!r}. Last count: {last_count!r}"
        )

    def wait_for_replication_ready(
        self,
        primary_node: str,
        standby_node: str,
        *,
        timeout_s: float = 60.0,
        stable_polls: int = 6,
    ) -> None:
        import psycopg

        primary_query = """
            SELECT COUNT(*)
            FROM pg_stat_replication
            WHERE state = 'streaming'
        """
        standby_query = """
            SELECT status
            FROM pg_stat_wal_receiver
        """

        deadline = time.monotonic() + timeout_s
        last_primary_count: int | None = None
        last_standby_status: str | None = None
        stable_matches = 0
        while time.monotonic() < deadline:
            try:
                with (
                    psycopg.connect(self.admin_node_dsn(primary_node), autocommit=True) as conn,
                    conn.cursor() as cur,
                ):
                    cur.execute(primary_query)
                    row = cur.fetchone()
                    last_primary_count = int(row[0]) if row else None
                with (
                    psycopg.connect(self.admin_node_dsn(standby_node), autocommit=True) as conn,
                    conn.cursor() as cur,
                ):
                    cur.execute(standby_query)
                    row = cur.fetchone()
                    last_standby_status = str(row[0]) if row else None
                if (last_primary_count or 0) >= 1 and last_standby_status == "streaming":
                    stable_matches += 1
                    if stable_matches >= stable_polls:
                        return
                else:
                    stable_matches = 0
            except Exception:
                stable_matches = 0
            time.sleep(0.5)

        raise RuntimeError(
            "Timed out waiting for Postgres replication readiness: "
            f"primary={primary_node} replicas={last_primary_count!r}, "
            f"standby={standby_node} wal_receiver={last_standby_status!r}"
        )

    def failover_primary(self, *, timeout_s: float = 90.0) -> tuple[str, str]:
        primary = self.current_primary(timeout_s=timeout_s)
        self.stop_node(primary)

        deadline = time.monotonic() + timeout_s
        promoted: str | None = None
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            for node_name in self.nodes:
                if node_name == primary:
                    continue
                try:
                    if _postgres_is_primary(self.node_dsn(node_name)):
                        promoted = node_name
                        break
                except Exception as exc:  # pragma: no cover - best effort poller
                    last_error = exc
            if promoted is not None:
                break
            time.sleep(0.5)

        if promoted is None:
            raise RuntimeError(f"Postgres standby was not promoted after failover: {last_error}")

        remaining_timeout = self._remaining_timeout(deadline)
        self.wait_for_node_role(
            promoted,
            primary=True,
            timeout_s=remaining_timeout,
            stable_polls=6,
        )
        self.wait_for_client_route_ready(
            timeout_s=self._remaining_timeout(deadline),
            stable_polls=3,
        )
        return primary, promoted

    def failover_cycle(
        self,
        *,
        timeout_s: float = 120.0,
        preferred_primary: str | None = None,
    ) -> tuple[str, str]:
        failed_primary, promoted_primary = self.failover_primary(timeout_s=timeout_s)
        deadline = time.monotonic() + timeout_s
        rejoin_node = failed_primary
        last_error: RuntimeError | None = None
        while time.monotonic() < deadline:
            self._recreate_node_service(rejoin_node, primary_host=promoted_primary)
            try:
                self.wait_for_node_role(
                    promoted_primary,
                    primary=True,
                    timeout_s=self._remaining_timeout(deadline),
                    stable_polls=4,
                )
                self.wait_for_node_role(
                    rejoin_node,
                    primary=False,
                    timeout_s=self._remaining_timeout(deadline),
                    stable_polls=4,
                )
                self.wait_for_replication_ready(
                    promoted_primary,
                    rejoin_node,
                    timeout_s=self._remaining_timeout(deadline),
                )
                self.wait_for_client_route_ready(
                    timeout_s=self._remaining_timeout(deadline),
                    stable_polls=3,
                )
                return failed_primary, promoted_primary
            except RuntimeError as exc:
                last_error = exc
                if self._remaining_timeout(deadline) <= 1.0:
                    break
                time.sleep(1.0)

        raise RuntimeError(
            f"Postgres failover cycle did not rejoin the previous primary as standby: {last_error}"
        )

    def failover_loop(self, *, cycles: int, timeout_s: float = 120.0) -> list[tuple[str, str]]:
        transitions: list[tuple[str, str]] = []
        for _ in range(max(cycles, 0)):
            transitions.append(self.failover_cycle(timeout_s=timeout_s))
        return transitions

    def _remaining_timeout(self, deadline: float) -> float:
        return max(deadline - time.monotonic(), 0.1)

    def _ordered_node_names(self, preferred_primary: str) -> list[str]:
        return [preferred_primary, *[name for name in self.nodes if name != preferred_primary]]

    def _start_cluster_services(self, preferred_primary: str) -> None:
        service_names = [
            self.nodes[node_name].service
            for node_name in self._ordered_node_names(preferred_primary)
        ]
        _run_docker_compose("up", "-d", *service_names, compose_file=self.compose_file)

    def _down_cluster_services(self) -> None:
        _run_docker_compose("down", "-v", compose_file=self.compose_file)

    def reset_cluster(
        self,
        *,
        preferred_primary: str = "postgres-primary",
        timeout_s: float | None = None,
    ) -> None:
        timeout_s = self.reset_timeout_s if timeout_s is None else timeout_s
        self._down_cluster_services()
        self._start_cluster_services(preferred_primary)
        deadline = time.monotonic() + timeout_s
        self.wait_for_node_role(
            preferred_primary,
            primary=True,
            timeout_s=self._remaining_timeout(deadline),
        )

        for node_name in self._ordered_node_names(preferred_primary):
            if node_name == preferred_primary:
                continue
            self.wait_for_node_role(
                node_name,
                primary=False,
                timeout_s=self._remaining_timeout(deadline),
            )
            self.wait_for_replication_ready(
                preferred_primary,
                node_name,
                timeout_s=self._remaining_timeout(deadline),
            )

        self.wait_for_client_route_ready(timeout_s=self._remaining_timeout(deadline))

    def restore_cluster_for_teardown(
        self,
        *,
        preferred_primary: str = "postgres-primary",
        timeout_s: float | None = None,
    ) -> None:
        with contextlib.suppress(RuntimeError):
            self._down_cluster_services()


@dataclass(frozen=True, slots=True)
class RedisSentinelNode:
    service: str
    host: str
    port: int
    internal_host: str


@dataclass(frozen=True, slots=True)
class RedisSentinelControl:
    compose_file: Path
    proxy_host: str
    proxy_port: int
    sentinel_service: str
    sentinel_port: int
    master_name: str
    nodes: dict[str, RedisSentinelNode]

    def stop_node(self, node_name: str) -> None:
        node = self.nodes[node_name]
        _stop_local_broker(compose_file=self.compose_file, service=node.service)

    def start_node(self, node_name: str) -> None:
        node = self.nodes[node_name]
        _start_local_broker(
            compose_file=self.compose_file,
            service=node.service,
            host=node.host,
            port=node.port,
        )

    def start_sentinel(self) -> None:
        _start_local_broker(
            compose_file=self.compose_file,
            service=self.sentinel_service,
            host="127.0.0.1",
            port=self.sentinel_port,
        )

    def start_proxy(self) -> None:
        _start_local_broker(
            compose_file=self.compose_file,
            service="redis-master-proxy",
            host=self.proxy_host,
            port=self.proxy_port,
        )

    def ensure_topology_ready(self, *, timeout_s: float = 30.0) -> None:
        deadline = time.monotonic() + timeout_s
        for node_name, node in self.nodes.items():
            if not _tcp_endpoint_ready(node.host, node.port):
                self.start_node(node_name)
        if not _tcp_endpoint_ready("127.0.0.1", self.sentinel_port):
            self.start_sentinel()
        if not _tcp_endpoint_ready(self.proxy_host, self.proxy_port):
            self.start_proxy()
        while time.monotonic() < deadline:
            try:
                self.wait_for_replication_ready(timeout_s=5.0)
                self.wait_for_proxy_writable(timeout_s=5.0)
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("Timed out waiting for Redis Sentinel topology to become ready.")

    def wait_for_replication_ready(self, *, timeout_s: float = 30.0) -> str:
        deadline = time.monotonic() + timeout_s
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                current_master = self.current_master_node(timeout_s=5.0)
                standby_node = next(
                    node_name for node_name in self.nodes if node_name != current_master
                )
                master_info = self._node_replication_info(current_master)
                standby_info = self._node_replication_info(standby_node)
                expected_master_hosts = self._node_identity_candidates(self.nodes[current_master])
                if (
                    master_info.get("role") == "master"
                    and int(master_info.get("connected_slaves", "0")) >= 1
                    and standby_info.get("role") == "slave"
                    and standby_info.get("master_host") in expected_master_hosts
                    and standby_info.get("master_link_status") == "up"
                ):
                    return current_master
            except Exception as exc:  # pragma: no cover - best effort poller
                last_error = exc
            time.sleep(0.5)
        raise RuntimeError(
            f"Timed out waiting for Redis Sentinel replication readiness: {last_error}"
        )

    def current_master_node(self, *, timeout_s: float = 30.0) -> str:
        deadline = time.monotonic() + timeout_s
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                raw = _run_docker_compose_output(
                    "exec",
                    "-T",
                    self.sentinel_service,
                    "redis-cli",
                    "--raw",
                    "-p",
                    str(self.sentinel_port),
                    "SENTINEL",
                    "get-master-addr-by-name",
                    self.master_name,
                    compose_file=self.compose_file,
                )
                parts = [line.strip() for line in raw.splitlines() if line.strip()]
                if not parts:
                    raise RuntimeError("Redis Sentinel did not return a current master.")
                internal_host = parts[0]
                for node_name, node in self.nodes.items():
                    if internal_host in self._node_identity_candidates(node):
                        return node_name
                raise RuntimeError(f"Unknown Redis Sentinel master host: {internal_host}")
            except Exception as exc:  # pragma: no cover - best effort poller
                last_error = exc
                time.sleep(0.5)
        raise RuntimeError(f"Timed out waiting for Redis Sentinel master: {last_error}")

    def wait_for_proxy_ready(self, *, timeout_s: float = 30.0) -> None:
        _require_tcp_endpoint(self.proxy_host, self.proxy_port, timeout_s=timeout_s)

    def wait_for_proxy_writable(self, *, timeout_s: float = 30.0) -> None:
        import redis

        deadline = time.monotonic() + timeout_s
        probe_key = f"agora:redis:sentinel:probe:{time.monotonic_ns()}"
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            client = redis.Redis(
                host=self.proxy_host,
                port=self.proxy_port,
                decode_responses=True,
            )
            try:
                client.set(probe_key, "ready", ex=10)
                client.delete(probe_key)
                return
            except Exception as exc:  # pragma: no cover - best effort poller
                last_error = exc
                time.sleep(0.25)
            finally:
                with contextlib.suppress(Exception):
                    client.close()
        raise RuntimeError(
            f"Timed out waiting for Redis Sentinel proxy to become writable: {last_error}"
        )

    def failover_current_master(self, *, timeout_s: float = 30.0) -> str:
        failed_node = self.current_master_node(timeout_s=timeout_s)
        self.stop_node(failed_node)

        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                current = self.current_master_node(timeout_s=5.0)
                if current != failed_node:
                    self.wait_for_replication_ready(timeout_s=5.0)
                    self.wait_for_proxy_writable(timeout_s=5.0)
                    return failed_node
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError("Timed out waiting for Redis Sentinel failover to promote a new master.")

    def trigger_sentinel_failover(self, *, timeout_s: float = 30.0) -> str:
        previous_master = self.current_master_node(timeout_s=timeout_s)
        _run_docker_compose_output(
            "exec",
            "-T",
            self.sentinel_service,
            "redis-cli",
            "--raw",
            "-p",
            str(self.sentinel_port),
            "SENTINEL",
            "FAILOVER",
            self.master_name,
            compose_file=self.compose_file,
        )

        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                current = self.current_master_node(timeout_s=5.0)
                if current != previous_master:
                    self.wait_for_replication_ready(timeout_s=5.0)
                    self.wait_for_proxy_writable(timeout_s=5.0)
                    return previous_master
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            "Timed out waiting for Redis Sentinel failover command to promote a new master."
        )

    def crash_failover(self, *, timeout_s: float = 30.0) -> str:
        return self.failover_current_master(timeout_s=timeout_s)

    def graceful_failover(self, *, timeout_s: float = 30.0) -> str:
        return self.trigger_sentinel_failover(timeout_s=timeout_s)

    def _node_identity_candidates(self, node: RedisSentinelNode) -> set[str]:
        identities = {node.internal_host}
        try:
            raw = _run_docker_compose_output(
                "exec",
                "-T",
                node.service,
                "hostname",
                "-i",
                compose_file=self.compose_file,
            )
        except Exception:
            return identities
        identities.update(part.strip() for part in raw.split() if part.strip())
        return identities

    def _node_replication_info(self, node_name: str) -> dict[str, str]:
        node = self.nodes[node_name]
        raw = _run_docker_compose_output(
            "exec",
            "-T",
            node.service,
            "redis-cli",
            "INFO",
            "replication",
            compose_file=self.compose_file,
        )
        return _parse_redis_info(raw)


@pytest.fixture
def kafka_broker_flap_control() -> callable:
    _require_docker_control_enabled()
    return restart_local_kafka_broker


@pytest.fixture
def kafka_secure_broker_flap_control() -> callable:
    _require_docker_control_enabled()
    return restart_local_secure_kafka_broker


@pytest.fixture
def postgres_service_control() -> callable:
    _require_docker_control_enabled()
    return restart_local_postgres


@pytest.fixture
def redis_service_control() -> callable:
    _require_docker_control_enabled()
    return restart_local_redis


@pytest.fixture
def redis_sentinel_control() -> RedisSentinelControl:
    _require_docker_control_enabled()
    return RedisSentinelControl(
        compose_file=_redis_sentinel_docker_compose_file(),
        proxy_host="127.0.0.1",
        proxy_port=16383,
        sentinel_service="redis-sentinel",
        sentinel_port=26379,
        master_name="mymaster",
        nodes={
            "primary": RedisSentinelNode(
                service="redis-primary",
                host="127.0.0.1",
                port=16381,
                internal_host="redis-primary",
            ),
            "replica": RedisSentinelNode(
                service="redis-replica",
                host="127.0.0.1",
                port=16382,
                internal_host="redis-replica",
            ),
        },
    )


@pytest.fixture
def postgres_ha_control() -> PostgresHaControl:
    _require_docker_control_enabled()
    dsn = os.getenv(
        "AGORA_TEST_POSTGRES_HA_DSN",
        "postgresql://agora:agora@127.0.0.1:15435,127.0.0.1:15436/agora_test?target_session_attrs=read-write",
    )
    return _build_postgres_ha_control(dsn)


def _build_postgres_ha_control(postgres_ha_dsn: str) -> PostgresHaControl:
    parsed = urlparse(postgres_ha_dsn)
    return PostgresHaControl(
        compose_file=_postgres_ha_docker_compose_file(),
        dsn=postgres_ha_dsn,
        nodes={
            "postgres-primary": PostgresHaNode("postgres-primary", "127.0.0.1", 15435),
            "postgres-standby": PostgresHaNode("postgres-standby", "127.0.0.1", 15436),
        },
        database=(parsed.path or "/agora_test").lstrip("/") or "agora_test",
        username=parsed.username or "agora",
        password=parsed.password or "agora",
    )


@pytest.fixture(scope="session")
def kafka_cluster_bootstrap() -> str:
    pytest.importorskip("aiokafka")
    _require_integration_enabled()

    bootstrap = os.getenv(
        "AGORA_TEST_KAFKA_CLUSTER_BOOTSTRAP",
        "127.0.0.1:19192,127.0.0.1:19193,127.0.0.1:19194",
    )
    _wait_for_bootstrap_servers(bootstrap)
    return bootstrap


@pytest.fixture
def kafka_cluster_control(kafka_cluster_bootstrap: str) -> KafkaClusterControl:
    _require_docker_control_enabled()
    return KafkaClusterControl(
        compose_file=_cluster_docker_compose_file(),
        bootstrap_servers=kafka_cluster_bootstrap,
        brokers={
            1: KafkaClusterBroker(1, "kafka-1", "127.0.0.1", 19192),
            2: KafkaClusterBroker(2, "kafka-2", "127.0.0.1", 19193),
            3: KafkaClusterBroker(3, "kafka-3", "127.0.0.1", 19194),
        },
    )


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
        "postgresql://agora:agora@127.0.0.1:15432/agora_test",
    )
    parsed = urlparse(dsn)
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 5432)
    return dsn


@pytest.fixture(scope="session")
def postgres_ha_dsn() -> str:
    pytest.importorskip("psycopg")
    _require_integration_enabled()

    return os.getenv(
        "AGORA_TEST_POSTGRES_HA_DSN",
        "postgresql://agora:agora@127.0.0.1:15435,127.0.0.1:15436/agora_test?target_session_attrs=read-write",
    )


@pytest.fixture(scope="session")
def postgres_ha_soak_cycles() -> int:
    _require_integration_enabled()
    return max(_env_int("AGORA_TEST_POSTGRES_HA_SOAK_CYCLES", 2), 1)


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    pytest.importorskip("aiokafka")
    _require_integration_enabled()

    bootstrap = os.getenv("AGORA_TEST_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    _wait_for_bootstrap_servers(bootstrap)
    return bootstrap


@pytest.fixture(scope="session")
def kafka_scram_bootstrap() -> str:
    pytest.importorskip("aiokafka")
    _require_integration_enabled()

    bootstrap = _require_env_var("AGORA_TEST_KAFKA_SCRAM_BOOTSTRAP")
    host, port = bootstrap.rsplit(":", 1)
    _wait_for_tcp_endpoint(host, int(port))
    return bootstrap


@pytest.fixture(scope="session")
def kafka_mtls_bootstrap() -> str:
    pytest.importorskip("aiokafka")
    _require_integration_enabled()

    bootstrap = _require_env_var("AGORA_TEST_KAFKA_MTLS_BOOTSTRAP")
    host, port = bootstrap.rsplit(":", 1)
    _wait_for_tcp_endpoint(host, int(port))
    return bootstrap


@pytest.fixture(scope="session")
def kafka_secure_assets() -> dict[str, str]:
    _require_integration_enabled()
    ca_file = _require_env_var("AGORA_TEST_KAFKA_CA_FILE")
    asset_dir = os.path.dirname(ca_file)
    return {
        "scram_username": _require_env_var("AGORA_TEST_KAFKA_SCRAM_USERNAME"),
        "scram_password_file": _require_env_var("AGORA_TEST_KAFKA_SCRAM_PASSWORD_FILE"),
        "ca_file": ca_file,
        "client_cert_file": _require_env_var("AGORA_TEST_KAFKA_CLIENT_CERT_FILE"),
        "client_key_file": _require_env_var("AGORA_TEST_KAFKA_CLIENT_KEY_FILE"),
        "rogue_client_cert_file": os.path.join(asset_dir, "rogue-client.crt"),
        "rogue_client_key_file": os.path.join(asset_dir, "rogue-client.key"),
        "schema_registry_url": _require_env_var("AGORA_TEST_SCHEMA_REGISTRY_URL"),
        "schema_registry_mtls_url": _require_env_var("AGORA_TEST_SCHEMA_REGISTRY_MTLS_URL"),
        "schema_registry_username": _require_env_var("AGORA_TEST_SCHEMA_REGISTRY_USERNAME"),
        "schema_registry_password_file": _require_env_var(
            "AGORA_TEST_SCHEMA_REGISTRY_PASSWORD_FILE"
        ),
    }


@pytest.fixture(scope="session")
def kafka_scram_plugin_config(
    kafka_scram_bootstrap: str,
    kafka_secure_assets: dict[str, str],
) -> KafkaPluginConfig:
    return KafkaPluginConfig(
        bootstrap_servers=kafka_scram_bootstrap,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username_env="AGORA_TEST_KAFKA_SCRAM_USERNAME",
        sasl_password_file=kafka_secure_assets["scram_password_file"],
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
    )


@pytest.fixture(scope="session")
def kafka_mtls_plugin_config(
    kafka_mtls_bootstrap: str,
) -> KafkaPluginConfig:
    return KafkaPluginConfig(
        bootstrap_servers=kafka_mtls_bootstrap,
        security_protocol="SSL",
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        ssl_certfile_env="AGORA_TEST_KAFKA_CLIENT_CERT_FILE",
        ssl_keyfile_env="AGORA_TEST_KAFKA_CLIENT_KEY_FILE",
    )


@pytest.fixture(scope="session")
def kafka_secure_schema_registry_config(
    kafka_scram_bootstrap: str,
    kafka_secure_assets: dict[str, str],
) -> KafkaPluginConfig:
    parsed = urlparse(kafka_secure_assets["schema_registry_url"])
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 443)
    return KafkaPluginConfig(
        bootstrap_servers=kafka_scram_bootstrap,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username_env="AGORA_TEST_KAFKA_SCRAM_USERNAME",
        sasl_password_file=kafka_secure_assets["scram_password_file"],
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_url=kafka_secure_assets["schema_registry_url"],
        schema_registry_username_env="AGORA_TEST_SCHEMA_REGISTRY_USERNAME",
        schema_registry_password_file=kafka_secure_assets["schema_registry_password_file"],
        schema_registry_ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
    )


@pytest.fixture(scope="session")
def kafka_secure_schema_registry_mtls_config(
    kafka_scram_bootstrap: str,
    kafka_secure_assets: dict[str, str],
) -> KafkaPluginConfig:
    parsed = urlparse(kafka_secure_assets["schema_registry_mtls_url"])
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 443)
    return KafkaPluginConfig(
        bootstrap_servers=kafka_scram_bootstrap,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username_env="AGORA_TEST_KAFKA_SCRAM_USERNAME",
        sasl_password_file=kafka_secure_assets["scram_password_file"],
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_url=kafka_secure_assets["schema_registry_mtls_url"],
        schema_registry_username_env="AGORA_TEST_SCHEMA_REGISTRY_USERNAME",
        schema_registry_password_file=kafka_secure_assets["schema_registry_password_file"],
        schema_registry_ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_ssl_certfile_env="AGORA_TEST_KAFKA_CLIENT_CERT_FILE",
        schema_registry_ssl_keyfile_env="AGORA_TEST_KAFKA_CLIENT_KEY_FILE",
    )


@pytest.fixture(scope="session")
def kafka_secure_schema_registry_mtls_no_client_cert_config(
    kafka_scram_bootstrap: str,
    kafka_secure_assets: dict[str, str],
) -> KafkaPluginConfig:
    parsed = urlparse(kafka_secure_assets["schema_registry_mtls_url"])
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 443)
    return KafkaPluginConfig(
        bootstrap_servers=kafka_scram_bootstrap,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username_env="AGORA_TEST_KAFKA_SCRAM_USERNAME",
        sasl_password_file=kafka_secure_assets["scram_password_file"],
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_url=kafka_secure_assets["schema_registry_mtls_url"],
        schema_registry_username_env="AGORA_TEST_SCHEMA_REGISTRY_USERNAME",
        schema_registry_password_file=kafka_secure_assets["schema_registry_password_file"],
        schema_registry_ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
    )


@pytest.fixture(scope="session")
def kafka_secure_schema_registry_mtls_bad_client_cert_config(
    kafka_scram_bootstrap: str,
    kafka_secure_assets: dict[str, str],
) -> KafkaPluginConfig:
    parsed = urlparse(kafka_secure_assets["schema_registry_mtls_url"])
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 443)
    if not os.path.exists(kafka_secure_assets["rogue_client_cert_file"]):
        pytest.skip("Secure Kafka test assets are missing rogue-client.crt.")
    if not os.path.exists(kafka_secure_assets["rogue_client_key_file"]):
        pytest.skip("Secure Kafka test assets are missing rogue-client.key.")
    return KafkaPluginConfig(
        bootstrap_servers=kafka_scram_bootstrap,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username_env="AGORA_TEST_KAFKA_SCRAM_USERNAME",
        sasl_password_file=kafka_secure_assets["scram_password_file"],
        ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_url=kafka_secure_assets["schema_registry_mtls_url"],
        schema_registry_username_env="AGORA_TEST_SCHEMA_REGISTRY_USERNAME",
        schema_registry_password_file=kafka_secure_assets["schema_registry_password_file"],
        schema_registry_ssl_cafile_env="AGORA_TEST_KAFKA_CA_FILE",
        schema_registry_ssl_certfile=kafka_secure_assets["rogue_client_cert_file"],
        schema_registry_ssl_keyfile=kafka_secure_assets["rogue_client_key_file"],
    )


@pytest.fixture(scope="session")
def redis_url() -> str:
    pytest.importorskip("redis")
    _require_integration_enabled()

    url = os.getenv("AGORA_TEST_REDIS_URL", "redis://127.0.0.1:16379/0")
    parsed = urlparse(url)
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 6379)
    return url


@pytest.fixture(scope="session")
def redis_sentinel_url() -> str:
    pytest.importorskip("redis")
    _require_integration_enabled()

    url = os.getenv("AGORA_TEST_REDIS_SENTINEL_URL", "redis://127.0.0.1:16383/0")
    parsed = urlparse(url)
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 6379)
    return url


@pytest.fixture(scope="session")
def redis_cluster_url() -> str:
    pytest.importorskip("redis")
    _require_integration_enabled()

    url = os.getenv("AGORA_TEST_REDIS_CLUSTER_URL", "redis://127.0.0.1:16385/0")
    parsed = urlparse(url)
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 6379)
    return url


@pytest.fixture(scope="session")
def redis_stack_url() -> str:
    pytest.importorskip("redis")
    _require_integration_enabled()

    url = os.getenv("AGORA_TEST_REDIS_STACK_URL", "redis://127.0.0.1:16388/0")
    parsed = urlparse(url)
    _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 6379)
    return url


@pytest.fixture(scope="session")
def redis_redlock_urls() -> list[str]:
    pytest.importorskip("redis")
    _require_integration_enabled()

    raw_urls = os.getenv(
        "AGORA_TEST_REDIS_REDLOCK_URLS",
        "redis://127.0.0.1:16389/0,redis://127.0.0.1:16390/0,redis://127.0.0.1:16391/0",
    )
    urls = [url.strip() for url in raw_urls.split(",") if url.strip()]
    if len(urls) < 3:
        pytest.skip("Set AGORA_TEST_REDIS_REDLOCK_URLS to at least 3 Redis URLs.")
    for url in urls:
        parsed = urlparse(url)
        _wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 6379)
    return urls


def _build_secure_redis_url(
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    ca_file: str | None,
) -> str:
    query: dict[str, str] = {"ssl_check_hostname": "false"}
    if ca_file is not None:
        query["ssl_ca_certs"] = ca_file
    return f"rediss://{username}:{password}@{host}:{port}/0?{urlencode(query)}"


@pytest.fixture(scope="session")
def redis_secure_assets() -> dict[str, str]:
    pytest.importorskip("redis")
    _require_integration_enabled()

    host = os.getenv("AGORA_TEST_REDIS_SECURE_HOST", "127.0.0.1")
    port = int(os.getenv("AGORA_TEST_REDIS_SECURE_PORT", "16384"))
    _wait_for_tcp_endpoint(host, port)

    password_file = _require_env_var("AGORA_TEST_REDIS_SECURE_PASSWORD_FILE")
    with open(password_file, encoding="utf-8") as handle:
        password = handle.read().strip()

    return {
        "host": host,
        "port": str(port),
        "username": _require_env_var("AGORA_TEST_REDIS_SECURE_USERNAME"),
        "password": password,
        "ca_file": _require_env_var("AGORA_TEST_REDIS_SECURE_CA_FILE"),
        "rogue_ca_file": _require_env_var("AGORA_TEST_REDIS_SECURE_ROGUE_CA_FILE"),
    }


@pytest.fixture(scope="session")
def redis_secure_url(redis_secure_assets: dict[str, str]) -> str:
    return _build_secure_redis_url(
        host=redis_secure_assets["host"],
        port=int(redis_secure_assets["port"]),
        username=redis_secure_assets["username"],
        password=redis_secure_assets["password"],
        ca_file=redis_secure_assets["ca_file"],
    )


@pytest.fixture(scope="session")
def redis_broker_flap_cycles() -> int:
    _require_integration_enabled()
    return max(_env_int("AGORA_TEST_REDIS_BROKER_FLAP_CYCLES", 3), 1)


@pytest.fixture(scope="session")
def redis_sentinel_failover_cycles() -> int:
    _require_integration_enabled()
    return max(_env_int("AGORA_TEST_REDIS_SENTINEL_FAILOVER_CYCLES", 3), 1)


@pytest.fixture
def unique_suffix() -> str:
    return uuid.uuid4().hex[:8]
