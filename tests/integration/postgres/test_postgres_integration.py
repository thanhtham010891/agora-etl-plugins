from __future__ import annotations

import asyncio
import json
from datetime import datetime

import pytest
from agora import DeliveryConfig, InMemoryCheckpointStore, IterableSource, Pipeline
from agora.core.dlq import DLQRecord
from agora.core.middleware import Middleware
from agora.core.retry import RetryPolicy
from agora.core.types import CheckpointFailurePolicy, DLQFailurePolicy, SinkFailurePolicy

from agora_plugins.dlq_policy import DLQPayloadPolicy
from agora_plugins.postgres import (
    PostgresDLQSink,
    PostgresDLQSource,
    PostgresReplicaStalenessError,
    PostgresSink,
    PostgresSinkWriteError,
    PostgresSource,
    PostgresSourceRecoveryMode,
    PostgresWriteSafetyPolicy,
)

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 30.0
_REPLICA_STALE_LAG_S = 0.1
_REPLICA_LAG_TIMEOUT_S = 10.0
_POSTGRES_HA_STEP_TIMEOUT_S = 45.0


class _ReverseCipher:
    def encrypt(self, payload: bytes) -> bytes:
        return payload[::-1]

    def decrypt(self, payload: bytes) -> bytes:
        return payload[::-1]


class _CollectSink:
    sink_name = "collect"

    def __init__(self, target: list[dict] | None = None) -> None:
        self._target = target if target is not None else []

    async def open(self) -> None:
        return None

    async def write(self, record: dict) -> None:
        self._target.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CollectDLQSink:
    sink_name = "collect_dlq"

    def __init__(self, target: list[object]) -> None:
        self._target = target

    async def open(self) -> None:
        return None

    async def write(self, record: object) -> None:
        self._target.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FailingCheckpointStore:
    async def load(self, key: str):
        return None

    async def save(self, key: str, checkpoint) -> None:
        raise RuntimeError("checkpoint broke")

    async def close(self) -> None:
        return None


async def _set_replica_replay_paused(
    admin_dsn: str,
    *,
    paused: bool,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    command = "SELECT pg_wal_replay_pause()" if paused else "SELECT pg_wal_replay_resume()"
    expected = paused
    deadline = asyncio.get_running_loop().time() + _REPLICA_LAG_TIMEOUT_S
    async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(command)
        while asyncio.get_running_loop().time() < deadline:
            async with conn.cursor() as cur:
                await cur.execute("SELECT pg_is_wal_replay_paused()")
                row = await cur.fetchone()
            if bool(row[0]) is expected:
                return
            await asyncio.sleep(0.1)
    raise RuntimeError(f"Postgres standby replay pause state did not reach {expected!r}.")


async def _wait_for_replica_replay_lag_at_least(
    node_dsn: str,
    *,
    min_lag_s: float,
    timeout_s: float = _REPLICA_LAG_TIMEOUT_S,
) -> float:
    psycopg = pytest.importorskip("psycopg")
    deadline = asyncio.get_running_loop().time() + timeout_s
    last_lag_s = -1.0
    async with await psycopg.AsyncConnection.connect(node_dsn, autocommit=True) as conn:
        while asyncio.get_running_loop().time() < deadline:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT COALESCE(
                        EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())::double precision,
                        1000000000.0
                    )
                    """
                )
                row = await cur.fetchone()
            last_lag_s = float(row[0]) if row is not None else -1.0
            if last_lag_s >= min_lag_s:
                return last_lag_s
            await asyncio.sleep(0.1)
    raise RuntimeError(
        f"Replica replay lag did not reach expected threshold: {last_lag_s:.3f}s < {min_lag_s:.3f}s"
    )


async def _run_postgres_ha_step(
    func, /, *args, step_timeout_s: float = _POSTGRES_HA_STEP_TIMEOUT_S, **kwargs
):
    return await asyncio.wait_for(
        asyncio.to_thread(func, *args, **kwargs),
        timeout=step_timeout_s,
    )


@pytest.mark.asyncio
async def test_postgres_sink_round_trips_against_real_database(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_it_{unique_suffix}"

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        sink = PostgresSink(
            dsn=postgres_dsn,
            table=table,
            row_mapper=lambda row: row,
            conflict_key="slug",
            batch_size=2,
        )

        first_summary = await asyncio.wait_for(
            (
                Pipeline(
                    IterableSource(
                        [
                            {"slug": "a", "display_name": "alpha"},
                            {"slug": "b", "display_name": "bravo"},
                        ]
                    )
                )
                .build(sink)
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        second_summary = await asyncio.wait_for(
            (
                Pipeline(
                    IterableSource(
                        [
                            {"slug": "a", "display_name": "alpha-updated"},
                            {"slug": "c", "display_name": "charlie"},
                        ]
                    )
                )
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="slug",
                        batch_size=2,
                    )
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert first_summary.records_written == 2
    assert second_summary.records_written == 2
    assert rows == [
        ("a", "alpha-updated"),
        ("b", "bravo"),
        ("c", "charlie"),
    ]


@pytest.mark.asyncio
async def test_postgres_sink_upsert_preflight_requires_real_constraint(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    missing_constraint_table = f"agora_it_upsert_missing_{unique_suffix}"
    pk_table = f"agora_it_upsert_pk_{unique_suffix}"

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{missing_constraint_table}" (
                    slug TEXT NOT NULL,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.execute(
                f"""
                CREATE TABLE "{pk_table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        missing_constraint_sink = PostgresSink(
            dsn=postgres_dsn,
            table=missing_constraint_table,
            row_mapper=lambda row: row,
            conflict_key="slug",
            batch_size=2,
        )
        with pytest.raises(ValueError, match="PRIMARY KEY or UNIQUE constraint"):
            await missing_constraint_sink.open()

        pk_sink = PostgresSink(
            dsn=postgres_dsn,
            table=pk_table,
            row_mapper=lambda row: row,
            conflict_key="slug",
            batch_size=2,
        )
        await pk_sink.open()
        try:
            await pk_sink.write_batch([{"slug": "a", "display_name": "alpha"}])
        finally:
            await pk_sink.close()

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{pk_table}"')
            rows = await cur.fetchall()
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{missing_constraint_table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{pk_table}"')
        await conn.close()

    assert rows == [("a", "alpha")]


@pytest.mark.asyncio
@pytest.mark.parametrize("insert_mode", ["sql", "copy_merge"])
async def test_postgres_sink_upsert_last_wins_across_batches_against_real_database(
    postgres_dsn: str,
    unique_suffix: str,
    insert_mode: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_upsert_{insert_mode}_{unique_suffix}"
    records = [
        {"slug": "a", "display_name": "alpha-v1", "revision": 1},
        {"slug": "b", "display_name": "bravo-v1", "revision": 1},
        {"slug": "a", "display_name": "alpha-v2", "revision": 2},
        {"slug": "c", "display_name": "charlie-v1", "revision": 1},
        {"slug": "b", "display_name": "bravo-v2", "revision": 2},
        {"slug": "a", "display_name": "alpha-v3", "revision": 3},
    ]

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    revision INTEGER NOT NULL
                )
                """
            )

        summary = await asyncio.wait_for(
            (
                Pipeline(IterableSource(records))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="slug",
                        batch_size=2,
                        insert_mode=insert_mode,  # type: ignore[arg-type]
                    )
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name, revision FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert summary.records_written == len(records)
    assert rows == [
        ("a", "alpha-v3", 3),
        ("b", "bravo-v2", 2),
        ("c", "charlie-v1", 1),
    ]


@pytest.mark.asyncio
async def test_postgres_sink_copy_mode_round_trips_against_real_database(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_copy_it_{unique_suffix}"

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        sink = PostgresSink(
            dsn=postgres_dsn,
            table=table,
            row_mapper=lambda row: row,
            conflict_key="slug",
            batch_size=2,
            upsert=False,
            insert_mode="copy",
        )

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    IterableSource(
                        [
                            {"slug": "a", "display_name": "alpha"},
                            {"slug": "b", "display_name": "bravo"},
                            {"slug": "c", "display_name": "charlie"},
                        ]
                    )
                )
                .build(sink)
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert summary.records_written == 3
    assert rows == [
        ("a", "alpha"),
        ("b", "bravo"),
        ("c", "charlie"),
    ]


@pytest.mark.asyncio
async def test_postgres_sink_copy_mode_does_not_retry_after_commit_loss(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_copy_commit_loss_{unique_suffix}"

    class _CommitLossConnection:
        def __init__(self, conn) -> None:
            self._conn = conn
            self.commit_calls = 0
            self.rollback_calls = 0

        def cursor(self, *args, **kwargs):
            return self._conn.cursor(*args, **kwargs)

        async def commit(self) -> None:
            self.commit_calls += 1
            await self._conn.commit()
            raise psycopg.OperationalError("connection lost after commit acknowledgement")

        async def rollback(self) -> None:
            self.rollback_calls += 1
            await self._conn.rollback()

        async def close(self) -> None:
            await self._conn.close()

    setup_conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    wrapped_conn: _CommitLossConnection | None = None
    try:
        async with setup_conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT NOT NULL,
                    display_name TEXT NOT NULL
                )
                """
            )

        real_conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=False)
        wrapped_conn = _CommitLossConnection(real_conn)
        sink = PostgresSink(
            dsn=postgres_dsn,
            table=table,
            row_mapper=lambda row: row,
            conflict_key="slug",
            batch_size=2,
            upsert=False,
            insert_mode="copy",
            retry_policy=RetryPolicy(
                max_attempts=3,
                initial_backoff_s=0.0,
                retry_exceptions=(psycopg.OperationalError,),
            ),
        )
        sink._conn = wrapped_conn  # type: ignore[attr-defined]

        with pytest.raises(PostgresSinkWriteError, match="connection lost after commit"):
            await sink.write_batch(
                [
                    {"slug": "a", "display_name": "alpha"},
                    {"slug": "b", "display_name": "bravo"},
                ]
            )

        async with setup_conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        if wrapped_conn is not None:
            await wrapped_conn.close()
        async with setup_conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await setup_conn.close()

    assert wrapped_conn is not None
    assert wrapped_conn.commit_calls == 1
    assert wrapped_conn.rollback_calls == 1
    assert sink.metrics_snapshot().retry_count == 0
    assert rows == [
        ("a", "alpha"),
        ("b", "bravo"),
    ]


@pytest.mark.asyncio
async def test_postgres_sink_sql_reconnects_after_database_restart_with_retry(
    postgres_dsn: str,
    unique_suffix: str,
    postgres_service_control,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_restart_sql_it_{unique_suffix}"

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=2,
        retry_policy=RetryPolicy(
            max_attempts=4,
            initial_backoff_s=0.25,
            max_backoff_s=1.0,
            retry_exceptions=(psycopg.Error,),
        ),
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        await sink.connection()
        await asyncio.to_thread(postgres_service_control)

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    IterableSource(
                        [
                            {"slug": "a", "display_name": "alpha"},
                            {"slug": "b", "display_name": "bravo"},
                        ]
                    )
                )
                .build(sink)
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
        metrics = sink.metrics_snapshot()

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert summary.records_written == 2
    assert metrics.retry_count >= 1
    assert rows == [
        ("a", "alpha"),
        ("b", "bravo"),
    ]


@pytest.mark.asyncio
async def test_postgres_sink_copy_merge_reconnects_after_database_restart_with_pool(
    postgres_dsn: str,
    unique_suffix: str,
    postgres_service_control,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_restart_copy_merge_it_{unique_suffix}"

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=2,
        insert_mode="copy_merge",
        pool_size=2,
        retry_policy=RetryPolicy(
            max_attempts=4,
            initial_backoff_s=0.25,
            max_backoff_s=1.0,
            retry_exceptions=(psycopg.Error,),
        ),
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        await sink.write_batch(
            [
                {"slug": "a", "display_name": "alpha"},
                {"slug": "b", "display_name": "bravo"},
            ]
        )
        await sink.flush()

        await asyncio.to_thread(postgres_service_control)

        await sink.write_batch(
            [
                {"slug": "a", "display_name": "alpha-updated"},
                {"slug": "c", "display_name": "charlie"},
            ]
        )
        await sink.flush()
        metrics = sink.metrics_snapshot()

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert metrics.retry_count >= 1
    assert rows == [
        ("a", "alpha-updated"),
        ("b", "bravo"),
        ("c", "charlie"),
    ]


@pytest.mark.asyncio
async def test_postgres_sink_align_to_target_survives_dropped_column_drift(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_align_drift_it_{unique_suffix}"

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    legacy_note TEXT
                )
                """
            )
            await cur.execute(f'ALTER TABLE "{table}" DROP COLUMN legacy_note')

        sink = PostgresSink(
            dsn=postgres_dsn,
            table=table,
            row_mapper=lambda row: row,
            conflict_key="slug",
            batch_size=2,
            write_safety_policy=PostgresWriteSafetyPolicy.ALIGN_TO_TARGET,
        )
        try:
            summary = await asyncio.wait_for(
                (
                    Pipeline(
                        IterableSource(
                            [
                                {
                                    "slug": "a",
                                    "display_name": "alpha",
                                    "legacy_note": "drop-me",
                                },
                                {"slug": "b", "display_name": "bravo"},
                            ]
                        )
                    )
                    .build(sink)
                    .run()
                ),
                timeout=_INTEGRATION_TIMEOUT_S,
            )
            metrics = sink.metrics_snapshot()
        finally:
            await sink.close()

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert summary.records_written == 2
    assert rows == [
        ("a", "alpha"),
        ("b", "bravo"),
    ]
    assert metrics.write_safety_policy == "align_to_target"
    assert metrics.schema_refresh_count >= 1
    assert metrics.schema_drift_detected_count >= 1
    assert metrics.schema_drift_aligned_count >= 1


@pytest.mark.asyncio
async def test_postgres_sink_routes_missing_required_column_failures_to_postgres_dlq(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_required_drift_it_{unique_suffix}"
    dlq_table = f"agora_required_drift_dlq_it_{unique_suffix}"
    dlq_records: list[object] = []

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.execute(f'ALTER TABLE "{table}" ADD COLUMN tenant_id TEXT NOT NULL')

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    IterableSource(
                        [
                            {"slug": "a", "display_name": "alpha"},
                            {"slug": "b", "display_name": "bravo"},
                        ]
                    )
                )
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="slug",
                        batch_size=2,
                        write_safety_policy=PostgresWriteSafetyPolicy.ALIGN_TO_TARGET,
                    ),
                    config=DeliveryConfig(
                        dlq=PostgresDLQSink(dsn=postgres_dsn, table=dlq_table),
                        sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
                    ),
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name, tenant_id FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink(dlq_records))  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert rows == []
    assert len(dlq_records) == 2
    for dlq_record in dlq_records:
        assert dlq_record.stage == "sink_write"
        assert dlq_record.error_type == "PostgresSinkWriteError"
        assert dlq_record.details["postgres"]["classification"] == "schema_drift"
        assert dlq_record.details["postgres"]["reason"] == "missing_required_columns"
        assert dlq_record.details["postgres"]["details"]["missing_required_columns"] == [
            "tenant_id"
        ]


@pytest.mark.asyncio
async def test_postgres_dlq_sink_dedupes_reconnect_after_commit_loss(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_dlq_commit_loss_{unique_suffix}"

    class _CommitLossOnceConnection:
        def __init__(self, conn) -> None:
            self._conn = conn
            self.commit_calls = 0
            self.rollback_calls = 0
            self.closed = False

        def cursor(self, *args, **kwargs):
            return self._conn.cursor(*args, **kwargs)

        async def commit(self) -> None:
            self.commit_calls += 1
            await self._conn.commit()
            raise psycopg.OperationalError("connection lost after dlq commit")

        async def rollback(self) -> None:
            self.rollback_calls += 1
            await self._conn.rollback()

        async def close(self) -> None:
            self.closed = True
            await self._conn.close()

    admin_conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    sink = PostgresDLQSink(dsn=postgres_dsn, table=table)
    wrapped_conn: _CommitLossOnceConnection | None = None
    try:
        await sink.open()
        real_conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=False)
        wrapped_conn = _CommitLossOnceConnection(real_conn)
        sink._conn = wrapped_conn  # type: ignore[attr-defined]
        sink._table_ready = True  # type: ignore[attr-defined]

        record = DLQRecord(
            pipeline_id="orders",
            run_id=f"run-{unique_suffix}",
            stage="sink_write",
            error_type="PostgresSinkWriteError",
            error_message="sink write failed",
            record={"slug": "a"},
            source="orders-source",
            checkpoint={"offset": 7},
            details={"postgres": {"reason": "commit_loss"}},
            middleware=None,
            sink="postgres",
            created_at=datetime.now().astimezone(),
            attempt=0,
            max_attempts=3,
        )

        await sink.write(record)

        async with admin_conn.cursor() as cur:
            await cur.execute(f'SELECT COUNT(*), MIN(id), MAX(id) FROM "{table}"')
            row = await cur.fetchone()
            await cur.execute(
                f'SELECT pipeline_id, run_id, stage, error_type FROM "{table}" ORDER BY id'
            )
            rows = await cur.fetchall()
    finally:
        await sink.close()
        async with admin_conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await admin_conn.close()

    assert wrapped_conn is not None
    assert wrapped_conn.commit_calls == 1
    assert wrapped_conn.rollback_calls == 1
    assert wrapped_conn.closed is True
    assert row == (1, record._storage_id, record._storage_id)
    assert rows == [
        (
            "orders",
            f"run-{unique_suffix}",
            "sink_write",
            "PostgresSinkWriteError",
        )
    ]
    metrics = sink.metrics_snapshot()
    assert metrics.inserted_record_count == 0
    assert metrics.upserted_record_count == 1
    assert metrics.updated_record_count == 1


@pytest.mark.asyncio
async def test_postgres_dlq_payload_policy_redacts_and_encrypts_against_real_database(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    redacted_table = f"agora_dlq_redacted_{unique_suffix}"
    encrypted_table = f"agora_dlq_encrypted_{unique_suffix}"
    redacted_policy = DLQPayloadPolicy.redacted(redact_fields=("ssn",))
    encrypted_policy = DLQPayloadPolicy.encrypted(
        encryptor=_ReverseCipher(),
        encryption_algorithm="reverse-test",
        encryption_key_id="integration-test",
    )

    redacted_sink = PostgresDLQSink(
        dsn=postgres_dsn,
        table=redacted_table,
        payload_policy=redacted_policy,
    )
    encrypted_sink = PostgresDLQSink(
        dsn=postgres_dsn,
        table=encrypted_table,
        payload_policy=encrypted_policy,
    )
    encrypted_source = PostgresDLQSource(
        dsn=postgres_dsn,
        table=encrypted_table,
        payload_policy=encrypted_policy,
    )
    admin_conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        redacted_record = DLQRecord(
            pipeline_id="orders",
            run_id=f"redacted-{unique_suffix}",
            stage="sink_write",
            error_type="PostgresSinkWriteError",
            error_message="sink write failed",
            record={"slug": "a", "password": "plain-secret"},
            original_record={"token": "raw-token"},
            processed_record={"ssn": "111-22-3333"},
            source="orders-source",
            checkpoint={"offset": 7},
            details={"client_secret": "client-secret"},
            middleware=None,
            sink="postgres",
            created_at=datetime.now().astimezone(),
            attempt=0,
            max_attempts=3,
        )
        encrypted_record = DLQRecord(
            pipeline_id="orders",
            run_id=f"encrypted-{unique_suffix}",
            stage="sink_write",
            error_type="PostgresSinkWriteError",
            error_message="sink write failed",
            record={"slug": "b", "password": "plain-secret"},
            original_record={"token": "raw-token"},
            processed_record={"ssn": "111-22-3333"},
            source="orders-source",
            checkpoint={"offset": 8, "api_key": "secret-api-key"},
            details={"client_secret": "client-secret"},
            middleware=None,
            sink="postgres",
            created_at=datetime.now().astimezone(),
            attempt=0,
            max_attempts=3,
        )

        await redacted_sink.open()
        await encrypted_sink.open()
        await redacted_sink.write(redacted_record)
        await encrypted_sink.write(encrypted_record)

        async with admin_conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT record, original_record, processed_record, checkpoint, details
                FROM "{redacted_table}"
                """
            )
            redacted_row = await cur.fetchone()
            await cur.execute(
                f"""
                SELECT record, original_record, processed_record, checkpoint, details
                FROM "{encrypted_table}"
                """
            )
            encrypted_row = await cur.fetchone()

        await encrypted_source.open()
        encrypted_records = [record async for record in encrypted_source.stream()]
    finally:
        await redacted_sink.close()
        await encrypted_sink.close()
        await encrypted_source.close()
        async with admin_conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{redacted_table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{encrypted_table}"')
        await admin_conn.close()

    redacted_storage = json.dumps(redacted_row, ensure_ascii=False, sort_keys=True, default=str)
    encrypted_storage = json.dumps(encrypted_row, ensure_ascii=False, sort_keys=True, default=str)
    for secret in ("plain-secret", "raw-token", "111-22-3333", "client-secret"):
        assert secret not in redacted_storage
        assert secret not in encrypted_storage
    assert "[REDACTED]" in redacted_storage
    assert "payload_encoding" in encrypted_storage
    assert encrypted_row[1:] == (None, None, None, None)
    assert len(encrypted_records) == 1
    assert encrypted_records[0].record == encrypted_record.record
    assert encrypted_records[0].original_record == encrypted_record.original_record
    assert encrypted_records[0].checkpoint == encrypted_record.checkpoint
    assert encrypted_records[0].details == encrypted_record.details


@pytest.mark.parametrize(
    ("case_name", "ddl_sql", "records", "expected_reason", "expected_row_count"),
    [
        (
            "not_null",
            """
            CREATE TABLE "{table}" (
                slug TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                tenant_id TEXT NOT NULL
            )
            """,
            [
                {"slug": "a", "display_name": "alpha"},
                {"slug": "b", "display_name": "bravo"},
            ],
            "not_null_violation",
            0,
        ),
        (
            "unique",
            """
            CREATE TABLE "{table}" (
                slug TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE
            )
            """,
            [
                {"slug": "a", "display_name": "alpha", "email": "dupe@example.com"},
                {"slug": "b", "display_name": "bravo", "email": "dupe@example.com"},
            ],
            "unique_violation",
            1,
        ),
        (
            "check",
            """
            CREATE TABLE "{table}" (
                slug TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0)
            )
            """,
            [
                {"slug": "a", "display_name": "alpha", "quantity": 0},
                {"slug": "b", "display_name": "bravo", "quantity": 0},
            ],
            "check_violation",
            0,
        ),
    ],
)
@pytest.mark.asyncio
async def test_postgres_sink_routes_constraint_violation_matrix_to_postgres_dlq(
    postgres_dsn: str,
    unique_suffix: str,
    case_name: str,
    ddl_sql: str,
    records: list[dict[str, object]],
    expected_reason: str,
    expected_row_count: int,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_constraint_{case_name}_it_{unique_suffix}"
    dlq_table = f"agora_constraint_{case_name}_dlq_it_{unique_suffix}"
    dlq_records: list[object] = []

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(ddl_sql.format(table=table))
            if case_name == "unique":
                await cur.execute(
                    f'INSERT INTO "{table}" (slug, display_name, email) VALUES (%s, %s, %s)',
                    ("seed", "seed", "dupe@example.com"),
                )

        summary = await asyncio.wait_for(
            (
                Pipeline(IterableSource(records))
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="slug",
                        batch_size=1,
                    ),
                    config=DeliveryConfig(
                        dlq=PostgresDLQSink(dsn=postgres_dsn, table=dlq_table),
                        sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
                    ),
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            row_count = (await cur.fetchone())[0]

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink(dlq_records))  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    assert summary.records_written == 0
    assert summary.records_errored == len(records)
    assert row_count == expected_row_count
    assert len(dlq_records) == len(records)
    for dlq_record in dlq_records:
        assert dlq_record.stage == "sink_write"
        assert dlq_record.error_type == "PostgresSinkWriteError"
        assert dlq_record.details["postgres"]["classification"] == "constraint_violation"
        assert dlq_record.details["postgres"]["reason"] == expected_reason


@pytest.mark.asyncio
async def test_postgres_sink_routes_foreign_key_violation_to_postgres_dlq(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    parent_table = f"agora_fk_parent_it_{unique_suffix}"
    child_table = f"agora_fk_child_it_{unique_suffix}"
    dlq_table = f"agora_fk_child_dlq_it_{unique_suffix}"
    dlq_records: list[object] = []

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{parent_table}" (
                    id BIGINT PRIMARY KEY
                )
                """
            )
            await cur.execute(
                f"""
                CREATE TABLE "{child_table}" (
                    slug TEXT PRIMARY KEY,
                    parent_id BIGINT NOT NULL REFERENCES "{parent_table}" (id)
                )
                """
            )

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    IterableSource(
                        [
                            {"slug": "a", "parent_id": 999},
                            {"slug": "b", "parent_id": 999},
                        ]
                    )
                )
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=child_table,
                        row_mapper=lambda row: row,
                        conflict_key="slug",
                        batch_size=1,
                    ),
                    config=DeliveryConfig(
                        dlq=PostgresDLQSink(dsn=postgres_dsn, table=dlq_table),
                        sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
                    ),
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT COUNT(*) FROM "{child_table}"')
            row_count = (await cur.fetchone())[0]

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink(dlq_records))  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{child_table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{parent_table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert row_count == 0
    assert len(dlq_records) == 2
    for dlq_record in dlq_records:
        assert dlq_record.stage == "sink_write"
        assert dlq_record.error_type == "PostgresSinkWriteError"
        assert dlq_record.details["postgres"]["classification"] == "constraint_violation"
        assert dlq_record.details["postgres"]["reason"] == "foreign_key_violation"


@pytest.mark.asyncio
async def test_postgres_sink_routes_type_mismatch_to_postgres_dlq(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_type_mismatch_it_{unique_suffix}"
    dlq_table = f"agora_type_mismatch_dlq_it_{unique_suffix}"
    dlq_records: list[object] = []

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    quantity BIGINT NOT NULL
                )
                """
            )

        summary = await asyncio.wait_for(
            (
                Pipeline(
                    IterableSource(
                        [
                            {"slug": "a", "quantity": "not-a-number"},
                            {"slug": "b", "quantity": "still-not-a-number"},
                        ]
                    )
                )
                .build(
                    PostgresSink(
                        dsn=postgres_dsn,
                        table=table,
                        row_mapper=lambda row: row,
                        conflict_key="slug",
                        batch_size=1,
                    ),
                    config=DeliveryConfig(
                        dlq=PostgresDLQSink(dsn=postgres_dsn, table=dlq_table),
                        sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
                    ),
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        async with conn.cursor() as cur:
            await cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            row_count = (await cur.fetchone())[0]

        await asyncio.wait_for(
            (
                Pipeline(PostgresDLQSource(dsn=postgres_dsn, table=dlq_table))
                .build(_CollectDLQSink(dlq_records))  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            await cur.execute(f'DROP TABLE IF EXISTS "{dlq_table}"')
        await conn.close()

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert row_count == 0
    assert len(dlq_records) == 2
    for dlq_record in dlq_records:
        assert dlq_record.stage == "sink_write"
        assert dlq_record.error_type == "PostgresSinkWriteError"
        assert dlq_record.details["postgres"]["classification"] == "type_mismatch"
        assert dlq_record.details["postgres"]["reason"] == "invalid_text_representation"


@pytest.mark.asyncio
async def test_postgres_source_resumes_from_checkpoint_cursor_against_real_database(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_src_it_{unique_suffix}"
    store = InMemoryCheckpointStore()

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.executemany(
                f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                [
                    (1, "alpha"),
                    (2, "bravo"),
                    (3, "charlie"),
                    (4, "delta"),
                ],
            )

        first_records: list[dict] = []
        second_records: list[dict] = []

        query = f'SELECT id, display_name FROM "{table}" WHERE id > %(last_id)s ORDER BY id'

        first_summary = await asyncio.wait_for(
            (
                Pipeline(
                    PostgresSource(
                        dsn=postgres_dsn,
                        query=query,
                        params={"last_id": 0},
                        row_mapper=lambda row: row,
                        batch_size=2,
                        checkpoint_field="id",
                        checkpoint_param="last_id",
                    )
                )
                .build(_CollectSink(first_records), checkpoint=store)  # type: ignore[arg-type]
                .run(max_records=2)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        second_summary = await asyncio.wait_for(
            (
                Pipeline(
                    PostgresSource(
                        dsn=postgres_dsn,
                        query=query,
                        params={"last_id": 0},
                        row_mapper=lambda row: row,
                        batch_size=2,
                        checkpoint_field="id",
                        checkpoint_param="last_id",
                    )
                )
                .build(_CollectSink(second_records), checkpoint=store)  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert first_records == [
        {"id": 1, "display_name": "alpha"},
        {"id": 2, "display_name": "bravo"},
    ]
    assert second_records == [
        {"id": 3, "display_name": "charlie"},
        {"id": 4, "display_name": "delta"},
    ]
    assert first_summary.last_checkpoint is not None
    assert first_summary.last_checkpoint.value["cursor"] == 2
    assert second_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint.value["cursor"] == 4


@pytest.mark.asyncio
async def test_postgres_source_checkpoint_resume_survives_database_restart(
    postgres_dsn: str,
    unique_suffix: str,
    postgres_service_control,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_src_restart_it_{unique_suffix}"
    store = InMemoryCheckpointStore()
    first_records: list[dict] = []
    second_records: list[dict] = []

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.executemany(
                f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                [
                    (1, "alpha"),
                    (2, "bravo"),
                    (3, "charlie"),
                    (4, "delta"),
                ],
            )

        query = f'SELECT id, display_name FROM "{table}" WHERE id > %(last_id)s ORDER BY id'

        first_summary = await asyncio.wait_for(
            (
                Pipeline(
                    PostgresSource(
                        dsn=postgres_dsn,
                        query=query,
                        params={"last_id": 0},
                        row_mapper=lambda row: row,
                        batch_size=1,
                        checkpoint_field="id",
                        checkpoint_param="last_id",
                    )
                )
                .build(  # type: ignore[arg-type]
                    _CollectSink(first_records),
                    config=DeliveryConfig(checkpoint=store),
                )
                .run(max_records=1)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        await asyncio.to_thread(postgres_service_control)

        second_summary = await asyncio.wait_for(
            (
                Pipeline(
                    PostgresSource(
                        dsn=postgres_dsn,
                        query=query,
                        params={"last_id": 0},
                        row_mapper=lambda row: row,
                        batch_size=1,
                        checkpoint_field="id",
                        checkpoint_param="last_id",
                    )
                )
                .build(  # type: ignore[arg-type]
                    _CollectSink(second_records),
                    config=DeliveryConfig(checkpoint=store),
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert first_records == [
        {"id": 1, "display_name": "alpha"},
    ]
    assert second_records == [
        {"id": 2, "display_name": "bravo"},
        {"id": 3, "display_name": "charlie"},
        {"id": 4, "display_name": "delta"},
    ]
    assert first_summary.last_checkpoint is not None
    assert first_summary.last_checkpoint.value["cursor"] == 1
    assert second_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint.value["cursor"] == 4


@pytest.mark.asyncio
async def test_postgres_sink_ha_failover_reconnects_via_client_route(
    postgres_ha_dsn: str,
    unique_suffix: str,
    postgres_ha_control,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_ha_sink_it_{unique_suffix}"

    await asyncio.to_thread(postgres_ha_control.reset_cluster)
    current_primary = await asyncio.to_thread(postgres_ha_control.current_primary)
    replica_node = (
        "postgres-standby" if current_primary == "postgres-primary" else "postgres-primary"
    )

    conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_ha_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=2,
        retry_policy=RetryPolicy(
            max_attempts=8,
            initial_backoff_s=0.25,
            max_backoff_s=2.0,
            retry_exceptions=(psycopg.Error,),
        ),
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        await sink.write_batch(
            [
                {"slug": "a", "display_name": "alpha"},
                {"slug": "b", "display_name": "bravo"},
            ]
        )
        await sink.flush()

        postgres_ha_control.wait_for_table_row_count(
            replica_node,
            table,
            expected_count=2,
        )
        failed_primary, promoted_primary = await asyncio.to_thread(
            postgres_ha_control.failover_primary
        )

        await sink.write_batch(
            [
                {"slug": "a", "display_name": "alpha-updated"},
                {"slug": "c", "display_name": "charlie"},
            ]
        )
        await sink.flush()
        metrics = sink.metrics_snapshot()

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        await sink.close()
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()
        await asyncio.to_thread(postgres_ha_control.restore_cluster_for_teardown)

    assert failed_primary == current_primary
    assert promoted_primary == replica_node
    assert metrics.retry_count >= 1
    assert rows == [
        ("a", "alpha-updated"),
        ("b", "bravo"),
        ("c", "charlie"),
    ]


@pytest.mark.asyncio
async def test_postgres_source_ha_failover_resume_semantics_hold_across_client_route(
    postgres_ha_dsn: str,
    unique_suffix: str,
    postgres_ha_control,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_ha_source_it_{unique_suffix}"
    store = InMemoryCheckpointStore()
    first_records: list[dict] = []
    second_records: list[dict] = []

    await asyncio.to_thread(postgres_ha_control.reset_cluster)
    current_primary = await asyncio.to_thread(postgres_ha_control.current_primary)
    replica_node = (
        "postgres-standby" if current_primary == "postgres-primary" else "postgres-primary"
    )

    conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.executemany(
                f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                [
                    (1, "alpha"),
                    (2, "bravo"),
                    (3, "charlie"),
                    (4, "delta"),
                ],
            )

        postgres_ha_control.wait_for_table_row_count(
            replica_node,
            table,
            expected_count=4,
        )

        query = f'SELECT id, display_name FROM "{table}" WHERE id > %(last_id)s ORDER BY id'

        def _build_source() -> PostgresSource[dict]:
            source = PostgresSource(
                dsn=postgres_ha_dsn,
                query=query,
                params={"last_id": 0},
                row_mapper=lambda row: row,
                batch_size=1,
                checkpoint_field="id",
                checkpoint_param="last_id",
            )
            contract = source.recovery_contract()
            assert contract.mode is PostgresSourceRecoveryMode.CHECKPOINT_RERUN
            assert contract.supports_checkpoint is True
            assert contract.requires_pipeline_rerun is True
            assert contract.transparent_failover is False
            return source

        first_summary = await asyncio.wait_for(
            (
                Pipeline(_build_source())
                .build(  # type: ignore[arg-type]
                    _CollectSink(first_records),
                    config=DeliveryConfig(checkpoint=store),
                )
                .run(max_records=2)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        failed_primary, promoted_primary = await asyncio.to_thread(
            postgres_ha_control.failover_primary
        )

        second_summary = await asyncio.wait_for(
            (
                Pipeline(_build_source())
                .build(  # type: ignore[arg-type]
                    _CollectSink(second_records),
                    config=DeliveryConfig(checkpoint=store),
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()
        await asyncio.to_thread(postgres_ha_control.restore_cluster_for_teardown)

    assert failed_primary == current_primary
    assert promoted_primary == replica_node
    assert first_records == [
        {"id": 1, "display_name": "alpha"},
        {"id": 2, "display_name": "bravo"},
    ]
    assert second_records == [
        {"id": 3, "display_name": "charlie"},
        {"id": 4, "display_name": "delta"},
    ]
    assert first_summary.last_checkpoint is not None
    assert first_summary.last_checkpoint.value["cursor"] == 2
    assert second_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint.value["cursor"] == 4


@pytest.mark.asyncio
async def test_postgres_source_ha_failover_soak_preserves_resume_semantics_with_retry_and_safety_controls(
    postgres_ha_dsn: str,
    unique_suffix: str,
    postgres_ha_control,
    postgres_ha_soak_cycles: int,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_ha_source_cycle_it_{unique_suffix}"
    store = InMemoryCheckpointStore()
    phase_count = postgres_ha_soak_cycles + 1
    all_rows = [
        {"id": index, "display_name": f"customer-{index}"} for index in range(1, phase_count + 1)
    ]
    collected_records: list[dict] = []
    phase_metrics = []
    transitions: list[tuple[str, str]] = []
    source_dsn = f"{postgres_ha_dsn}&connect_timeout=2"

    await _run_postgres_ha_step(postgres_ha_control.reset_cluster)
    standby_node = await _run_postgres_ha_step(postgres_ha_control.current_standby)

    conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.executemany(
                f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                [(row["id"], row["display_name"]) for row in all_rows],
            )

        postgres_ha_control.wait_for_table_row_count(
            standby_node,
            table,
            expected_count=phase_count,
        )

        query = f'SELECT id, display_name FROM "{table}" WHERE id > %(last_id)s ORDER BY id'

        def _build_source(phase_index: int) -> PostgresSource[dict]:
            source = PostgresSource(
                dsn=source_dsn,
                query=query,
                params={"last_id": 0},
                row_mapper=lambda row: row,
                batch_size=1,
                checkpoint_field="id",
                checkpoint_param="last_id",
                retry_policy=RetryPolicy(
                    max_attempts=8,
                    initial_backoff_s=0.1,
                    max_backoff_s=0.5,
                    retry_exceptions=(psycopg.Error,),
                ),
                statement_timeout_ms=5_000,
                transaction_read_only=True,
                transaction_isolation_level="repeatable_read",
                fetch_strategy="server_side",
                server_side_cursor_name=f"agora_ha_source_soak_{phase_index}",
                server_side_cursor_withhold=True,
            )
            contract = source.recovery_contract()
            assert contract.mode is PostgresSourceRecoveryMode.CHECKPOINT_RERUN
            assert contract.supports_checkpoint is True
            assert contract.requires_pipeline_rerun is True
            assert contract.transparent_failover is False
            return source

        for phase_index in range(phase_count):
            source = _build_source(phase_index)
            summary = await asyncio.wait_for(
                (
                    Pipeline(source)
                    .build(  # type: ignore[arg-type]
                        _CollectSink(collected_records),
                        config=DeliveryConfig(checkpoint=store),
                    )
                    .run(max_records=1)
                ),
                timeout=_INTEGRATION_TIMEOUT_S,
            )

            metrics = source.metrics_snapshot()
            phase_metrics.append(metrics)

            assert summary.last_checkpoint is not None
            assert summary.last_checkpoint.value["cursor"] == phase_index + 1
            assert metrics.fetch_strategy == "server_side"
            assert metrics.statement_timeout_ms == 5_000
            assert metrics.transaction_read_only is True
            assert metrics.transaction_isolation_level == "repeatable_read"
            assert metrics.server_side_cursor_withhold is True
            assert metrics.rows_seen == 1
            assert metrics.last_checkpoint_cursor_present is True
            assert metrics.last_stream_succeeded is True
            assert metrics.retry_count == 0
            assert metrics.query_execution_count == 1
            if phase_index < postgres_ha_soak_cycles:
                transitions.append(await asyncio.to_thread(postgres_ha_control.failover_cycle))
    finally:
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()
        await asyncio.to_thread(postgres_ha_control.restore_cluster_for_teardown)

    assert len(transitions) == postgres_ha_soak_cycles
    assert {node for transition in transitions for node in transition} == {
        "postgres-primary",
        "postgres-standby",
    }
    assert len(phase_metrics) == phase_count
    assert collected_records == all_rows


@pytest.mark.asyncio
async def test_postgres_source_standby_route_fail_closes_when_replica_is_stale(
    postgres_ha_dsn: str,
    unique_suffix: str,
    postgres_ha_control,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_ha_source_stale_it_{unique_suffix}"
    source_dsn = f"{postgres_ha_dsn}&connect_timeout=2"

    await asyncio.to_thread(postgres_ha_control.reset_cluster)
    standby_node = await asyncio.to_thread(postgres_ha_control.current_standby)
    standby_admin_dsn = postgres_ha_control.admin_node_dsn(standby_node)
    standby_node_dsn = postgres_ha_control.node_dsn(standby_node)

    conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.executemany(
                f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                [
                    (1, "alpha"),
                    (2, "bravo"),
                ],
            )

        await _run_postgres_ha_step(
            postgres_ha_control.wait_for_table_row_count,
            standby_node,
            table,
            expected_count=2,
        )
        await _set_replica_replay_paused(standby_admin_dsn, paused=True)

        async with conn.cursor() as cur:
            await cur.execute(
                f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                (3, "charlie"),
            )

        await _wait_for_replica_replay_lag_at_least(
            standby_node_dsn,
            min_lag_s=_REPLICA_STALE_LAG_S,
        )

        source = PostgresSource(
            dsn=source_dsn,
            query=f'SELECT id, display_name FROM "{table}" ORDER BY id',
            row_mapper=lambda row: row,
            read_routing="standby",
            max_replica_replay_lag_s=_REPLICA_STALE_LAG_S,
        )

        with pytest.raises(PostgresReplicaStalenessError, match="replay lag exceeded"):
            [record async for record in source.stream()]

        metrics = source.metrics_snapshot()
    finally:
        try:
            await _set_replica_replay_paused(standby_admin_dsn, paused=False)
            await _run_postgres_ha_step(
                postgres_ha_control.wait_for_table_row_count,
                standby_node,
                table,
                expected_count=3,
            )
        except Exception:
            pass
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()
        await _run_postgres_ha_step(postgres_ha_control.restore_cluster_for_teardown)

    assert metrics.connected_server_role == "standby"
    assert metrics.last_replica_replay_lag_s is not None
    assert metrics.last_replica_replay_lag_s >= _REPLICA_STALE_LAG_S
    assert metrics.staleness_guard_block_count == 1
    assert metrics.staleness_guard_primary_fallback_count == 0


@pytest.mark.asyncio
async def test_postgres_source_prefer_standby_falls_back_to_primary_across_multi_cycle_failover(
    postgres_ha_dsn: str,
    unique_suffix: str,
    postgres_ha_control,
    postgres_ha_soak_cycles: int,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_ha_route_it_{unique_suffix}"
    store = InMemoryCheckpointStore()
    phase_count = postgres_ha_soak_cycles + 1
    source_dsn = f"{postgres_ha_dsn}&connect_timeout=2"
    collected_records: list[dict] = []
    transitions: list[tuple[str, str]] = []
    phase_metrics = []

    await _run_postgres_ha_step(postgres_ha_control.reset_cluster)

    conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        for phase_index in range(phase_count):
            standby_node = await _run_postgres_ha_step(postgres_ha_control.current_standby)
            standby_admin_dsn = postgres_ha_control.admin_node_dsn(standby_node)
            standby_node_dsn = postgres_ha_control.node_dsn(standby_node)

            await _set_replica_replay_paused(standby_admin_dsn, paused=True)
            writer_conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
            try:
                async with writer_conn.cursor() as cur:
                    await cur.execute(
                        f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                        (phase_index + 1, f"customer-{phase_index + 1}"),
                    )
            finally:
                await writer_conn.close()

            await _wait_for_replica_replay_lag_at_least(
                standby_node_dsn,
                min_lag_s=_REPLICA_STALE_LAG_S,
            )

            source = PostgresSource(
                dsn=source_dsn,
                query=f'SELECT id, display_name FROM "{table}" WHERE id > %(last_id)s ORDER BY id',
                params={"last_id": 0},
                row_mapper=lambda row, context: {
                    **row,
                    "server_role": context["connected_server_role"],
                },
                batch_size=1,
                checkpoint_field="id",
                checkpoint_param="last_id",
                read_routing="prefer_standby",
                max_replica_replay_lag_s=_REPLICA_STALE_LAG_S,
                on_replica_stale="route_primary",
            )

            summary = await asyncio.wait_for(
                (
                    Pipeline(source)
                    .build(  # type: ignore[arg-type]
                        _CollectSink(collected_records),
                        config=DeliveryConfig(checkpoint=store),
                    )
                    .run(max_records=1)
                ),
                timeout=_INTEGRATION_TIMEOUT_S,
            )

            metrics = source.metrics_snapshot()
            phase_metrics.append(metrics)

            assert summary.last_checkpoint is not None
            assert summary.last_checkpoint.value["cursor"] == phase_index + 1
            assert metrics.connected_server_role == "primary"
            assert metrics.staleness_guard_block_count == 0
            assert metrics.staleness_guard_primary_fallback_count == 1

            await _set_replica_replay_paused(standby_admin_dsn, paused=False)
            await _run_postgres_ha_step(
                postgres_ha_control.wait_for_table_row_count,
                standby_node,
                table,
                expected_count=phase_index + 1,
            )
            current_primary = await _run_postgres_ha_step(postgres_ha_control.current_primary)
            await _run_postgres_ha_step(
                postgres_ha_control.wait_for_replication_ready,
                current_primary,
                standby_node,
            )

            if phase_index < postgres_ha_soak_cycles:
                transitions.append(await _run_postgres_ha_step(postgres_ha_control.failover_cycle))
    finally:
        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()
        await _run_postgres_ha_step(postgres_ha_control.restore_cluster_for_teardown)

    assert len(transitions) == postgres_ha_soak_cycles
    assert {node for transition in transitions for node in transition} == {
        "postgres-primary",
        "postgres-standby",
    }
    assert len(phase_metrics) == phase_count
    assert collected_records == [
        {
            "id": index,
            "display_name": f"customer-{index}",
            "server_role": "primary",
        }
        for index in range(1, phase_count + 1)
    ]


@pytest.mark.asyncio
async def test_postgres_sink_ha_failover_reconnects_across_multi_cycle_route_changes(
    postgres_ha_dsn: str,
    unique_suffix: str,
    postgres_ha_control,
    postgres_ha_soak_cycles: int,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_ha_sink_cycle_it_{unique_suffix}"
    batch_size = 2
    phase_count = postgres_ha_soak_cycles + 1
    all_rows = [
        {
            "slug": f"customer-{index}",
            "display_name": f"customer-{index}".upper(),
        }
        for index in range(phase_count * batch_size)
    ]

    await asyncio.to_thread(postgres_ha_control.reset_cluster)

    conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
    sink = PostgresSink(
        dsn=postgres_ha_dsn,
        table=table,
        row_mapper=lambda row: row,
        conflict_key="slug",
        batch_size=batch_size,
        retry_policy=RetryPolicy(
            max_attempts=8,
            initial_backoff_s=0.25,
            max_backoff_s=2.0,
            retry_exceptions=(psycopg.Error,),
        ),
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )

        transitions: list[tuple[str, str]] = []
        delivered_count = 0
        for phase_index in range(phase_count):
            batch = all_rows[phase_index * batch_size : (phase_index + 1) * batch_size]
            await sink.write_batch(batch)
            await sink.flush()
            delivered_count += len(batch)

            standby_node = await asyncio.to_thread(postgres_ha_control.current_standby)
            await asyncio.to_thread(
                postgres_ha_control.wait_for_table_row_count,
                standby_node,
                table,
                expected_count=delivered_count,
            )

            if phase_index < postgres_ha_soak_cycles:
                transitions.append(await asyncio.to_thread(postgres_ha_control.failover_cycle))

        metrics = sink.metrics_snapshot()

        await conn.close()
        conn = await psycopg.AsyncConnection.connect(postgres_ha_dsn, autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f'SELECT slug, display_name FROM "{table}" ORDER BY slug')
            rows = await cur.fetchall()
    finally:
        await sink.close()
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()
        await asyncio.to_thread(postgres_ha_control.restore_cluster_for_teardown)

    assert len(transitions) == postgres_ha_soak_cycles
    assert {node for transition in transitions for node in transition} == {
        "postgres-primary",
        "postgres-standby",
    }
    assert metrics.retry_count >= postgres_ha_soak_cycles
    assert rows == [
        (str(row["slug"]), str(row["display_name"]))
        for row in sorted(all_rows, key=lambda item: str(item["slug"]))
    ]


@pytest.mark.asyncio
async def test_postgres_source_can_log_and_continue_when_checkpoint_store_fails(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_cp_it_{unique_suffix}"

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.executemany(
                f'INSERT INTO "{table}" (id, display_name) VALUES (%s, %s)',
                [(1, "alpha"), (2, "bravo")],
            )

        collected: list[dict] = []
        summary = await asyncio.wait_for(
            (
                Pipeline(
                    PostgresSource(
                        dsn=postgres_dsn,
                        query=f'SELECT id, display_name FROM "{table}" WHERE id > %(last_id)s ORDER BY id',
                        row_mapper=lambda row: row,
                        params={"last_id": 0},
                        batch_size=2,
                        checkpoint_field="id",
                        checkpoint_param="last_id",
                    )
                )
                .build(  # type: ignore[arg-type]
                    _CollectSink(collected),
                    checkpoint=_FailingCheckpointStore(),
                    checkpoint_failure_policy=CheckpointFailurePolicy.LOG_AND_CONTINUE,
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert collected == [
        {"id": 1, "display_name": "alpha"},
        {"id": 2, "display_name": "bravo"},
    ]
    assert summary.runtime.checkpoint_failure_count == 2
    assert summary.runtime.checkpoint_save_count == 0


@pytest.mark.asyncio
async def test_postgres_source_resumes_from_composite_checkpoint_cursor_against_real_database(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    table = f"agora_src_cmp_it_{unique_suffix}"
    store = InMemoryCheckpointStore()

    conn = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    created_at TIMESTAMP NOT NULL,
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )
                """
            )
            await cur.executemany(
                f'INSERT INTO "{table}" (created_at, id, display_name) VALUES (%s, %s, %s)',
                [
                    ("2024-01-01 00:00:00", 1, "alpha"),
                    ("2024-01-01 00:00:00", 2, "bravo"),
                    ("2024-01-01 00:00:00", 3, "charlie"),
                    ("2024-01-02 00:00:00", 4, "delta"),
                ],
            )

        first_records: list[dict] = []
        second_records: list[dict] = []

        query = (
            f'SELECT created_at, id, display_name FROM "{table}" '
            "WHERE created_at > %(last_created_at)s "
            "OR (created_at = %(last_created_at)s AND id > %(last_id)s) "
            "ORDER BY created_at, id"
        )

        first_summary = await asyncio.wait_for(
            (
                Pipeline(
                    PostgresSource(
                        dsn=postgres_dsn,
                        query=query,
                        params={"last_created_at": "2023-12-31 00:00:00", "last_id": 0},
                        row_mapper=lambda row: row,
                        batch_size=2,
                        checkpoint_fields=["created_at", "id"],
                        checkpoint_params={"created_at": "last_created_at", "id": "last_id"},
                    )
                )
                .build(_CollectSink(first_records), checkpoint=store)  # type: ignore[arg-type]
                .run(max_records=2)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )

        second_summary = await asyncio.wait_for(
            (
                Pipeline(
                    PostgresSource(
                        dsn=postgres_dsn,
                        query=query,
                        params={"last_created_at": "2023-12-31 00:00:00", "last_id": 0},
                        row_mapper=lambda row: row,
                        batch_size=2,
                        checkpoint_fields=["created_at", "id"],
                        checkpoint_params={"created_at": "last_created_at", "id": "last_id"},
                    )
                )
                .build(_CollectSink(second_records), checkpoint=store)  # type: ignore[arg-type]
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    finally:
        async with conn.cursor() as cur:
            await cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        await conn.close()

    assert first_records == [
        {"created_at": datetime(2024, 1, 1, 0, 0, 0), "id": 1, "display_name": "alpha"},
        {"created_at": datetime(2024, 1, 1, 0, 0, 0), "id": 2, "display_name": "bravo"},
    ]
    assert second_records == [
        {"created_at": datetime(2024, 1, 1, 0, 0, 0), "id": 3, "display_name": "charlie"},
        {"created_at": datetime(2024, 1, 2, 0, 0, 0), "id": 4, "display_name": "delta"},
    ]
    assert first_summary.last_checkpoint is not None
    assert first_summary.last_checkpoint.value["cursor"]["id"] == 2
    assert second_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint.value["cursor"]["id"] == 4


@pytest.mark.asyncio
async def test_pipeline_can_raise_when_dlq_write_fails_in_integration_mode() -> None:
    class _BoomMiddleware(Middleware[dict, dict]):
        name = "boom"

        async def process(self, record: dict, ctx):
            raise RuntimeError("middleware blew up")

    class _FailingDLQSink:
        sink_name = "failing_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            raise RuntimeError("dlq broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="dlq broke"):
        await asyncio.wait_for(
            (
                Pipeline(IterableSource([{"id": 1}]))
                .pipe(_BoomMiddleware())
                .build(  # type: ignore[arg-type]
                    _CollectSink([]),
                    config=DeliveryConfig(
                        dlq=_FailingDLQSink(),
                        dlq_failure_policy=DLQFailurePolicy.RAISE,
                    ),
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
