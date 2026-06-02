from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from agora import InMemoryCheckpointStore, IterableSource, Pipeline
from agora.core.middleware import Middleware
from agora.core.types import CheckpointFailurePolicy, DLQFailurePolicy

from agora_plugins.postgres import PostgresSink, PostgresSource

pytestmark = pytest.mark.integration
_INTEGRATION_TIMEOUT_S = 30.0


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


class _FailingCheckpointStore:
    async def load(self, key: str):
        return None

    async def save(self, key: str, checkpoint) -> None:
        raise RuntimeError("checkpoint broke")

    async def close(self) -> None:
        return None


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
                    dlq=_FailingDLQSink(),
                    dlq_failure_policy=DLQFailurePolicy.RAISE,
                )
                .run()
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
