"""Connection-scoped PostgreSQL sink write strategies."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import logstruct
from agora.core.retry import RetryPolicy, retry_async

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from contextlib import AbstractAsyncContextManager

    from agora_plugins.postgres.sinks._identifiers import QuotedIdentifier

logger = logstruct.getLogger("agora_plugins.postgres.sinks.postgres")


class PostgresWriteOwner(Protocol):
    """Minimal sink surface required by SQL/COPY strategy execution."""

    _table: str | QuotedIdentifier

    def _write_connection(self) -> AbstractAsyncContextManager[Any]: ...

    def _iter_sql_chunks(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> Iterator[list[dict[str, Any]]]: ...

    def _build_batch_upsert_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        *,
        row_count: int,
    ) -> str: ...

    def _flatten_rows(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> list[Any]: ...

    def _build_copy_sql(self, columns: Sequence[str | QuotedIdentifier]) -> str: ...

    def _build_stage_table_name(self) -> str: ...

    def _build_create_temp_table_sql(self, staging_table: str) -> str: ...

    def _build_copy_sql_for_table(
        self,
        table: str | QuotedIdentifier,
        columns: Sequence[str | QuotedIdentifier],
    ) -> str: ...

    def _build_copy_merge_sql(
        self,
        columns: Sequence[str | QuotedIdentifier],
        staging_table: str,
    ) -> str: ...

    def _observe_retry(self) -> None: ...

    def _wrap_write_error(
        self,
        exc: Exception,
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> Exception: ...


async def flush_via_sql(
    owner: PostgresWriteOwner,
    rows: list[dict[str, Any]],
    columns: list[str],
    count: int,
    policy: RetryPolicy[Any],
) -> None:
    def _on_retry(attempt: int, exc: Exception, delay: float) -> None:
        logger.warning(
            "postgres_flush_retry",
            table=owner._table,
            count=count,
            attempt=attempt,
            wait_s=delay,
            error=str(exc),
        )
        owner._observe_retry()

    async def _execute_flush() -> None:
        async with owner._write_connection() as conn:
            try:
                async with conn.cursor() as cur:
                    for chunk in owner._iter_sql_chunks(rows, columns):
                        sql = owner._build_batch_upsert_sql(columns, row_count=len(chunk))
                        params = owner._flatten_rows(chunk, columns)
                        await cur.execute(sql, params)
                await conn.commit()
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception(
                        "postgres_rollback_error",
                        table=owner._table,
                        count=count,
                    )
                raise

    try:
        await retry_async(
            _execute_flush,
            policy=policy,
            on_retry=_on_retry,
        )
    except Exception as exc:
        logger.exception("postgres_flush_error", table=owner._table, count=count)
        raise owner._wrap_write_error(exc, rows=rows, columns=columns) from exc


async def flush_via_copy(
    owner: PostgresWriteOwner,
    rows: list[dict[str, Any]],
    columns: list[str],
    count: int,
) -> None:
    sql = owner._build_copy_sql(columns)

    async def _execute_copy() -> None:
        async with owner._write_connection() as conn:
            try:
                async with conn.cursor() as cur, cur.copy(sql) as copy:
                    for row in rows:
                        await copy.write_row([row[column] for column in columns])
                await conn.commit()
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception(
                        "postgres_rollback_error",
                        table=owner._table,
                        count=count,
                    )
                raise

    try:
        await _execute_copy()
    except Exception as exc:
        logger.exception("postgres_copy_error", table=owner._table, count=count)
        raise owner._wrap_write_error(exc, rows=rows, columns=columns) from exc


async def flush_via_copy_merge(
    owner: PostgresWriteOwner,
    rows: list[dict[str, Any]],
    columns: list[str],
    count: int,
    policy: RetryPolicy[Any],
) -> None:
    staging_table = owner._build_stage_table_name()
    create_sql = owner._build_create_temp_table_sql(staging_table)
    copy_sql = owner._build_copy_sql_for_table(staging_table, columns)
    merge_sql = owner._build_copy_merge_sql(columns, staging_table)

    def _on_retry(attempt: int, exc: Exception, delay: float) -> None:
        logger.warning(
            "postgres_copy_merge_retry",
            table=owner._table,
            count=count,
            attempt=attempt,
            wait_s=delay,
            error=str(exc),
        )
        owner._observe_retry()

    async def _execute_copy_merge() -> None:
        async with owner._write_connection() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(create_sql)
                async with conn.cursor() as cur, cur.copy(copy_sql) as copy:
                    for row in rows:
                        await copy.write_row([row[column] for column in columns])
                async with conn.cursor() as cur:
                    await cur.execute(merge_sql)
                await conn.commit()
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception(
                        "postgres_rollback_error",
                        table=owner._table,
                        count=count,
                    )
                raise

    try:
        await retry_async(
            _execute_copy_merge,
            policy=policy,
            on_retry=_on_retry,
        )
    except Exception as exc:
        logger.exception("postgres_copy_merge_error", table=owner._table, count=count)
        raise owner._wrap_write_error(exc, rows=rows, columns=columns) from exc
