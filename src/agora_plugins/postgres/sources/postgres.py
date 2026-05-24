"""
agora_plugins.postgres.sources.postgres
=======================================
Async PostgreSQL source that streams rows from a SQL query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct
from agora.core.source import BaseSource, SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


class PostgresSource(BaseSource[T], Generic[T]):
    """Async PostgreSQL source that streams rows from a SQL query."""

    source_name = "postgres"

    def __init__(
        self,
        dsn: str,
        query: str,
        row_mapper: Callable[[dict[str, Any]], T | None],
        params: dict[str, Any] | None = None,
        batch_size: int = 500,
        checkpoint_field: str | None = None,
        checkpoint_param: str | None = None,
        checkpoint_fields: list[str] | None = None,
        checkpoint_params: dict[str, str] | None = None,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
    ) -> None:
        singular_config = checkpoint_field is not None or checkpoint_param is not None
        composite_config = checkpoint_fields is not None or checkpoint_params is not None

        if singular_config and composite_config:
            raise ValueError(
                "Use either checkpoint_field/checkpoint_param or "
                "checkpoint_fields/checkpoint_params, not both."
            )
        if singular_config and (checkpoint_field is None or checkpoint_param is None):
            raise ValueError("checkpoint_field and checkpoint_param must be provided together.")
        if composite_config and (not checkpoint_fields or not checkpoint_params):
            raise ValueError("checkpoint_fields and checkpoint_params must be provided together.")
        if checkpoint_fields is not None and checkpoint_params is not None:
            missing = [field for field in checkpoint_fields if field not in checkpoint_params]
            if missing:
                raise ValueError(
                    "checkpoint_params must provide a query parameter for every "
                    f"checkpoint field. Missing: {missing!r}"
                )

        self._dsn = dsn
        self._query = query
        self._row_mapper = row_mapper
        self._base_params = dict(params or {})
        self._params = dict(self._base_params)
        self._batch_size = batch_size
        self._checkpoint_field = checkpoint_field
        self._checkpoint_param = checkpoint_param
        self._checkpoint_fields = list(checkpoint_fields or [])
        self._checkpoint_params = dict(checkpoint_params or {})
        self._on_record_error = on_record_error
        self.supports_checkpoint = bool(
            (checkpoint_field and checkpoint_param)
            or (self._checkpoint_fields and self._checkpoint_params)
        )
        self._rows_seen = 0
        self._last_checkpoint_cursor: Any | None = None
        self._record_error_count = 0
        self._record_drop_count = 0

    async def prepare_resume(self, checkpoint) -> None:
        self._reset_progress()
        self._params = dict(self._base_params)
        if checkpoint is None or not self.supports_checkpoint:
            return

        value = checkpoint.value if isinstance(checkpoint.value, dict) else {}
        if "cursor" not in value:
            return
        cursor = value["cursor"]

        if self._checkpoint_param is not None:
            self._params[self._checkpoint_param] = cursor
            return

        if not isinstance(cursor, dict):
            raise TypeError(
                "Composite PostgresSource checkpoints require cursor values to be dicts."
            )
        for field in self._checkpoint_fields:
            param_name = self._checkpoint_params[field]
            if field not in cursor:
                raise ValueError(
                    f"Checkpoint cursor is missing composite field {field!r}: {cursor!r}"
                )
            self._params[param_name] = cursor[field]

    def current_checkpoint(self) -> dict[str, Any] | None:
        if self._rows_seen <= 0 and self._last_checkpoint_cursor is None:
            return None
        checkpoint: dict[str, Any] = {"row_number": self._rows_seen}
        if self._last_checkpoint_cursor is not None:
            checkpoint["cursor"] = self._last_checkpoint_cursor
        return checkpoint

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    async def stream(self) -> AsyncGenerator[T, None]:  # type: ignore[override]
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError(
                "PostgresSource requires psycopg. Install via: pip install 'agora-postgres'"
            ) from None

        logger.info("postgres_source_start", query=self._query[:80])
        self._reset_progress()
        fetched = 0

        async with (
            await psycopg.AsyncConnection.connect(self._dsn, row_factory=dict_row) as conn,
            conn.cursor() as cur,
        ):
            await cur.execute(self._query, self._params if self._params else None)
            while True:
                rows = await cur.fetchmany(self._batch_size)
                if not rows:
                    break
                for row in rows:
                    row_dict: dict[str, Any] | None = None
                    try:
                        row_dict = dict(row)
                        self._rows_seen += 1
                        self._last_checkpoint_cursor = self._extract_checkpoint_cursor(row_dict)
                        record = self._row_mapper(row_dict)
                        if record is not None:
                            fetched += 1
                            yield record
                        else:
                            self._record_drop_count += 1
                    except Exception as exc:
                        self._record_error_count += 1
                        logger.warning("postgres_source_row_error", error=str(exc))
                        if self._on_record_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                            self._record_drop_count += 1
                            continue
                        failed_record = row_dict if row_dict is not None else row
                        raise SourceRecordError(
                            exc,
                            record=failed_record,
                            checkpoint=self.current_checkpoint(),
                            source=self.source_name,
                        ) from exc

        logger.info("postgres_source_done", records=fetched)

    def _extract_checkpoint_cursor(self, row_dict: dict[str, Any]) -> Any | None:
        if self._checkpoint_field is not None:
            if self._checkpoint_field not in row_dict:
                raise KeyError(f"Checkpoint field {self._checkpoint_field!r} missing from row")
            return row_dict[self._checkpoint_field]

        if self._checkpoint_fields:
            cursor: dict[str, Any] = {}
            for field in self._checkpoint_fields:
                if field not in row_dict:
                    raise KeyError(f"Checkpoint field {field!r} missing from row")
                cursor[field] = row_dict[field]
            return cursor

        return None

    def _reset_progress(self) -> None:
        self._rows_seen = 0
        self._last_checkpoint_cursor = None
        self._record_error_count = 0
        self._record_drop_count = 0


__all__ = ["PostgresSource"]
