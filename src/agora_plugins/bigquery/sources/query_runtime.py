"""Query planning and batch-pump runtime for BigQuery sources."""

from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal
from itertools import islice
from queue import Full, Queue
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from threading import Event

_BQ_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class BigQuerySourceQueryRuntime:
    """Public-facing query planning and execution runtime for BigQuery sources."""

    def __init__(
        self,
        *,
        mode: Literal["table", "query"],
        table: str | None,
        query: str | None,
        query_parameters: dict[str, Any],
        columns: tuple[str, ...],
        order_by: tuple[str, ...],
        checkpoint_column: str | None,
        checkpoint_tiebreaker_column: str | None,
        batch_size: int,
        supports_checkpoint: bool,
    ) -> None:
        self._mode = mode
        self._table = table
        self._query = query
        self._query_parameters = dict(query_parameters)
        self._columns = columns
        self._order_by = order_by
        self._checkpoint_column = checkpoint_column
        self._checkpoint_tiebreaker_column = checkpoint_tiebreaker_column
        self._batch_size = batch_size
        self._supports_checkpoint = supports_checkpoint

    @staticmethod
    def validate_field_name(value: str) -> str:
        if not _BQ_IDENTIFIER.match(value):
            raise ValueError(
                "BigQuery field names must match ^[A-Za-z_][A-Za-z0-9_]*$ in the "
                f"official plugin v1 surface. Got {value!r}."
            )
        return value

    def build_query(self, *, resume_cursor: Any | None) -> tuple[str, dict[str, Any]]:
        if self._mode == "query":
            assert self._query is not None
            return self._query, dict(self._query_parameters)
        assert self._table is not None
        selected = ", ".join(self._columns) if self._columns else "*"
        query = f"SELECT {selected} FROM {self._quote_table_identifier(self._table)}"
        parameters = dict(self._query_parameters)
        if self._supports_checkpoint and resume_cursor is not None:
            assert self._checkpoint_column is not None
            if self._checkpoint_tiebreaker_column is None:
                query += f" WHERE {self._checkpoint_column} > @checkpoint_cursor"
                parameters["checkpoint_cursor"] = resume_cursor
            else:
                if not isinstance(resume_cursor, dict):
                    raise ValueError(
                        "BigQuery composite checkpoint resume requires a checkpoint value "
                        "containing both cursor and tiebreaker_cursor."
                    )
                try:
                    checkpoint_cursor = resume_cursor["cursor"]
                    tiebreaker_cursor = resume_cursor["tiebreaker_cursor"]
                except KeyError as exc:
                    raise ValueError(
                        "BigQuery composite checkpoint resume requires both 'cursor' and "
                        "'tiebreaker_cursor'."
                    ) from exc
                query += (
                    f" WHERE ({self._checkpoint_column} > @checkpoint_cursor "
                    f"OR ({self._checkpoint_column} = @checkpoint_cursor "
                    f"AND {self._checkpoint_tiebreaker_column} > @checkpoint_tiebreaker_cursor))"
                )
                parameters["checkpoint_cursor"] = checkpoint_cursor
                parameters["checkpoint_tiebreaker_cursor"] = tiebreaker_cursor
        if self._order_by:
            query += " ORDER BY " + ", ".join(self._order_by)
        return query, parameters

    def start_query(
        self,
        *,
        client: Any,
        query_text: str,
        parameters: dict[str, Any],
    ) -> tuple[Any, str | None]:
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError(
                "BigQuery plugins require google-cloud-bigquery. "
                "Install via: pip install 'agora-etl-plugins[bigquery]'"
            ) from None

        query_params = [
            bigquery.ScalarQueryParameter(name, self._bq_scalar_type(value), value)
            for name, value in parameters.items()
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_job = client.query(query_text, job_config=job_config)
        result = query_job.result(page_size=self._batch_size)
        return iter(result), getattr(query_job, "job_id", None)

    def pump_query_batches(
        self,
        *,
        client: Any,
        event_queue: Queue[tuple[str, Any]],
        stop_event: Event,
        query_text: str,
        parameters: dict[str, Any],
    ) -> None:
        try:
            rows_iter, job_id = self.start_query(
                client=client,
                query_text=query_text,
                parameters=parameters,
            )
            if not self.queue_put_with_stop(event_queue, ("started", job_id), stop_event):
                return
            while not stop_event.is_set():
                batch = self.read_row_batch(rows_iter)
                if not batch:
                    break
                if not self.queue_put_with_stop(event_queue, ("batch", batch), stop_event):
                    return
            self.queue_put_with_stop(event_queue, ("completed", None), stop_event)
        except Exception as exc:
            self.queue_put_with_stop(event_queue, ("error", exc), stop_event)

    def read_row_batch(self, rows_iter: Any) -> list[Any]:
        return list(islice(rows_iter, self._batch_size))

    @staticmethod
    def queue_put_with_stop(
        event_queue: Queue[tuple[str, Any]],
        item: tuple[str, Any],
        stop_event: Event,
    ) -> bool:
        while not stop_event.is_set():
            try:
                event_queue.put(item, timeout=0.1)
                return True
            except Full:
                continue
        return False

    @staticmethod
    def _quote_table_identifier(value: str) -> str:
        if "`" in value or not value.strip():
            raise ValueError(f"Invalid BigQuery table identifier: {value!r}")
        return f"`{value}`"

    @staticmethod
    def _bq_scalar_type(value: Any) -> str:
        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, int):
            return "INT64"
        if isinstance(value, float):
            return "FLOAT64"
        if isinstance(value, Decimal):
            return "NUMERIC"
        if isinstance(value, datetime):
            return "TIMESTAMP"
        if isinstance(value, date) and not isinstance(value, datetime):
            return "DATE"
        if isinstance(value, time):
            return "TIME"
        if value is None:
            return "STRING"
        return "STRING"


__all__ = ["BigQuerySourceQueryRuntime"]
