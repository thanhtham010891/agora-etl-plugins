"""Stream orchestration runtime for BigQuery sources."""

from __future__ import annotations

import asyncio
from queue import Queue
from threading import Event
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora.core.source import SourceRecordError
from agora.core.types import SourceRecordFailurePolicy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from agora_plugins.bigquery.sources.query_runtime import BigQuerySourceQueryRuntime

T = TypeVar("T")


class BigQuerySourceStreamRuntime(Generic[T]):
    """Public-facing read-loop runtime for BigQuery sources."""

    def __init__(
        self,
        *,
        query_runtime: BigQuerySourceQueryRuntime,
        ensure_open: Callable[[], Awaitable[None]],
        client_provider: Callable[[], Any | None],
        row_to_dict: Callable[[Any], dict[str, Any]],
        apply_row_mapper: Callable[[dict[str, Any]], Awaitable[T | None]],
        current_checkpoint: Callable[[], dict[str, Any] | None],
        on_stream_started: Callable[[], None],
        on_query_started: Callable[[str | None], None],
        on_row_seen: Callable[[], None],
        on_record_error: Callable[[Exception], None],
        on_record_dropped: Callable[[], None],
        on_checkpoint_cursor: Callable[[Any], None],
        on_record_emitted: Callable[[], None],
        on_stream_failed: Callable[[Exception], None],
        on_stream_completed: Callable[[bool], None],
        logger: Any,
        query_mode: str,
        supports_checkpoint: bool,
        checkpoint_column: str | None,
        checkpoint_tiebreaker_column: str | None,
        on_record_error_policy: SourceRecordFailurePolicy,
        source_name: str,
        metrics_provider: Callable[[], dict[str, Any]],
    ) -> None:
        self._query_runtime = query_runtime
        self._ensure_open = ensure_open
        self._client_provider = client_provider
        self._row_to_dict = row_to_dict
        self._apply_row_mapper = apply_row_mapper
        self._current_checkpoint = current_checkpoint
        self._on_stream_started = on_stream_started
        self._on_query_started = on_query_started
        self._on_row_seen = on_row_seen
        self._on_record_error = on_record_error
        self._on_record_dropped = on_record_dropped
        self._on_checkpoint_cursor = on_checkpoint_cursor
        self._on_record_emitted = on_record_emitted
        self._on_stream_failed = on_stream_failed
        self._on_stream_completed = on_stream_completed
        self._logger = logger
        self._query_mode = query_mode
        self._supports_checkpoint = supports_checkpoint
        self._checkpoint_column = checkpoint_column
        self._checkpoint_tiebreaker_column = checkpoint_tiebreaker_column
        self._on_record_error_policy = on_record_error_policy
        self._source_name = source_name
        self._metrics_provider = metrics_provider

    async def stream(self, *, resume_cursor: Any | None) -> AsyncGenerator[T, None]:
        if self._client_provider() is None:
            await self._ensure_open()
        client = self._client_provider()
        assert client is not None

        self._on_stream_started()
        query_text, parameters = self._query_runtime.build_query(resume_cursor=resume_cursor)
        success = False
        event_queue: Queue[tuple[str, Any]] = Queue(maxsize=1)
        stop_event = Event()
        pump_task = asyncio.create_task(
            asyncio.to_thread(
                self._query_runtime.pump_query_batches,
                client=client,
                event_queue=event_queue,
                stop_event=stop_event,
                query_text=query_text,
                parameters=parameters,
            )
        )
        try:
            while True:
                event, payload = await asyncio.to_thread(event_queue.get)
                if event == "started":
                    job_id = cast("str | None", payload)
                    self._on_query_started(job_id)
                    self._logger.info(
                        "bigquery_source_query_started",
                        mode=self._query_mode,
                        job_id=job_id,
                        supports_checkpoint=self._supports_checkpoint,
                    )
                    continue
                if event == "completed":
                    break
                if event == "error":
                    raise cast("Exception", payload)
                for row in cast("list[Any]", payload):
                    self._on_row_seen()
                    row_dict = self._row_to_dict(row)
                    try:
                        mapped = await self._apply_row_mapper(row_dict)
                    except Exception as exc:
                        self._on_record_error(exc)
                        if (
                            self._on_record_error_policy
                            == SourceRecordFailurePolicy.LOG_AND_CONTINUE
                        ):
                            self._on_record_dropped()
                            continue
                        raise SourceRecordError(
                            exc,
                            record=row_dict,
                            checkpoint=self._current_checkpoint(),
                            source=self._source_name,
                        ) from exc
                    if mapped is None:
                        self._on_record_dropped()
                        continue
                    if self._supports_checkpoint:
                        assert self._checkpoint_column is not None
                        cursor: Any = row_dict.get(self._checkpoint_column)
                        if self._checkpoint_tiebreaker_column is not None:
                            cursor = {
                                "cursor": cursor,
                                "tiebreaker_cursor": row_dict.get(
                                    self._checkpoint_tiebreaker_column
                                ),
                            }
                        self._on_checkpoint_cursor(cursor)
                    self._on_record_emitted()
                    yield mapped
            await pump_task
            success = True
        except Exception as exc:
            self._on_stream_failed(exc)
            raise
        finally:
            stop_event.set()
            if not pump_task.done():
                await pump_task
            self._on_stream_completed(success)
            self._logger.info(
                "bigquery_source_query_completed",
                mode=self._query_mode,
                **self._metrics_provider(),
            )


__all__ = ["BigQuerySourceStreamRuntime"]
