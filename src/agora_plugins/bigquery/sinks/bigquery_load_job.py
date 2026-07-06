"""Load-job collaborator for BigQuery dataset sinks."""

from __future__ import annotations

from typing import Any, Literal


class BigQuerySinkLoadJobRuntime:
    """Public-facing load-job submission runtime for BigQuery sinks."""

    def __init__(
        self,
        *,
        table: str,
        create_disposition: Literal["create_if_needed", "create_never"],
    ) -> None:
        self._table = table
        self._create_disposition = create_disposition

    def submit_load_job(
        self,
        *,
        client: Any,
        rows: list[dict[str, Any]],
        effective_write_disposition: Literal["append", "truncate"],
    ) -> tuple[str | None, int]:
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError(
                "BigQuery plugins require google-cloud-bigquery. "
                "Install via: pip install 'agora-etl-plugins[bigquery]'"
            ) from None
        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if effective_write_disposition == "truncate"
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        create_disposition = (
            bigquery.CreateDisposition.CREATE_IF_NEEDED
            if self._create_disposition == "create_if_needed"
            else bigquery.CreateDisposition.CREATE_NEVER
        )
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            autodetect=self._create_disposition == "create_if_needed",
        )
        load_job = client.load_table_from_json(rows, self._table, job_config=job_config)
        try:
            load_job.result()
        except Exception as exc:
            from agora_plugins.bigquery.sinks.bigquery import BigQuerySinkWriteError

            errors = getattr(load_job, "errors", None)
            raise BigQuerySinkWriteError(
                str(exc),
                job_id=getattr(load_job, "job_id", None),
                errors=errors,
            ) from exc
        errors = getattr(load_job, "errors", None)
        if errors:
            from agora_plugins.bigquery.sinks.bigquery import BigQuerySinkWriteError

            raise BigQuerySinkWriteError(
                "BigQuery load job reported row load errors.",
                job_id=getattr(load_job, "job_id", None),
                errors=errors,
            )
        return getattr(load_job, "job_id", None), len(rows)


__all__ = ["BigQuerySinkLoadJobRuntime"]
