"""Compatibility checks for the decomposed PostgreSQL sink modules."""

from agora_plugins.postgres import PostgresSink, PostgresSinkMetricsSnapshot, QuotedIdentifier
from agora_plugins.postgres.sinks._identifiers import (
    QuotedIdentifier as InternalQuotedIdentifier,
)
from agora_plugins.postgres.sinks._metrics import (
    PostgresSinkMetricsSnapshot as InternalPostgresSinkMetricsSnapshot,
)


def test_postgres_sink_types_keep_their_existing_public_import_identity() -> None:
    assert QuotedIdentifier is InternalQuotedIdentifier
    assert PostgresSinkMetricsSnapshot is InternalPostgresSinkMetricsSnapshot


def test_postgres_sink_sql_wrappers_delegate_without_changing_output() -> None:
    sink = PostgresSink(
        dsn="postgresql://localhost/app",
        table="events",
        row_mapper=lambda record: record,
        conflict_key="id",
    )

    columns = ["id", "payload"]
    assert sink._build_batch_upsert_sql(columns, row_count=2) == (
        sink._write_planner.build_batch_upsert_sql(columns, row_count=2)  # type: ignore[attr-defined]
    )
    assert sink._build_copy_sql(columns) == sink._write_planner.build_copy_sql(  # type: ignore[attr-defined]
        columns
    )
