"""Surface-specific enterprise acceptance evaluators for BigQuery plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class _BaseBigQueryAcceptanceEvaluator:
    def __init__(
        self,
        *,
        component: str,
        finding_factory: Callable[..., Any],
        report_factory: Callable[..., Any],
    ) -> None:
        self._component = component
        self._finding_factory = finding_factory
        self._report_factory = report_factory

    def _finding(
        self,
        metric: str,
        message: str,
        value: Any,
        threshold: Any,
    ) -> Any:
        return self._finding_factory(
            component=self._component,
            metric=metric,
            message=message,
            value=value,
            threshold=threshold,
        )

    def _check_max(
        self,
        findings: list[Any],
        *,
        metric: str,
        value: int | float | None,
        threshold: int | float | None,
    ) -> None:
        if threshold is None or value is None:
            return
        if value > threshold:
            findings.append(
                self._finding(
                    metric,
                    f"{self._component}.{metric} exceeded enterprise threshold.",
                    value,
                    threshold,
                )
            )

    def _report(self, *, thresholds: Any, findings: list[Any]) -> Any:
        return self._report_factory(
            component=self._component,
            passed=not findings,
            thresholds=thresholds,
            findings=tuple(findings),
        )


class BigQuerySourceAcceptanceEvaluator(_BaseBigQueryAcceptanceEvaluator):
    """Evaluate BigQuery source snapshots against enterprise thresholds."""

    def evaluate(self, snapshot: Any, thresholds: Any) -> Any:
        findings: list[Any] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "connection_ready",
                    "BigQuery source connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        if thresholds.require_query_execution and snapshot.query_execution_count <= 0:
            findings.append(
                self._finding(
                    "query_execution_count",
                    "BigQuery source has not executed a query yet.",
                    snapshot.query_execution_count,
                    1,
                )
            )
        if (
            thresholds.require_last_job_id_after_query
            and snapshot.query_execution_count > 0
            and snapshot.last_job_id is None
        ):
            findings.append(
                self._finding(
                    "last_job_id",
                    "BigQuery source did not retain the last query job id.",
                    snapshot.last_job_id,
                    "non-null",
                )
            )
        if thresholds.require_last_stream_success and snapshot.last_stream_succeeded is not True:
            findings.append(
                self._finding(
                    "last_stream_succeeded",
                    "BigQuery source last stream did not complete successfully.",
                    snapshot.last_stream_succeeded,
                    True,
                )
            )
        if (
            thresholds.require_checkpoint_support
            and not snapshot.recovery_contract.supports_checkpoint
        ):
            findings.append(
                self._finding(
                    "recovery_contract.supports_checkpoint",
                    "BigQuery source does not support checkpoint-based resume.",
                    snapshot.recovery_contract.supports_checkpoint,
                    True,
                )
            )
        self._check_max(
            findings,
            metric="record_error_count",
            value=snapshot.record_error_count,
            threshold=thresholds.max_record_error_count,
        )
        self._check_max(
            findings,
            metric="record_drop_count",
            value=snapshot.record_drop_count,
            threshold=thresholds.max_record_drop_count,
        )
        self._check_max(
            findings,
            metric="active_stream_count",
            value=snapshot.active_stream_count,
            threshold=thresholds.max_active_stream_count,
        )
        return self._report(thresholds=thresholds, findings=findings)


class BigQuerySinkAcceptanceEvaluator(_BaseBigQueryAcceptanceEvaluator):
    """Evaluate BigQuery sink snapshots against enterprise thresholds."""

    def evaluate(self, snapshot: Any, thresholds: Any) -> Any:
        findings: list[Any] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "connection_ready",
                    "BigQuery sink connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        if thresholds.require_flush_activity and snapshot.flush_count <= 0:
            findings.append(
                self._finding(
                    "flush_count",
                    "BigQuery sink has not completed a flush yet.",
                    snapshot.flush_count,
                    1,
                )
            )
        if (
            thresholds.require_last_job_id_after_flush
            and snapshot.flush_count > 0
            and snapshot.last_job_id is None
        ):
            findings.append(
                self._finding(
                    "last_job_id",
                    "BigQuery sink did not retain the last load job id.",
                    snapshot.last_job_id,
                    "non-null",
                )
            )
        if thresholds.require_last_flush_success and snapshot.last_flush_succeeded is not True:
            findings.append(
                self._finding(
                    "last_flush_succeeded",
                    "BigQuery sink last flush did not complete successfully.",
                    snapshot.last_flush_succeeded,
                    True,
                )
            )
        if thresholds.require_loaded_row_count_match and (
            snapshot.loaded_row_count != snapshot.submitted_row_count
        ):
            findings.append(
                self._finding(
                    "loaded_row_count",
                    "BigQuery sink loaded row count does not match submitted row count.",
                    snapshot.loaded_row_count,
                    snapshot.submitted_row_count,
                )
            )
        self._check_max(
            findings,
            metric="buffered_row_count",
            value=snapshot.buffered_row_count,
            threshold=thresholds.max_buffered_row_count,
        )
        self._check_max(
            findings,
            metric="flush_error_count",
            value=snapshot.flush_error_count,
            threshold=thresholds.max_flush_error_count,
        )
        return self._report(thresholds=thresholds, findings=findings)


class BigQueryStorageWriteSinkAcceptanceEvaluator(_BaseBigQueryAcceptanceEvaluator):
    """Evaluate BigQuery Storage Write sink snapshots against enterprise thresholds."""

    def evaluate(self, snapshot: Any, thresholds: Any) -> Any:
        findings: list[Any] = []
        if thresholds.require_connection_ready and not snapshot.connection_ready:
            findings.append(
                self._finding(
                    "connection_ready",
                    "BigQuery Storage Write sink connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        if thresholds.require_flush_activity and snapshot.flush_count <= 0:
            findings.append(
                self._finding(
                    "flush_count",
                    "BigQuery Storage Write sink has not completed a flush yet.",
                    snapshot.flush_count,
                    1,
                )
            )
        if thresholds.require_stream_name and snapshot.stream_name is None:
            findings.append(
                self._finding(
                    "stream_name",
                    "BigQuery Storage Write sink does not have an active write stream name.",
                    snapshot.stream_name,
                    "non-null",
                )
            )
        if thresholds.require_last_append_success and snapshot.last_append_succeeded is not True:
            findings.append(
                self._finding(
                    "last_append_succeeded",
                    "BigQuery Storage Write sink last append did not complete successfully.",
                    snapshot.last_append_succeeded,
                    True,
                )
            )
        if thresholds.require_appended_row_count_match and (
            snapshot.appended_row_count != snapshot.submitted_row_count
        ):
            findings.append(
                self._finding(
                    "appended_row_count",
                    "BigQuery Storage Write sink appended row count does not match submitted row count.",
                    snapshot.appended_row_count,
                    snapshot.submitted_row_count,
                )
            )
        self._check_max(
            findings,
            metric="buffered_row_count",
            value=snapshot.buffered_row_count,
            threshold=thresholds.max_buffered_row_count,
        )
        self._check_max(
            findings,
            metric="append_error_count",
            value=snapshot.append_error_count,
            threshold=thresholds.max_append_error_count,
        )
        return self._report(thresholds=thresholds, findings=findings)


__all__ = [
    "BigQuerySinkAcceptanceEvaluator",
    "BigQuerySourceAcceptanceEvaluator",
    "BigQueryStorageWriteSinkAcceptanceEvaluator",
]
