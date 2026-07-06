"""Resume and checkpoint state helpers for PostgreSQL sources."""

from __future__ import annotations

from typing import Any


class PostgresSourceResumeRuntime:
    """Owns checkpoint param application and row-progress reset logic."""

    def __init__(self, source: Any) -> None:
        self._source = source

    async def prepare_resume(self, checkpoint: Any) -> None:
        source = self._source
        source._resume_prepare_count += 1
        self.reset_progress()
        source._params = dict(source._base_params)
        if checkpoint is None or not source.supports_checkpoint:
            return

        value = checkpoint.value if isinstance(checkpoint.value, dict) else {}
        if "cursor" not in value:
            return
        cursor = value["cursor"]

        if source._checkpoint_param is not None:
            source._params[source._checkpoint_param] = cursor
            source._resume_checkpoint_apply_count += 1
            return

        if not isinstance(cursor, dict):
            raise TypeError(
                "Composite PostgresSource checkpoints require cursor values to be dicts."
            )
        for field in source._checkpoint_fields:
            param_name = source._checkpoint_params[field]
            if field not in cursor:
                raise ValueError(
                    f"Checkpoint cursor is missing composite field {field!r}: {cursor!r}"
                )
            source._params[param_name] = cursor[field]
        source._resume_checkpoint_apply_count += 1

    def current_checkpoint(self) -> dict[str, Any] | None:
        source = self._source
        if source._rows_seen <= 0 and source._last_checkpoint_cursor is None:
            return None
        checkpoint: dict[str, Any] = {"row_number": source._rows_seen}
        if source._last_checkpoint_cursor is not None:
            checkpoint["cursor"] = source._last_checkpoint_cursor
        return checkpoint

    def extract_checkpoint_cursor(self, row_dict: dict[str, Any]) -> Any | None:
        source = self._source
        if source._checkpoint_field is not None:
            if source._checkpoint_field not in row_dict:
                raise KeyError(f"Checkpoint field {source._checkpoint_field!r} missing from row")
            return row_dict[source._checkpoint_field]

        if source._checkpoint_fields:
            cursor: dict[str, Any] = {}
            for field in source._checkpoint_fields:
                if field not in row_dict:
                    raise KeyError(f"Checkpoint field {field!r} missing from row")
                cursor[field] = row_dict[field]
            return cursor

        return None

    def reset_progress(self) -> None:
        source = self._source
        source._rows_seen = 0
        source._last_checkpoint_cursor = None
        source._current_row_checkpoint_cursor = None
        source._record_error_count = 0
        source._record_drop_count = 0
        source._retry_count = 0
        source._staleness_guard_block_count = 0
        source._staleness_guard_primary_fallback_count = 0
        source._connected_server_role = None
        source._last_replica_replay_lag_s = None
        source._last_health_error = None
        source._last_health_checked_at = None
