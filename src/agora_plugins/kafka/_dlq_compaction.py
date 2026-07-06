"""Compaction-state helpers for Kafka DLQ source scans."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from agora.core.dlq import DLQRecord

    from agora_plugins.dlq_policy import DLQPayloadPolicy


class KafkaDLQCompactionState:
    """Tracks latest DLQ state in memory, spilling to SQLite when needed."""

    def __init__(
        self,
        *,
        spill_threshold: int | None,
        payload_policy: DLQPayloadPolicy | None = None,
        sqlite_module: Any,
        json_module: Any,
        record_to_payload: Any,
        payload_to_record: Any,
        decode_stored_payload: Any,
    ) -> None:
        self._spill_threshold = spill_threshold
        self._payload_policy = payload_policy
        self._sqlite3 = sqlite_module
        self._json = json_module
        self._record_to_payload = record_to_payload
        self._payload_to_record = payload_to_record
        self._decode_stored_payload = decode_stored_payload
        self._memory: dict[str, tuple[int, DLQRecord | None]] = {}
        self._conn: Any | None = None

    def update(self, *, sequence: int, storage_key: str, record: DLQRecord | None) -> None:
        stored_record = self._apply_payload_policy(record)
        if self._conn is None:
            should_spill = (
                self._spill_threshold is not None
                and len(self._memory) >= self._spill_threshold
                and storage_key not in self._memory
            )
            if should_spill:
                self._spill_to_sqlite()
        if self._conn is None:
            self._memory[storage_key] = (sequence, stored_record)
            return

        payload = None if stored_record is None else self._record_payload_json(stored_record)
        self._conn.execute(
            """
            INSERT INTO dlq_compaction(storage_key, sequence, payload, deleted)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(storage_key) DO UPDATE SET
                sequence=excluded.sequence,
                payload=excluded.payload,
                deleted=excluded.deleted
            """,
            (storage_key, sequence, payload, int(stored_record is None)),
        )

    def live_records(self) -> list[tuple[int, DLQRecord]]:
        if self._conn is None:
            return sorted(
                ((order, record) for order, record in self._memory.values() if record is not None),
                key=lambda item: item[0],
            )

        rows = self._conn.execute(
            """
            SELECT storage_key, sequence, payload
            FROM dlq_compaction
            WHERE deleted = 0 AND payload IS NOT NULL
            ORDER BY sequence ASC
            """
        ).fetchall()
        records: list[tuple[int, DLQRecord]] = []
        for storage_key, sequence, payload in rows:
            record_payload = self._decode_record_payload_json(cast("str", payload))
            record = self._payload_to_record(record_payload)
            object.__setattr__(record, "_storage_id", storage_key)
            records.append((int(sequence), record))
        return records

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _spill_to_sqlite(self) -> None:
        # Keep compaction state off the filesystem so raw payload mode never lands
        # cleartext DLQ content in a temporary on-disk SQLite database.
        self._conn = self._sqlite3.connect(":memory:")
        self._conn.execute(
            """
            CREATE TABLE dlq_compaction (
                storage_key TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL,
                payload TEXT,
                deleted INTEGER NOT NULL
            )
            """
        )
        for storage_key, (sequence, record) in self._memory.items():
            payload = None if record is None else self._record_payload_json(record)
            self._conn.execute(
                """
                INSERT INTO dlq_compaction(storage_key, sequence, payload, deleted)
                VALUES (?, ?, ?, ?)
                """,
                (storage_key, sequence, payload, int(record is None)),
            )
        self._memory.clear()

    def _apply_payload_policy(self, record: DLQRecord | None) -> DLQRecord | None:
        if (
            record is None
            or self._payload_policy is None
            or self._payload_policy.mode in {"raw", "encrypted"}
        ):
            return record
        payload = self._record_to_payload(record, payload_policy=self._payload_policy)
        redacted = self._payload_to_record(payload)
        object.__setattr__(redacted, "_storage_id", record._storage_id)
        return redacted

    def _record_payload_json(self, record: DLQRecord) -> str:
        payload = self._record_to_payload(record, payload_policy=self._payload_policy)
        if self._payload_policy is not None and self._payload_policy.mode == "encrypted":
            return self._json.dumps(
                self._payload_policy.encrypt_payload(payload),
                ensure_ascii=False,
                sort_keys=True,
            )
        return self._json.dumps(payload, ensure_ascii=False)

    def _decode_record_payload_json(self, payload_json: str) -> dict[str, Any]:
        payload = cast("dict[str, Any]", self._json.loads(payload_json))
        if "pipeline_id" in payload:
            return payload
        decoded = self._decode_stored_payload(payload, payload_policy=self._payload_policy)
        if decoded is None:
            raise ValueError("Kafka DLQ compaction payload is missing.")
        return decoded
