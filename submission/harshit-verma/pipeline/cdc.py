"""
CDC capture layer.

Simulates WAL-based change capture: every insert/update/delete produces a
CDCRecord with a monotonically increasing sequence number (the offset / LSN
analogue). After a restart, a consumer replays only unprocessed changes by
calling records_since(last_processed_sequence) — so a restart loses nothing and
reprocesses nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

VALID_OPERATIONS = frozenset({"insert", "update", "delete"})


@dataclass
class CDCRecord:
    """One captured source change.

    `data` is the full row image after the change (for a delete, the last-known
    image), so applying it downstream is a simple overwrite rather than a merge.
    """

    operation: str
    table: str
    primary_key: str
    data: dict[str, Any]
    sequence: int = 0
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if self.operation not in VALID_OPERATIONS:
            raise ValueError(f"invalid CDC operation: {self.operation!r}")


class CDCCapture:
    """In-process CDC log.

    Production analogue: a Debezium/Kafka connector reading the Postgres WAL.
    `sequence` is the equivalent of a Kafka offset / Postgres LSN, used for
    checkpoint-based replay after a restart.
    """

    def __init__(self) -> None:
        self._log: list[CDCRecord] = []
        self._seq = 0

    def insert(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        return self._record("insert", table, pk, data)

    def update(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        return self._record("update", table, pk, data)

    def delete(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        return self._record("delete", table, pk, data)

    def records_since(self, offset: int = 0) -> list[CDCRecord]:
        """Every record with sequence > offset (used to replay after a restart)."""
        return [record for record in self._log if record.sequence > offset]

    @property
    def latest_sequence(self) -> int:
        return self._seq

    @property
    def log(self) -> list[CDCRecord]:
        return list(self._log)

    def _record(
        self, operation: str, table: str, pk: str, data: dict[str, Any]
    ) -> CDCRecord:
        self._seq += 1
        record = CDCRecord(operation, table, pk, dict(data), self._seq)
        self._log.append(record)
        return record
