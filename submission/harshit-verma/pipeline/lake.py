"""
Lake layer: durable, append-only history of every change.

Each captured CDCRecord is written to the lake_cdc_events table, keyed by its
unique sequence number. The lake is append-only and immutable — we never update
or delete rows, and re-writing a record that is already there is a no-op (safe
under at-least-once delivery). Reading it back, optionally only up to a given
sequence, is how the warehouse is rebuilt and how we travel back in time.

The change payload is stored as JSON text (the bronze/raw convention); the
warehouse later casts it into typed columns. A money value like "80.00" survives
the round-trip exactly when cast to DECIMAL, so reconciliation stays exact.
"""

from __future__ import annotations

import json

import duckdb

from pipeline.cdc import CDCRecord

CREATE_LAKE = """
CREATE TABLE IF NOT EXISTS lake_cdc_events (
    sequence    BIGINT PRIMARY KEY,
    operation   VARCHAR NOT NULL,
    table_name  VARCHAR NOT NULL,
    primary_key VARCHAR NOT NULL,
    data        VARCHAR NOT NULL,            -- change payload as JSON text
    captured_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);
"""


def create_lake(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(CREATE_LAKE)


def write(con: duckdb.DuckDBPyConnection, record: CDCRecord) -> None:
    """Append one change record. Idempotent: a repeated sequence is ignored."""
    con.execute(
        "INSERT INTO lake_cdc_events "
        "(sequence, operation, table_name, primary_key, data, captured_at) "
        "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (sequence) DO NOTHING",
        [
            record.sequence,
            record.operation,
            record.table,
            record.primary_key,
            json.dumps(record.data, default=str),
            record.captured_at,
        ],
    )


def write_all(con: duckdb.DuckDBPyConnection, records: list[CDCRecord]) -> None:
    for record in records:
        write(con, record)


def read_records(
    con: duckdb.DuckDBPyConnection, up_to_sequence: int | None = None
) -> list[CDCRecord]:
    """Read the stored changes back as CDCRecords, in sequence order.

    If up_to_sequence is given, only changes with sequence <= it are returned
    (point-in-time replay / time travel).
    """
    sql = (
        "SELECT sequence, operation, table_name, primary_key, data, captured_at "
        "FROM lake_cdc_events"
    )
    params: list[int] = []
    if up_to_sequence is not None:
        sql += " WHERE sequence <= ?"
        params.append(up_to_sequence)
    sql += " ORDER BY sequence"

    return [
        CDCRecord(
            operation=operation,
            table=table_name,
            primary_key=primary_key,
            data=json.loads(data),
            sequence=sequence,
            captured_at=captured_at,
        )
        for sequence, operation, table_name, primary_key, data, captured_at in con.execute(
            sql, params
        ).fetchall()
    ]
