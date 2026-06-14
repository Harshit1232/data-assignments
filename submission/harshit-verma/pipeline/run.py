"""
End-to-end pipeline runner (the driver).

Seeds the source, captures every existing row as an insert change, stores those
changes in the lake, and builds the warehouse from them. Returns one DuckDB
connection holding all three layers (source tables, lake_cdc_events, wh_*), which
the data-quality script and the tests run against.

Capturing the seed as inserts is the CDC "initial snapshot load": the first time
a connector attaches to a database it emits every current row as an insert.
"""

from __future__ import annotations

import duckdb

from pipeline.cdc import CDCCapture, CDCRecord
from pipeline.lake import create_lake, write_all
from pipeline.warehouse import build_warehouse
from source.seed import build_source_db

PRIMARY_KEYS = {
    "customer": ["customer_id"],
    "wallet": ["wallet_id"],
    "wallet_ledger_entry": ["wallet_id", "entry_seq"],
}


def capture_source(con: duckdb.DuckDBPyConnection) -> list[CDCRecord]:
    """Capture every row in the source as an insert (initial snapshot load)."""
    cap = CDCCapture()
    for table, key_columns in PRIMARY_KEYS.items():
        rows = con.execute(f"SELECT * FROM {table}").fetchall()
        columns = [desc[0] for desc in con.description]
        for row in rows:
            data = dict(zip(columns, row))
            pk = "|".join(str(data[col]) for col in key_columns)
            cap.insert(table, pk, data)
    return cap.log


def run_pipeline() -> duckdb.DuckDBPyConnection:
    """Seed the source, capture it, store it in the lake, build the warehouse."""
    con = build_source_db()
    records = capture_source(con)
    create_lake(con)
    write_all(con, records)
    build_warehouse(con, records)
    return con
