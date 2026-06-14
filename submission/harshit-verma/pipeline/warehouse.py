"""
Warehouse layer: current-state snapshot built from the change log.

Each source table has a wh_* table holding the latest row per key plus three
metadata columns: _cdc_seq (sequence of the last applied change), _deleted (true
if the row was deleted in the source), and _loaded_at.

build_warehouse folds the records with last-write-wins by sequence — so
duplicates and out-of-order arrivals converge on the highest-sequence state —
then rebuilds the tables. Because it rebuilds from a plain list of records, time
travel is just build_warehouse(con, lake.read_records(con, up_to_sequence=N)).
"""

from __future__ import annotations

import duckdb

from pipeline.cdc import CDCRecord

WAREHOUSE_TABLES = {
    "customer": "wh_customers",
    "wallet": "wh_wallets",
    "wallet_ledger_entry": "wh_wallet_ledger",
}

_METADATA = "_cdc_seq BIGINT, _deleted BOOLEAN, _loaded_at TIMESTAMP"

WAREHOUSE_DDL = [
    f"""
    CREATE OR REPLACE TABLE wh_customers (
        customer_id VARCHAR PRIMARY KEY,
        name VARCHAR, email VARCHAR, status VARCHAR, country VARCHAR,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        {_METADATA}
    );
    """,
    f"""
    CREATE OR REPLACE TABLE wh_wallets (
        wallet_id VARCHAR PRIMARY KEY,
        customer_id VARCHAR, currency VARCHAR, balance DECIMAL(18, 2),
        status VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP,
        {_METADATA}
    );
    """,
    f"""
    CREATE OR REPLACE TABLE wh_wallet_ledger (
        wallet_id VARCHAR, entry_seq INTEGER,
        amount DECIMAL(18, 2), entry_type VARCHAR, balance_after DECIMAL(18, 2),
        external_ref VARCHAR, created_at TIMESTAMP,
        {_METADATA},
        PRIMARY KEY (wallet_id, entry_seq)
    );
    """,
]


def create_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    for ddl in WAREHOUSE_DDL:
        con.execute(ddl)


def latest_by_sequence(records: list[CDCRecord]) -> dict[tuple[str, str], CDCRecord]:
    """Keep the highest-sequence record per (table, primary_key).

    This single rule is what makes the warehouse converge regardless of
    duplicate or out-of-order delivery.
    """
    winners: dict[tuple[str, str], CDCRecord] = {}
    for record in records:
        key = (record.table, record.primary_key)
        if key not in winners or record.sequence > winners[key].sequence:
            winners[key] = record
    return winners


def build_warehouse(con: duckdb.DuckDBPyConnection, records: list[CDCRecord]) -> None:
    """(Re)build the current-state tables from a list of change records."""
    create_warehouse(con)
    for record in latest_by_sequence(records).values():
        _write_row(con, record)


def _write_row(con: duckdb.DuckDBPyConnection, record: CDCRecord) -> None:
    table = WAREHOUSE_TABLES[record.table]
    columns = list(record.data)
    placeholders = ", ".join("?" for _ in columns)
    con.execute(
        f"INSERT INTO {table} ({', '.join(columns)}, _cdc_seq, _deleted, _loaded_at) "
        f"VALUES ({placeholders}, ?, ?, current_timestamp)",
        [
            *(record.data[c] for c in columns),
            record.sequence,
            record.operation == "delete",
        ],
    )
