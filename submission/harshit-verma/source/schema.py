"""Source-system schema: DDL and enum domains for the transactional store.

This module is the single source of truth for the three source tables — two
strong entities (``customer``, ``wallet``) and one weak entity
(``wallet_ledger_entry``) whose identity depends on its parent wallet — plus
their constraints and indexes.

The enum domains are defined here once and reused by the DDL's CHECK
constraints, the seed data, and (later) the schema-contract check, so every
layer agrees on the allowed values. Keeping enums as CHECK lists rather than a
native ENUM type also makes a change to an allowed domain a *visible,
detectable* schema change for the stop-the-line logic.
"""

from __future__ import annotations

import duckdb

# --- Enum domains (defined once; referenced by the DDL CHECK constraints) ----
CUSTOMER_STATUSES = ("active", "suspended", "closed")
WALLET_STATUSES = ("active", "frozen", "closed")
CURRENCIES = ("USD", "EUR", "INR")
ENTRY_TYPES = ("credit", "debit", "adjustment")


def _in_list(column: str, values: tuple[str, ...]) -> str:
    """Render ``column IN ('a', 'b', ...)`` for a CHECK constraint."""
    quoted = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({quoted})"


# customer — strong entity (independent identity, own primary key).
CUSTOMER_DDL = f"""
CREATE TABLE customer (
    customer_id VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    email       VARCHAR NOT NULL,
    status      VARCHAR NOT NULL CHECK ({_in_list("status", CUSTOMER_STATUSES)}),
    country     VARCHAR,                       -- nullable optional attribute
    created_at  TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);
"""

# wallet — strong entity; belongs to a customer (1—N) but has its own identity.
WALLET_DDL = f"""
CREATE TABLE wallet (
    wallet_id   VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL REFERENCES customer(customer_id),
    currency    VARCHAR NOT NULL CHECK ({_in_list("currency", CURRENCIES)}),
    balance     DECIMAL(18, 2) NOT NULL CHECK (balance >= 0),    -- B1
    status      VARCHAR NOT NULL CHECK ({_in_list("status", WALLET_STATUSES)}),
    created_at  TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);
"""

# wallet_ledger_entry — WEAK entity; identity depends on the parent wallet, so
# the parent's foreign key is part of the composite primary key. Append-only,
# immutable balance-history facts.
LEDGER_DDL = f"""
CREATE TABLE wallet_ledger_entry (
    wallet_id     VARCHAR NOT NULL REFERENCES wallet(wallet_id),
    entry_seq     INTEGER NOT NULL,
    amount        DECIMAL(18, 2) NOT NULL,
    entry_type    VARCHAR NOT NULL CHECK ({_in_list("entry_type", ENTRY_TYPES)}),
    balance_after DECIMAL(18, 2) NOT NULL CHECK (balance_after >= 0),   -- B1
    external_ref  VARCHAR,                     -- nullable optional reference
    created_at    TIMESTAMP NOT NULL,
    PRIMARY KEY (wallet_id, entry_seq),        -- composite PK = weak entity
    CHECK (                                    -- B3: amount sign matches type
        (entry_type = 'credit' AND amount > 0)
        OR (entry_type = 'debit' AND amount < 0)
        OR (entry_type = 'adjustment')
    )
);
"""

# Indexes where lookup or change-capture performance reasonably matters.
INDEX_DDL = [
    # Foreign-key columns: speed up joins and child lookups.
    "CREATE INDEX idx_wallet_customer_id ON wallet(customer_id);",
    "CREATE INDEX idx_ledger_wallet_id ON wallet_ledger_entry(wallet_id);",
    # updated_at is the watermark a timestamp-based CDC extractor would scan.
    "CREATE INDEX idx_customer_updated_at ON customer(updated_at);",
    "CREATE INDEX idx_wallet_updated_at ON wallet(updated_at);",
    # created_at time-orders the append-only ledger facts.
    "CREATE INDEX idx_ledger_created_at ON wallet_ledger_entry(created_at);",
]

TABLE_DDL = [CUSTOMER_DDL, WALLET_DDL, LEDGER_DDL]


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create the three source tables and their indexes on ``con``.

    Tables are created parent-first (customer -> wallet -> ledger) so each
    foreign key references an already-existing table.
    """
    for ddl in TABLE_DDL:
        con.execute(ddl)
    for ddl in INDEX_DDL:
        con.execute(ddl)
