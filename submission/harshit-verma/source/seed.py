"""Build a fully-seeded in-memory source database for tests and the pipeline.

``build_source_db()`` returns an open DuckDB connection with the schema created
and a small, deliberately-shaped seed loaded:

* three customers — one with a NULL ``country`` (nullable attribute exercised),
  spanning ``active`` and ``suspended`` statuses;
* three wallets — one per currency (USD / EUR / INR), spanning ``active`` and
  ``frozen`` statuses;
* six ledger entries demonstrating credits and debits, a multi-entry running
  balance (W1), a single-entry wallet (W2), and a wallet driven down to exactly
  ``0.00`` (W3 — the B1 non-negative boundary), with one NULL ``external_ref``.

Every business invariant the source enforces (B1, B2, B3, B5) holds on this seed
by construction; ``test_source.py`` asserts each one independently so the
downstream warehouse parity checks have a known-correct baseline.

Money is loaded as :class:`decimal.Decimal` and timestamps as
:class:`datetime.datetime` so values land in DECIMAL / TIMESTAMP columns exactly,
with no float rounding or string-cast ambiguity.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import duckdb

from source.schema import create_schema


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


# (customer_id, name, email, status, country, created_at, updated_at)
_CUSTOMERS = [
    (
        "C1",
        "Ana Iyer",
        "ana@example.com",
        "active",
        "US",
        _dt("2024-01-01 09:00:00"),
        _dt("2024-01-01 09:00:00"),
    ),
    (
        "C2",
        "Ben Cole",
        "ben@example.com",
        "suspended",
        "DE",
        _dt("2024-01-01 10:00:00"),
        _dt("2024-01-05 11:00:00"),
    ),
    (
        "C3",
        "Chen Li",
        "chen@example.com",
        "active",
        None,
        _dt("2024-01-02 08:00:00"),
        _dt("2024-01-02 08:00:00"),
    ),
]

# (wallet_id, customer_id, currency, balance, status, created_at, updated_at)
_WALLETS = [
    (
        "W1",
        "C1",
        "USD",
        Decimal("80.00"),
        "active",
        _dt("2024-01-02 09:00:00"),
        _dt("2024-01-04 12:00:00"),
    ),
    (
        "W2",
        "C2",
        "EUR",
        Decimal("200.00"),
        "frozen",
        _dt("2024-01-02 10:00:00"),
        _dt("2024-01-03 09:00:00"),
    ),
    (
        "W3",
        "C3",
        "INR",
        Decimal("0.00"),
        "active",
        _dt("2024-01-03 08:00:00"),
        _dt("2024-01-03 16:00:00"),
    ),
]

# (wallet_id, entry_seq, amount, entry_type, balance_after, external_ref, created_at)
# W1: +100 -70 +50 = 80 (multi-entry running balance, one NULL external_ref).
# W2: +200 = 200 (single entry).
# W3: +500 -500 = 0 (driven to the non-negative boundary).
_LEDGER = [
    (
        "W1",
        1,
        Decimal("100.00"),
        "credit",
        Decimal("100.00"),
        "ext-w1-001",
        _dt("2024-01-02 09:30:00"),
    ),
    (
        "W1",
        2,
        Decimal("-70.00"),
        "debit",
        Decimal("30.00"),
        None,
        _dt("2024-01-03 14:00:00"),
    ),
    (
        "W1",
        3,
        Decimal("50.00"),
        "credit",
        Decimal("80.00"),
        "ext-w1-003",
        _dt("2024-01-04 12:00:00"),
    ),
    (
        "W2",
        1,
        Decimal("200.00"),
        "credit",
        Decimal("200.00"),
        "ext-w2-001",
        _dt("2024-01-02 10:30:00"),
    ),
    (
        "W3",
        1,
        Decimal("500.00"),
        "credit",
        Decimal("500.00"),
        "ext-w3-001",
        _dt("2024-01-03 09:00:00"),
    ),
    (
        "W3",
        2,
        Decimal("-500.00"),
        "debit",
        Decimal("0.00"),
        "ext-w3-002",
        _dt("2024-01-03 16:00:00"),
    ),
]


def build_source_db(path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Create the schema and load the seed; return the open connection.

    In-memory by default so each test gets a fully isolated database.
    """
    con = duckdb.connect(path)
    create_schema(con)
    con.executemany("INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?)", _CUSTOMERS)
    con.executemany("INSERT INTO wallet VALUES (?, ?, ?, ?, ?, ?, ?)", _WALLETS)
    con.executemany(
        "INSERT INTO wallet_ledger_entry VALUES (?, ?, ?, ?, ?, ?, ?)", _LEDGER
    )
    return con
