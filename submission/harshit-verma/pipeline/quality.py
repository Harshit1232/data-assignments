"""
Data-quality checks: re-assert the source's rules against the warehouse.

Source constraints do not travel through CDC — only the data does — so we check
the same rules independently on the warehouse. run_quality_checks returns a list
of failure messages; an empty list means everything passed.
"""

from __future__ import annotations

import duckdb


def run_quality_checks(con: duckdb.DuckDBPyConnection) -> list[str]:
    failures: list[str] = []

    def count(sql: str) -> int:
        return con.execute(sql).fetchone()[0]

    # B1 — non-negative wallet balance.
    if count("SELECT count(*) FROM wh_wallets WHERE NOT _deleted AND balance < 0"):
        failures.append("B1: a wallet has a negative balance")

    # B2 — wallet balance equals the sum of its ledger amounts.
    if count("""
        SELECT count(*)
        FROM wh_wallets w
        JOIN (
            SELECT wallet_id, SUM(amount) AS total
            FROM wh_wallet_ledger
            GROUP BY wallet_id
        ) l ON l.wallet_id = w.wallet_id
        WHERE w.balance <> l.total
        """):
        failures.append("B2: a wallet balance does not match its ledger sum")

    # Referential integrity — every wallet points at a customer in the warehouse.
    if count(
        "SELECT count(*) FROM wh_wallets w "
        "LEFT JOIN wh_customers c ON c.customer_id = w.customer_id "
        "WHERE c.customer_id IS NULL"
    ):
        failures.append("RI: a wallet references a missing customer")

    # Referential integrity — every ledger entry points at a wallet.
    if count(
        "SELECT count(*) FROM wh_wallet_ledger l "
        "LEFT JOIN wh_wallets w ON w.wallet_id = l.wallet_id "
        "WHERE w.wallet_id IS NULL"
    ):
        failures.append("RI: a ledger entry references a missing wallet")

    return failures
