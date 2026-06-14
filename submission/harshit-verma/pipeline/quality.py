"""
Data-quality checks: re-assert the source's rules against the warehouse.

Source constraints (PK, FK, NOT NULL, CHECK) do not travel through CDC — only the
data does — so we re-check the same rules independently on the warehouse. They
fall into two groups, exactly as the assignment asks:

  * system validations   — primary-key uniqueness, not-null, referential
                           integrity, and domain/enum validity;
  * business validations — the wallet/ledger invariants (B1, B2).

run_quality_checks returns a list of failure messages; an empty list means
everything passed.
"""

from __future__ import annotations

import duckdb


def run_quality_checks(con: duckdb.DuckDBPyConnection) -> list[str]:
    failures: list[str] = []

    def count(sql: str) -> int:
        return con.execute(sql).fetchone()[0]

    # ── System validations ───────────────────────────────────────────────

    # Primary-key uniqueness — no duplicate keys in any warehouse table.
    if count("SELECT count(*) - count(DISTINCT customer_id) FROM wh_customers"):
        failures.append("PK: duplicate customer_id in wh_customers")
    if count("SELECT count(*) - count(DISTINCT wallet_id) FROM wh_wallets"):
        failures.append("PK: duplicate wallet_id in wh_wallets")
    if count(
        "SELECT count(*) FROM ("
        "SELECT 1 FROM wh_wallet_ledger GROUP BY wallet_id, entry_seq "
        "HAVING count(*) > 1)"
    ):
        failures.append("PK: duplicate (wallet_id, entry_seq) in wh_wallet_ledger")

    # Not-null — columns that are required in the source must be populated.
    if count(
        "SELECT count(*) FROM wh_customers "
        "WHERE customer_id IS NULL OR name IS NULL OR email IS NULL OR status IS NULL"
    ):
        failures.append("NOT NULL: a required customer column is null")
    if count(
        "SELECT count(*) FROM wh_wallets "
        "WHERE wallet_id IS NULL OR customer_id IS NULL "
        "OR balance IS NULL OR status IS NULL"
    ):
        failures.append("NOT NULL: a required wallet column is null")

    # Referential integrity — every child points at a parent that exists.
    if count(
        "SELECT count(*) FROM wh_wallets w "
        "LEFT JOIN wh_customers c ON c.customer_id = w.customer_id "
        "WHERE c.customer_id IS NULL"
    ):
        failures.append("RI: a wallet references a missing customer")
    if count(
        "SELECT count(*) FROM wh_wallet_ledger l "
        "LEFT JOIN wh_wallets w ON w.wallet_id = l.wallet_id "
        "WHERE w.wallet_id IS NULL"
    ):
        failures.append("RI: a ledger entry references a missing wallet")

    # Domain / enum — values stay within the allowed sets (mirrors the source).
    if count(
        "SELECT count(*) FROM wh_customers "
        "WHERE status NOT IN ('active', 'suspended', 'closed')"
    ):
        failures.append("DOMAIN: invalid customer status")
    if count(
        "SELECT count(*) FROM wh_wallets "
        "WHERE currency NOT IN ('USD', 'EUR', 'INR') "
        "OR status NOT IN ('active', 'frozen', 'closed')"
    ):
        failures.append("DOMAIN: invalid wallet currency or status")
    if count(
        "SELECT count(*) FROM wh_wallet_ledger "
        "WHERE entry_type NOT IN ('credit', 'debit', 'adjustment')"
    ):
        failures.append("DOMAIN: invalid ledger entry_type")

    # ── Business validations ─────────────────────────────────────────────

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

    return failures
