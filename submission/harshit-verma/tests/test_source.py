"""Source-layer tests (Modeling & Constraint category).

Two classes, 13 tests:

* ``TestSourceConstraints`` (8) — each test proves the source *rejects* one
  class of invalid data, so we know the system and business constraints are
  actually enforced (not merely declared): primary-key uniqueness (simple and
  composite), foreign-key integrity, NOT NULL, enum domains, the non-negative
  balance rule (B1), and the amount-sign-matches-type rule (B3).

* ``TestInvariantsHoldOnSeed`` (5) — each test proves a business invariant
  holds on the seed (B1, B2 two ways, B3, B5), establishing the known-correct
  baseline the warehouse parity checks are measured against.
"""

import duckdb
import pytest


class TestSourceConstraints:
    """Every test asserts an invalid insert is rejected by the source."""

    def test_customer_pk_uniqueness(self, source_con):
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO customer VALUES "
                "('C1', 'Dup', 'dup@example.com', 'active', NULL, "
                "'2024-02-01 00:00:00', '2024-02-01 00:00:00')"
            )

    def test_customer_status_enum_rejected(self, source_con):
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO customer VALUES "
                "('C9', 'Bad', 'bad@example.com', 'vip', NULL, "
                "'2024-02-01 00:00:00', '2024-02-01 00:00:00')"
            )

    def test_customer_name_not_null(self, source_con):
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO customer VALUES "
                "('C9', NULL, 'x@example.com', 'active', NULL, "
                "'2024-02-01 00:00:00', '2024-02-01 00:00:00')"
            )

    def test_wallet_fk_requires_existing_customer(self, source_con):
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO wallet VALUES "
                "('W9', 'NOPE', 'USD', 10.00, 'active', "
                "'2024-02-01 00:00:00', '2024-02-01 00:00:00')"
            )

    def test_wallet_currency_enum_rejected(self, source_con):
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO wallet VALUES "
                "('W9', 'C1', 'GBP', 10.00, 'active', "
                "'2024-02-01 00:00:00', '2024-02-01 00:00:00')"
            )

    def test_wallet_balance_non_negative(self, source_con):  # B1
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO wallet VALUES "
                "('W9', 'C1', 'USD', -0.01, 'active', "
                "'2024-02-01 00:00:00', '2024-02-01 00:00:00')"
            )

    def test_ledger_composite_pk_uniqueness(self, source_con):
        # (W1, 1) already exists in the seed; the row is otherwise valid, so
        # the only violation is the duplicate composite primary key.
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO wallet_ledger_entry VALUES "
                "('W1', 1, 5.00, 'credit', 5.00, NULL, '2024-02-01 00:00:00')"
            )

    def test_ledger_amount_sign_matches_type(self, source_con):  # B3
        # entry_seq 9 is new and the wallet exists, so the only violation is a
        # credit with a non-positive amount.
        with pytest.raises(duckdb.ConstraintException):
            source_con.execute(
                "INSERT INTO wallet_ledger_entry VALUES "
                "('W1', 9, -5.00, 'credit', 5.00, NULL, '2024-02-01 00:00:00')"
            )


class TestInvariantsHoldOnSeed:
    """Every test asserts a business invariant holds on the seed data."""

    def test_b1_non_negative_balances(self, source_con):
        bad_wallets = source_con.execute(
            "SELECT count(*) FROM wallet WHERE balance < 0"
        ).fetchone()[0]
        bad_entries = source_con.execute(
            "SELECT count(*) FROM wallet_ledger_entry WHERE balance_after < 0"
        ).fetchone()[0]
        assert bad_wallets == 0
        assert bad_entries == 0

    def test_b2_balance_equals_sum_of_amounts(self, source_con):
        mismatches = source_con.execute("""
            SELECT w.wallet_id
            FROM wallet w
            JOIN (
                SELECT wallet_id, SUM(amount) AS total
                FROM wallet_ledger_entry
                GROUP BY wallet_id
            ) l ON l.wallet_id = w.wallet_id
            WHERE w.balance <> l.total
            """).fetchall()
        assert mismatches == []

    def test_b2_balance_equals_latest_balance_after(self, source_con):
        mismatches = source_con.execute("""
            SELECT w.wallet_id
            FROM wallet w
            JOIN (
                SELECT wallet_id, balance_after,
                       ROW_NUMBER() OVER (
                           PARTITION BY wallet_id ORDER BY entry_seq DESC
                       ) AS rn
                FROM wallet_ledger_entry
            ) latest ON latest.wallet_id = w.wallet_id AND latest.rn = 1
            WHERE w.balance <> latest.balance_after
            """).fetchall()
        assert mismatches == []

    def test_b3_amount_sign_matches_entry_type(self, source_con):
        violations = source_con.execute("""
            SELECT count(*) FROM wallet_ledger_entry
            WHERE (entry_type = 'credit' AND amount <= 0)
               OR (entry_type = 'debit' AND amount >= 0)
            """).fetchone()[0]
        assert violations == 0

    def test_b5_ledger_not_predated(self, source_con):
        violations = source_con.execute("""
            SELECT count(*)
            FROM wallet_ledger_entry l
            JOIN wallet w ON w.wallet_id = l.wallet_id
            WHERE l.created_at < w.created_at
            """).fetchone()[0]
        assert violations == 0
