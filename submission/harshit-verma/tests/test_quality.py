"""Validation tests: the data-quality checks pass on good data and catch bad data.

One test per validation category so each kind of parity check is exercised.
"""

from pipeline.quality import run_quality_checks
from pipeline.run import run_pipeline


def test_quality_passes_on_seed():
    con = run_pipeline()
    assert run_quality_checks(con) == []


def test_quality_catches_negative_balance():  # business rule (B1)
    con = run_pipeline()
    con.execute("UPDATE wh_wallets SET balance = -1 WHERE wallet_id = 'W1'")
    assert any("B1" in failure for failure in run_quality_checks(con))


def test_quality_catches_invalid_enum():  # domain / enum
    con = run_pipeline()
    con.execute("UPDATE wh_wallets SET status = 'banana' WHERE wallet_id = 'W1'")
    assert any("DOMAIN" in failure for failure in run_quality_checks(con))


def test_quality_catches_null_required_field():  # not-null
    con = run_pipeline()
    con.execute("UPDATE wh_customers SET email = NULL WHERE customer_id = 'C1'")
    assert any("NOT NULL" in failure for failure in run_quality_checks(con))
