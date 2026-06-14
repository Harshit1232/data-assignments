"""Validation tests: the data-quality checks pass on good data and catch bad data."""

from pipeline.quality import run_quality_checks
from pipeline.run import run_pipeline


def test_quality_passes_on_seed():
    con = run_pipeline()
    assert run_quality_checks(con) == []


def test_quality_catches_negative_balance():
    con = run_pipeline()
    # Inject a bad value into the warehouse; the B1 check must catch it.
    con.execute("UPDATE wh_wallets SET balance = -1 WHERE wallet_id = 'W1'")

    failures = run_quality_checks(con)
    assert any("B1" in failure for failure in failures)
