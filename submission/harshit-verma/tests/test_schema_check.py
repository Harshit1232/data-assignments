"""Schema-change detection tests (stop-the-line)."""

import pytest

from pipeline.schema_check import SchemaChangeError, check_schema, read_columns


def test_unchanged_schema_passes(source_con):
    expected = read_columns(source_con, "wallet")
    # Re-reading the unchanged table is compatible: no error raised.
    check_schema("wallet", expected, read_columns(source_con, "wallet"))


def test_added_column_is_allowed(source_con):
    expected = read_columns(source_con, "wallet_ledger_entry")
    source_con.execute("ALTER TABLE wallet_ledger_entry ADD COLUMN note VARCHAR")
    actual = read_columns(source_con, "wallet_ledger_entry")

    check_schema("wallet_ledger_entry", expected, actual)  # no error


def test_dropped_column_stops_the_line():
    expected = {"wallet_id": "VARCHAR", "external_ref": "VARCHAR"}
    actual = {"wallet_id": "VARCHAR"}  # external_ref dropped
    with pytest.raises(SchemaChangeError, match="external_ref"):
        check_schema("wallet_ledger_entry", expected, actual)


def test_retyped_column_stops_the_line():
    expected = {"amount": "DECIMAL(18,2)"}
    actual = {"amount": "VARCHAR"}  # type changed
    with pytest.raises(SchemaChangeError, match="amount"):
        check_schema("wallet_ledger_entry", expected, actual)
