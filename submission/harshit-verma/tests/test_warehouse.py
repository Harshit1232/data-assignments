"""Warehouse tests: latest snapshot, convergence, soft delete, and time travel."""

from decimal import Decimal

import duckdb
import pytest

from pipeline.cdc import CDCCapture, CDCRecord
from pipeline.lake import create_lake, read_records, write_all
from pipeline.warehouse import build_warehouse


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


def _wallet(balance, status="active"):
    return {
        "wallet_id": "W1",
        "customer_id": "C1",
        "currency": "USD",
        "balance": balance,
        "status": status,
        "created_at": "2024-01-01 00:00:00",
        "updated_at": "2024-01-01 00:00:00",
    }


def test_warehouse_holds_latest_snapshot(con):
    cap = CDCCapture()
    cap.insert("wallet", "W1", _wallet("100.00"))
    cap.update("wallet", "W1", _wallet("120.00"))
    build_warehouse(con, cap.log)

    row = con.execute(
        "SELECT balance, _cdc_seq, _deleted FROM wh_wallets WHERE wallet_id = 'W1'"
    ).fetchone()
    assert row == (Decimal("120.00"), 2, False)


def test_out_of_order_converges(con):
    # The higher sequence (3) must win even though it is applied first.
    records = [
        CDCRecord("update", "wallet", "W1", _wallet("250.00"), sequence=3),
        CDCRecord("update", "wallet", "W1", _wallet("100.00"), sequence=2),
    ]
    build_warehouse(con, records)

    balance = con.execute(
        "SELECT balance FROM wh_wallets WHERE wallet_id = 'W1'"
    ).fetchone()[0]
    assert balance == Decimal("250.00")


def test_duplicate_record_is_idempotent(con):
    record = CDCRecord("insert", "wallet", "W1", _wallet("100.00"), sequence=1)
    build_warehouse(con, [record, record, record])  # redelivered

    count = con.execute("SELECT count(*) FROM wh_wallets").fetchone()[0]
    assert count == 1


def test_delete_soft_deletes(con):
    cap = CDCCapture()
    cap.insert("wallet", "W1", _wallet("100.00"))
    cap.delete("wallet", "W1", _wallet("100.00"))
    build_warehouse(con, cap.log)

    deleted = con.execute(
        "SELECT _deleted FROM wh_wallets WHERE wallet_id = 'W1'"
    ).fetchone()[0]
    active = con.execute(
        "SELECT count(*) FROM wh_wallets WHERE NOT _deleted"
    ).fetchone()[0]
    assert deleted is True
    assert active == 0


def test_each_source_table_routed_to_its_wh_table(con):
    cap = CDCCapture()
    cap.insert(
        "customer",
        "C1",
        {
            "customer_id": "C1",
            "name": "Ana",
            "email": "a@example.com",
            "status": "active",
            "country": None,
            "created_at": "2024-01-01 00:00:00",
            "updated_at": "2024-01-01 00:00:00",
        },
    )
    cap.insert("wallet", "W1", _wallet("100.00"))
    cap.insert(
        "wallet_ledger_entry",
        "W1|1",
        {
            "wallet_id": "W1",
            "entry_seq": 1,
            "amount": "100.00",
            "entry_type": "credit",
            "balance_after": "100.00",
            "external_ref": None,
            "created_at": "2024-01-01 00:00:00",
        },
    )
    build_warehouse(con, cap.log)

    assert con.execute("SELECT count(*) FROM wh_customers").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM wh_wallets").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM wh_wallet_ledger").fetchone()[0] == 1


def test_rebuild_from_lake_enables_time_travel(con):
    create_lake(con)
    cap = CDCCapture()
    cap.insert("wallet", "W1", _wallet("100.00"))  # sequence 1
    cap.update("wallet", "W1", _wallet("120.00"))  # sequence 2
    cap.delete("wallet", "W1", _wallet("120.00"))  # sequence 3
    write_all(con, cap.log)

    # Rebuild as of sequence 2: the wallet exists at balance 120, not deleted.
    build_warehouse(con, read_records(con, up_to_sequence=2))
    balance, deleted = con.execute(
        "SELECT balance, _deleted FROM wh_wallets WHERE wallet_id = 'W1'"
    ).fetchone()
    assert balance == Decimal("120.00")
    assert deleted is False
