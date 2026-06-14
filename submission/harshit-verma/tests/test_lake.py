"""Lake layer tests: completeness, append-only idempotency, and replay."""

import duckdb
import pytest

from pipeline.cdc import CDCCapture
from pipeline.lake import create_lake, read_records, write, write_all


@pytest.fixture
def lake_con():
    con = duckdb.connect(":memory:")
    create_lake(con)
    try:
        yield con
    finally:
        con.close()


def _sample_records():
    cap = CDCCapture()
    cap.insert("wallet", "W1", {"wallet_id": "W1", "balance": "100.00"})
    cap.update("wallet", "W1", {"wallet_id": "W1", "balance": "30.00"})
    cap.delete("wallet", "W1", {"wallet_id": "W1", "balance": "30.00"})
    return cap.log


def test_every_change_is_stored(lake_con):
    write_all(lake_con, _sample_records())

    stored = read_records(lake_con)
    assert [r.sequence for r in stored] == [1, 2, 3]
    assert [r.operation for r in stored] == ["insert", "update", "delete"]
    assert stored[0].data["balance"] == "100.00"  # payload round-trips


def test_duplicate_write_is_ignored(lake_con):
    records = _sample_records()
    write_all(lake_con, records)
    write_all(lake_con, records)  # redeliver everything

    stored = read_records(lake_con)
    assert [r.sequence for r in stored] == [1, 2, 3]  # no duplicates appended


def test_replay_up_to_sequence(lake_con):
    write_all(lake_con, _sample_records())

    # Time travel: state as of sequence 2 excludes the later delete.
    replayed = read_records(lake_con, up_to_sequence=2)
    assert [r.sequence for r in replayed] == [1, 2]


def test_read_orders_by_sequence(lake_con):
    cap = CDCCapture()
    cap.insert("customer", "C1", {})
    cap.insert("customer", "C2", {})

    # Write out of order; the lake still returns records in sequence order.
    write(lake_con, cap.log[1])
    write(lake_con, cap.log[0])

    assert [r.sequence for r in read_records(lake_con)] == [1, 2]
