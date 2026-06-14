"""CDC capture tests: records are produced correctly and replay is safe."""

import pytest

from pipeline.cdc import CDCCapture, CDCRecord


def test_insert_update_delete_captured():
    cap = CDCCapture()

    r1 = cap.insert("customer", "C1", {"customer_id": "C1", "name": "Ana"})
    r2 = cap.update("wallet", "W1", {"wallet_id": "W1", "balance": 120})
    r3 = cap.delete("customer", "C1", {"customer_id": "C1"})

    assert [r.operation for r in (r1, r2, r3)] == ["insert", "update", "delete"]
    assert [r.sequence for r in (r1, r2, r3)] == [1, 2, 3]  # monotonic
    assert r1.table == "customer"
    assert r1.primary_key == "C1"
    assert r2.data["balance"] == 120  # full after-image
    assert cap.latest_sequence == 3
    assert len(cap.log) == 3


def test_records_since_returns_only_unprocessed():
    cap = CDCCapture()
    for n in range(5):
        cap.insert("wallet", f"W{n}", {"n": n})

    assert [r.sequence for r in cap.records_since(3)] == [4, 5]
    assert cap.records_since(5) == []
    assert len(cap.records_since(0)) == 5


def test_restart_replays_without_loss_or_duplication():
    cap = CDCCapture()
    for n in range(5):
        cap.update("wallet", "W1", {"balance": n})

    processed = []
    checkpoint = 0
    # A consumer processes two records, committing the checkpoint, then crashes.
    for record in cap.records_since(checkpoint)[:2]:
        processed.append(record.sequence)
        checkpoint = record.sequence
    assert checkpoint == 2

    # A new consumer resumes from the checkpoint.
    for record in cap.records_since(checkpoint):
        processed.append(record.sequence)
        checkpoint = record.sequence

    assert processed == [1, 2, 3, 4, 5]  # no loss, no duplication
    assert checkpoint == 5


def test_invalid_operation_rejected():
    with pytest.raises(ValueError):
        CDCRecord(operation="upsert", table="wallet", primary_key="W1", data={})
