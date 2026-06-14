# How to Run & Verify the CDC Lakehouse Pipeline

A step-by-step guide to **run** the pipeline and **prove** it satisfies every
requirement in the assignment. (For the design rationale, see
[`../design.md`](../design.md); for a short overview, [`../README.md`](../README.md).)

All commands below assume you are **inside the submission folder**:

```bash
cd submission/harshit-verma
```

---

## 1. Prerequisites

- **Python 3.10 or newer**
- Install the two dependencies (DuckDB + pytest):

```bash
pip install -r requirements.txt
```

That's the entire setup — everything runs locally, in-memory, no server or Docker.

---

## 2. Run the pipeline

The pipeline is run by three small scripts. Each **prints `OK: ...` and exits 0
on success**, or prints `FAIL: ...` and exits 1. Run all three:

```bash
python scripts/check_schema_contracts.py      # source schema is intact
python scripts/run_data_quality_checks.py     # build pipeline + validate warehouse
python scripts/validate_catalog.py            # catalog covers every dataset
```

Expected output:

```
OK: source schema matches the contract
OK: all data-quality checks passed
OK: catalog covers all 4 datasets
```

`run_data_quality_checks.py` is the important one — it runs the **whole pipeline
end to end**: seed the source → capture every row as a change → write to the lake
→ build the warehouse → re-check all the business rules.

### See the data it produces (optional)

To watch the pipeline actually build the lake and warehouse, run these one-liners
(still from inside `submission/harshit-verma`):

```bash
# Current-state warehouse: latest balance per wallet
python -c "from pipeline.run import run_pipeline; c=run_pipeline(); print(c.execute('SELECT wallet_id, balance, _deleted FROM wh_wallets ORDER BY wallet_id').fetchall())"

# Lake: how many change events were stored (the full history)
python -c "from pipeline.run import run_pipeline; c=run_pipeline(); print('lake events:', c.execute('SELECT count(*) FROM lake_cdc_events').fetchone()[0])"

# Time travel: rebuild the warehouse as of an earlier sequence
python -c "from pipeline.run import run_pipeline; from pipeline.lake import read_records; from pipeline.warehouse import build_warehouse; c=run_pipeline(); build_warehouse(c, read_records(c, up_to_sequence=5)); print('rows as of seq 5:', c.execute('SELECT count(*) FROM wh_wallets').fetchone()[0])"
```

You should see the three wallets with their balances (W1=80.00, W2=200.00,
W3=0.00) and a non-zero lake event count.

---

## 3. Run the tests (the proof it behaves correctly)

```bash
pytest -q
```

Expected result:

```
35 passed
```

If all 35 pass, every layer — and every failure case (duplicates, out-of-order
events, deletes, restarts, schema breaks) — is working as designed.

> Running from the **repo root** instead? Use the same form CI uses:
> `pytest submission/harshit-verma/ -q` and
> `python submission/harshit-verma/scripts/<name>.py`.

---

## 4. Verify every assignment requirement

Each requirement maps to a command or test you can run to prove it. After
`pytest -q` and the three scripts pass, **everything below is satisfied**.

| # | Assignment requirement | How to verify | What proves it |
|---|---|---|---|
| 1 | **Source data model** (strong + weak entities, keys, indexes, types) | `pytest -q tests/test_source.py` | 13 tests: the source rejects bad data (PK, FK, NOT NULL, enums, balance ≥ 0) and invariants hold on the seed |
| 2 | **Capture all inserts/updates/deletes** | `pytest -q tests/test_cdc.py` | Each op produces a correctly numbered `CDCRecord` |
| 3 | **Lake keeps every change** (append-only) | `pytest -q tests/test_lake.py` | Every change stored; duplicate write ignored; ordered by sequence |
| 4 | **Warehouse = latest snapshot** | `pytest -q tests/test_warehouse.py` | Highest-sequence row wins per key |
| 5 | **Duplicates / out-of-order / restart** | `tests/test_warehouse.py` + `tests/test_cdc.py` | Idempotent apply, out-of-order converges, `records_since` replays only new records |
| 6 | **Deletes handled** | `tests/test_warehouse.py::test_delete_soft_deletes` | Deleted row kept with `_deleted = true` |
| 7 | **Schema change detection + safe stop** | `python scripts/check_schema_contracts.py` + `tests/test_schema_check.py` | Dropped/retyped column raises `SchemaChangeError` |
| 8 | **Time travel / historical recovery** | `tests/test_warehouse.py::test_rebuild_from_lake_enables_time_travel` | Warehouse rebuilt from the lake up to a chosen sequence |
| 9 | **Validation parity** (source rules re-checked downstream) | `python scripts/run_data_quality_checks.py` + `tests/test_quality.py` | B1 (non-negative), B2 (balance = ledger sum), referential integrity; catches an injected bad value |
| 10 | **Catalog exposure** | `python scripts/validate_catalog.py` + `tests/test_catalog.py` | Every lake/warehouse dataset is registered with owner, consumers, cadence, schema |
| 11 | **Tests prove behavior, not just happy path** | `pytest -q` | 35 tests, most covering failure cases |
| 12 | **Reproducible, one-command run** | `pip install -r requirements.txt && pytest` | Clean clone → green |

---

## 5. What "success" looks like (checklist)

- [ ] `pip install -r requirements.txt` completes
- [ ] `pytest -q` → **35 passed**
- [ ] `python scripts/check_schema_contracts.py` → `OK` (exit 0)
- [ ] `python scripts/run_data_quality_checks.py` → `OK` (exit 0)
- [ ] `python scripts/validate_catalog.py` → `OK` (exit 0)

If all five are ticked, the pipeline runs end to end and meets every condition in
the brief.

---

## 6. Troubleshooting

- **`ModuleNotFoundError: No module named 'pipeline'`** — you're not in the right
  folder. `cd submission/harshit-verma` first (or run the scripts by full path
  from the repo root; they add themselves to the path automatically).
- **`pytest` not found** — install dev tools: `pip install pytest`.
- **A script prints `FAIL: ...`** — the message names exactly what's wrong (a
  missing column, a failing data-quality rule, or an uncatalogued dataset).
