# CDC Lakehouse — Wallet / Ledger

A small, reliable **change-data-capture pipeline** that keeps a **lake** (full
change history) and a **warehouse** (current-state snapshot) in sync with a
transactional source, and behaves correctly under duplicates, restarts,
out-of-order events, deletes, and incompatible schema changes.

Full design rationale is in **[design.md](design.md)**. This README is setup +
how to run.

## The idea in one picture

```
source (DuckDB, seeded)
      │  capture every change as a numbered record
      ▼
  cdc.py ──► lake.py  (lake_cdc_events: append-only history, keyed by sequence)
      │                         │ replay (optionally up to sequence N = time travel)
      │                         ▼
      └────────────────► warehouse.py  (wh_*: latest row per key, soft-deletes)
                                │
                 quality.py (re-assert source rules) · catalog.json (discoverability)
```

- **Lake** is sacred and immutable — the source of truth for replay/recovery.
- **Warehouse** is derived and disposable — always rebuildable from the lake.

## Layout

```
source/         schema.py (DDL), seed.py (sample data)
pipeline/       cdc.py (capture) · schema_check.py (stop-the-line) ·
                lake.py (history) · warehouse.py (current state) ·
                quality.py (validation) · run.py (end-to-end driver)
scripts/        check_schema_contracts.py · run_data_quality_checks.py ·
                validate_catalog.py        (CI gates; each exits 0/1)
catalog/        catalog.json (dataset metadata)
tests/          one test file per layer
```

## Setup

```bash
pip install -r requirements.txt    # duckdb, pytest
```

Requires Python 3.10+.

## Run

```bash
# Run the whole pipeline and the data-quality checks (from this folder):
python scripts/run_data_quality_checks.py

# The other two CI gates:
python scripts/check_schema_contracts.py     # source schema matches the contract
python scripts/validate_catalog.py           # catalog covers every dataset
```

Each script prints `OK: ...` and exits 0 on success, or prints `FAIL: ...` and
exits 1. They run from any directory (and from the repo root, as CI does:
`python submission/harshit-verma/scripts/<name>.py`).

## Tests

```bash
pytest                 # from this folder
# or, from the repo root, as CI runs it:
pytest submission/harshit-verma/ -q
```

The suite covers every required category: source constraints & invariants, CDC
capture & replay, schema-change safety, lake storage & time-travel, warehouse
snapshot / convergence / soft-delete, validation parity, and catalog coverage.

## How the reliability requirements are met

| Requirement | Where |
|---|---|
| Capture every insert/update/delete | `cdc.py` — one numbered `CDCRecord` per change |
| Durable full history | `lake.py` — append-only `lake_cdc_events` |
| Latest snapshot, near real-time | `warehouse.py` — last-write-wins by sequence |
| Duplicates / out-of-order | last-write-wins fold + lake `ON CONFLICT DO NOTHING` |
| Restart after checkpoint | `records_since(offset)` replays only unprocessed records |
| Deletes | soft delete — `_deleted` flag in the warehouse |
| Time travel / restore | rebuild warehouse from `read_records(up_to_sequence=N)` |
| Incompatible schema change | `schema_check.py` raises `SchemaChangeError` (stop the line) |
| Validation parity | `quality.py` re-asserts source rules on the warehouse |
