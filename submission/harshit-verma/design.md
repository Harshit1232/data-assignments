# CDC Lakehouse — Design Document

**Status:** Documentation-first design (written before implementation, per assignment).
**Domain:** Wallet / ledger.
**CDC mode:** Simulated, with a documented seam where production log-based CDC would plug in.
**Stack:** Python + DuckDB + pytest. JSON catalog. (Rationale in §13.)
**Scope stance:** Deliberately small and deep — 3 source tables, every reliability property provably tested. SCD2 and a payments entity were considered and *deferred* with documented extension paths (§6.2, §14). The assignment optimizes for depth and engineering judgment over breadth; so does this design.

> This document is the design contract for the system and the spine of the PR description. Implementation should not diverge without updating this file first. Section numbers map onto the PR requirements.

---

## 1. Overview & Scope

We build a small, reliable change-data-capture pipeline that keeps two analytical stores in sync with a transactional source:

- a **lake** that durably retains *every* change (append-only, immutable) — the source of truth for replay, audit, and recovery;
- a **warehouse** that holds the *current state* (latest snapshot per row), modeled for analytical consumers.

The whole point is **correctness under failure**, not feature count: duplicates, restarts, out-of-order and late events, deletes, breaking schema changes, and point-in-time recovery. The design is intentionally narrow so each of these can be *demonstrated* with a test rather than merely asserted.

### In scope
- A self-designed relational source schema — **3 tables**, with a strong/weak entity split and a non-trivial monetary invariant.
- Simulated CDC capture producing an ordered, append-only change log.
- Lake (append-only change history) and warehouse (current-state snapshot).
- Schema-drift detection with stop-the-line behaviour.
- Time-travel / restore via lake replay.
- Validation parity (system + business rules) re-asserted against the warehouse.
- A minimal catalog exposing lake and warehouse datasets.
- Tests across the five required categories, written test-first.

### Out of scope (explicit, deliberate)
- **SCD2 history tables** — current-state warehouse + lake replay already satisfies "move backward and restore via the warehouse." SCD2 is documented as the scale-up (§6.2), not built. A half-built temporal model is a liability; a documented deliberate deferral is a seniority signal.
- **A payments/merchant entity** — the wallet+ledger spine already exercises strong/weak entities, relationships, every required type family, and a real invariant. Adding payments is a documented extension (§14).
- Real log-based CDC infra (Debezium/WAL) — simulated; production seam in §4.4.
- Cloud deployment, IAM, BI dashboards, orchestration frameworks, automatic migration of breaking changes, hard deletes.

---

## 2. Architecture

Six layers, one responsibility each, clean contracts between them.

```
                          ┌─────────────────────────────┐
                          │  Schema Contract / Drift     │
                          │  check (STOP-THE-LINE)       │
                          └──────────────┬──────────────┘
                                         │ (only compatible changes pass)
 ┌────────────┐   change   ┌─────────────▼─────────────┐   ordered events
 │  SOURCE    │  events     │   CDC CAPTURE             │  (seq, op, pk, data, ts)
 │  (OLTP)    ├────────────►│   assigns monotonic seq   ├──────────────┐
 │  relational│             │   = offset/LSN analogue   │              │
 └────────────┘             └───────────────────────────┘              │
                                                                        │
                 ┌──────────────────────────────────────┐              │
                 │  LAKE  (append-only, immutable)        │◄────────────┤
                 │  every insert/update/delete, forever   │             │
                 └───────────────┬──────────────────────-┘             │
                                 │ replay / rebuild                     │ upsert by seq
                                 │ (recovery + time travel)             │ (last-write-wins)
                 ┌───────────────▼──────────────────────────-┐         │
                 │  WAREHOUSE (current snapshot)              │◄────────┘
                 │  latest state per PK, soft-deletes flagged │
                 └───────────────┬───────────────────────────┘
                                 │
              ┌──────────────────┼───────────────────┐
   ┌──────────▼─────────┐            ┌────────────────▼────────────┐
   │  VALIDATION/QUALITY │            │  CATALOG / ACCESS           │
   │  parity checks      │            │  metadata for lake + wh     │
   │  (system + business)│            │  owners, consumers, schema  │
   └─────────────────────┘            └─────────────────────────────┘
```

Key asymmetry: **the lake is sacred and durable; the warehouse is derived and disposable.** The warehouse can always be rebuilt by replaying the lake. That is the entire recovery and time-travel story, and it only holds because the lake is immutable.

---

## 3. Source Data Model (3 tables)

### 3.1 Domain rationale
Wallet/ledger is chosen for one elegant reason beyond fitting the requirements: **the source's own ledger is itself an append-only log**, mirroring the CDC lake. The same mental model — immutable, ordered, replayable — appears in both the domain and the platform. It also yields a genuinely non-trivial invariant (running-balance reconciliation) to test parity against.

### 3.2 Tables
Two strong entities (independent identity, own primary key) and one weak entity (identity depends on its parent; primary key is partly the parent's foreign key).

| Table | Kind | Primary key | Notes |
|---|---|---|---|
| `customer` | strong | `customer_id` | account holder |
| `wallet` | strong | `wallet_id` | belongs to a customer (1—N) but has its own surrogate identity |
| `wallet_ledger_entry` | **weak** | (`wallet_id`, `entry_seq`) | a balance-history line; meaningless without its wallet |

### 3.3 Columns, types, enums, nullability

**customer** — `customer_id` (PK), `name` (text), `email` (text), `status` (enum: `active|suspended|closed`), `country` (text, **nullable**), `created_at` (ts), `updated_at` (ts).

**wallet** — `wallet_id` (PK), `customer_id` (FK→customer), `currency` (enum/ISO code: `USD|EUR|INR`), `balance` (**decimal(18,2)**), `status` (enum: `active|frozen|closed`), `created_at` (ts), `updated_at` (ts).

**wallet_ledger_entry** — (`wallet_id` FK, `entry_seq` int) PK, `amount` (**decimal(18,2)**, signed: + credit / − debit), `entry_type` (enum: `credit|debit|adjustment`), `balance_after` (decimal(18,2)), `external_ref` (text, **nullable** — optional external transaction reference), `created_at` (ts).

Covers every required type family: currency/decimal, dates/timestamps, enum/status, identifiers/FKs, nullable optional attributes.

### 3.4 Relationships
- `customer` 1—N `wallet`
- `wallet` 1—N `wallet_ledger_entry`

### 3.5 Indexes (where lookup or change-capture performance matters)
- Primary keys on all tables.
- Foreign keys: `wallet.customer_id`, `wallet_ledger_entry.wallet_id`.
- `updated_at` on `customer` and `wallet` — the watermark a timestamp-based extractor would use (documents the production analogue even though we simulate).
- `created_at` on `wallet_ledger_entry` for time-ordering.

### 3.6 Invariants

**System:** PK uniqueness on every table; referential integrity on every FK; not-null on all non-nullable columns; values within declared enum domains and types.

**Business:**
- **B1 — Non-negative balance:** `wallet.balance >= 0` and `wallet_ledger_entry.balance_after >= 0` at all times (no overdraft).
- **B2 — Ledger reconciliation:** `wallet.balance` equals the `balance_after` of the wallet's latest ledger entry, which equals the signed sum of all that wallet's ledger amounts.
- **B3 — Amount sign matches type:** `credit` amounts are > 0, `debit` amounts are < 0 (`adjustment` may be either).
- **B4 — Status transitions:** `active ↔ suspended/frozen`; any non-terminal → `closed`; `closed` is terminal (applies to `customer.status` and `wallet.status`).
- **B5 — No pre-dated entries:** `wallet_ledger_entry.created_at >= wallet.created_at`.
- **B6 — Append-only facts:** `wallet_ledger_entry` is insert-only and immutable; never updated or (logically) deleted.

### 3.7 Expected change patterns (drives CDC and warehouse modeling)
- `customer`: low churn (occasional profile/status updates).
- `wallet`: **high churn** on `balance`/`updated_at` (changes on every transaction) — update-heavy state.
- `wallet_ledger_entry`: **insert-only**, immutable facts.

The state-vs-fact distinction matters: `wallet` needs upsert; `wallet_ledger_entry` only ever appends, so dedup-on-insert is the only concern.

---

## 4. CDC Design

### 4.1 Change-event contract
Every captured change is one event with this envelope — the contract between capture and the lake/warehouse:

| Field | Type | Meaning |
|---|---|---|
| `sequence` | int, monotonic, globally unique | offset / LSN analogue; total order of changes |
| `operation` | `insert \| update \| delete` | change type |
| `table` | text | source table |
| `primary_key` | text (composite serialized) | the row's PK |
| `data` | JSON | full after-image row snapshot (for delete: last-known image) |
| `captured_at` | timestamp (UTC) | when the change was captured |

**Guarantees:** events are totally ordered by `sequence`. Delivery is **at-least-once** (duplicates possible; consumers must be idempotent). We capture the full after-image so the warehouse apply is a simple set-based overwrite rather than a column-merge.

### 4.2 Capture (replay / restart)
- Capture assigns the next `sequence` to every insert/update/delete on the source.
- A **checkpoint** records the last successfully processed `sequence`, persisted durably (a checkpoint table/file).
- On restart, the consumer requests `records_since(checkpoint)` — every event with `sequence > checkpoint`. This is what makes restarts lose nothing and reprocess nothing.
- The checkpoint advances **only after** the lake/warehouse write for that event succeeds, so a crash mid-write replays that event rather than skipping it.

### 4.3 Duplicates, deletes, out-of-order, late events
- **Duplicates:** the lake key is `sequence` (unique). Re-seeing a `sequence` is a no-op. The warehouse applies last-write-wins by `sequence`, so re-applying is idempotent.
- **Deletes:** captured as a `delete` event. The lake **appends** it (nothing is ever removed). The warehouse sets `_deleted = true` and records `_cdc_seq`; consumers filter `WHERE NOT _deleted`. Append-only ledger rows are never deleted (B6).
- **Out-of-order / late updates:** the warehouse overwrites a row **only if** the incoming `sequence` is greater than the row's stored `_cdc_seq`. A late/out-of-order event with a lower sequence is still written to the lake (history stays complete) but does not clobber newer warehouse state.

### 4.4 What is simulated vs. production

| Concern | This assignment (simulated) | Production |
|---|---|---|
| Source of changes | In-process CDC log; a driver calls insert/update/delete with each changed row image. The seeded source DB is the initial snapshot and schema-check target | Debezium/connector tailing the DB write-ahead log |
| `sequence` | In-process monotonic counter | Postgres LSN / Kafka offset |
| Transport | Function calls / in-memory log | Kafka topic / Kinesis stream |
| Delivery semantics | At-least-once (duplicates modeled explicitly) | At-least-once (real) |
| Checkpoint store | Local table/file | Connector offset store / consumer-group offsets |

The capture interface (`records_since(offset)`, `latest_sequence`) is written so swapping the in-process source for a real connector does not change the lake/warehouse/validation layers.

---

## 5. Lake Data Model

A single append-only dataset, `lake_cdc_events`, with the §4.1 envelope plus an `ingested_at` column.

- **Append-only and immutable.** No updates, no deletes, ever. Enforced by convention in the simulated store; enforced by an immutable table format (Delta/Iceberg) or write-once object storage in production.
- **Completeness.** Every insert/update/delete for every table lands here as its own row. A row updated five times then deleted produces seven events.
- **Partitioning (production analogue).** Partition by `table_name` and date of `captured_at`; store as Parquet. Locally one table for simplicity; analogue noted.
- **Purpose.** The replay source for rebuilding the warehouse and for point-in-time reconstruction (§8), and the audit record of "what actually happened."

---

## 6. Warehouse Data Model

### 6.1 Current-state tables
One snapshot table per source table — `wh_customers`, `wh_wallets`, `wh_wallet_ledger` — each carrying the source columns plus metadata:

| Metadata column | Meaning |
|---|---|
| `_cdc_seq` | `sequence` of the last event applied to this row (drives last-write-wins) |
| `_deleted` | true if the source row was deleted |
| `_loaded_at` | when the warehouse applied the change |

**Apply logic:** consume events in `sequence` order; for each, upsert the row keyed on its PK, but overwrite only when `incoming.sequence > existing._cdc_seq` (handles out-of-order/late events and duplicates). Delete events set `_deleted = true`.

### 6.2 Historical reconstruction — and why SCD2 is deferred
The three representations the assignment asks to distinguish:
- **Event/change history** → the lake.
- **Current-state snapshot** → the `wh_*` tables.
- **Historical reconstruction** → **lake replay** (§8): replay events up to a target sequence/timestamp to rebuild any past state.

SCD2 versioned tables are the standard *optimization* when analysts need frequent, fast point-in-time queries without paying replay cost each time. It is **deliberately not built here** because lake replay already satisfies the requirement, and a half-built temporal model adds risk without adding correctness.

*How I would add it (extension path):* a `wh_wallets_history` table with `wallet_id`, the versioned attributes, `valid_from`, `valid_to` (null = current), `is_current`. On each wallet update, close the prior version (`valid_to = event time`) and open a new one. Point-in-time then becomes `WHERE valid_from <= T AND (valid_to > T OR valid_to IS NULL)`.

---

## 7. Schema Evolution Policy & Stop-the-Line

### 7.1 Contract
We snapshot each source table as `column name → type` (`schema_check.read_columns`). Before ingesting we re-read the source and compare against the snapshot. The implementation focuses on the highest-signal, unambiguous cases — a dropped or retyped column; the nullability / enum-domain / PK-FK rows below are natural extensions of the same column comparison.

### 7.2 Compatible vs. breaking

| Change | Classification | Action |
|---|---|---|
| Add a new **nullable** column (or with default) | compatible (additive) | continue; log; optionally evolve contract |
| Widen a type (e.g. int→bigint) | compatible | continue; log |
| **Drop** a column | breaking | **stop the line** |
| **Rename** a column (= drop + add to a diff) | breaking | **stop the line** |
| **Change** a column's type | breaking | **stop the line** |
| **Narrow/remove** an enum value in use | breaking | **stop the line** |
| Nullable → **non-nullable** where existing data violates | breaking | **stop the line** |
| Change PK/FK definition | breaking | **stop the line** |

Principle: additive changes leave existing downstream logic correct, so we keep running; destructive or type-changing changes can make downstream logic read something gone or re-meaninged, so we halt. Under "no backward-compatibility guarantee," unknown blast radius defaults to halt.

### 7.3 Behaviour on a breaking change
1. **Stop ingestion** — raise `SchemaChangeError`; the consumer halts.
2. **Emit a clear signal** — log at ERROR with offending table/column/diff; exit non-zero / write a failure marker an alerting system would page on.
3. **Do not write** the offending event or any subsequent event to lake or warehouse.
4. **Leave the checkpoint at the last good sequence**, so after a human fixes the contract or reverts the source, ingestion resumes cleanly.

We deliberately do **not** auto-migrate. The requirement is to fail safely and observably.

---

## 8. Time-Travel & Restore

### 8.1 Mechanism — lake replay
To reconstruct state as of time `T` (or sequence `N`), replay all `lake_cdc_events` with `captured_at <= T` (or `sequence <= N`) and fold them into state. Works for any table, any point — compute-on-read from immutable history. (SCD2 would make this a fast query for wallets; deferred, §6.2.)

### 8.2 Restore / rollback runbook
1. Identify the bad sequence `N` (e.g. corruption introduced after a transform bug).
2. Truncate/rebuild the affected `wh_*` table(s).
3. Replay `lake_cdc_events` with `sequence <= N-1` to regenerate clean current state.
4. Re-run validation (§9) to confirm parity before reopening downstream access.

Only possible because the lake is immutable and complete (§5).

---

## 9. Validation & Data Quality

Every rule the source guarantees is **independently re-asserted against the warehouse** — the source's constraints do not travel through CDC, only the data does.

### 9.1 System parity
- PK uniqueness: no duplicate PKs in any `wh_*` table.
- Referential integrity: every `wh_wallets.customer_id` exists in `wh_customers`; every `wh_wallet_ledger.wallet_id` exists in `wh_wallets`.
- Not-null: required columns are non-null.
- Domains/types: enum columns contain only declared values.

### 9.2 Business parity
- B1 non-negative balance: `SELECT count(*) FROM wh_wallets WHERE balance < 0` must be 0.
- B2 ledger reconciliation: per wallet, signed sum of `wh_wallet_ledger.amount` equals `wh_wallets.balance`.
- B3 amount sign matches type.
- B4 status transitions: validated against the lake's ordered status changes vs. the allowed graph.
- B5 no pre-dated entries.
- B6 immutability: no updates/deletes observed on ledger rows in the lake.

### 9.3 Freshness / completeness / schema
- **Completeness:** `max(_cdc_seq)` across the warehouse equals `max(sequence)` in the lake (no lost events); row-count parity per table after replay.
- **Schema:** the contract check (§7) runs as part of validation.

### 9.4 Surfacing
Validations run after each warehouse build. Violations fail the run (non-zero exit), are written to a quarantine/report table, and would alert in production. Never logged-and-ignored.

---

## 10. Catalog & Access

A single `catalog/catalog.json` registers every dataset, each publishing: `name`, `layer` (`lake|warehouse`), `description`, `owner`, `consumers`, `update_cadence`, `schema` (column → type + description), `access_path` (DuckDB table / file path).

Datasets:
- `lake_cdc_events` (lake; owner data-platform; consumers data-platform/audit; cadence real-time).
- `wh_customers`, `wh_wallets`, `wh_wallet_ledger` (warehouse; consumers analytics/finance/product; cadence near-real-time).

A `validate_catalog` check asserts that every physical dataset has a catalog entry and that the published schema matches the actual schema (no drift between docs and reality).

**Production analogue:** the JSON stands in for AWS Glue Data Catalog, Databricks Unity Catalog, or DataHub, which would also enforce access boundaries and lineage.

---

## 11. Reliability Handling (requirement → mechanism)

| Reliability requirement | How the design handles it |
|---|---|
| Duplicate CDC events | Lake keyed by unique `sequence` (no-op on repeat); warehouse last-write-wins by `sequence` |
| Retries after partial failure | Idempotent writes; checkpoint advances only after a successful write |
| Out-of-order arrival | Order by `sequence`; warehouse overwrites only when incoming `sequence` > stored `_cdc_seq` |
| Restart after checkpoint | `records_since(checkpoint)` resumes exactly where it stopped |
| Incompatible schema change | Stop-the-line (§7): halt, alert, don't ingest |
| Deletes in the source | Soft delete — delete event appended to lake, `_deleted` flag in warehouse |
| Late-arriving updates | Written to lake for completeness; sequence guard prevents clobbering newer state |
| Recovery from historical data | Rebuild warehouse by replaying the immutable lake to any sequence/point in time |

---

## 12. Testing Strategy

Test-first (**Red → Blue → Green**: write a failing test, implement the minimum to pass, then refactor while preserving behaviour). The commit history will reflect this rhythm. Each test proves one property:

| Test | Category | Property it proves |
|---|---|---|
| `test_source_constraints` | Modeling | PK uniqueness, FK integrity, not-null, enum domains hold |
| `test_invariants_hold_on_seed` | Modeling | B1–B6 hold on valid source data |
| `test_insert_update_delete_captured` | CDC | every op produces a correct lake event |
| `test_replay_is_deterministic` | CDC | replaying the lake twice yields an identical warehouse |
| `test_restart_no_dup_no_loss` | CDC | crash after N events → resume from checkpoint, no duplicate, no loss |
| `test_out_of_order_converges` | CDC | a lower-sequence late event does not clobber newer warehouse state |
| `test_duplicate_event_is_noop` | CDC | a re-delivered event changes nothing |
| `test_breaking_schema_halts` | Schema safety | dropped/renamed/retyped column stops ingestion + emits a signal |
| `test_additive_schema_continues` | Schema safety | a new nullable column does **not** stop the line |
| `test_delete_soft_deletes` | Warehouse | deleted row → lake event + `_deleted` flag, filtered from active views |
| `test_warehouse_is_latest_snapshot` | Warehouse | each `wh` row matches the highest-sequence event for its PK |
| `test_ledger_reconciliation` | Validation | `balance` == signed sum of ledger entries (B2) |
| `test_parity_catches_negative_balance` | Validation | an injected bad state fails validation loudly |
| `test_time_travel_reconstructs` | Warehouse | replay-to-sequence-N matches expected historical state |
| `test_catalog_covers_all_datasets` | Catalog | every physical dataset has a catalog entry and the schema matches |

A reviewer can read this table and immediately see the non-happy-path is covered. That legibility is the point.

---

## 13. Tech Stack & How to Run

- **DuckDB** for both layers (lake = append-only table / Parquet; warehouse = current-state tables): in-process, full SQL, trivial to test, behaves like a local analytical warehouse. "Lake" and "warehouse" are *roles* within it, kept logically separate — not two separate products.
- **Python** for the simulated source, capture, and apply logic.
- **pytest** for all validation and behaviour tests.
- **JSON** for the catalog.

Target: a single command runs the full pipeline and all tests green from a clean clone (`pip install -r requirements.txt && pytest`). Exact module layout mirrors §2 and is finalized alongside implementation.

---

## 14. Assumptions, Tradeoffs & Limitations

- **Simulated CDC.** We model at-least-once delivery, duplicates, and out-of-order events explicitly, but do not exercise a real WAL. §4.4 documents exactly what changes in production. This is the main correctness caveat.
- **Three tables, by choice.** Enough for strong/weak entities, a FK chain, every type family, and a real invariant — sized to be done deeply within the time budget rather than broadly and shallowly.
- **Full after-image capture** simplifies warehouse apply (set-based overwrite) at the cost of larger events than a changed-columns-only approach. Acceptable at this scale.
- **Single global sequence** gives clean total ordering; a real multi-partition stream would order per-key and need merge-on-read.
- **Soft deletes only.** Hard-delete / erasure from an immutable lake is out of scope and needs a separate tombstoning/compaction design.
- **DuckDB caveats (found during implementation).** DuckDB cannot UPDATE/DELETE a row still referenced by a foreign key, and cannot DROP a column while an index sits on a later column. Neither affects this design: CDC capture is an in-memory log that never writes to the source, so in-place source updates never happen; and schema detection only *reads* `information_schema`. A breaking change is simulated in tests at the dict level (and ADD COLUMN works directly against DuckDB). A production Postgres source has neither limitation.

### What I'd do with more time
- Add **SCD2** on `wallet` for fast point-in-time queries (design in §6.2).
- Add a **payments + merchant** entity to exercise a richer status lifecycle and a second FK.
- Swap the simulated source for **real log-based CDC** (Postgres logical replication / Debezium) behind the existing capture interface.
- Add **hard-delete / GDPR erasure** handling via tombstoning + lake compaction.

---

## 15. Responsible AI Usage

> Fill this in to match what you actually did — the hiring team explicitly values candour and judgment over generated volume. The template below reflects a typical honest split; adjust it to the truth.

- **Where AI helped:** clarifying the problem, structuring this design document, drafting boilerplate (schema DDL, test scaffolding), and articulating tradeoffs.
- **What I personally reviewed/validated:** the source schema and invariants (I chose the domain, entities, and business rules and confirmed they are internally consistent); the CDC correctness reasoning (duplicate / restart / out-of-order handling); every test assertion; and the compatible-vs-breaking classification.
- **What I corrected:** _(list concrete changes you made from any AI-suggested version — e.g. tightening an invariant, fixing a sequence-guard edge case, deciding to defer SCD2.)_
- **What I would not delegate:** the correctness model (lake-as-source-of-truth / warehouse-as-disposable) and the failure-mode handling — these are the engineering judgment the assignment is testing.
