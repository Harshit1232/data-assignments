"""CI gate: verify the source schema still matches the expected contract.

Exits 0 if every source table exposes its expected columns, 1 otherwise.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from pipeline.schema_check import read_columns  # noqa: E402
from source.seed import build_source_db  # noqa: E402

# The data contract: the columns each source table must expose downstream.
EXPECTED_COLUMNS = {
    "customer": {
        "customer_id",
        "name",
        "email",
        "status",
        "country",
        "created_at",
        "updated_at",
    },
    "wallet": {
        "wallet_id",
        "customer_id",
        "currency",
        "balance",
        "status",
        "created_at",
        "updated_at",
    },
    "wallet_ledger_entry": {
        "wallet_id",
        "entry_seq",
        "amount",
        "entry_type",
        "balance_after",
        "external_ref",
        "created_at",
    },
}


def main() -> int:
    con = build_source_db()
    for table, expected in EXPECTED_COLUMNS.items():
        actual = set(read_columns(con, table))
        missing = expected - actual
        if missing:
            print(f"FAIL: {table} is missing columns {sorted(missing)}")
            return 1
    print("OK: source schema matches the contract")
    return 0


if __name__ == "__main__":
    sys.exit(main())
