"""CI gate: verify the catalog covers every dataset with the required metadata.

Exits 0 if every physical lake/warehouse table has a catalog entry and every
entry carries the required fields, 1 otherwise.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from pipeline.run import run_pipeline  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
CATALOG = ROOT / "catalog" / "catalog.json"
REQUIRED_FIELDS = {
    "name",
    "layer",
    "description",
    "owner",
    "consumers",
    "update_cadence",
    "schema",
}


def main() -> int:
    catalog = json.loads(CATALOG.read_text())
    entries = {dataset["name"]: dataset for dataset in catalog["datasets"]}

    con = run_pipeline()
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'lake_cdc_events' OR table_name LIKE 'wh_%'"
        ).fetchall()
    }

    problems = [f"{t} has no catalog entry" for t in sorted(tables - entries.keys())]
    for name, entry in entries.items():
        missing = REQUIRED_FIELDS - entry.keys()
        if missing:
            problems.append(f"{name} is missing fields {sorted(missing)}")

    if problems:
        for problem in problems:
            print(f"FAIL: {problem}")
        return 1
    print(f"OK: catalog covers all {len(entries)} datasets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
