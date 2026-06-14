"""CI gate: build the pipeline and run the data-quality checks on the warehouse.

Exits 0 if every check passes, 1 if any check fails.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from pipeline.quality import run_quality_checks  # noqa: E402
from pipeline.run import run_pipeline  # noqa: E402


def main() -> int:
    con = run_pipeline()
    failures = run_quality_checks(con)
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print("OK: all data-quality checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
