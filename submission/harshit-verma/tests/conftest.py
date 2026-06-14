"""Shared pytest fixtures for the source-layer tests.

The ``sys.path`` shim lets the suite be run as ``pytest submission/harshit-verma/``
from the repo root (as CI does) while still importing the ``source`` package.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from source.seed import build_source_db  # noqa: E402


@pytest.fixture
def source_con():
    """A fresh, fully-seeded in-memory source database, torn down per test."""
    con = build_source_db()
    try:
        yield con
    finally:
        con.close()
