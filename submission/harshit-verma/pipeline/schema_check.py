"""
Schema change detection (stop-the-line).

The source gives no backward-compatibility guarantee, so before ingesting we
compare the source's current columns against a snapshot taken earlier. A dropped
or retyped column is breaking: we raise SchemaChangeError and stop the pipeline
rather than write corrupt data downstream. A newly added column is
backward-compatible and allowed.
"""

from __future__ import annotations

import duckdb


class SchemaChangeError(Exception):
    """Raised when the source schema changes incompatibly."""


def read_columns(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    """Return {column_name: data_type} for a source table."""
    rows = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return {name: data_type for name, data_type in rows}


def check_schema(table: str, expected: dict[str, str], actual: dict[str, str]) -> None:
    """Raise SchemaChangeError if an existing column was dropped or retyped.

    Adding a new column is allowed (backward-compatible).
    """
    for column, data_type in expected.items():
        if column not in actual:
            raise SchemaChangeError(f"{table}: column {column!r} was dropped")
        if actual[column] != data_type:
            raise SchemaChangeError(
                f"{table}: column {column!r} type changed "
                f"{data_type} -> {actual[column]}"
            )
