"""Catalog tests: every dataset is listed with the required metadata."""

import json
import pathlib

from pipeline.run import run_pipeline

CATALOG = pathlib.Path(__file__).resolve().parents[1] / "catalog" / "catalog.json"
REQUIRED_FIELDS = {
    "name",
    "layer",
    "description",
    "owner",
    "consumers",
    "update_cadence",
    "schema",
}


def _catalog():
    return json.loads(CATALOG.read_text())


def test_catalog_covers_every_dataset():
    names = {dataset["name"] for dataset in _catalog()["datasets"]}

    con = run_pipeline()
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'lake_cdc_events' OR table_name LIKE 'wh_%'"
        ).fetchall()
    }
    assert tables <= names  # every physical lake/warehouse table is catalogued


def test_catalog_entries_have_required_fields():
    for dataset in _catalog()["datasets"]:
        assert REQUIRED_FIELDS <= dataset.keys()
