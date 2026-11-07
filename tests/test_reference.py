"""Tests for the reference seed files and their loader, using the real files in reference/."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.iot import reference as ref

REFERENCE_DIR = Path(__file__).resolve().parents[1] / "reference"


@pytest.mark.parametrize("name", sorted(ref.REFERENCE_TABLES))
def test_every_seed_file_loads_with_its_declared_types(spark, name):
    df = ref.load_reference_table(spark, REFERENCE_DIR, name)
    declared = ref.REFERENCE_TABLES[name]

    assert df.columns == list(declared)
    assert df.count() > 0
    for column, dtype in declared.items():
        if dtype != "string":
            assert df.filter(f"{column} IS NULL").count() == 0, f"{name}.{column} has unparseable values"


def test_known_vendors_come_from_the_contract_file():
    assert ref.known_vendors(REFERENCE_DIR) == ["ats", "fpl", "gtk", "tmx"]


def test_retired_contract_vendor_is_not_known(tmp_path):
    (tmp_path / "vendor_contract_versions.csv").write_text(
        "vendor_id,contract,version,status,supported_from,notes\n"
        "tmx,tmx.telemetry.v1,1,current,2026-10-01,\n"
        "old,old.telemetry.v1,1,retired,2020-01-01,\n"
    )
    assert ref.known_vendors(tmp_path) == ["tmx"]


def test_missing_column_is_reported(spark, tmp_path):
    (tmp_path / "fault_codes.csv").write_text("code,description\nP0128,x\n")
    with pytest.raises(ValueError, match="severity"):
        ref.load_reference_table(spark, tmp_path, "fault_codes")


def test_every_vendor_maps_every_standard_field():
    rows = ref.read_rows(REFERENCE_DIR / "vendor_field_mappings.csv")
    fields_by_vendor: dict[str, set[str]] = {}
    for r in rows:
        fields_by_vendor.setdefault(r["vendor_id"], set()).add(r["target_field"])

    expected = fields_by_vendor["tmx"]
    assert len(expected) == 21
    assert all(fields == expected for fields in fields_by_vendor.values())