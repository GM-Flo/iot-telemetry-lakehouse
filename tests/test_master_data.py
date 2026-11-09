"""Tests for registry parsing, using a real export from the producer.

Building the input with the producer itself means these tests also prove the
silver schema matches what the producer actually writes.
"""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from src.iot.silver import master_data as md
from src.producer.fleet import Fleet
from src.producer.registry import select_clients


@pytest.fixture(scope="module")
def export():
    return Fleet(select_clients(1), seed=42).registry_export(today=date(2026, 11, 7))


@pytest.fixture
def bronze_of(spark):
    def _build(*snapshots):
        """snapshots: (snapshot_date, ingested_at, registry_dict) tuples."""
        rows = [(json.dumps(content), d, ts) for d, ts, content in snapshots]
        return spark.createDataFrame(
            rows, "raw_content string, snapshot_date date, ingested_at timestamp")

    return _build


@pytest.fixture
def parsed(bronze_of, export):
    return md.parse_registry(bronze_of((date(2026, 11, 7), datetime(2026, 11, 7, 4), export)))


class TestParsing:
    def test_every_section_parses(self, parsed, export):
        for section in md.SECTION_SCHEMAS:
            rows = md.registry_section(parsed, section).count()
            assert rows == len(export[section]), section

    def test_vehicle_columns_are_typed(self, parsed):
        row = md.registry_section(parsed, "vehicles").first()

        assert isinstance(row["model_year"], int)
        assert isinstance(row["tank_capacity_l"], float)
        assert row["snapshot_date"] == date(2026, 11, 7)

    def test_open_assignment_has_null_valid_to(self, parsed):
        rows = md.registry_section(parsed, "device_vehicle_assignments").collect()
        active = [r for r in rows if r["status"] == "active"]

        assert active
        assert all(r["valid_to"] is None for r in active)
        assert all(isinstance(r["valid_from"], date) for r in rows)

    def test_unregistered_device_is_absent(self, parsed):
        """C006-V001's device is registered late, so today's file lacks it."""
        vehicles = {r["vehicle_id"] for r in md.registry_section(parsed, "device_vehicle_assignments").collect()}
        assert "C006-V001" not in vehicles

    def test_unknown_section_rejected(self, parsed):
        with pytest.raises(ValueError):
            md.registry_section(parsed, "spaceships")


class TestSnapshots:
    def test_version_round_trip(self):
        assert md.snapshot_version(date(2026, 11, 7)) == 20261107
        assert md.version_to_date(20261107) == date(2026, 11, 7)

    def test_versions_sort_like_dates(self):
        days = [date(2026, 12, 1), date(2026, 11, 30), date(2027, 1, 2)]
        assert sorted(days) == [md.version_to_date(v) for v in sorted(map(md.snapshot_version, days))]

    def test_reuploaded_snapshot_replaces_the_earlier_file(self, bronze_of, export):
        corrected = {**export, "vendors": export["vendors"][:1]}
        bronze = bronze_of(
            (date(2026, 11, 7), datetime(2026, 11, 7, 4), export),
            (date(2026, 11, 7), datetime(2026, 11, 9, 9), corrected),
            (date(2026, 11, 8), datetime(2026, 11, 8, 4), export),
        )
        latest = md.latest_file_per_snapshot(bronze)

        assert latest.count() == 2
        nov7 = md.parse_registry(latest.filter("snapshot_date = '2026-11-07'"))
        assert md.registry_section(nov7, "vendors").count() == 1
