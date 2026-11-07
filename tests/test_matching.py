"""Tests for matching: the producer plants one integration issue per client, and
matching must find each one, while every honest reading passes.
"""

from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.iot import reference as ref
from src.iot.silver.matching import as_of_ranges, match_registry, quarantine_registry_mismatches
from src.iot.silver.normalize import normalize
from src.iot.silver.telemetry import (
    QUARANTINE_COLUMNS,
    check_contract,
    load_contract_schemas,
    parse_envelope,
)
from src.iot.silver.validity import check_validity
from src.producer.fleet import Fleet
from src.producer.registry import VENDORS, select_clients
from src.producer.vendors import Gateway

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 6, 18, 0, tzinfo=timezone.utc)
SNAPSHOT_VERSION = 20261107  # first registry snapshot is the day after the readings

EXPECTED_REASON = {
    "retired_device_still_transmitting": "inactive_device",
    "device_reports_wrong_vin": "device_vin_mismatch",
    "device_not_yet_registered": "unknown_device",
    "device_reports_wrong_account": "client_vendor_mismatch",
}


@pytest.fixture(scope="module")
def fleet():
    return Fleet(select_clients(1), seed=31, glitch_fraction=0)


@pytest.fixture(scope="module")
def registry(spark, fleet):
    """Registry tables as silver holds them: assignments with business dates,
    clients and drivers as SCD type 2 with their first version from the first snapshot."""
    export = fleet.registry_export(today=date(2026, 11, 7))

    assignments = spark.createDataFrame(
        [(a["device_id"], a["vendor_id"], a["vin"], a["vehicle_id"], a["client_id"],
          date.fromisoformat(a["valid_from"]), a["valid_to"] and date.fromisoformat(a["valid_to"]), a["status"])
         for a in export["device_vehicle_assignments"]],
        "device_id string, vendor_id string, vin string, vehicle_id string, client_id string, "
        "valid_from date, valid_to date, status string",
    )
    clients = spark.createDataFrame(
        [(c["client_id"], c["vendor_id"], SNAPSHOT_VERSION, None) for c in export["clients"]],
        "client_id string, vendor_id string, __START_AT int, __END_AT int",
    )
    drivers = spark.createDataFrame(
        [(d["driver_id"], d["client_id"], d["fob_id"], SNAPSHOT_VERSION, None) for d in export["drivers"]],
        "driver_id string, client_id string, fob_id string, __START_AT int, __END_AT int",
    )
    return assignments, clients, drivers


@pytest.fixture(scope="module")
def validated(spark, fleet):
    gateway = Gateway(random.Random(31))
    envelopes = []
    for tick in range(3):
        now = NOW + timedelta(seconds=30 * tick)
        fleet.advance(30, now=now)
        envelopes += [e for _, e in gateway.messages(fleet.readings(now=now, late_fraction=0), now)]
    envelopes += [e for _, e in gateway.flush(NOW)]

    rows = [(json.dumps(e), "KEY", 0, i, NOW + timedelta(minutes=2), NOW) for i, e in enumerate(envelopes)]
    bronze = spark.createDataFrame(
        rows,
        "raw_payload string, partition_key string, kafka_partition int, kafka_offset long, "
        "eventhub_enqueued_at timestamp, ingested_at timestamp",
    )
    accepted = ref.accepted_contracts(ROOT / "reference")
    checked = check_contract(
        parse_envelope(bronze, sorted(VENDORS)), load_contract_schemas(ROOT / "contracts", list(accepted)), accepted
    )
    readings = normalize(checked, ref.field_mappings_by_contract(ROOT / "reference"))
    return check_validity(readings, ref.validity_limits(ROOT / "reference")).cache()


@pytest.fixture(scope="module")
def matched(validated, registry):
    return match_registry(validated, *registry).cache()


def expected_reasons(fleet) -> dict[str, str]:
    """device_id -> the reason matching must give, for every device with a planted issue."""
    out = {}
    for v in fleet.vehicles:
        issue = fleet.integration_issues.get(v.vehicle_id)
        if issue and issue != "retired_device_still_transmitting":
            out[v.device_id] = EXPECTED_REASON[issue]
    for ghost in fleet.ghost_devices:
        out[ghost.device_id] = "inactive_device"
    return out


def honest_devices(fleet) -> list[str]:
    return [v.device_id for v in fleet.vehicles if v.vehicle_id not in fleet.integration_issues]


class TestPlantedIssues:
    def test_every_planted_issue_is_found(self, fleet, matched):
        expected = expected_reasons(fleet)
        found = {r["device_id"]: r["match_error"] for r in matched.filter("record_status = 'fail'").collect()}

        assert set(expected.values()) == set(EXPECTED_REASON.values())
        assert found == expected

    def test_honest_devices_all_match(self, fleet, matched):
        planted = set(expected_reasons(fleet))
        honest = matched.filter(~F.col("device_id").isin(list(planted)))

        assert honest.filter("record_status = 'fail'").count() == 0
        assert honest.filter("vehicle_id IS NULL OR client_id IS NULL").count() == 0

    def test_identity_comes_from_the_registry(self, fleet, matched):
        vehicle_of = {v.device_id: (v.vehicle_id, v.client.client_id) for v in fleet.vehicles}
        for r in matched.filter("record_status <> 'fail'").collect():
            assert (r["vehicle_id"], r["client_id"]) == vehicle_of[r["device_id"]]

    def test_failed_readings_carry_no_identity(self, matched):
        assert matched.filter("record_status = 'fail' AND vehicle_id IS NOT NULL").count() == 0

    def test_readings_before_the_first_snapshot_still_find_their_driver(self, fleet, matched):
        """Readings are from 11-06, the first driver snapshot is 11-07. Open start makes them match.
        Only temporary drivers, who are never registered, stay unmatched."""
        temp_fobs = {d.fob_id for d in fleet.temp_drivers}
        with_fob = matched.filter("record_status <> 'fail' AND driver_fob_id IS NOT NULL")
        unmatched = {r["driver_fob_id"] for r in with_fob.filter("driver_id IS NULL").collect()}

        assert with_fob.filter("driver_id IS NOT NULL").count() > 0
        assert unmatched <= temp_fobs

    def test_temporary_drivers_are_warnings(self, fleet, matched):
        temp_fobs = [d.fob_id for d in fleet.temp_drivers]
        statuses = {r["record_status"] for r in matched.filter(F.col("driver_fob_id").isin(temp_fobs)).collect()}
        assert statuses <= {"warn", "fail"}

    def test_details_explain_the_mismatch(self, matched):
        details = {r["match_error"]: r["match_detail"] for r in matched.filter("record_status = 'fail'").collect()}
        assert details["unknown_device"].endswith("is not in the registry")
        assert "has no assignment covering 2026-11-06" in details["inactive_device"]
        assert details["device_vin_mismatch"].startswith("reported VIN ")
        assert details["client_vendor_mismatch"].startswith("reported account ")


class TestDriverWarning:
    def test_unknown_fob_warns_but_keeps_the_reading(self, validated, registry, fleet):
        reading = validated.filter(F.col("device_id").isin(honest_devices(fleet))).limit(1)
        row = match_registry(reading.withColumn("driver_fob_id", F.lit("FOB-TEMP0001")), *registry).first()

        assert row["record_status"] == "warn"
        assert row["match_warning"] == "driver_unmatched"
        assert row["vehicle_id"] is not None
        assert row["driver_id"] is None

    def test_fob_of_another_clients_driver_does_not_match(self, validated, registry, fleet):
        reading = validated.filter(F.col("device_id").isin(honest_devices(fleet))).limit(1)
        own_client = reading.first()["reported_client_id"]
        foreign = next(d for d in fleet.drivers if d.client_id != own_client)

        row = match_registry(reading.withColumn("driver_fob_id", F.lit(foreign.fob_id)), *registry).first()
        assert row["match_warning"] == "driver_unmatched"

    def test_no_fob_is_not_a_warning(self, validated, registry, fleet):
        reading = validated.filter(F.col("device_id").isin(honest_devices(fleet))).limit(1)
        row = match_registry(reading.withColumn("driver_fob_id", F.lit(None).cast("string")), *registry).first()

        assert row["record_status"] == "pass"
        assert row["driver_id"] is None


class TestPointInTime:
    def test_device_swapped_last_month_matches_by_event_date(self, spark, registry, validated, fleet):
        """valid_from is inclusive, valid_to exclusive: on the swap day the new device is the one."""
        assignments, clients, drivers = registry
        device_id = honest_devices(fleet)[0]
        reading = validated.filter(F.col("device_id") == device_id).limit(1)
        event_day = reading.select(F.to_date("event_ts")).first()[0]

        moved = assignments.withColumn(
            "valid_from", F.when(F.col("device_id") == device_id, F.lit(event_day + timedelta(days=1)))
            .otherwise(F.col("valid_from"))
        )
        assert match_registry(reading, moved, clients, drivers).first()["match_error"] == "inactive_device"
        assert match_registry(reading, assignments, clients, drivers).first()["match_error"] is None

    def test_as_of_ranges_open_only_the_first_version(self, spark):
        history = spark.createDataFrame(
            [("D1", 20261107, 20261110), ("D1", 20261110, None), ("D2", 20261109, None)],
            "driver_id string, __START_AT int, __END_AT int",
        )
        rows = {(r["driver_id"], r["__START_AT"]): r for r in as_of_ranges(history, "driver_id").collect()}

        assert rows[("D1", 20261107)]["valid_from_version"] == 0
        assert rows[("D1", 20261110)]["valid_from_version"] == 20261110
        assert rows[("D2", 20261109)]["valid_from_version"] == 0


class TestReconciliationAndQuarantine:
    def test_every_validated_reading_comes_out_once(self, validated, matched):
        assert matched.count() == validated.filter("record_status = 'pass'").count()
        assert matched.select("measurement_id").distinct().count() == matched.count()

    def test_quarantine_shape(self, matched):
        quarantined = quarantine_registry_mismatches(matched)
        row = quarantined.first()

        assert quarantined.columns == QUARANTINE_COLUMNS
        assert row["quarantine_stage"] == "matching"
        assert row["measurement_id"] is not None
        assert row["raw_payload"] is None
        assert quarantined.count() == matched.filter("record_status = 'fail'").count()