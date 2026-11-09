"""The whole silver flow on realistic producer data, checked by the reconciliation.

Two hours of the dev fleet with everything the producer does wrong on purpose:
corrupted messages, sensor glitches, clock-behind readings, planted registry
faults, half of FleetPulse on contract v2, and GeoTrak's second-precision
timestamps that make two different readings collide. Every stage runs as in the
pipeline, with latest_per_key standing in for AUTO CDC. Nothing may be lost and
nothing invented.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.iot import reference as ref
from src.iot.operations import silver_reconciliation
from src.iot.silver import matching, measurements, normalize, telemetry, validity
from src.producer.fleet import Fleet
from src.producer.produce import build_items
from src.producer.registry import select_clients
from src.producer.vendors import Gateway

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "reference"
START = datetime(2026, 11, 8, 14, 0, tzinfo=timezone.utc)  # 08:00 local: a working day
SNAPSHOT_VERSION = 20261107


@pytest.fixture(scope="module")
def silver(spark):
    fleet = Fleet(select_clients(1), seed=17, glitch_fraction=0.01)
    gateway = Gateway(random.Random(17), fpl_v2_share=0.5)
    rng = random.Random(17)
    bronze_rows = []
    for tick in range(int(2 * 3600 / 15)):
        now = START + timedelta(seconds=15 * tick, microseconds=rng.randint(0, 999_999))
        fleet.advance(15, now=now)
        items = build_items(fleet, gateway, now, late_fraction=0.05, malformed_fraction=0.01, rng=rng)
        bronze_rows += [(body, key, 0, len(bronze_rows) + i, now, now) for i, (key, body) in enumerate(items)]
    bronze = spark.createDataFrame(
        bronze_rows,
        "raw_payload string, partition_key string, kafka_partition int, kafka_offset long, "
        "eventhub_enqueued_at timestamp, ingested_at timestamp",
    ).cache()

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

    accepted = ref.accepted_contracts(REFERENCE)
    schemas = telemetry.load_contract_schemas(ROOT / "contracts", list(accepted))
    mappings = ref.field_mappings_by_contract(REFERENCE)

    parsed = telemetry.parse_envelope(bronze, ref.known_vendors(REFERENCE))
    checked = telemetry.check_contract(parsed, schemas, accepted)
    readings = normalize.normalize(checked, mappings)
    validated = validity.check_validity(readings, ref.validity_limits(REFERENCE))
    matched = matching.match_registry(validated, assignments, clients, drivers).cache()

    tables = {
        "bronze": bronze,
        "unparseable": telemetry.quarantine_unparseable(parsed),
        "contract": telemetry.quarantine_contract_violations(checked),
        "invalid": validity.quarantine_invalid_values(validated),
        "mismatched": matching.quarantine_registry_mismatches(matched),
        "stored": measurements.latest_per_key(measurements.to_store(matched)),
        "expected": normalize.normalize(
            telemetry.check_contract(telemetry.parse_envelope(bronze, ref.known_vendors(REFERENCE)), schemas, accepted),
            mappings,
        ),
    }
    result = silver_reconciliation(
        tables["bronze"], tables["expected"], tables["unparseable"], tables["contract"],
        tables["stored"], tables["invalid"], tables["mismatched"],
        content_columns=list(normalize.STANDARD_FIELDS),
    ).first()
    yield tables, result

    # Two hours of fleet data is large for a local test session: release it so the
    # test files that run after this one have their memory back.
    bronze.unpersist()
    matched.unpersist()
    spark.catalog.clearCache()


def test_nothing_is_lost(silver):
    _, result = silver
    assert result["lost_readings"] == 0


def test_nothing_is_invented(silver):
    _, result = silver
    assert result["unexpected_readings"] == 0


def test_every_kind_of_fault_was_present(silver):
    """The test only means something if the data really was messy."""
    tables, result = silver
    assert result["unparseable_messages"] > 0
    assert result["invalid_readings"] > 0
    assert result["mismatched_readings"] > 0
    assert {r["contract"] for r in tables["stored"].select("contract").distinct().collect()} >= {
        "fpl.telemetry.v1", "fpl.telemetry.v2"}


def test_geotrak_collisions_are_counted_as_conflicts(silver):
    """Second-precision timestamps plus clock-behind readings: two different readings,
    one identity. They are reported, not hidden."""
    tables, result = silver
    assert result["duplicate_readings"] > 0
    assert result["conflicting_duplicates"] == result["duplicate_readings"]
    collided = (
        tables["expected"].groupBy("device_id", "event_ts").count().filter("count > 1")
        .select("device_id").distinct().collect()
    )
    assert {r["device_id"].split("-")[0] for r in collided} == {"GTK"}


def test_stored_readings_are_unique_by_identity(silver):
    tables, _ = silver
    stored = tables["stored"]
    assert stored.count() == stored.select("device_id", "event_ts").distinct().count()


def test_quarantined_readings_say_which_device_and_when(silver):
    tables, _ = silver
    for name in ("invalid", "mismatched"):
        rows = tables[name]
        assert rows.filter("device_id IS NULL OR event_ts IS NULL").count() == rows.filter(
            "quarantine_reason = 'missing_required_value'").count()