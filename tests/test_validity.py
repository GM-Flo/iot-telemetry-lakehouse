"""Tests for validity: impossible values fail, alarming but real values pass."""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.iot import reference as ref
from src.iot.silver.normalize import STANDARD_FIELDS, normalize
from src.iot.silver.telemetry import (
    QUARANTINE_COLUMNS,
    check_contract,
    load_contract_schemas,
    parse_envelope,
)
from src.iot.silver.validity import check_validity, quarantine_invalid_values
from src.producer.fleet import Fleet
from src.producer.registry import VENDORS, select_clients
from src.producer.vendors import Gateway

ROOT = Path(__file__).resolve().parents[1]
LIMITS = ref.validity_limits(ROOT / "reference")
NOW = datetime(2026, 11, 6, 18, 0, tzinfo=timezone.utc)

GOOD = {
    "measurement_id": "m-1", "message_id": "msg-1", "reading_index": 0, "vendor_id": "tmx",
    "kafka_partition": 0, "kafka_offset": 7, "eventhub_enqueued_at": NOW + timedelta(seconds=5), "ingested_at": NOW,
    "device_id": "TMX-000001", "reported_vin": "1HGBH41JXMN109186", "reported_client_id": "C001",
    "driver_fob_id": "FOB-00000001", "event_ts": NOW, "sent_ts": NOW + timedelta(seconds=1), "ignition_on": True,
    "speed_kph": 60.0, "engine_rpm": 2000, "engine_temperature_celsius": 90.0, "coolant_pressure_kpa": 100.0,
    "oil_pressure_kpa": 300.0, "battery_voltage": 14.0, "fuel_level_percent": 50.0, "odometer_km": 10000.0,
    "latitude": 29.8, "longitude": -95.5, "heading_degrees": 90.0, "fault_code": None,
    "harsh_brake_count": 0, "harsh_accel_count": 0,
}
SCHEMA = (
    "measurement_id string, message_id string, reading_index int, vendor_id string, kafka_partition int, "
    "kafka_offset long, eventhub_enqueued_at timestamp, ingested_at timestamp, "
    + ", ".join(f"{name} {dtype}" for name, dtype in STANDARD_FIELDS.items())
)


@pytest.fixture
def validate(spark):
    def _run(**changes):
        row = {**GOOD, **changes}
        columns = [c.split()[0] for c in SCHEMA.split(", ")]
        df = spark.createDataFrame([tuple(row[c] for c in columns)], SCHEMA)
        return check_validity(df, LIMITS).first()

    return _run


class TestRules:
    def test_a_good_reading_passes(self, validate):
        row = validate()
        assert row["record_status"] == "pass"
        assert row["validity_error"] is None and row["validity_detail"] is None

    def test_overheating_is_real_and_passes(self, validate):
        """120 °C is above the critical anomaly threshold, and exactly what customers pay to see."""
        assert validate(engine_temperature_celsius=120.0)["record_status"] == "pass"

    @pytest.mark.parametrize("changes, reason", [
        ({"device_id": None}, "missing_required_value"),
        ({"event_ts": None}, "missing_required_value"),
        ({"latitude": 0.0, "longitude": 0.0}, "gps_null_island"),
        ({"latitude": 95.0}, "coordinates_out_of_range"),
        ({"longitude": -181.0}, "coordinates_out_of_range"),
        ({"event_ts": NOW + timedelta(hours=3)}, "event_in_future"),
        ({"engine_temperature_celsius": 999.9}, "sensor_out_of_range"),
        ({"speed_kph": -5.0}, "sensor_out_of_range"),
        ({"fuel_level_percent": 101.0}, "sensor_out_of_range"),
    ])
    def test_impossible_values_fail(self, validate, changes, reason):
        row = validate(**changes)
        assert row["record_status"] == "fail"
        assert row["validity_error"] == reason

    def test_detail_names_the_value_and_the_limit(self, validate):
        assert validate(engine_temperature_celsius=999.9)["validity_detail"] == (
            "engine_temperature_celsius=999.9 outside [-40, 200]"
        )

    def test_first_reason_wins_and_detail_lists_all(self, validate):
        row = validate(latitude=0.0, longitude=0.0, speed_kph=900.0)
        assert row["validity_error"] == "gps_null_island"
        assert "latitude and longitude are both 0" in row["validity_detail"]
        assert "speed_kph=900.0" in row["validity_detail"]

    def test_future_detail_shows_readable_timestamps(self, validate):
        detail = validate(event_ts=NOW + timedelta(hours=3))["validity_detail"]
        assert detail == "event_ts 2026-11-06 21:00:00 is after it was received at 2026-11-06 18:00:01"

    def test_small_clock_skew_is_tolerated(self, validate):
        assert validate(event_ts=NOW + timedelta(minutes=3))["record_status"] == "pass"

    def test_replayed_old_data_is_not_in_the_future(self, validate):
        """A week-old reading processed today is late, not invalid."""
        old = NOW - timedelta(days=7)
        row = validate(event_ts=old, sent_ts=old, eventhub_enqueued_at=old + timedelta(seconds=5))
        assert row["record_status"] == "pass"

    def test_missing_sensor_value_is_not_out_of_range(self, validate):
        assert validate(battery_voltage=None)["record_status"] == "pass"

    def test_limit_for_an_unknown_field_stops_the_pipeline(self, spark):
        df = spark.createDataFrame([(1.0,)], "speed_kph double")
        with pytest.raises(ValueError, match="no_such_field"):
            check_validity(df, {"no_such_field": (0.0, 1.0)})


def readings_from_fleet(spark, glitch_fraction):
    fleet = Fleet(select_clients(1), seed=21, glitch_fraction=glitch_fraction)
    gateway = Gateway(random.Random(21))
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
        parse_envelope(bronze, sorted(VENDORS)),
        load_contract_schemas(ROOT / "contracts", list(accepted)),
        accepted,
    )
    return normalize(checked, ref.field_mappings_by_contract(ROOT / "reference"))


class TestWithProducerData:
    def test_a_healthy_fleet_passes_completely(self, spark):
        validated = check_validity(readings_from_fleet(spark, glitch_fraction=0), LIMITS)
        failures = validated.filter("record_status = 'fail'").select("validity_detail").collect()
        assert failures == []

    def test_every_producer_glitch_is_caught(self, spark):
        validated = check_validity(readings_from_fleet(spark, glitch_fraction=1), LIMITS)
        reasons = {r["validity_error"] for r in validated.collect()}

        assert None not in reasons
        assert reasons == {"gps_null_island", "event_in_future", "sensor_out_of_range"}


class TestQuarantineInvalidValues:
    def test_only_the_bad_reading_is_quarantined(self, spark):
        validated = check_validity(readings_from_fleet(spark, glitch_fraction=0), LIMITS)
        total = validated.count()
        one_bad = validated.limit(1).select("measurement_id").first()[0]

        broken = readings_from_fleet(spark, glitch_fraction=0).withColumn(
            "engine_temperature_celsius",
            F.when(F.col("measurement_id") == one_bad, F.lit(999.9)).otherwise(F.col("engine_temperature_celsius")),
        )
        rechecked = check_validity(broken, LIMITS)
        quarantined = quarantine_invalid_values(rechecked).collect()

        assert [r["measurement_id"] for r in quarantined] == [one_bad]
        assert rechecked.filter("record_status = 'pass'").count() == total - 1

    def test_quarantine_row_shape(self, spark):
        df = spark.createDataFrame(
            [tuple({**GOOD, "speed_kph": 900.0}[c.split()[0]] for c in SCHEMA.split(", "))], SCHEMA
        )
        quarantined = quarantine_invalid_values(check_validity(df, LIMITS))
        row = quarantined.first()

        assert quarantined.columns == QUARANTINE_COLUMNS
        assert row["quarantine_stage"] == "validity"
        assert row["measurement_id"] == "m-1"
        assert row["reading_index"] == 0
        assert row["raw_payload"] is None
        assert row["kafka_offset"] == 7