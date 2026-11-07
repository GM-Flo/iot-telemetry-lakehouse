"""Tests for normalize: real producer messages in, standard readings out.

The strongest check is the round trip: the producer knows each reading's true
values before a vendor format disguises them, so normalize must give them back.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.iot import reference as ref
from src.iot.silver.normalize import (
    CONVERSIONS,
    STANDARD_FIELDS,
    normalize,
    payload_schema,
    validate_mappings,
)
from src.iot.silver.telemetry import check_contract, load_contract_schemas, parse_envelope
from src.producer.fleet import Fleet
from src.producer.registry import VENDORS, select_clients
from src.producer.vendors import Gateway

ROOT = Path(__file__).resolve().parents[1]
ACCEPTED = ref.accepted_contracts(ROOT / "reference")
SCHEMAS = load_contract_schemas(ROOT / "contracts", list(ACCEPTED))
MAPPINGS = ref.field_mappings_by_contract(ROOT / "reference")
NOW = datetime(2026, 11, 6, 18, 0, tzinfo=timezone.utc)

# Vendor formats round values, so a converted value can differ slightly from the original.
TOLERANCE = {
    "speed_kph": 0.1,
    "engine_temperature_celsius": 0.1,
    "coolant_pressure_kpa": 0.1,
    "oil_pressure_kpa": 0.1,
    "odometer_km": 0.01,
}


@pytest.fixture(scope="module")
def produced():
    """(original readings, envelopes) from a few ticks of the dev fleet, all four vendors."""
    fleet = Fleet(select_clients(1), seed=11, glitch_fraction=0)
    gateway = Gateway(random.Random(11))
    readings, envelopes = [], []
    for tick in range(4):
        now = NOW + timedelta(seconds=30 * tick)
        fleet.advance(30, now=now)
        batch = fleet.readings(now=now, late_fraction=0)
        readings += batch
        envelopes += [e for _, e in gateway.messages(batch, now)]
    envelopes += [e for _, e in gateway.flush(NOW)]
    return readings, envelopes


@pytest.fixture
def normalized_of(spark):
    def _build(envelopes):
        rows = [(json.dumps(e), "KEY", 0, i, NOW, NOW) for i, e in enumerate(envelopes)]
        bronze = spark.createDataFrame(
            rows,
            "raw_payload string, partition_key string, kafka_partition int, kafka_offset long, "
            "eventhub_enqueued_at timestamp, ingested_at timestamp",
        )
        checked = check_contract(parse_envelope(bronze, sorted(VENDORS)), SCHEMAS, ACCEPTED)
        return normalize(checked, MAPPINGS)

    return _build


def epoch_rows(df):
    """Collect with timestamps as epoch seconds, so the local machine's time zone cannot shift them."""
    return [
        r.asDict()
        for r in df.withColumn("event_epoch", F.col("event_ts").cast("double"))
        .withColumn("sent_epoch", F.col("sent_ts").cast("double"))
        .collect()
    ]


class TestRoundTrip:
    def test_every_reading_comes_back_once(self, produced, normalized_of):
        readings, envelopes = produced
        rows = epoch_rows(normalized_of(envelopes))

        assert len(rows) == len(readings)
        assert {r["vendor_id"] for r in rows} == set(VENDORS)

    def test_values_survive_every_vendor_format(self, produced, normalized_of):
        readings, envelopes = produced
        by_key = {(r["device_id"], int(r["event_epoch"])): r for r in epoch_rows(normalized_of(envelopes))}

        source_names = {
            "reported_vin": "vin", "reported_client_id": "client_id", "engine_temperature_celsius": "engine_temp_c",
            "fuel_level_percent": "fuel_level_pct", "heading_degrees": "heading_deg", "fault_code": "dtc_code",
        }
        for original in readings:
            row = by_key[(original["device_id"], int(original["event_ts"].timestamp()))]
            for field in STANDARD_FIELDS:
                if field in ("event_ts", "sent_ts"):
                    continue
                expected = original[source_names.get(field, field)]
                if field in TOLERANCE:
                    assert row[field] == pytest.approx(expected, abs=TOLERANCE[field]), (row["vendor_id"], field)
                else:
                    assert row[field] == expected, (row["vendor_id"], field)

    def test_timestamps_are_utc(self, produced, normalized_of):
        readings, envelopes = produced
        rows = epoch_rows(normalized_of(envelopes))
        expected = {round(r["event_ts"].timestamp()) for r in readings}

        assert {round(r["event_epoch"]) for r in rows} == expected


class TestAutoSenseBatches:
    def test_one_message_becomes_one_row_per_reading(self, produced, normalized_of):
        _, envelopes = produced
        batch = next(e for e in envelopes if e["vendor_id"] == "ats" and len(e["payload"]["readings"]) > 1)
        rows = normalized_of([batch]).orderBy("reading_index").collect()

        assert [r["reading_index"] for r in rows] == list(range(len(batch["payload"]["readings"])))
        assert {r["message_id"] for r in rows} == {batch["message_id"]}
        assert {r["device_id"] for r in rows} == {batch["payload"]["dev"]}

    def test_other_vendors_have_reading_index_zero(self, produced, normalized_of):
        _, envelopes = produced
        rows = normalized_of([e for e in envelopes if e["vendor_id"] != "ats"]).collect()
        assert {r["reading_index"] for r in rows} == {0}


class TestMeasurementId:
    def test_unique_per_reading(self, produced, normalized_of):
        _, envelopes = produced
        rows = normalized_of(envelopes).collect()
        assert len({r["measurement_id"] for r in rows}) == len(rows)

    def test_same_message_gives_same_ids(self, produced, normalized_of):
        """A replayed message must not look like new data."""
        _, envelopes = produced
        first = sorted(r["measurement_id"] for r in normalized_of(envelopes[:10]).collect())
        again = sorted(r["measurement_id"] for r in normalized_of(envelopes[:10]).collect())
        assert first == again


class TestConversions:
    @pytest.mark.parametrize("name, text, expected", [
        ("none", "abc", "abc"),
        ("mph_to_kph", "62.1", 99.94),
        ("fahrenheit_to_celsius", "212", 100.0),
        ("psi_to_kpa", "14.5", 100.0),
        ("miles_to_km", "100", 160.934),
        ("on_off_to_boolean", "ON", True),
        ("on_off_to_boolean", "off", False),
        ("on_off_to_boolean", "maybe", None),
        ("one_zero_to_boolean", "1", True),
        ("one_zero_to_boolean", "0", False),
        ("first_array_element", '["P0128", "P0300"]', "P0128"),
        ("first_array_element", "[]", None),
    ])
    def test_value_conversions(self, spark, name, text, expected):
        value = spark.createDataFrame([(text,)], "v string").select(CONVERSIONS[name](F.col("v"))).first()[0]
        assert value == expected

    @pytest.mark.parametrize("name, text", [
        ("iso_timestamp", "2026-11-06T18:00:00+00:00"),
        ("iso_timestamp", "2026-11-06T13:00:00-05:00"),
        ("epoch_milliseconds", "1793988000000"),
        ("epoch_seconds", "1793988000.0"),
        ("naive_utc_timestamp", "2026-11-06 18:00:00"),
    ])
    def test_timestamp_conversions_all_mean_the_same_instant(self, spark, name, text):
        value = spark.createDataFrame([(text,)], "v string").select(CONVERSIONS[name](F.col("v")).cast("double")).first()[0]
        assert value == NOW.timestamp()

    def test_every_conversion_in_the_csv_exists(self):
        used = {m["conversion"] for mappings in MAPPINGS.values() for m in mappings}
        assert used <= set(CONVERSIONS)


class TestPayloadSchema:
    def test_nested_paths_become_nested_structs(self):
        schema = payload_schema(["engine.rpm", "engine.on", "dtc"])
        assert schema.simpleString() == "struct<engine:struct<rpm:string,on:string>,dtc:string>"

    def test_array_paths_become_an_array_of_structs(self):
        schema = payload_schema(["dev", "readings[].spd", "readings[].t"])
        assert schema.simpleString() == "struct<dev:string,readings:array<struct<spd:string,t:string>>>"


class TestValidateMappings:
    def test_real_mappings_are_valid(self):
        for contract, mappings in MAPPINGS.items():
            validate_mappings(contract, mappings)

    def test_unknown_conversion_stops_the_pipeline(self):
        broken = [dict(m) for m in MAPPINGS["tmx.telemetry.v1"]]
        broken[0]["conversion"] = "value * 1.609"
        with pytest.raises(ValueError, match="unknown conversions"):
            validate_mappings("tmx.telemetry.v1", broken)

    def test_missing_field_stops_the_pipeline(self):
        broken = [m for m in MAPPINGS["tmx.telemetry.v1"] if m["target_field"] != "speed_kph"]
        with pytest.raises(ValueError, match="speed_kph"):
            validate_mappings("tmx.telemetry.v1", broken)

    def test_contract_without_mappings_stops_the_pipeline(self):
        with pytest.raises(ValueError, match="missing target fields"):
            validate_mappings("tmx.telemetry.v2", [])