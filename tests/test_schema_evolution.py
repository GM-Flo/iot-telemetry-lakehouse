"""FleetPulse contract v2: a vendor changes its format while old devices keep sending v1.

The point of these tests: the pipeline handles v2 with configuration only, a JSON
Schema and mapping rows, and no change to silver code. v1 and v2 messages flow
side by side and come out as the same standard readings.
"""

from __future__ import annotations

import csv
import json
import random
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pyspark.sql import functions as F

from src.iot import reference as ref
from src.iot.silver.normalize import STANDARD_FIELDS, normalize
from src.iot.silver.telemetry import check_contract, load_contract_schemas, parse_envelope
from src.producer.fleet import Fleet
from src.producer.registry import VENDORS, select_clients
from src.producer.vendors import FPL_V2_CONTRACT, Gateway, format_fpl, format_fpl_v2, is_upgraded

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 8, 18, 0, tzinfo=timezone.utc)


def schema(name):
    return Draft202012Validator(json.loads((ROOT / "contracts" / f"{name}.json").read_text()))


def produce(share, ticks=3, seed=12):
    fleet = Fleet(select_clients(1), seed=seed, glitch_fraction=0)
    gateway = Gateway(random.Random(seed), fpl_v2_share=share)
    readings, envelopes = [], []
    for tick in range(ticks):
        now = NOW + timedelta(seconds=30 * tick)
        fleet.advance(30, now=now)
        batch = fleet.readings(now=now, late_fraction=0)
        readings += batch
        envelopes += [e for _, e in gateway.messages(batch, now)]
    envelopes += [e for _, e in gateway.flush(NOW)]
    return fleet, readings, envelopes


def run_silver(spark, envelopes, reference_dir=ROOT / "reference"):
    rows = [(json.dumps(e), "KEY", 0, i, NOW, NOW) for i, e in enumerate(envelopes)]
    bronze = spark.createDataFrame(
        rows,
        "raw_payload string, partition_key string, kafka_partition int, kafka_offset long, "
        "eventhub_enqueued_at timestamp, ingested_at timestamp",
    )
    accepted = ref.accepted_contracts(reference_dir)
    checked = check_contract(
        parse_envelope(bronze, ref.known_vendors(reference_dir)),
        load_contract_schemas(ROOT / "contracts", list(accepted)),
        accepted,
    )
    return checked, normalize(checked, ref.field_mappings_by_contract(reference_dir))


class TestContract:
    def test_v2_payloads_conform_to_v2_and_not_to_v1(self):
        _, _, envelopes = produce(share=1.0)
        v2 = [e for e in envelopes if e["contract"] == FPL_V2_CONTRACT]
        assert v2
        for e in v2:
            schema(FPL_V2_CONTRACT).validate(e["payload"])
            assert not schema("fpl.telemetry.v1").is_valid(e["payload"])

    def test_the_new_field_is_declared_but_not_mapped(self):
        mapped = {m["source_path"] for m in ref.field_mappings_by_contract(ROOT / "reference")[FPL_V2_CONTRACT]}
        assert "ambientTempC" in json.loads((ROOT / "contracts" / f"{FPL_V2_CONTRACT}.json").read_text())["required"]
        assert "ambientTempC" not in mapped


class TestRollout:
    def test_a_device_stays_upgraded_and_a_larger_share_only_adds_devices(self):
        devices = [f"FPL-{i:06X}" for i in range(400)]
        half = {d for d in devices if is_upgraded(d, 0.5)}
        most = {d for d in devices if is_upgraded(d, 0.8)}
        assert half <= most
        assert 0.4 < len(half) / len(devices) < 0.6
        assert not any(is_upgraded(d, 0.0) for d in devices)
        assert all(is_upgraded(d, 1.0) for d in devices)

    def test_only_fleetpulse_changes_contract(self):
        _, _, envelopes = produce(share=1.0)
        for e in envelopes:
            expected = FPL_V2_CONTRACT if e["vendor_id"] == "fpl" else VENDORS[e["vendor_id"]].contract
            assert e["contract"] == expected


class TestSideBySide:
    def test_v1_and_v2_flow_together_and_all_pass(self, spark):
        fleet, readings, envelopes = produce(share=0.5, ticks=4)
        contracts = {e["contract"] for e in envelopes if e["vendor_id"] == "fpl"}
        assert contracts == {"fpl.telemetry.v1", FPL_V2_CONTRACT}

        checked, normalized = run_silver(spark, envelopes)
        assert checked.filter("record_status <> 'pass'").count() == 0
        assert normalized.count() == len(readings)

    def test_the_same_reading_in_v1_and_v2_normalizes_to_the_same_values(self, spark):
        fleet, readings, _ = produce(share=0.0)
        reading = next(r for r in readings if r["vendor_id"] == "fpl")
        gateway = Gateway(random.Random(1))
        as_v1 = gateway.envelope("fpl", format_fpl(reading), NOW)
        as_v2 = gateway.envelope("fpl", format_fpl_v2(reading), NOW, contract=FPL_V2_CONTRACT)

        _, normalized = run_silver(spark, [as_v1, as_v2])
        rows = [r.asDict() for r in normalized.withColumn("event_epoch", F.col("event_ts").cast("double"))
                .orderBy("contract").collect()]
        v1, v2 = rows

        assert int(v1["event_epoch"]) == int(v2["event_epoch"])
        for field in STANDARD_FIELDS:
            if field in ("event_ts", "sent_ts"):
                continue
            if isinstance(v1[field], float):
                # v1 rounds to imperial units on the way; v2 sends metric unrounded.
                assert v1[field] == pytest.approx(v2[field], abs=0.1), field
            else:
                assert v1[field] == v2[field], field

    def test_v2_values_survive_exactly(self, spark):
        fleet, readings, envelopes = produce(share=1.0)
        _, normalized = run_silver(spark, envelopes)
        by_device = {
            (r["device_id"], round(r["event_epoch"], 3)): r
            for r in normalized.filter(F.col("contract") == FPL_V2_CONTRACT)
            .withColumn("event_epoch", F.col("event_ts").cast("double")).collect()
        }
        originals = [r for r in readings if r["vendor_id"] == "fpl"]
        assert originals
        for original in originals:
            row = by_device[(original["device_id"], round(original["event_ts"].timestamp(), 3))]
            assert row["speed_kph"] == original["speed_kph"]
            assert row["engine_temperature_celsius"] == original["engine_temp_c"]
            assert row["odometer_km"] == original["odometer_km"]
            assert row["ignition_on"] == original["ignition_on"]


class TestRetiringV1:
    def test_a_device_still_on_v1_is_quarantined_once_v1_is_retired(self, spark, tmp_path):
        """The truck that missed the firmware update: its messages are refused, with a reason
        the vendor can act on, while upgraded devices carry on."""
        reference_dir = tmp_path / "reference"
        shutil.copytree(ROOT / "reference", reference_dir)
        versions = reference_dir / "vendor_contract_versions.csv"
        rows = list(csv.DictReader(versions.open()))
        for r in rows:
            if r["contract"] == "fpl.telemetry.v1":
                r["status"] = "retired"
        with versions.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

        _, _, envelopes = produce(share=0.5, ticks=4)
        checked, _ = run_silver(spark, envelopes, reference_dir)
        fpl = {r["contract"]: r for r in checked.filter(F.col("vendor_id") == "fpl").collect()}

        assert fpl["fpl.telemetry.v1"]["contract_error"] == "unsupported_contract"
        assert fpl["fpl.telemetry.v1"]["contract_detail"] == "vendor fpl sent contract fpl.telemetry.v1"
        assert fpl[FPL_V2_CONTRACT]["record_status"] == "pass"
