"""Tests for the medallion transformations.

Fixtures are small and hand-built. Each test asserts one behaviour, including
the paths that are easy to get wrong and rarely tested: quarantine routing,
late-arrival handling, and the boundary conditions on anomaly thresholds.
"""

from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from src.iot import transforms as T
from tests.conftest import reading


class TestParseAndQuarantine:
    def test_well_formed_payload_parses(self, make_bronze):
        df = T.parse_payload(make_bronze([reading()]))
        row = df.collect()[0]

        assert row["_parse_error"] is None
        assert row["_parsed"]["vin"] == "1HGBH41JXMN109186"

    def test_truncated_json_is_quarantined(self, make_bronze):
        truncated = json.dumps(reading())[:40]
        clean, bad = T.split_quarantine(T.parse_payload(make_bronze([truncated])))

        assert clean.count() == 0
        assert bad.count() == 1
        assert bad.collect()[0]["quarantine_reason"] == "malformed_json"

    def test_non_json_payload_is_quarantined(self, make_bronze):
        clean, bad = T.split_quarantine(
            T.parse_payload(make_bronze(["vin=ABC;temp=94"]))
        )

        assert clean.count() == 0
        assert bad.count() == 1

    def test_missing_vin_is_quarantined_with_reason(self, make_bronze):
        payload = reading()
        del payload["vin"]
        _, bad = T.split_quarantine(T.parse_payload(make_bronze([payload])))

        assert bad.collect()[0]["quarantine_reason"] == "missing_vin"

    def test_uncastable_numeric_is_quarantined_not_nulled(self, make_bronze):
        """The important case: valid JSON whose numeric field is a word.

        A naive cast would null the column and let the row through, which
        silently corrupts every downstream average.
        """
        payload = reading()
        payload["engine_temp_c"] = "ninety-four"
        clean, bad = T.split_quarantine(T.parse_payload(make_bronze([payload])))

        assert clean.count() == 0
        assert bad.collect()[0]["quarantine_reason"] == "uncastable_engine_temp"

    def test_quarantine_preserves_original_payload_for_replay(self, make_bronze):
        broken = "not json at all"
        _, bad = T.split_quarantine(T.parse_payload(make_bronze([broken])))
        row = bad.collect()[0]

        assert row["raw_payload"] == broken
        assert row["kafka_offset"] is not None
        assert row["_quarantined_at"] is not None

    def test_mixed_batch_splits_both_ways(self, make_bronze):
        payloads = [reading(), "garbage", reading(vin="2T1BURHE0JC014214"), "{"]
        clean, bad = T.split_quarantine(T.parse_payload(make_bronze(payloads)))

        assert clean.count() == 2
        assert bad.count() == 2

    def test_null_payload_is_quarantined(self, make_bronze):
        clean, bad = T.split_quarantine(T.parse_payload(make_bronze([None])))

        assert clean.count() == 0
        assert bad.collect()[0]["quarantine_reason"] == "null_payload"


class TestSilver:
    def test_numeric_fields_are_cast(self, silver_of):
        row = silver_of([reading(engine_temp_c=97.5, engine_rpm=3100)]).collect()[0]

        assert isinstance(row["engine_temp_c"], float)
        assert row["engine_temp_c"] == pytest.approx(97.5)
        assert row["engine_rpm"] == 3100

    def test_event_ts_parsed_and_event_date_derived(self, silver_of):
        row = silver_of([reading()]).collect()[0]

        assert row["event_ts"] is not None
        assert str(row["event_date"]) == "2026-11-04"

    def test_late_arrival_is_flagged(self, silver_of):
        """An event that reached the hub 10 minutes after it occurred is late."""
        row = silver_of([reading(offset_seconds=-600)], ingested_offset_seconds=0).collect()[0]

        assert row["is_late_arrival"] is True
        assert row["event_lag_seconds"] >= 600

    def test_lateness_is_replay_invariant(self, silver_of):
        """Replaying old events much later must not mark them all late.

        Lateness is a property of the producer, not of when the backfill ran.
        Measuring it against processing time instead would flag every row of a
        historical replay.
        """
        rows = silver_of(
            [reading(offset_seconds=0), reading(offset_seconds=30)],
            ingested_offset_seconds=30,
            processed_offset_seconds=30 + 6 * 3600,  # replayed six hours later
        ).collect()

        assert all(r["is_late_arrival"] is False for r in rows)
        assert all(r["ingest_lag_seconds"] >= 6 * 3600 for r in rows)

    def test_emitted_at_used_when_broker_metadata_missing(self, silver_of):
        """Falls back to the producer's own transmission time.

        A file replay or a source without broker metadata has no enqueue
        timestamp, and lateness must still be computable.
        """
        row = silver_of(
            [reading(offset_seconds=-600, emitted_offset_seconds=0)],
            ingested_offset_seconds=None,
        ).collect()[0]

        assert row["eh_enqueued_at"] is None
        assert row["event_lag_seconds"] == 600
        assert row["is_late_arrival"] is True

    def test_prompt_arrival_is_not_flagged_late(self, silver_of):
        row = silver_of([reading(offset_seconds=0)], ingested_offset_seconds=5).collect()[0]

        assert row["is_late_arrival"] is False

    def test_implausible_temperature_is_dropped(self, silver_of):
        assert silver_of([reading(engine_temp_c=9999.0)]).count() == 0

    def test_hot_but_plausible_temperature_is_kept(self, silver_of):
        """A genuinely overheating engine must survive into gold.

        Dropping it as 'bad data' would delete the signal the project exists
        to detect.
        """
        df = silver_of([reading(engine_temp_c=T.ENGINE_TEMP_CRIT_C + 5)])

        assert df.count() == 1
        assert df.collect()[0]["is_critical_temp"] is True

    def test_negative_rpm_is_dropped(self, silver_of):
        assert silver_of([reading(engine_rpm=-5)]).count() == 0


class TestAnomalyFlags:
    def test_nominal_reading_scores_zero(self, silver_of):
        row = silver_of([reading()]).collect()[0]

        assert row["anomaly_score"] == 0
        assert row["is_overheating"] is False

    def test_warn_threshold_is_exclusive(self, silver_of):
        """Exactly at the threshold is not yet an anomaly."""
        at = silver_of([reading(engine_temp_c=T.ENGINE_TEMP_WARN_C)]).collect()[0]
        above = silver_of([reading(engine_temp_c=T.ENGINE_TEMP_WARN_C + 0.1)]).collect()[0]

        assert at["is_overheating"] is False
        assert above["is_overheating"] is True

    def test_critical_temp_outranks_warning(self, silver_of):
        warn = silver_of([reading(engine_temp_c=T.ENGINE_TEMP_WARN_C + 1)]).collect()[0]
        crit = silver_of([reading(engine_temp_c=T.ENGINE_TEMP_CRIT_C + 1)]).collect()[0]

        assert crit["anomaly_score"] > warn["anomaly_score"]
        assert crit["is_critical_temp"] is True

    def test_pressure_flags_suppressed_when_engine_off(self, silver_of):
        """Oil pressure is zero with the ignition off. That is not a fault."""
        row = silver_of([
            reading(ignition_on=False, oil_pressure_kpa=0.0, coolant_pressure_kpa=0.0)
        ]).collect()[0]

        assert row["is_oil_pressure_low"] is False
        assert row["is_coolant_pressure_low"] is False

    def test_low_oil_pressure_flagged_when_running(self, silver_of):
        row = silver_of([
            reading(ignition_on=True, oil_pressure_kpa=T.OIL_PRESSURE_MIN_KPA - 10)
        ]).collect()[0]

        assert row["is_oil_pressure_low"] is True

    def test_dtc_presence_contributes_to_score(self, silver_of):
        without = silver_of([reading()]).collect()[0]
        with_dtc = silver_of([reading(dtc_code="P0420")]).collect()[0]

        assert with_dtc["has_dtc"] is True
        assert with_dtc["anomaly_score"] > without["anomaly_score"]

    def test_score_is_capped_at_100(self, silver_of):
        row = silver_of([
            reading(
                engine_temp_c=T.ENGINE_TEMP_CRIT_C + 5,
                oil_pressure_kpa=10.0,
                coolant_pressure_kpa=10.0,
                battery_voltage=10.0,
                engine_rpm=T.RPM_REDLINE + 500,
                dtc_code="P0128",
            )
        ]).collect()[0]

        assert row["anomaly_score"] == 100


class TestWindowedAggregates:
    def test_readings_group_into_five_minute_windows(self, silver_of):
        payloads = [reading(offset_seconds=s) for s in (0, 60, 120, 400, 460)]
        gold = T.windowed_telemetry(silver_of(payloads)).collect()

        assert len(gold) == 2
        assert sorted(r["reading_count"] for r in gold) == [2, 3]

    def test_window_metrics_are_correct(self, silver_of):
        payloads = [
            reading(offset_seconds=0, engine_temp_c=90.0),
            reading(offset_seconds=60, engine_temp_c=100.0),
            reading(offset_seconds=120, engine_temp_c=110.0),
        ]
        row = T.windowed_telemetry(silver_of(payloads)).collect()[0]

        assert row["reading_count"] == 3
        assert row["avg_engine_temp_c"] == pytest.approx(100.0)
        assert row["max_engine_temp_c"] == pytest.approx(110.0)
        assert row["min_engine_temp_c"] == pytest.approx(90.0)

    def test_distance_derived_from_odometer_delta(self, silver_of):
        payloads = [
            reading(offset_seconds=0, odometer_km=100_000.0),
            reading(offset_seconds=120, odometer_km=100_004.5),
        ]
        row = T.windowed_telemetry(silver_of(payloads)).collect()[0]

        assert row["distance_km"] == pytest.approx(4.5)

    def test_vehicles_aggregate_independently(self, silver_of):
        payloads = [
            reading(vin="AAA", vehicle_id="VEH-0001", engine_temp_c=90.0),
            reading(vin="BBB", vehicle_id="VEH-0002", engine_temp_c=120.0),
        ]
        gold = T.windowed_telemetry(silver_of(payloads))

        assert gold.count() == 2
        hot = gold.filter(F.col("vehicle_id") == "VEH-0002").collect()[0]
        assert hot["max_engine_temp_c"] == pytest.approx(120.0)

    def test_overheating_rate_is_a_fraction_of_readings(self, silver_of):
        payloads = [
            reading(offset_seconds=0, engine_temp_c=90.0),
            reading(offset_seconds=30, engine_temp_c=90.0),
            reading(offset_seconds=60, engine_temp_c=90.0),
            reading(offset_seconds=90, engine_temp_c=T.ENGINE_TEMP_WARN_C + 5),
        ]
        row = T.windowed_telemetry(silver_of(payloads)).collect()[0]

        assert row["overheating_readings"] == 1
        assert row["overheating_rate"] == pytest.approx(0.25)

    def test_late_event_folds_into_its_own_window(self, silver_of):
        """A 10-minute-late event belongs to the window of its event time.

        Assigning it to processing time instead is the classic streaming bug,
        and it would quietly inflate the current window.
        """
        payloads = [
            reading(offset_seconds=0),
            reading(offset_seconds=30),
            reading(offset_seconds=-600),
        ]
        gold = T.windowed_telemetry(silver_of(payloads)).collect()
        by_start = {str(r["window_start"]): r["reading_count"] for r in gold}

        assert len(gold) == 2
        assert sorted(by_start.values()) == [1, 2]


class TestVehicleDimension:
    def test_one_row_per_vin(self, silver_of):
        payloads = [
            reading(vin="AAA", offset_seconds=0),
            reading(vin="AAA", offset_seconds=60),
            reading(vin="BBB", offset_seconds=0),
        ]
        dim = T.vehicle_dimension(silver_of(payloads))

        assert dim.count() == 2

    def test_latest_attributes_win(self, silver_of):
        payloads = [
            reading(vin="AAA", offset_seconds=0, fleet_region="houston-north"),
            reading(vin="AAA", offset_seconds=600, fleet_region="austin"),
        ]
        row = T.vehicle_dimension(silver_of(payloads)).collect()[0]

        assert row["fleet_region"] == "austin"

    def test_attribute_hash_detects_change(self, silver_of):
        a = T.vehicle_dimension(
            silver_of([reading(vin="AAA", fleet_region="houston-north")])
        ).collect()[0]
        b = T.vehicle_dimension(
            silver_of([reading(vin="AAA", fleet_region="austin")])
        ).collect()[0]

        assert a["attribute_hash"] != b["attribute_hash"]

    def test_attribute_hash_stable_when_unchanged(self, silver_of):
        """Odometer moves every reading but is not a tracked attribute.

        If it changed the hash, an SCD2 merge would open a new version on every
        single reading.
        """
        a = T.vehicle_dimension(
            silver_of([reading(vin="AAA", odometer_km=100.0)])
        ).collect()[0]
        b = T.vehicle_dimension(
            silver_of([reading(vin="AAA", odometer_km=999.0)])
        ).collect()[0]

        assert a["attribute_hash"] == b["attribute_hash"]


class TestFleetHealth:
    def test_healthy_vehicle_classified_healthy(self, silver_of):
        gold = T.windowed_telemetry(silver_of([reading(engine_temp_c=90.0)]))
        row = T.fleet_health_summary(gold).collect()[0]

        assert row["health_status"] == "healthy"

    def test_critical_vehicle_classified_critical(self, silver_of):
        gold = T.windowed_telemetry(
            silver_of([reading(engine_temp_c=T.ENGINE_TEMP_CRIT_C + 5)])
        )
        row = T.fleet_health_summary(gold).collect()[0]

        assert row["health_status"] == "critical"

    def test_degraded_sits_between(self, silver_of):
        gold = T.windowed_telemetry(
            silver_of([reading(engine_temp_c=T.ENGINE_TEMP_WARN_C + 2)])
        )
        row = T.fleet_health_summary(gold).collect()[0]

        assert row["health_status"] == "degraded"

    def test_worst_vehicle_ranks_first(self, silver_of):
        payloads = [
            reading(vin="HEALTHY", vehicle_id="VEH-0001", engine_temp_c=90.0),
            reading(vin="SICK", vehicle_id="VEH-0002",
                    engine_temp_c=T.ENGINE_TEMP_CRIT_C + 5),
        ]
        gold = T.windowed_telemetry(silver_of(payloads))
        rows = T.fleet_health_summary(gold).collect()

        assert rows[0]["vehicle_id"] == "VEH-0002"


class TestBronzeContract:
    def test_bronze_keeps_payload_opaque(self, spark):
        """Bronze must not parse. Replayability depends on it."""
        kafka_like = spark.createDataFrame(
            [(b'{"vin":"AAA"}', b"key1", "vehicle-telemetry", 0, 42)],
            "value binary, key binary, topic string, partition int, offset long",
        ).withColumn("timestamp", F.current_timestamp())

        bronze = T.to_bronze(kafka_like)
        row = bronze.collect()[0]

        assert row["raw_payload"] == '{"vin":"AAA"}'
        assert row["kafka_offset"] == 42
        assert row["_ingested_at"] is not None
        assert "vin" not in bronze.columns