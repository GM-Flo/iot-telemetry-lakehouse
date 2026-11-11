"""Tests for the data products and the security that guards them."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pyspark.sql.types import _parse_datatype_string

from src.iot.gold import security
from src.iot.gold.dimensions import UNKNOWN, UNKNOWN_KEY
from src.iot.gold.products import (
    VEHICLE_HEALTH_SCHEMA,
    driver_safety,
    fleet_cost_utilization,
    platform_billing,
    security_misuse,
    vehicle_health,
    vendor_data_quality,
)

UTC = timezone.utc
DAY = date(2026, 11, 8)
VEHICLE_DAY_SCHEMA = (
    "vehicle_key string, client_key string, date_key int, vehicle_id string, client_id string, local_date date, "
    "seconds_above_warning_temperature bigint, seconds_above_critical_temperature bigint, "
    "distinct_fault_codes bigint, distinct_high_severity_codes bigint, fault_raise_events bigint, "
    "silent_minutes bigint, readings bigint, max_engine_temperature_celsius double, trips bigint, "
    "distance_km double, driving_seconds bigint, idle_seconds bigint, fuel_used_l double, fuel_cost_usd double, "
    "idle_cost_usd double, refuel_cost_usd double, after_hours_trips bigint, after_hours_distance_km double, "
    "geofence_exits bigint"
)
VEHICLE_DAY_DEFAULTS = {
    "vehicle_key": "vk1", "client_key": "ck1", "date_key": 20261108, "vehicle_id": "V1", "client_id": "C001",
    "local_date": DAY, "seconds_above_warning_temperature": 0, "seconds_above_critical_temperature": 0,
    "distinct_fault_codes": 0, "distinct_high_severity_codes": 0, "fault_raise_events": 0, "silent_minutes": 0,
    "readings": 100, "max_engine_temperature_celsius": 90.0, "trips": 1, "distance_km": 100.0,
    "driving_seconds": 7200, "idle_seconds": 600, "fuel_used_l": 10.0, "fuel_cost_usd": 20.0, "idle_cost_usd": 2.0,
    "refuel_cost_usd": 0.0, "after_hours_trips": 0, "after_hours_distance_km": 0.0, "geofence_exits": 0,
}


@pytest.fixture
def vehicle_days(spark):
    def _build(*changes):
        columns = [c.split()[0] for c in VEHICLE_DAY_SCHEMA.split(", ")]
        rows = [tuple({**VEHICLE_DAY_DEFAULTS, **change}[c] for c in columns) for change in changes]
        return spark.createDataFrame(rows, VEHICLE_DAY_SCHEMA)

    return _build


@pytest.fixture
def dim_vehicle(spark):
    return spark.createDataFrame(
        [("vk1", "1HGBH41JXMN109186", "Ford", "Transit", 2021)],
        "vehicle_key string, vin string, make string, model string, model_year int",
    )


class TestSecurityFunctions:
    @pytest.fixture
    def access(self, spark):
        spark.createDataFrame(
            [("ana@acme.com", "C001"), ("ops@platform.com", "*")], "principal string, client_id string"
        ).createOrReplaceTempView("client_access_test")

    @pytest.mark.parametrize("principal, client, visible", [
        ("ana@acme.com", "C001", True),
        ("ana@acme.com", "C002", False),
        ("ops@platform.com", "C002", True),
        ("stranger@x.com", "C001", False),
    ])
    def test_row_visibility(self, spark, access, principal, client, visible):
        condition = security.ACCESS_CONDITION.format(
            access_table="client_access_test", principal=f"'{principal}'", client=f"'{client}'"
        )
        assert spark.sql(f"SELECT {condition}").first()[0] is visible

    def test_vin_is_masked_to_its_last_four(self, spark):
        masked = spark.sql(f"SELECT {security.MASKED_VIN.format(vin=chr(39) + '1HGBH41JXMN109186' + chr(39))}")
        assert masked.first()[0] == "*************9186"

    def test_setup_uses_only_the_given_catalog(self):
        sql = " ".join(security.setup_statements("iot_prod", "admin@x.com"))
        assert "iot_prod.gold.client_access" in sql
        assert "iot_prod.gold.client_row_filter" in sql
        assert "iot_dev" not in sql

    @pytest.mark.parametrize("bad", ["", "x'; DROP TABLE t; --", "a\\b"])
    def test_unsafe_principal_is_refused(self, bad):
        with pytest.raises(ValueError):
            security.setup_statements("iot_dev", bad)

    def test_clauses(self):
        assert security.row_filter_clause("iot_dev") == "ROW FILTER iot_dev.gold.client_row_filter ON (client_id)"
        assert security.vin_mask_clause("iot_dev") == "MASK iot_dev.gold.mask_vin"


class TestVehicleHealth:
    def status(self, vehicle_days, dim_vehicle, **change):
        return vehicle_health(vehicle_days(change), dim_vehicle, critical_seconds=300, warning_seconds=600).first()

    @pytest.mark.parametrize("change, expected", [
        ({}, "ok"),
        ({"seconds_above_warning_temperature": 600}, "attention"),
        ({"seconds_above_warning_temperature": 599}, "ok"),
        ({"distinct_fault_codes": 1}, "attention"),
        ({"seconds_above_critical_temperature": 300}, "critical"),
        ({"distinct_fault_codes": 1, "distinct_high_severity_codes": 1}, "critical"),
        ({"readings": 0}, "no_data"),
        ({"readings": 0, "max_engine_temperature_celsius": None}, "no_data"),
    ])
    def test_status_rules(self, vehicle_days, dim_vehicle, change, expected):
        assert self.status(vehicle_days, dim_vehicle, **change)["health_status"] == expected

    def test_schema_with_mask_matches_what_the_function_returns(self, spark, vehicle_days, dim_vehicle):
        declared = _parse_datatype_string(VEHICLE_HEALTH_SCHEMA.format(vin_mask=""))
        actual = vehicle_health(vehicle_days({}), dim_vehicle, 300, 600).schema
        assert [(f.name, f.dataType) for f in declared] == [(f.name, f.dataType) for f in actual]


class TestDriverSafety:
    SCHEMA = (
        "driver_key string, client_key string, date_key int, driver_id string, client_id string, local_date date, "
        "trips bigint, distance_km double, driving_seconds bigint, idle_seconds bigint, after_hours_trips bigint, "
        "harsh_brakes bigint, harsh_accelerations bigint, speeding_events bigint, speeding_seconds bigint"
    )

    def score(self, spark, distance, brakes, accels, speeding_seconds, key="dk1"):
        days = spark.createDataFrame(
            [(key, "ck1", 20261108, "D1", "C001", DAY, 1, distance, 3600, 0, 0, brakes, accels, 1, speeding_seconds)],
            self.SCHEMA,
        )
        names = spark.createDataFrame([("dk1", "Ana")], "driver_key string, driver_name string")
        return driver_safety(days, names, harsh_weight=5, speeding_weight=2, minimum_distance_km=10).first()

    def test_score_formula(self, spark):
        row = self.score(spark, distance=100.0, brakes=1, accels=1, speeding_seconds=300)
        assert row["harsh_events_per_100_km"] == 2.0
        assert row["speeding_minutes_per_100_km"] == 5.0
        assert row["safety_score"] == 80.0  # 100 - 5 x 2 - 2 x 5

    def test_score_never_goes_below_zero(self, spark):
        assert self.score(spark, distance=20.0, brakes=50, accels=0, speeding_seconds=0)["safety_score"] == 0.0

    def test_too_little_driving_gets_no_score(self, spark):
        row = self.score(spark, distance=3.0, brakes=2, accels=0, speeding_seconds=0)
        assert row["safety_score"] is None
        assert row["harsh_events_per_100_km"] is not None

    def test_unknown_driver_is_marked(self, spark):
        assert self.score(spark, 100.0, 0, 0, 0, key=UNKNOWN_KEY)["is_unknown_driver"] is True


class TestFleetCost:
    def test_per_client_totals_and_ratios(self, vehicle_days):
        days = vehicle_days({}, {"vehicle_id": "V2", "vehicle_key": "vk2", "trips": 0, "distance_km": 0.0,
                                 "driving_seconds": 0, "fuel_cost_usd": 0.0, "idle_cost_usd": 0.0})
        row = fleet_cost_utilization(days).first()

        assert (row["vehicles_reporting"], row["vehicles_active"], row["share_of_vehicles_active"]) == (2, 1, 0.5)
        assert row["fuel_cost_per_km_usd"] == 0.2
        assert row["idle_cost_share"] == 0.1
        assert row["driving_hours_per_active_vehicle"] == 2.0

    def test_a_day_with_no_driving_has_no_ratios(self, vehicle_days):
        row = fleet_cost_utilization(vehicle_days({"trips": 0, "distance_km": 0.0, "driving_seconds": 0,
                                                   "fuel_cost_usd": 0.0})).first()
        assert row["fuel_cost_per_km_usd"] is None
        assert row["driving_hours_per_active_vehicle"] is None


class TestSecurityMisuse:
    def test_only_days_worth_reviewing(self, spark, vehicle_days):
        days = vehicle_days(
            {"vehicle_id": "V1", "after_hours_trips": 1},
            {"vehicle_id": "V2"},
            {"vehicle_id": "V3", "geofence_exits": 2},
            {"vehicle_id": "V4"},
        )
        trips = spark.createDataFrame(
            [("V4", datetime(2026, 11, 8, 10, tzinfo=UTC), UNKNOWN_KEY), ("V2", datetime(2026, 11, 8, 10, tzinfo=UTC), "dk")],
            "vehicle_id string, local_start_ts timestamp, driver_key string",
        )
        rows = {r["vehicle_id"]: r for r in security_misuse(days, trips).collect()}

        assert set(rows) == {"V1", "V3", "V4"}
        assert rows["V4"]["trips_without_registered_driver"] == 1


class TestPlatformBilling:
    def test_billable_devices_per_client(self, spark):
        t = datetime(2026, 11, 8, 16, tzinfo=UTC)
        beats = spark.createDataFrame(
            [("DEV-1", "V1", "C001", t, 10, "reporting"), ("DEV-1", "V1", "C001", t, 5, "reporting"),
             ("DEV-2", "V2", "C001", t, 0, "silent"), ("DEV-X", None, None, t, 9, "reporting")],
            "device_id string, vehicle_id string, client_id string, bucket_start timestamp, "
            "messages_received bigint, status string",
        )
        vehicles = spark.createDataFrame(
            [("V1", "houston", True), ("V2", "houston", True)], "vehicle_id string, region string, is_current boolean"
        )
        regions = spark.createDataFrame([("houston", -6)], "region string, utc_offset_hours int")
        clients = spark.createDataFrame(
            [("ck1", "C001", "Acme", "tmx", True), (UNKNOWN_KEY, UNKNOWN, None, None, True)],
            "client_key string, client_id string, client_name string, vendor_id string, is_current boolean",
        )
        rows = platform_billing(beats, vehicles, regions, clients).collect()

        assert len(rows) == 1
        assert (rows[0]["client_id"], rows[0]["billable_devices"], rows[0]["messages_received"]) == ("C001", 1, 15)


class TestVendorDataQuality:
    def test_message_and_reading_rates_are_kept_apart(self, spark):
        t = datetime(2026, 11, 8, 12, tzinfo=UTC)
        bronze = spark.createDataFrame(
            [('{"vendor_id": "ats"}', t)] * 4 + [("not json", t)], "raw_payload string, eventhub_enqueued_at timestamp"
        )
        measurements = spark.createDataFrame([("ats", t)] * 9, "vendor_id string, eventhub_enqueued_at timestamp")
        quarantine = "vendor_id string, eventhub_enqueued_at timestamp"
        parse = spark.createDataFrame([(None, t)], quarantine)
        contract = spark.createDataFrame([("ats", t)], quarantine)
        invalid = spark.createDataFrame([("ats", t)] * 3, quarantine)
        mismatched = spark.createDataFrame([], quarantine)

        rows = {r["vendor_id"]: r for r in vendor_data_quality(
            bronze, measurements, [parse, contract], [invalid, mismatched]).collect()}

        assert (rows["ats"]["messages_received"], rows["ats"]["messages_quarantined"]) == (4, 1)
        assert rows["ats"]["message_quarantine_rate"] == 0.25
        assert rows["ats"]["reading_quarantine_rate"] == 0.25  # 3 of 12 readings
        assert (rows[UNKNOWN]["messages_received"], rows[UNKNOWN]["messages_quarantined"]) == (1, 1)
