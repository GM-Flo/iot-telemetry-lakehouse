"""Tests for gold dimensions: stable keys, history ranges, and the unknown member."""

from __future__ import annotations

from datetime import date

import pytest

from src.iot.gold.dimensions import (
    BEGINNING,
    UNKNOWN,
    UNKNOWN_KEY,
    dim_client,
    dim_date,
    dim_device,
    dim_driver,
    dim_vehicle,
    surrogate_key,
)


@pytest.fixture
def drivers(spark):
    return spark.createDataFrame(
        [
            ("D1", "C001", "Ana", "FOB-1", date(2015, 1, 1), 20261107, 20261110),
            ("D1", "C002", "Ana", "FOB-1", date(2015, 1, 1), 20261110, None),
            ("D2", "C001", "Bo", "FOB-2", date(2020, 1, 1), 20261109, None),
        ],
        "driver_id string, client_id string, name string, fob_id string, licensed_since date, "
        "__START_AT int, __END_AT int",
    )


@pytest.fixture
def clients(spark):
    return spark.createDataFrame(
        [("C001", "tmx", "Acme", "houston", "06-20", 20261107, None)],
        "client_id string, vendor_id string, name string, region string, operating_hours string, "
        "__START_AT int, __END_AT int",
    )


@pytest.fixture
def vendors(spark):
    return spark.createDataFrame([("tmx", "TelemaxPro", "tmx.telemetry.v1")], "vendor_id string, name string, contract string")


def rows_by(df, column):
    return {r[column]: r for r in df.collect()}


class TestHistory:
    def test_first_version_is_open_from_the_beginning(self, drivers):
        versions = [r for r in dim_driver(drivers).collect() if r["driver_id"] == "D1"]
        first = min(versions, key=lambda r: r["valid_from"])
        assert first["valid_from"] == BEGINNING
        assert first["valid_to"] == date(2026, 11, 10)
        assert first["is_current"] is False

    def test_later_version_starts_when_it_was_learned(self, drivers):
        current = [r for r in dim_driver(drivers).collect() if r["driver_id"] == "D1" and r["is_current"]][0]
        assert (current["valid_from"], current["valid_to"], current["client_id"]) == (date(2026, 11, 10), None, "C002")

    def test_a_driver_first_seen_later_is_still_open_from_the_beginning(self, drivers):
        assert rows_by(dim_driver(drivers), "driver_id")["D2"]["valid_from"] == BEGINNING


class TestSurrogateKeys:
    def test_keys_are_the_same_on_every_rebuild(self, drivers):
        first = sorted(r["driver_key"] for r in dim_driver(drivers).collect())
        again = sorted(r["driver_key"] for r in dim_driver(drivers.orderBy("name")).collect())
        assert first == again

    def test_each_version_has_its_own_key(self, drivers):
        keys = [r["driver_key"] for r in dim_driver(drivers).collect()]
        assert len(keys) == len(set(keys)) == 4  # three versions and the unknown member


class TestUnknownMember:
    @pytest.mark.parametrize("build, key, natural", [
        (lambda d: dim_driver(d["drivers"]), "driver_key", "driver_id"),
        (lambda d: dim_client(d["clients"], d["vendors"]), "client_key", "client_id"),
    ])
    def test_every_dimension_has_one_unknown_row(self, drivers, clients, vendors, build, key, natural):
        dimension = build({"drivers": drivers, "clients": clients, "vendors": vendors})
        unknown = [r for r in dimension.collect() if r[natural] == UNKNOWN]

        assert len(unknown) == 1
        assert unknown[0][key] is not None
        assert unknown[0]["is_current"] is True

    def test_python_unknown_key_matches_the_spark_hash(self, spark):
        from pyspark.sql import functions as F
        assert spark.range(1).select(surrogate_key(F.lit(UNKNOWN))).first()[0] == UNKNOWN_KEY

    def test_unknown_key_is_the_same_in_every_dimension(self, drivers, clients, vendors):
        driver_unknown = rows_by(dim_driver(drivers), "driver_id")[UNKNOWN]["driver_key"]
        client_unknown = rows_by(dim_client(clients, vendors), "client_id")[UNKNOWN]["client_key"]
        assert driver_unknown == client_unknown


class TestOtherDimensions:
    def test_client_carries_its_vendor_name(self, clients, vendors):
        assert rows_by(dim_client(clients, vendors), "client_id")["C001"]["vendor_name"] == "TelemaxPro"

    def test_vehicle(self, spark):
        vehicles = spark.createDataFrame(
            [("C001-V001", "VIN1", "Ford", "Transit", 2021, 80.0, "C001", "houston", 20261107, None)],
            "vehicle_id string, vin string, make string, model string, model_year int, tank_capacity_l double, "
            "client_id string, region string, __START_AT int, __END_AT int",
        )
        row = rows_by(dim_vehicle(vehicles), "vehicle_id")["C001-V001"]
        assert (row["make"], row["valid_from"], row["is_current"]) == ("Ford", BEGINNING, True)

    def test_device_uses_the_registry_business_dates(self, spark):
        assignments = spark.createDataFrame(
            [("DEV-1", "tmx", "C001-V001", "C001", "1.0", "retired", date(2024, 1, 1), date(2025, 6, 1)),
             ("DEV-2", "tmx", "C001-V001", "C001", "2.0", "active", date(2025, 6, 1), None)],
            "device_id string, vendor_id string, vehicle_id string, client_id string, firmware_version string, "
            "status string, valid_from date, valid_to date",
        )
        devices = rows_by(dim_device(assignments), "device_id")
        assert devices["DEV-1"]["is_current"] is False
        assert devices["DEV-2"]["valid_from"] == date(2025, 6, 1)


class TestDimDate:
    def test_one_row_per_day_with_a_readable_key(self, spark):
        days = dim_date(spark, date(2026, 11, 6), date(2026, 11, 8)).orderBy("date").collect()

        assert [d["date_key"] for d in days] == [20261106, 20261107, 20261108]
        assert [d["weekday_name"] for d in days] == ["Friday", "Saturday", "Sunday"]
        assert [d["is_weekend"] for d in days] == [False, True, True]
