"""Tests for gold facts: "as was" dimension keys, local days, costs and daily totals."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from src.iot.gold import dimensions as dims
from src.iot.gold.facts import (
    fact_driver_day,
    fact_trip,
    fact_vehicle_day,
    is_after_hours,
    lookup_key,
)

UTC = timezone.utc
TRIP_SCHEMA = (
    "trip_id string, vehicle_id string, client_id string, device_id string, driver_id string, "
    "start_ts timestamp, end_ts timestamp, duration_seconds long, distance_km double, gps_distance_km double, "
    "fuel_used_l double, idle_seconds long, harsh_brakes long, harsh_accelerations long, max_speed_kph double, "
    "average_speed_kph double, is_complete boolean, out_of_sequence_readings long"
)


@pytest.fixture
def dimensions(spark):
    clients = spark.createDataFrame(
        [("C001", "tmx", "Acme", "houston", "06-20", 20261107, None),
         ("C002", "tmx", "Bolt", "houston", "24x7", 20261107, None)],
        "client_id string, vendor_id string, name string, region string, operating_hours string, "
        "__START_AT int, __END_AT int",
    )
    vendors = spark.createDataFrame([("tmx", "TelemaxPro", "tmx.telemetry.v1")], "vendor_id string, name string, contract string")
    vehicles = spark.createDataFrame(
        [("V1", "VIN1", "Ford", "Transit", 2021, 100.0, "C001", "houston", 20261107, None)],
        "vehicle_id string, vin string, make string, model string, model_year int, tank_capacity_l double, "
        "client_id string, region string, __START_AT int, __END_AT int",
    )
    drivers = spark.createDataFrame(
        [("D1", "C001", "Ana", "FOB-1", date(2015, 1, 1), 20261107, 20261110),
         ("D1", "C002", "Ana", "FOB-1", date(2015, 1, 1), 20261110, None)],
        "driver_id string, client_id string, name string, fob_id string, licensed_since date, "
        "__START_AT int, __END_AT int",
    )
    devices = spark.createDataFrame(
        [("DEV-1", "tmx", "V1", "C001", "1.0", "active", date(2025, 1, 1), None)],
        "device_id string, vendor_id string, vehicle_id string, client_id string, firmware_version string, "
        "status string, valid_from date, valid_to date",
    )
    return {
        "dim_client": dims.dim_client(clients, vendors),
        "dim_vehicle": dims.dim_vehicle(vehicles),
        "dim_driver": dims.dim_driver(drivers),
        "dim_device": dims.dim_device(devices),
    }


@pytest.fixture
def regions(spark):
    return spark.createDataFrame([("houston", -6)], "region string, utc_offset_hours int")


@pytest.fixture
def fuel_prices(spark):
    return spark.createDataFrame(
        [("houston", date(2026, 11, d), float(d)) for d in range(1, 15)],
        "region string, price_date date, price_per_litre_usd double",
    )


def trip(trip_id, start_utc, driver="D1", client="C001", fuel=10.0, distance=50.0):
    return (trip_id, "V1", client, "DEV-1", driver, start_utc, start_utc + timedelta(hours=1), 3600, distance,
            distance, fuel, 300, 2, 1, 110.0, 50.0, True, 0)


@pytest.fixture
def trips_of(spark, dimensions, regions, fuel_prices):
    empty_events = "trip_id string, duration_seconds long, fuel_wasted_l double"

    def _build(rows, idling=None, speeding=None):
        trips = spark.createDataFrame(rows, TRIP_SCHEMA)
        idle = spark.createDataFrame(idling or [], empty_events)
        fast = spark.createDataFrame(speeding or [], "trip_id string, duration_seconds long")
        out = fact_trip(trips, idle, fast, dimensions, regions, fuel_prices)
        return {r["trip_id"]: r for r in out.collect()}, out

    return _build


def key_of(dimension, natural, natural_key, on):
    return next(
        r[f"{natural_key}_key"] for r in dimension.collect()
        if r[f"{natural_key}_id"] == natural and r["valid_from"] <= on and (r["valid_to"] is None or on < r["valid_to"])
    )


class TestAsWas:
    def test_driver_key_is_the_version_valid_on_the_trip_day(self, trips_of, dimensions):
        rows, _ = trips_of([
            trip("before_move", datetime(2026, 11, 8, 15, tzinfo=UTC)),
            trip("after_move", datetime(2026, 11, 11, 15, tzinfo=UTC)),
        ])
        drivers = dimensions["dim_driver"]
        assert rows["before_move"]["driver_key"] == key_of(drivers, "D1", "driver", date(2026, 11, 8))
        assert rows["after_move"]["driver_key"] == key_of(drivers, "D1", "driver", date(2026, 11, 11))
        assert rows["before_move"]["driver_key"] != rows["after_move"]["driver_key"]

    def test_no_driver_points_to_the_unknown_row(self, trips_of):
        rows, _ = trips_of([trip("no_fob", datetime(2026, 11, 8, 15, tzinfo=UTC), driver=None)])
        assert rows["no_fob"]["driver_key"] == dims.UNKNOWN_KEY

    def test_every_trip_has_every_key(self, trips_of):
        _, out = trips_of([trip("t", datetime(2026, 11, 8, 15, tzinfo=UTC))])
        row = out.first()
        assert all(row[k] is not None for k in ("vehicle_key", "driver_key", "client_key", "device_key"))

    def test_lookup_key_boundaries(self, spark, dimensions):
        """valid_from is inclusive, valid_to exclusive: the move day belongs to the new version."""
        facts = spark.createDataFrame([("D1", date(2026, 11, 10))], "driver_id string, d date")
        row = lookup_key(facts, dimensions["dim_driver"], "driver_id", "driver_key", facts["d"]).first()
        assert row["driver_key"] == key_of(dimensions["dim_driver"], "D1", "driver", date(2026, 11, 10))


class TestLocalDayAndCost:
    def test_late_evening_utc_belongs_to_the_previous_local_day(self, trips_of):
        """03:00 UTC on 7 Nov is 21:00 on 6 Nov in Houston (UTC-6)."""
        rows, _ = trips_of([trip("evening", datetime(2026, 11, 7, 3, tzinfo=UTC))])
        assert rows["evening"]["start_date_key"] == 20261106
        assert rows["evening"]["fuel_price_per_litre_usd"] == 6.0  # the price of 6 Nov
        assert rows["evening"]["fuel_cost_usd"] == 60.0

    def test_idle_and_speeding_totals(self, trips_of):
        rows, _ = trips_of(
            [trip("t", datetime(2026, 11, 8, 15, tzinfo=UTC))],
            idling=[("t", 180, 0.5), ("t", 120, 0.25)],
            speeding=[("t", 300)],
        )
        row = rows["t"]
        assert (row["idle_fuel_l"], row["idle_cost_usd"]) == (0.75, 6.0)  # 0.75 L at 8 USD
        assert (row["speeding_events"], row["speeding_seconds"]) == (1, 300)


class TestAfterHours:
    @pytest.mark.parametrize("local_hour, hours, expected", [
        (10, "06-20", False), (5, "06-20", True), (20, "06-20", True), (19, "06-20", False),
        (3, "24x7", False), (3, None, None),
    ])
    def test_operating_window(self, spark, local_hour, hours, expected):
        df = spark.createDataFrame([(datetime(2026, 11, 8, local_hour, tzinfo=UTC), hours)], "ts timestamp, hours string")
        assert df.select(is_after_hours(df["ts"], df["hours"])).first()[0] is expected

    def test_trip_flag_uses_local_time_and_the_clients_hours(self, trips_of):
        rows, _ = trips_of([
            trip("night", datetime(2026, 11, 8, 4, tzinfo=UTC)),               # 22:00 local, C001 06-20
            trip("day", datetime(2026, 11, 8, 16, tzinfo=UTC)),                # 10:00 local
            trip("night_24x7", datetime(2026, 11, 8, 4, tzinfo=UTC), client="C002"),
        ])
        assert (rows["night"]["is_after_hours"], rows["day"]["is_after_hours"]) == (True, False)
        assert rows["night_24x7"]["is_after_hours"] is False


MEASUREMENT_SCHEMA = (
    "measurement_id string, vehicle_id string, client_id string, event_ts timestamp, engine_temperature_celsius double"
)


class TestVehicleDay:
    @pytest.fixture
    def build(self, spark, trips_of, dimensions, regions, fuel_prices):
        def _build(readings, faults=(), heartbeats=()):
            measurements = spark.createDataFrame(readings, MEASUREMENT_SCHEMA)
            _, trip_facts = trips_of([trip("t", datetime(2026, 11, 8, 15, tzinfo=UTC))])
            diagnostics = spark.createDataFrame(
                [("V1", ts, "fault_raised", code, "high") for ts, code in faults],
                "vehicle_id string, event_ts timestamp, event_type string, fault_code string, severity string",
            )
            refuels = spark.createDataFrame([], "vehicle_id string, end_ts timestamp, litres_added double")
            geofence = spark.createDataFrame([], "vehicle_id string, event_ts timestamp, event_type string")
            beats = spark.createDataFrame(
                [("V1", ts, status) for ts, status in heartbeats],
                "vehicle_id string, bucket_start timestamp, status string",
            )
            out = fact_vehicle_day(
                measurements, trip_facts, diagnostics, refuels, geofence, beats, dimensions, regions, fuel_prices,
                warning_celsius=105, critical_celsius=115, maximum_step_seconds=600, heartbeat_minutes=15,
            )
            return {r["local_date"]: r for r in out.collect()}

        return _build

    T = datetime(2026, 11, 8, 16, tzinfo=UTC)  # 10:00 local on 8 Nov

    def test_time_above_temperature_counts_until_the_next_reading(self, build):
        readings = [(f"m{i}", "V1", "C001", self.T + timedelta(seconds=60 * i), t)
                    for i, t in enumerate([110.0, 120.0, 90.0, 90.0])]
        day = build(readings)[date(2026, 11, 8)]
        assert day["seconds_above_warning_temperature"] == 120
        assert day["seconds_above_critical_temperature"] == 60
        assert day["max_engine_temperature_celsius"] == 120.0

    def test_a_device_gap_is_not_counted_as_overheating(self, build):
        readings = [("a", "V1", "C001", self.T, 110.0), ("b", "V1", "C001", self.T + timedelta(hours=2), 90.0)]
        assert build(readings)[date(2026, 11, 8)]["seconds_above_warning_temperature"] == 600

    def test_a_flapping_fault_is_one_problem(self, build):
        readings = [("a", "V1", "C001", self.T, 90.0)]
        faults = [(self.T + timedelta(minutes=m), "P0128") for m in (1, 5, 9)]
        day = build(readings, faults=faults)[date(2026, 11, 8)]
        assert (day["fault_raise_events"], day["distinct_fault_codes"]) == (3, 1)

    def test_trip_totals_and_keys(self, build):
        day = build([("a", "V1", "C001", self.T, 90.0)])[date(2026, 11, 8)]
        assert (day["trips"], day["distance_km"], day["fuel_cost_usd"]) == (1, 50.0, 80.0)
        assert day["client_id"] == "C001"
        assert day["vehicle_key"] != dims.UNKNOWN_KEY

    def test_silent_day_appears_with_its_silent_minutes(self, build):
        silent_day = datetime(2026, 11, 9, 16, tzinfo=UTC)
        beats = [(silent_day + timedelta(minutes=15 * i), "silent") for i in range(4)]
        days = build([("a", "V1", "C001", self.T, 90.0)], heartbeats=beats)
        assert days[date(2026, 11, 9)]["silent_minutes"] == 60
        assert days[date(2026, 11, 9)]["readings"] == 0


class TestDriverDay:
    def test_harsh_driving_of_known_and_unknown_drivers(self, spark, trips_of, dimensions, regions):
        _, trip_facts = trips_of([trip("t", datetime(2026, 11, 8, 15, tzinfo=UTC))])
        t = datetime(2026, 11, 8, 15, 30, tzinfo=UTC)
        behavior = spark.createDataFrame(
            [("V1", "C001", "D1", t, "harsh_brake"), ("V1", "C001", "D1", t, "harsh_acceleration"),
             ("V1", "C001", None, t, "harsh_brake")],
            "vehicle_id string, client_id string, driver_id string, event_ts timestamp, event_type string",
        )
        speeding = spark.createDataFrame(
            [("V1", "C001", "D1", t, 300)],
            "vehicle_id string, client_id string, driver_id string, start_ts timestamp, duration_seconds long",
        )
        rows = {r["driver_id"]: r for r in fact_driver_day(trip_facts, behavior, speeding, dimensions, regions).collect()}

        assert (rows["D1"]["harsh_brakes"], rows["D1"]["harsh_accelerations"]) == (1, 1)
        assert (rows["D1"]["trips"], rows["D1"]["speeding_seconds"]) == (1, 300)
        assert rows[dims.UNKNOWN]["harsh_brakes"] == 1
        assert rows[dims.UNKNOWN]["driver_key"] == dims.UNKNOWN_KEY