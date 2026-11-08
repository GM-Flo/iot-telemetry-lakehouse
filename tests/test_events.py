"""Tests for silver event tables, on hand-built drives where the right answer is known."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from src.iot.silver.events import (
    flag_out_of_sequence,
    haversine_km,
    ignition_events,
    longest_non_decreasing,
    vehicle_trips,
)
from src.producer.fleet import Fleet
from src.producer.registry import select_clients

T0 = datetime(2026, 11, 6, 8, 0, tzinfo=timezone.utc)
STOP = 600
SCHEMA = (
    "measurement_id string, vehicle_id string, client_id string, device_id string, driver_id string, "
    "event_ts timestamp, ignition_on boolean, speed_kph double, latitude double, longitude double, "
    "odometer_km double, fuel_level_percent double, harsh_brake_count int, harsh_accel_count int"
)


def reading(vehicle, minute, ignition=True, speed=50.0, fuel=80.0, odometer=None, lat=29.8, brakes=0):
    return (
        f"{vehicle}-{minute}", vehicle, "C001", f"DEV-{vehicle}", "D001",
        T0 + timedelta(minutes=minute), ignition, speed, lat, -95.5,
        odometer if odometer is not None else 1000.0 + minute, fuel, brakes, 0,
    )


@pytest.fixture
def measurements(spark):
    def _build(rows):
        return spark.createDataFrame(rows, SCHEMA)

    return _build


@pytest.fixture
def vehicles(spark):
    return spark.createDataFrame(
        [("V1", 200.0, None), ("V2", 100.0, None), ("V1", 999.0, 20261101)],
        "vehicle_id string, tank_capacity_l double, __END_AT int",
    )


def trips_of(measurements, vehicles, rows):
    return vehicle_trips(measurements(rows), vehicles, STOP).orderBy("vehicle_id", "start_ts").collect()


class TestIgnitionEvents:
    def test_each_switch_is_one_event(self, measurements):
        rows = [reading("V1", m, ignition=i) for m, i in [(0, False), (1, True), (2, True), (3, False), (4, True)]]
        events = ignition_events(measurements(rows)).orderBy("event_ts").collect()
        assert [e["event_type"] for e in events] == ["ignition_on", "ignition_off", "ignition_on"]

    def test_first_reading_is_not_an_event(self, measurements):
        rows = [reading("V1", 0, ignition=True), reading("V1", 1, ignition=True)]
        assert ignition_events(measurements(rows)).count() == 0

    def test_vehicles_are_compared_only_with_themselves(self, measurements):
        rows = [reading("V1", 0, ignition=True), reading("V2", 1, ignition=False), reading("V1", 2, ignition=True)]
        assert ignition_events(measurements(rows)).count() == 0

    def test_order_of_arrival_does_not_matter(self, measurements):
        """A late reading is sorted into place by event time."""
        in_order = [reading("V1", m, ignition=i) for m, i in [(0, False), (1, True), (2, False)]]
        shuffled = [in_order[2], in_order[0], in_order[1]]
        first = sorted(e["event_id"] for e in ignition_events(measurements(in_order)).collect())
        again = sorted(e["event_id"] for e in ignition_events(measurements(shuffled)).collect())
        assert first == again and len(first) == 2


class TestTrips:
    def test_short_stop_stays_one_trip(self, measurements, vehicles):
        rows = [reading("V1", m) for m in range(0, 10)]
        rows += [reading("V1", m, ignition=False, speed=0.0) for m in range(10, 15)]
        rows += [reading("V1", m) for m in range(15, 25)]
        assert len(trips_of(measurements, vehicles, rows)) == 1

    def test_long_stop_splits_the_trip(self, measurements, vehicles):
        rows = [reading("V1", m) for m in range(0, 10)]
        rows += [reading("V1", m, ignition=False, speed=0.0) for m in range(10, 30)]
        rows += [reading("V1", m) for m in range(30, 40)]
        trips = trips_of(measurements, vehicles, rows)

        assert len(trips) == 2
        assert trips[0]["end_ts"] < trips[1]["start_ts"]

    def test_trip_measures(self, measurements, vehicles):
        rows = [reading("V1", m, odometer=1000.0 + m, fuel=80.0 - m) for m in range(0, 31)]
        trip = trips_of(measurements, vehicles, rows)[0]

        assert trip["duration_seconds"] == 30 * 60
        assert trip["distance_km"] == 30.0
        assert trip["average_speed_kph"] == 60.0
        assert trip["fuel_used_l"] == 60.0  # 30% of a 200 L tank, current version only
        assert trip["readings"] == 31

    def test_refuel_during_a_trip_does_not_hide_fuel_used(self, measurements, vehicles):
        fuel = [50.0, 40.0, 30.0, 90.0, 80.0]  # filled up at minute 3
        rows = [reading("V1", m, fuel=f) for m, f in enumerate(fuel)]
        assert trips_of(measurements, vehicles, rows)[0]["fuel_used_l"] == 60.0  # 30% of 200 L

    def test_idle_time_is_time_spent_stationary(self, measurements, vehicles):
        speeds = [50, 0, 0, 0, 50, 50]
        rows = [reading("V1", m, speed=float(s)) for m, s in enumerate(speeds)]
        assert trips_of(measurements, vehicles, rows)[0]["idle_seconds"] == 180

    def test_single_reading_is_not_a_trip(self, measurements, vehicles):
        assert trips_of(measurements, vehicles, [reading("V1", 0)]) == []

    def test_last_trip_is_incomplete_until_the_stop_is_long_enough(self, measurements, vehicles):
        driving = [reading("V1", m) for m in range(0, 10)]
        short_after = [reading("V1", m, ignition=False, speed=0.0) for m in range(10, 15)]
        long_after = [reading("V1", m, ignition=False, speed=0.0) for m in range(10, 25)]

        assert trips_of(measurements, vehicles, driving + short_after)[0]["is_complete"] is False
        assert trips_of(measurements, vehicles, driving + long_after)[0]["is_complete"] is True

    def test_trip_id_survives_the_trip_growing(self, measurements, vehicles):
        """An incomplete trip keeps its id when the next run extends it."""
        early = trips_of(measurements, vehicles, [reading("V1", m) for m in range(0, 10)])
        later = trips_of(measurements, vehicles, [reading("V1", m) for m in range(0, 40)])
        assert early[0]["trip_id"] == later[0]["trip_id"]

    def test_harsh_events_are_summed(self, measurements, vehicles):
        rows = [reading("V1", m, brakes=b) for m, b in enumerate([0, 1, 0, 2])]
        assert trips_of(measurements, vehicles, rows)[0]["harsh_brakes"] == 3


class TestLongestNonDecreasing:
    @pytest.mark.parametrize("values, keep", [
        ([100, 101, 102], [True, True, True]),
        ([100, 117, 101, 102], [True, False, True, True]),
        ([100, 117, 118, 101, 102, 103], [True, False, False, True, True, True]),
        ([99, 100, 95, 101], [True, True, False, True]),
        ([100, 100, 100], [True, True, True]),
        ([100, None, 101], [True, True, True]),
        ([], []),
    ])
    def test_keeps_the_largest_sequence_that_never_decreases(self, values, keep):
        assert longest_non_decreasing(values) == keep


class TestOutOfSequence:
    def flagged(self, measurements, odometers):
        rows = [reading("V1", m, odometer=o) for m, o in enumerate(odometers)]
        out = flag_out_of_sequence(measurements(rows)).orderBy("event_ts").collect()
        return [r["out_of_sequence"] for r in out]

    def test_jump_ahead_is_flagged_and_the_good_reading_after_it_is_not(self, measurements):
        assert self.flagged(measurements, [100.0, 101.0, 117.0, 103.0, 104.0]) == [False, False, True, False, False]

    def test_two_bad_readings_side_by_side(self, measurements):
        flags = self.flagged(measurements, [100.0, 117.0, 118.0, 101.0, 102.0, 103.0])
        assert flags == [False, True, True, False, False, False]

    def test_clean_drive_flags_nothing(self, measurements):
        assert not any(self.flagged(measurements, [100.0 + m for m in range(20)]))

    def test_columns_are_kept(self, measurements):
        rows = [reading("V1", m) for m in range(3)]
        assert set(measurements(rows).columns) < set(flag_out_of_sequence(measurements(rows)).columns)

    def test_out_of_sequence_reading_is_left_out_of_the_trip(self, measurements, vehicles):
        """Current values under an old timestamp: far ahead on the road and on the odometer."""
        rows = [reading("V1", m, odometer=1000.0 + m) for m in range(10)]
        rows[4] = reading("V1", 4, odometer=1020.0, lat=30.0)
        trip = trips_of(measurements, vehicles, rows)[0]

        assert trip["out_of_sequence_readings"] == 1
        assert trip["readings"] == 9
        assert trip["gps_distance_km"] == 0.0
        assert trip["distance_km"] == 9.0

    def test_out_of_sequence_reading_makes_no_false_ignition_switch(self, measurements):
        parked = [reading("V1", m, ignition=False, speed=0.0, odometer=500.0) for m in range(6)]
        parked[2] = reading("V1", 2, ignition=True, odometer=512.0)
        assert ignition_events(measurements(parked)).count() == 0


class TestHaversine:
    def test_one_degree_of_latitude(self, spark):
        df = spark.createDataFrame([(0.0, 0.0, 1.0, 0.0)], "a double, b double, c double, d double")
        km = df.select(haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d"))).first()[0]
        assert km == pytest.approx(111.19, abs=0.01)


def producer_measurements(spark, late_fraction, hours=4, step=30):
    """Producer readings shaped like vehicle_measurements, plus the ids of backdated readings:
    all of them, and those taken while moving. A backdated reading from a parked vehicle
    carries the same odometer as the readings around it: it cannot be detected, and does no harm."""
    fleet = Fleet(select_clients(1), seed=8, glitch_fraction=0)
    ghosts = {g.device_id for g in fleet.ghost_devices}
    rows, backdated, moving = [], set(), set()
    for tick in range(int(hours * 3600 / step)):
        now = T0 + timedelta(seconds=step * tick)
        fleet.advance(step, now=now)
        for r in fleet.readings(now=now, late_fraction=late_fraction):
            if r["device_id"] in ghosts:
                continue
            measurement_id = f"{r['device_id']}-{tick}"
            if r["event_ts"] != now:
                backdated.add(measurement_id)
                if r["speed_kph"] > 0:
                    moving.add(measurement_id)
            rows.append((
                measurement_id, r["vehicle_id"], r["client_id"], r["device_id"], None, r["event_ts"],
                r["ignition_on"], r["speed_kph"], r["latitude"], r["longitude"], r["odometer_km"],
                r["fuel_level_pct"], r["harsh_brake_count"], r["harsh_accel_count"],
            ))
    return spark.createDataFrame(rows, SCHEMA), backdated, moving


class TestWithProducerData:
    @pytest.mark.parametrize("late_fraction", [0.0, 0.05])
    def test_gps_distance_agrees_with_the_odometer(self, spark, vehicles, late_fraction):
        """With clock-behind readings left out, GPS and odometer tell the same story."""
        measurements, _, _ = producer_measurements(spark, late_fraction)
        trips = vehicle_trips(measurements, vehicles, STOP)
        totals = trips.agg(F.sum("distance_km").alias("odo"), F.sum("gps_distance_km").alias("gps")).first()

        assert trips.count() > 0
        assert trips.filter("distance_km < 0").count() == 0
        assert totals["gps"] == pytest.approx(totals["odo"], rel=0.01)

    def test_backdated_readings_are_found_and_good_ones_are_not(self, spark):
        measurements, backdated, moving = producer_measurements(spark, late_fraction=0.05)
        flagged = {
            r["measurement_id"]
            for r in flag_out_of_sequence(measurements).filter("out_of_sequence").select("measurement_id").collect()
        }

        assert len(moving) > 100
        assert len(flagged & moving) >= 0.98 * len(moving)
        assert flagged <= backdated  # no good reading is ever flagged