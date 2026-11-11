"""Tests for silver event tables, on hand-built drives where the right answer is known."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from src.iot.silver.events import (
    device_heartbeats,
    diagnostic_events,
    driver_behavior_events,
    driver_change_events,
    flag_out_of_sequence,
    geofence_events,
    haversine_km,
    idling_events,
    ignition_events,
    longest_non_decreasing,
    refuel_events,
    speeding_events,
    vehicle_trips,
)
from src.producer.fleet import Fleet
from src.producer.registry import select_clients

pytestmark = pytest.mark.slow

T0 = datetime(2026, 11, 6, 8, 0, tzinfo=timezone.utc)
STOP = 600
SCHEMA = (
    "measurement_id string, vehicle_id string, client_id string, device_id string, driver_id string, "
    "event_ts timestamp, ignition_on boolean, speed_kph double, latitude double, longitude double, "
    "odometer_km double, fuel_level_percent double, harsh_brake_count int, harsh_accel_count int, "
    "driver_fob_id string, fault_code string"
)


def reading(vehicle, minute, ignition=True, speed=50.0, fuel=80.0, odometer=None, lat=29.8, brakes=0,
            fob="FOB-1", driver="D001", fault=None, lon=-95.5):
    return (
        f"{vehicle}-{minute}", vehicle, "C001", f"DEV-{vehicle}", driver,
        T0 + timedelta(minutes=minute), ignition, speed, lat, lon,
        odometer if odometer is not None else 1000.0 + minute, fuel, brakes, 0, fob, fault,
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

    def test_a_change_of_driver_splits_the_trip(self, measurements, vehicles):
        """A shift handover with the engine running: two trips, one per driver."""
        rows = [reading("V1", m, fob="FOB-A", driver="D-A") for m in range(0, 10)]
        rows += [reading("V1", m, fob="FOB-B", driver="D-B") for m in range(10, 20)]
        trips = trips_of(measurements, vehicles, rows)

        assert [t["driver_id"] for t in trips] == ["D-A", "D-B"]
        assert [t["distance_km"] for t in trips] == [9.0, 9.0]
        assert trips[0]["end_ts"] < trips[1]["start_ts"]

    def test_a_reading_without_a_fob_does_not_split_the_trip(self, measurements, vehicles):
        rows = [reading("V1", m, fob="FOB-A", driver="D-A") for m in range(0, 5)]
        rows += [reading("V1", m, fob=None, driver=None) for m in range(5, 8)]
        rows += [reading("V1", m, fob="FOB-A", driver="D-A") for m in range(8, 12)]
        trips = trips_of(measurements, vehicles, rows)

        assert len(trips) == 1 and trips[0]["driver_id"] == "D-A"

    def test_the_trip_driver_is_the_first_to_log_in(self, measurements, vehicles):
        rows = [reading("V1", m, fob=None, driver=None) for m in range(0, 3)]
        rows += [reading("V1", m, fob="FOB-A", driver="D-A") for m in range(3, 10)]
        assert [t["driver_id"] for t in trips_of(measurements, vehicles, rows)] == ["D-A"]

    def test_an_unregistered_fob_is_a_driver_of_its_own(self, measurements, vehicles):
        """A temporary driver's fob is not in the registry, so its driver_id is null, but it is
        still a different person: the trip splits and the new trip has no registered driver."""
        rows = [reading("V1", m, fob="FOB-A", driver="D-A") for m in range(0, 5)]
        rows += [reading("V1", m, fob="FOB-TEMP", driver=None) for m in range(5, 10)]
        trips = trips_of(measurements, vehicles, rows)
        assert [t["driver_id"] for t in trips] == ["D-A", None]

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

    def test_duplicate_measurement_ids_do_not_mix_up_flags(self, measurements):
        """A vendor reusing message ids must not let one reading take another's flag."""
        rows = [reading("V1", m, odometer=o) for m, o in enumerate([100.0, 101.0, 117.0, 103.0, 104.0])]
        rows = [("same-id",) + r[1:] for r in rows]
        out = flag_out_of_sequence(measurements(rows)).orderBy("event_ts").collect()

        assert len(out) == 5
        assert [r["out_of_sequence"] for r in out] == [False, False, True, False, False]

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


def idling_of(measurements, vehicles, rows):
    df = measurements(rows)
    trips = vehicle_trips(df, vehicles, STOP)
    return idling_events(df, trips, vehicles, 120, STOP).orderBy("start_ts").collect()


def speeding_of(measurements, vehicles, rows):
    df = measurements(rows)
    trips = vehicle_trips(df, vehicles, STOP)
    return speeding_events(df, trips, 120.0, 240, STOP).orderBy("start_ts").collect()


class TestIdling:
    def test_three_minutes_stationary_with_engine_on_is_idling(self, measurements, vehicles):
        speeds = [50, 0, 0, 0, 0, 50]
        rows = [reading("V1", m, speed=float(s)) for m, s in enumerate(speeds)]
        events = idling_of(measurements, vehicles, rows)

        assert len(events) == 1
        assert events[0]["duration_seconds"] == 180
        assert events[0]["readings"] == 4

    def test_a_short_stop_is_not_idling(self, measurements, vehicles):
        rows = [reading("V1", m, speed=float(s)) for m, s in enumerate([50, 0, 0, 50])]
        assert idling_of(measurements, vehicles, rows) == []

    def test_engine_off_is_parked_not_idling(self, measurements, vehicles):
        rows = [reading("V1", m, ignition=False, speed=0.0) for m in range(10)]
        assert idling_of(measurements, vehicles, rows) == []

    def test_fuel_wasted_while_idling(self, measurements, vehicles):
        fuel = [80.0, 80.0, 79.5, 79.0, 79.0, 79.0]
        rows = [reading("V1", m, speed=0.0 if 0 < m < 5 else 50.0, fuel=f) for m, f in enumerate(fuel)]
        assert idling_of(measurements, vehicles, rows)[0]["fuel_wasted_l"] == 2.0  # 1% of 200 L

    def test_idling_carries_its_trip(self, measurements, vehicles):
        rows = [reading("V1", m, speed=0.0 if 2 <= m <= 6 else 50.0) for m in range(10)]
        trip = trips_of(measurements, vehicles, rows)[0]
        assert idling_of(measurements, vehicles, rows)[0]["trip_id"] == trip["trip_id"]


class TestSpeeding:
    def over(self, minutes, speed=130.0):
        rows = [reading("V1", 0, speed=100.0)]
        rows += [reading("V1", m, speed=speed) for m in range(1, minutes + 2)]
        rows += [reading("V1", minutes + 2, speed=100.0)]
        return rows

    def test_five_minutes_over_the_limit_is_speeding(self, measurements, vehicles):
        event = speeding_of(measurements, vehicles, self.over(5))[0]
        assert event["duration_seconds"] == 300
        assert event["max_speed_kph"] == 130.0
        assert event["max_over_limit_kph"] == 10.0
        assert event["speed_limit_kph"] == 120.0

    def test_exactly_four_minutes_counts(self, measurements, vehicles):
        assert len(speeding_of(measurements, vehicles, self.over(4))) == 1

    def test_a_burst_shorter_than_four_minutes_does_not(self, measurements, vehicles):
        assert speeding_of(measurements, vehicles, self.over(3)) == []

    def test_dropping_below_the_limit_splits_the_period(self, measurements, vehicles):
        speeds = [130.0] * 5 + [110.0] + [130.0] * 5
        rows = [reading("V1", m, speed=s) for m, s in enumerate(speeds)]
        assert [e["duration_seconds"] for e in speeding_of(measurements, vehicles, rows)] == [240, 240]

    def test_a_silent_device_splits_the_period(self, measurements, vehicles):
        """Fifteen minutes without data: we cannot claim the vehicle was speeding all along."""
        rows = [reading("V1", m, speed=130.0) for m in [0, 1, 2, 17, 18, 19]]
        assert speeding_of(measurements, vehicles, rows) == []

    def test_out_of_sequence_reading_cannot_extend_speeding(self, measurements, vehicles):
        rows = [reading("V1", m, speed=130.0) for m in range(4)]
        rows += [reading("V1", m, speed=100.0) for m in range(4, 8)]
        rows[4] = reading("V1", 4, speed=130.0, odometer=1050.0)
        assert speeding_of(measurements, vehicles, rows) == []


@pytest.fixture
def regions(spark):
    # Depot at (29.8, -95.5), 50 km fence. 0.45 degrees of latitude is about 50 km.
    return spark.createDataFrame(
        [("houston", 29.8, -95.5, 50.0)],
        "region string, depot_latitude double, depot_longitude double, geofence_radius_km double",
    )


@pytest.fixture
def fleet_vehicles(spark):
    return spark.createDataFrame(
        [("V1", 200.0, "houston", None)], "vehicle_id string, tank_capacity_l double, region string, __END_AT int"
    )


@pytest.fixture
def fault_codes(spark):
    return spark.createDataFrame(
        [("P0128", "Coolant thermostat", "high"), ("P0420", "Catalyst efficiency", "medium")],
        "code string, description string, severity string",
    )


def with_trips(measurements, vehicles, rows):
    df = measurements(rows)
    return df, vehicle_trips(df, vehicles, STOP)


class TestDriverChanges:
    def test_a_different_fob_is_a_change(self, measurements, vehicles):
        rows = [reading("V1", m, fob="FOB-A", driver="D-A") for m in range(3)]
        rows += [reading("V1", m, fob="FOB-B", driver="D-B") for m in range(3, 6)]
        events = driver_change_events(*with_trips(measurements, vehicles, rows)).collect()

        assert len(events) == 1
        assert (events[0]["previous_driver_id"], events[0]["new_driver_id"]) == ("D-A", "D-B")

    def test_same_driver_restarting_is_not_a_change(self, measurements, vehicles):
        rows = [reading("V1", m, fob="FOB-A") for m in range(3)]
        rows += [reading("V1", m, ignition=False, speed=0.0, fob=None) for m in range(3, 40)]
        rows += [reading("V1", m, fob="FOB-A") for m in range(40, 43)]
        assert driver_change_events(*with_trips(measurements, vehicles, rows)).count() == 0

    def test_change_after_a_break_with_no_fob(self, measurements, vehicles):
        rows = [reading("V1", m, fob="FOB-A") for m in range(3)]
        rows += [reading("V1", m, ignition=False, speed=0.0, fob=None) for m in range(3, 6)]
        rows += [reading("V1", m, fob="FOB-B", driver=None) for m in range(6, 9)]
        event = driver_change_events(*with_trips(measurements, vehicles, rows)).first()

        assert event["new_driver_fob_id"] == "FOB-B"
        assert event["new_driver_id"] is None  # unregistered fob: still a change


class TestDiagnostics:
    def events(self, measurements, vehicles, fault_codes, faults):
        rows = [reading("V1", m, fault=f) for m, f in enumerate(faults)]
        df, trips = with_trips(measurements, vehicles, rows)
        return diagnostic_events(df, trips, fault_codes).orderBy("event_ts", "event_type").collect()

    def test_fault_raised_and_cleared(self, measurements, vehicles, fault_codes):
        events = self.events(measurements, vehicles, fault_codes, [None, "P0128", "P0128", None])
        assert [(e["event_type"], e["fault_code"]) for e in events] == [
            ("fault_raised", "P0128"), ("fault_cleared", "P0128")]
        assert events[0]["severity"] == "high"

    def test_switching_codes_is_a_clear_and_a_raise(self, measurements, vehicles, fault_codes):
        events = self.events(measurements, vehicles, fault_codes, [None, "P0128", "P0420"])
        assert [(e["event_type"], e["fault_code"]) for e in events[1:]] == [
            ("fault_cleared", "P0128"), ("fault_raised", "P0420")]

    def test_unknown_code_is_kept_and_labelled(self, measurements, vehicles, fault_codes):
        events = self.events(measurements, vehicles, fault_codes, [None, "P9999"])
        assert (events[0]["severity"], events[0]["description"]) == ("unknown", "unknown code")

    def test_fault_active_on_the_first_reading_is_not_raised(self, measurements, vehicles, fault_codes):
        assert self.events(measurements, vehicles, fault_codes, ["P0128", "P0128"]) == []


class TestGeofence:
    def test_leaving_and_returning(self, measurements, fleet_vehicles, regions):
        latitudes = [29.8, 30.0, 30.4, 30.5, 30.0]
        rows = [reading("V1", m, lat=lat) for m, lat in enumerate(latitudes)]
        df, trips = with_trips(measurements, fleet_vehicles, rows)
        events = geofence_events(df, trips, fleet_vehicles, regions).orderBy("event_ts").collect()

        assert [e["event_type"] for e in events] == ["left_geofence", "returned_to_geofence"]
        assert events[0]["distance_from_depot_km"] > 50

    def test_staying_inside_is_not_an_event(self, measurements, fleet_vehicles, regions):
        rows = [reading("V1", m, lat=29.8 + m * 0.01) for m in range(10)]
        df, trips = with_trips(measurements, fleet_vehicles, rows)
        assert geofence_events(df, trips, fleet_vehicles, regions).count() == 0


class TestRefuel:
    def test_a_fill_up_is_one_refuel(self, measurements, fleet_vehicles):
        fuel = [30.0, 29.0, 90.0, 89.0]
        rows = [reading("V1", m, fuel=f) for m, f in enumerate(fuel)]
        event = refuel_events(measurements(rows), fleet_vehicles, 5).first()

        assert (event["fuel_before_percent"], event["fuel_after_percent"]) == (29.0, 90.0)
        assert event["litres_added"] == 122.0  # 61% of 200 L

    def test_slow_pump_over_several_readings_is_still_one_refuel(self, measurements, fleet_vehicles):
        fuel = [30.0, 45.0, 60.0, 75.0, 90.0, 89.0]
        rows = [reading("V1", m, ignition=False, speed=0.0, fuel=f) for m, f in enumerate(fuel)]
        events = refuel_events(measurements(rows), fleet_vehicles, 5).collect()

        assert len(events) == 1
        assert events[0]["added_percent"] == 60.0

    def test_sloshing_is_not_a_refuel(self, measurements, fleet_vehicles):
        fuel = [50.0, 49.0, 51.0, 48.0, 49.5]
        rows = [reading("V1", m, fuel=f) for m, f in enumerate(fuel)]
        assert refuel_events(measurements(rows), fleet_vehicles, 5).count() == 0


def harsh_reading(minute, brakes=0, accels=0, odometer=None, fob="FOB-1"):
    row = list(reading("V1", minute, brakes=brakes, odometer=odometer, fob=fob))
    row[13] = accels
    return tuple(row)


class TestDriverBehavior:
    def events(self, measurements, vehicles, rows):
        df, trips = with_trips(measurements, vehicles, rows)
        return driver_behavior_events(df, trips).orderBy("event_ts", "event_type", "event_index").collect()

    def test_a_count_of_two_is_two_events(self, measurements, vehicles):
        rows = [harsh_reading(0), harsh_reading(1, brakes=2), harsh_reading(2)]
        events = self.events(measurements, vehicles, rows)

        assert [(e["event_type"], e["event_index"]) for e in events] == [("harsh_brake", 1), ("harsh_brake", 2)]
        assert len({e["event_id"] for e in events}) == 2

    def test_brakes_and_accelerations_in_one_reading(self, measurements, vehicles):
        rows = [harsh_reading(0), harsh_reading(1, brakes=1, accels=1), harsh_reading(2)]
        assert sorted(e["event_type"] for e in self.events(measurements, vehicles, rows)) == [
            "harsh_acceleration", "harsh_brake"]

    def test_events_carry_their_trip_and_driver(self, measurements, vehicles):
        rows = [harsh_reading(m, brakes=1 if m == 2 else 0) for m in range(5)]
        trip = trips_of(measurements, vehicles, rows)[0]
        event = self.events(measurements, vehicles, rows)[0]

        assert event["trip_id"] == trip["trip_id"]
        assert event["driver_fob_id"] == "FOB-1"
        assert event["timestamp_trusted"] is True

    def test_out_of_sequence_events_are_kept_without_a_trip(self, measurements, vehicles):
        """Dropping them would undercount harsh driving; their time is wrong, their driver is not."""
        rows = [harsh_reading(m) for m in range(6)]
        rows[3] = harsh_reading(3, brakes=1, odometer=1050.0, fob="FOB-2")
        event = self.events(measurements, vehicles, rows)[0]

        assert event["timestamp_trusted"] is False
        assert event["trip_id"] is None
        assert event["driver_fob_id"] == "FOB-2"

    def test_no_harsh_driving_no_rows(self, measurements, vehicles):
        assert self.events(measurements, vehicles, [harsh_reading(m) for m in range(5)]) == []


BRONZE_SCHEMA = "partition_key string, eventhub_enqueued_at timestamp, raw_payload string"


@pytest.fixture
def assignments(spark):
    from datetime import date
    return spark.createDataFrame(
        [("DEV-A", "V1", "C001", date(2025, 1, 1), None)],
        "device_id string, vehicle_id string, client_id string, valid_from date, valid_to date",
    )


def heartbeats_of(spark, assignments, arrivals):
    bronze = spark.createDataFrame(
        [(device, T0 + timedelta(minutes=minute), "{}") for device, minute in arrivals], BRONZE_SCHEMA
    )
    rows = device_heartbeats(bronze, assignments, bucket_minutes=15).orderBy("device_id", "bucket_start").collect()
    return [(r["device_id"], int((r["bucket_start"].timestamp() - T0.timestamp()) // 60), r["messages_received"],
             r["status"]) for r in rows], rows


class TestDeviceHeartbeats:
    def test_buckets_count_messages_and_show_silence(self, spark, assignments):
        summary, _ = heartbeats_of(spark, assignments, [("DEV-A", 1), ("DEV-A", 7), ("DEV-A", 50)])
        assert summary == [
            ("DEV-A", 0, 2, "reporting"),
            ("DEV-A", 15, 0, "silent"),
            ("DEV-A", 30, 0, "silent"),
            ("DEV-A", 45, 1, "reporting"),
        ]

    def test_a_device_that_stops_stays_visible_as_silent(self, spark, assignments):
        summary, _ = heartbeats_of(spark, assignments, [("DEV-A", 1), ("DEV-B", 1), ("DEV-B", 40)])
        assert [s for s in summary if s[0] == "DEV-A"] == [
            ("DEV-A", 0, 1, "reporting"), ("DEV-A", 15, 0, "silent"), ("DEV-A", 30, 0, "silent")]

    def test_a_device_is_tracked_from_its_first_message(self, spark, assignments):
        summary, _ = heartbeats_of(spark, assignments, [("DEV-A", 1), ("DEV-B", 31)])
        assert [s[1] for s in summary if s[0] == "DEV-B"] == [30]

    def test_registered_device_has_its_vehicle_unknown_one_does_not(self, spark, assignments):
        _, rows = heartbeats_of(spark, assignments, [("DEV-A", 1), ("DEV-X", 1)])
        vehicle = {r["device_id"]: r["vehicle_id"] for r in rows}
        assert vehicle == {"DEV-A": "V1", "DEV-X": None}

    def test_broken_messages_still_prove_a_device_is_alive(self, spark, assignments):
        """Every producer message here is corrupted, yet each device reports in every bucket.
        (AutoSense sends one message per three readings, so readings come every minute.)"""
        import random

        from src.producer.produce import build_items
        from src.producer.vendors import Gateway

        fleet = Fleet(select_clients(1), seed=3, glitch_fraction=0)
        gateway, rng = Gateway(random.Random(3)), random.Random(3)
        rows = []
        for tick in range(30):
            now = T0 + timedelta(minutes=tick)
            fleet.advance(60, now=now)
            items = build_items(fleet, gateway, now, late_fraction=0, malformed_fraction=1.0, rng=rng)
            rows += [(key, now, body) for key, body in items]
        heartbeats = device_heartbeats(spark.createDataFrame(rows, BRONZE_SCHEMA), assignments, bucket_minutes=15)

        assert heartbeats.filter("status = 'silent'").count() == 0
        assert heartbeats.select("device_id").distinct().count() == len(fleet.transmitting_devices)

    def test_messages_without_a_partition_key_are_ignored(self, spark, assignments):
        summary, _ = heartbeats_of(spark, assignments, [("DEV-A", 1), (None, 2)])
        assert summary == [("DEV-A", 0, 1, "reporting")]


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
                measurement_id, r["vehicle_id"], r["client_id"], r["device_id"], r["driver_fob_id"], r["event_ts"],
                r["ignition_on"], r["speed_kph"], r["latitude"], r["longitude"], r["odometer_km"],
                r["fuel_level_pct"], r["harsh_brake_count"], r["harsh_accel_count"],
                r["driver_fob_id"], r["dtc_code"],
            ))
    return spark.createDataFrame(rows, SCHEMA), backdated, moving


class TestWithProducerData:
    def test_producer_refuels_are_found(self, spark, vehicles):
        """The producer refuels below a low level to 92-100%: every such jump is one event."""
        measurements, _, _ = producer_measurements(spark, late_fraction=0.05, hours=12, step=60)
        refuels = refuel_events(measurements, vehicles, 5)

        assert refuels.count() > 0
        assert refuels.filter("fuel_after_percent < 92").count() == 0

    def test_idling_and_speeding_happen_inside_trips(self, spark, vehicles):
        measurements, _, _ = producer_measurements(spark, late_fraction=0.05)
        trips = vehicle_trips(measurements, vehicles, STOP)
        idling = idling_events(measurements, trips, vehicles, 120, STOP)
        speeding = speeding_events(measurements, trips, 120.0, 240, STOP)

        assert idling.count() > 0
        assert idling.filter("trip_id IS NULL").count() == 0
        assert speeding.filter("trip_id IS NULL").count() == 0

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
