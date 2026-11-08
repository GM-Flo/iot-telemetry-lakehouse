"""Silver event tables, derived from vehicle_measurements.

Materialized views: recomputed on each run, so late readings are sorted into
place rather than dropped. Thresholds come from reference/anomaly_thresholds.csv.
"""

import sys

from pyspark import pipelines as dp

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot import reference as ref  # noqa: E402
from src.iot.silver import events as ev  # noqa: E402

CATALOG = spark.conf.get("catalog")
MEASUREMENTS = f"{CATALOG}.silver.vehicle_measurements"
VEHICLES = f"{CATALOG}.silver.vehicles"
REGIONS = f"{CATALOG}.silver.regions"
FAULT_CODES = f"{CATALOG}.reference.fault_codes"
DEVICE_ASSIGNMENTS = f"{CATALOG}.silver.device_assignments"
BRONZE_MESSAGES = f"{CATALOG}.bronze.raw_telemetry_messages"
THRESHOLDS = ref.thresholds(f"{ROOT}/reference")


@dp.materialized_view(comment="One row each time a vehicle's engine switches on or off.")
def vehicle_ignition_events():
    return ev.ignition_events(spark.read.table(MEASUREMENTS))


@dp.materialized_view(
    comment="One row per journey: duration, distance by odometer and GPS, fuel used, idle time, harsh events. "
    "is_complete is false while the trip may still continue.",
)
@dp.expect("ends_after_it_starts", "end_ts > start_ts")
@dp.expect("odometer_does_not_run_backwards", "distance_km >= 0")
def vehicle_trips():
    return ev.vehicle_trips(
        spark.read.table(MEASUREMENTS),
        spark.read.table(VEHICLES),
        THRESHOLDS["minimum_stop_between_trips"],
    )


@dp.materialized_view(comment="One row per idle period: engine on, stationary, at least the minimum idle duration.")
@dp.expect("long_enough", f"duration_seconds >= {THRESHOLDS['minimum_idle_duration']}")
def vehicle_idling_events():
    return ev.idling_events(
        spark.read.table(MEASUREMENTS),
        spark.read.table("vehicle_trips"),
        spark.read.table(VEHICLES),
        minimum_idle_seconds=THRESHOLDS["minimum_idle_duration"],
        maximum_gap_seconds=THRESHOLDS["minimum_stop_between_trips"],
    )


@dp.materialized_view(comment="One row per sustained overspeed: above the speed limit for at least the minimum duration.")
@dp.expect("above_the_limit", "max_speed_kph > speed_limit_kph")
def vehicle_speeding_events():
    return ev.speeding_events(
        spark.read.table(MEASUREMENTS),
        spark.read.table("vehicle_trips"),
        speed_limit_kph=THRESHOLDS["speeding_limit"],
        minimum_speeding_seconds=THRESHOLDS["minimum_speeding_duration"],
        maximum_gap_seconds=THRESHOLDS["minimum_stop_between_trips"],
    )


@dp.materialized_view(comment="One row each time a different driver's fob takes over a vehicle.")
def driver_change_events():
    return ev.driver_change_events(spark.read.table(MEASUREMENTS), spark.read.table("vehicle_trips"))


@dp.materialized_view(comment="One row each time a fault code appears or clears, with its description and severity.")
@dp.expect("known_fault_code", "severity <> 'unknown'")
def vehicle_diagnostic_events():
    return ev.diagnostic_events(
        spark.read.table(MEASUREMENTS), spark.read.table("vehicle_trips"), spark.read.table(FAULT_CODES)
    )


@dp.materialized_view(comment="One row each time a vehicle leaves its region's geofence, or returns to it.")
def geofence_events():
    return ev.geofence_events(
        spark.read.table(MEASUREMENTS),
        spark.read.table("vehicle_trips"),
        spark.read.table(VEHICLES),
        spark.read.table(REGIONS),
    )


@dp.materialized_view(comment="One row per refuel: fuel level before and after, litres added.")
@dp.expect("adds_fuel", "litres_added > 0")
def vehicle_refuel_events():
    return ev.refuel_events(
        spark.read.table(MEASUREMENTS),
        spark.read.table(VEHICLES),
        minimum_increase_percent=THRESHOLDS["minimum_refuel_increase"],
    )


@dp.materialized_view(
    comment="One row per harsh brake or harsh acceleration. Events from readings with an untrusted timestamp "
    "are kept, with timestamp_trusted = false and no trip_id.",
)
def driver_behavior_events():
    return ev.driver_behavior_events(spark.read.table(MEASUREMENTS), spark.read.table("vehicle_trips"))


@dp.materialized_view(
    comment="One row per device per time bucket: messages received from bronze, including broken ones, or silent.",
)
def device_heartbeats():
    return ev.device_heartbeats(
        spark.read.table(BRONZE_MESSAGES),
        spark.read.table(DEVICE_ASSIGNMENTS),
        bucket_minutes=int(THRESHOLDS["device_silent_after"]),
    )