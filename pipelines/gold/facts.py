"""Gold facts, rebuilt from silver events and the gold dimensions on every run."""

import sys

from pyspark import pipelines as dp

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot import reference as ref  # noqa: E402
from src.iot.gold import facts  # noqa: E402

CATALOG = spark.conf.get("catalog")
SILVER = f"{CATALOG}.silver"
THRESHOLDS = ref.thresholds(f"{ROOT}/reference")


def silver(name):
    return spark.read.table(f"{SILVER}.{name}")


def dimensions():
    return {name: spark.read.table(name) for name in ("dim_vehicle", "dim_driver", "dim_client", "dim_device")}


@dp.materialized_view(comment="One row per trip: costs, idle and speeding totals, after-hours flag. Keys point "
                      "to the dimension versions valid on the trip's local start day.")
@dp.expect("priced", "fuel_price_per_litre_usd IS NOT NULL")
@dp.expect("known_vehicle", "vehicle_id IS NOT NULL")
def fact_trip():
    return facts.fact_trip(
        silver("vehicle_trips"),
        silver("vehicle_idling_events"),
        silver("vehicle_speeding_events"),
        dimensions(),
        silver("regions"),
        silver("fuel_prices"),
    )


@dp.materialized_view(comment="One row per vehicle per local day: usage, cost, engine health, faults, "
                      "refuels, geofence exits, silent minutes.")
def fact_vehicle_day():
    return facts.fact_vehicle_day(
        silver("vehicle_measurements"),
        spark.read.table("fact_trip"),
        silver("vehicle_diagnostic_events"),
        silver("vehicle_refuel_events"),
        silver("geofence_events"),
        silver("device_heartbeats"),
        dimensions(),
        silver("regions"),
        silver("fuel_prices"),
        warning_celsius=THRESHOLDS["engine_temperature_warning"],
        critical_celsius=THRESHOLDS["engine_temperature_critical"],
        maximum_step_seconds=THRESHOLDS["minimum_stop_between_trips"],
        heartbeat_minutes=int(THRESHOLDS["device_silent_after"]),
    )


@dp.materialized_view(comment="One row per driver per local day: distance, driving time, harsh driving, "
                      "speeding, after-hours trips. Unregistered or missing fobs belong to the unknown driver.")
def fact_driver_day():
    return facts.fact_driver_day(
        spark.read.table("fact_trip"),
        silver("driver_behavior_events"),
        silver("vehicle_speeding_events"),
        dimensions(),
        silver("regions"),
    )