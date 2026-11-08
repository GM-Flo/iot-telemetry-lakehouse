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