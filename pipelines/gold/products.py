"""Gold data products: one table per business question.

The serving layer: these are the tables people open, so this is where row
filters (each client sees only its own rows) and the VIN mask are attached.
The functions they use are created once per catalog by
notebooks/03_setup_gold_security.py, before this pipeline first runs.
"""

import sys

from pyspark import pipelines as dp

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot import reference as ref  # noqa: E402
from src.iot.gold import products, security  # noqa: E402

CATALOG = spark.conf.get("catalog")
SILVER = f"{CATALOG}.silver"
BRONZE_MESSAGES = f"{CATALOG}.bronze.raw_telemetry_messages"
THRESHOLDS = ref.thresholds(f"{ROOT}/reference")
BY_CLIENT = security.row_filter_clause(CATALOG)


def silver(name):
    return spark.read.table(f"{SILVER}.{name}")


@dp.materialized_view(
    comment="Which vehicles need the workshop: health status per vehicle per day. VIN masked for non-admins.",
    schema=products.VEHICLE_HEALTH_SCHEMA.format(vin_mask=security.vin_mask_clause(CATALOG)),
    row_filter=BY_CLIENT,
)
def vehicle_health():
    return products.vehicle_health(
        spark.read.table("fact_vehicle_day"),
        spark.read.table("dim_vehicle"),
        critical_seconds=THRESHOLDS["health_critical_temperature_seconds"],
        warning_seconds=THRESHOLDS["health_warning_temperature_seconds"],
    )


@dp.materialized_view(
    comment="Who drives dangerously: harsh events and speeding per 100 km, and a 0-100 safety score.",
    row_filter=BY_CLIENT,
)
def driver_safety():
    return products.driver_safety(
        spark.read.table("fact_driver_day"),
        spark.read.table("dim_driver"),
        harsh_weight=THRESHOLDS["safety_harsh_event_weight"],
        speeding_weight=THRESHOLDS["safety_speeding_minute_weight"],
        minimum_distance_km=THRESHOLDS["safety_minimum_distance"],
    )


@dp.materialized_view(
    comment="What the fleet costs and how much it is used, per client per day.",
    row_filter=BY_CLIENT,
)
def fleet_cost_utilization():
    return products.fleet_cost_utilization(spark.read.table("fact_vehicle_day"))


@dp.materialized_view(
    comment="Vehicle days worth a security review: after-hours trips, geofence exits, trips with no registered driver.",
    row_filter=BY_CLIENT,
)
def security_misuse():
    return products.security_misuse(spark.read.table("fact_vehicle_day"), spark.read.table("fact_trip"))


@dp.materialized_view(
    comment="Billable devices per client per day: devices that sent at least one message.",
    row_filter=BY_CLIENT,
)
def platform_billing():
    return products.platform_billing(
        silver("device_heartbeats"), spark.read.table("dim_vehicle"), silver("regions"), spark.read.table("dim_client")
    )


@dp.materialized_view(
    comment="Per vendor per UTC day: share of messages and of readings quarantined. Internal: for the platform team.",
)
def vendor_data_quality():
    return products.vendor_data_quality(
        spark.read.table(BRONZE_MESSAGES),
        silver("vehicle_measurements"),
        message_quarantines=[silver("quarantine_unparseable_messages"), silver("quarantine_contract_violations")],
        reading_quarantines=[silver("quarantine_invalid_values"), silver("quarantine_registry_mismatches")],
    )