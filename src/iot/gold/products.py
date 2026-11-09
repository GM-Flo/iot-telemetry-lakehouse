"""Gold data products: one table per business question, ready for a fleet manager.

  vehicle_health            which vehicles need the workshop?
  driver_safety             who drives dangerously?
  fleet_cost_utilization    what does the fleet cost, and is it used?
  security_misuse           is anyone misusing vehicles?
  platform_billing          what do we bill each client?
  vendor_data_quality       which vendor sends bad data?

Rules such as "how long above the critical temperature makes a vehicle
critical" come from reference/anomaly_thresholds.csv, not from this code.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.iot.gold.dimensions import UNKNOWN, UNKNOWN_KEY
from src.iot.gold.facts import local_time, vehicle_time_zones

# Spark DDL of vehicle_health with its VIN mask; {vin_mask} is filled in by the
# pipeline. A test checks it against what vehicle_health() returns, so the two
# cannot drift apart.
VEHICLE_HEALTH_SCHEMA = """
    vehicle_key STRING, client_key STRING, date_key INT, vehicle_id STRING, client_id STRING, local_date DATE,
    vin STRING {vin_mask}, make STRING, model STRING, model_year INT, health_status STRING,
    max_engine_temperature_celsius DOUBLE, minutes_above_warning_temperature DOUBLE,
    minutes_above_critical_temperature DOUBLE, distinct_fault_codes BIGINT, distinct_high_severity_codes BIGINT,
    fault_raise_events BIGINT, silent_minutes BIGINT, readings BIGINT
"""

STATUS_CRITICAL = "critical"
STATUS_ATTENTION = "attention"
STATUS_OK = "ok"


def _per_100_km(count: Column, distance_km: Column) -> Column:
    return F.when(distance_km > 0, F.round(count / distance_km * 100, 2))


def _safe_ratio(part: Column, whole: Column, digits: int = 4) -> Column:
    return F.when(whole > 0, F.round(part / whole, digits))


def vehicle_health(vehicle_days: DataFrame, vehicles: DataFrame, critical_seconds: float, warning_seconds: float) -> DataFrame:
    """One row per vehicle per day with a health status.

      critical   at least critical_seconds above the critical temperature,
                 or a high-severity fault code
      attention  at least warning_seconds above the warning temperature,
                 or any fault code
      ok         otherwise

    Fault codes are counted as distinct codes, so a flapping code is one problem.
    """
    status = (
        F.when(
            (F.col("seconds_above_critical_temperature") >= critical_seconds)
            | (F.col("distinct_high_severity_codes") > 0),
            F.lit(STATUS_CRITICAL),
        )
        .when(
            (F.col("seconds_above_warning_temperature") >= warning_seconds) | (F.col("distinct_fault_codes") > 0),
            F.lit(STATUS_ATTENTION),
        )
        .otherwise(F.lit(STATUS_OK))
    )
    names = vehicles.select("vehicle_key", "vin", "make", "model", "model_year")
    return vehicle_days.join(names, "vehicle_key", "left").select(
        "vehicle_key", "client_key", "date_key", "vehicle_id", "client_id", "local_date",
        "vin", "make", "model", "model_year",
        status.alias("health_status"),
        "max_engine_temperature_celsius",
        F.round(F.col("seconds_above_warning_temperature") / 60, 1).alias("minutes_above_warning_temperature"),
        F.round(F.col("seconds_above_critical_temperature") / 60, 1).alias("minutes_above_critical_temperature"),
        "distinct_fault_codes", "distinct_high_severity_codes", "fault_raise_events",
        "silent_minutes", "readings",
    )


def driver_safety(
    driver_days: DataFrame,
    drivers: DataFrame,
    harsh_weight: float,
    speeding_weight: float,
    minimum_distance_km: float,
) -> DataFrame:
    """One row per driver per day: harsh events and speeding normalised per 100 km, and a 0-100 score.

    Per 100 km, because a driver who drives twice as far meets twice the traffic.
    score = 100 - harsh_weight x harsh events per 100 km
                - speeding_weight x speeding minutes per 100 km, floored at 0.
    No score below minimum_distance_km: two harsh brakes in 3 km says little.
    The unknown driver is kept, so a fleet can see how much driving nobody logged in for.
    """
    harsh = F.col("harsh_brakes") + F.col("harsh_accelerations")
    speeding_minutes = F.col("speeding_seconds") / 60
    harsh_rate = _per_100_km(harsh, F.col("distance_km"))
    speeding_rate = _per_100_km(speeding_minutes, F.col("distance_km"))
    score = F.greatest(
        F.lit(0.0),
        F.lit(100.0) - harsh_weight * F.coalesce(harsh_rate, F.lit(0.0))
        - speeding_weight * F.coalesce(speeding_rate, F.lit(0.0)),
    )
    names = drivers.select("driver_key", "driver_name")

    return driver_days.join(names, "driver_key", "left").select(
        "driver_key", "client_key", "date_key", "driver_id", "client_id", "local_date",
        "driver_name",
        (F.col("driver_key") == UNKNOWN_KEY).alias("is_unknown_driver"),
        "trips", "distance_km",
        F.round(F.col("driving_seconds") / 3600, 2).alias("driving_hours"),
        "harsh_brakes", "harsh_accelerations",
        harsh_rate.alias("harsh_events_per_100_km"),
        F.round(speeding_minutes, 1).alias("speeding_minutes"),
        speeding_rate.alias("speeding_minutes_per_100_km"),
        "after_hours_trips",
        F.when(F.col("distance_km") >= minimum_distance_km, F.round(score, 1)).alias("safety_score"),
    )


def fleet_cost_utilization(vehicle_days: DataFrame) -> DataFrame:
    """One row per client per day: what the fleet cost, and how much it was used.

    A vehicle counts as active on a day it made at least one trip. Idle cost
    share is the part of the fuel bill burned standing still.
    """
    per_client = vehicle_days.filter(F.col("client_id").isNotNull()).groupBy(
        "client_key", "client_id", "date_key", "local_date"
    ).agg(
        F.countDistinct("vehicle_id").alias("vehicles_reporting"),
        F.countDistinct(F.when(F.col("trips") > 0, F.col("vehicle_id"))).alias("vehicles_active"),
        F.sum("trips").alias("trips"),
        F.round(F.sum("distance_km"), 2).alias("distance_km"),
        F.sum("driving_seconds").alias("_driving_seconds"),
        F.sum("idle_seconds").alias("_idle_seconds"),
        F.round(F.sum("fuel_used_l"), 2).alias("fuel_used_l"),
        F.round(F.sum("fuel_cost_usd"), 2).alias("fuel_cost_usd"),
        F.round(F.sum("idle_cost_usd"), 2).alias("idle_cost_usd"),
        F.round(F.sum("refuel_cost_usd"), 2).alias("refuel_cost_usd"),
    )
    return per_client.select(
        "client_key", "client_id", "date_key", "local_date",
        "vehicles_reporting", "vehicles_active",
        _safe_ratio(F.col("vehicles_active"), F.col("vehicles_reporting")).alias("share_of_vehicles_active"),
        "trips", "distance_km",
        F.round(F.col("_driving_seconds") / 3600, 2).alias("driving_hours"),
        _safe_ratio(F.col("_driving_seconds") / 3600, F.col("vehicles_active"), 2).alias("driving_hours_per_active_vehicle"),
        F.round(F.col("_idle_seconds") / 3600, 2).alias("idle_hours"),
        "fuel_used_l", "fuel_cost_usd", "idle_cost_usd", "refuel_cost_usd",
        _safe_ratio(F.col("fuel_cost_usd"), F.col("distance_km")).alias("fuel_cost_per_km_usd"),
        _safe_ratio(F.col("idle_cost_usd"), F.col("fuel_cost_usd")).alias("idle_cost_share"),
    )


def security_misuse(vehicle_days: DataFrame, trip_facts: DataFrame) -> DataFrame:
    """One row per vehicle per day with anything worth a security review.

    after-hours trips, geofence exits, and trips with no registered driver
    (nobody logged in, or an unregistered fob). needs_review is true if any of
    them happened. Days with nothing to review are left out.
    """
    unknown_driver = trip_facts.groupBy("vehicle_id", F.to_date("local_start_ts").alias("local_date")).agg(
        F.sum((F.col("driver_key") == UNKNOWN_KEY).cast("int")).alias("trips_without_registered_driver")
    )
    days = vehicle_days.join(unknown_driver, ["vehicle_id", "local_date"], "left").withColumn(
        "trips_without_registered_driver", F.coalesce("trips_without_registered_driver", F.lit(0))
    )
    review = (
        (F.col("after_hours_trips") > 0) | (F.col("geofence_exits") > 0) | (F.col("trips_without_registered_driver") > 0)
    )
    return days.filter(review).select(
        "vehicle_key", "client_key", "date_key", "vehicle_id", "client_id", "local_date",
        "after_hours_trips", F.round("after_hours_distance_km", 2).alias("after_hours_distance_km"),
        "geofence_exits", "trips_without_registered_driver",
        F.lit(True).alias("needs_review"),
    )


def platform_billing(heartbeats: DataFrame, vehicles: DataFrame, regions: DataFrame, client_keys: DataFrame) -> DataFrame:
    """One row per client per local day: devices that reported, which is what we bill.

    A device is billable on a day when at least one of its heartbeat buckets
    received a message, broken or not: it used the platform. Devices the
    registry does not know have no client and are not billed.
    """
    zones = vehicle_time_zones(vehicles, regions)
    reporting = (
        heartbeats.filter(F.col("client_id").isNotNull() & (F.col("status") == "reporting"))
        .join(zones, "vehicle_id", "left")
        .withColumn("local_date", F.to_date(local_time("bucket_start")))
    )
    billed = reporting.groupBy("client_id", "local_date").agg(
        F.countDistinct("device_id").alias("billable_devices"),
        F.sum("messages_received").alias("messages_received"),
    )
    current_client = client_keys.filter(F.col("is_current") & (F.col("client_id") != UNKNOWN)).select(
        "client_id", "client_key", "client_name", "vendor_id"
    )
    return billed.join(current_client, "client_id", "left").select(
        "client_key", "client_id", "client_name", "vendor_id",
        F.date_format("local_date", "yyyyMMdd").cast("int").alias("date_key"), "local_date",
        "billable_devices", "messages_received",
    )


def vendor_data_quality(
    bronze_messages: DataFrame,
    measurements: DataFrame,
    message_quarantines: list[DataFrame],
    reading_quarantines: list[DataFrame],
) -> DataFrame:
    """One row per vendor per UTC day: how much of what they sent we could not use.

    Two rates, because two units: parse and contract checks judge whole messages,
    validity and matching judge single readings (an AutoSense message carries
    several). Mixing them in one rate would be wrong.

    The vendor is the envelope's vendor_id, read the same way silver reads it, so
    the counts here and in quarantine agree. Messages too broken to name a vendor
    count under "unknown". Days are UTC: a vendor serves every region.
    """
    def vendor_day(df: DataFrame, vendor: Column) -> DataFrame:
        return df.select(
            F.coalesce(vendor, F.lit(UNKNOWN)).alias("vendor_id"),
            F.to_date("eventhub_enqueued_at").alias("utc_date"),
        )

    keys = ["vendor_id", "utc_date"]
    received = vendor_day(bronze_messages, F.get_json_object("raw_payload", "$.vendor_id")).groupBy(*keys).agg(
        F.count("*").alias("messages_received")
    )
    stored = vendor_day(measurements, F.col("vendor_id")).groupBy(*keys).agg(F.count("*").alias("readings_stored"))

    def quarantined(frames: list[DataFrame], name: str) -> DataFrame:
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame)
        return vendor_day(combined, F.col("vendor_id")).groupBy(*keys).agg(F.count("*").alias(name))

    messages_bad = quarantined(message_quarantines, "messages_quarantined")
    readings_bad = quarantined(reading_quarantines, "readings_quarantined")

    combined = (
        received.join(stored, keys, "full")
        .join(messages_bad, keys, "full")
        .join(readings_bad, keys, "full")
        .fillna(0, subset=["messages_received", "readings_stored", "messages_quarantined", "readings_quarantined"])
    )
    return combined.select(
        "vendor_id",
        F.date_format("utc_date", "yyyyMMdd").cast("int").alias("date_key"), "utc_date",
        "messages_received", "messages_quarantined",
        _safe_ratio(F.col("messages_quarantined"), F.col("messages_received")).alias("message_quarantine_rate"),
        "readings_stored", "readings_quarantined",
        _safe_ratio(F.col("readings_quarantined"), F.col("readings_stored") + F.col("readings_quarantined"))
        .alias("reading_quarantine_rate"),
    )