"""Gold facts: measurable things at a clear grain, keyed to the dimensions.

  fact_trip          one row per trip
  fact_vehicle_day   one row per vehicle per local day
  fact_driver_day    one row per driver per local day

Every fact points to the version of each dimension that was valid when the
fact happened ("as was"). A driver who changes company keeps their past trips
with their old company, so past reports never change. A fact with no matching
version points to the dimension's unknown row.

Days are local days in the vehicle's region: a fleet manager's "Monday" is a
local Monday, not a UTC one.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.iot.gold.dimensions import UNKNOWN, UNKNOWN_KEY

DIMENSION_KEYS = {
    "vehicle_id": ("vehicle_key", "dim_vehicle"),
    "driver_id": ("driver_key", "dim_driver"),
    "client_id": ("client_key", "dim_client"),
    "device_id": ("device_key", "dim_device"),
}


def lookup_key(facts: DataFrame, dimension: DataFrame, natural_key: str, key: str, at: Column) -> DataFrame:
    """Add `key`: the dimension version valid at `at`, or the unknown row's key.

    valid_from is inclusive and valid_to exclusive, as in the dimensions.
    """
    versions = dimension.filter(F.col(natural_key) != UNKNOWN).select(
        F.col(natural_key).alias("_natural"),
        F.col(key).alias("_key"),
        F.col("valid_from").alias("_from"),
        F.col("valid_to").alias("_to"),
    )
    at_date = F.to_date(at)
    joined = facts.join(
        F.broadcast(versions),
        (F.col(natural_key) == F.col("_natural"))
        & (at_date >= F.col("_from"))
        & (F.col("_to").isNull() | (at_date < F.col("_to"))),
        "left",
    )
    return joined.withColumn(key, F.coalesce("_key", F.lit(UNKNOWN_KEY))).drop("_natural", "_key", "_from", "_to")


def vehicle_time_zones(vehicles: DataFrame, regions: DataFrame) -> DataFrame:
    """vehicle_id -> region and its UTC offset, from the vehicle's current version."""
    current = vehicles.filter(F.col("is_current") & (F.col("vehicle_id") != UNKNOWN))
    return current.select("vehicle_id", "region").join(
        regions.select("region", "utc_offset_hours"), "region", "left"
    )


def local_time(ts: str, offset_hours: str = "utc_offset_hours") -> Column:
    return F.col(ts) + F.make_interval(hours=F.coalesce(F.col(offset_hours), F.lit(0)).cast("int"))


def date_key(day: Column) -> Column:
    return F.date_format(day, "yyyyMMdd").cast("int")


def is_after_hours(local_ts: Column, operating_hours: Column) -> Column:
    """Outside the client's operating window, e.g. "06-20" means 06:00 to 19:59. "24x7" never is.

    A client with unknown hours is not judged.
    """
    start = F.split(operating_hours, "-").getItem(0).cast("int")
    end = F.split(operating_hours, "-").getItem(1).cast("int")
    hour = F.hour(local_ts)
    return (
        F.when(operating_hours.isNull(), F.lit(None).cast("boolean"))
        .when(operating_hours == "24x7", F.lit(False))
        .otherwise((hour < start) | (hour >= end))
    )


def _with_keys(facts: DataFrame, dimensions: dict[str, DataFrame], at: Column) -> DataFrame:
    for natural, (key, name) in DIMENSION_KEYS.items():
        if natural in facts.columns:
            facts = lookup_key(facts, dimensions[name], natural, key, at)
    return facts


def _vehicle_client_at(facts: DataFrame, vehicles: DataFrame, at: Column) -> DataFrame:
    """Add client_id: the client the vehicle belonged to on that day, from its dimension version."""
    versions = vehicles.filter(F.col("vehicle_id") != UNKNOWN).select(
        F.col("vehicle_id").alias("_vehicle"), "client_id",
        F.col("valid_from").alias("_from"), F.col("valid_to").alias("_to"),
    )
    at_date = F.to_date(at)
    return facts.join(
        F.broadcast(versions),
        (F.col("vehicle_id") == F.col("_vehicle"))
        & (at_date >= F.col("_from"))
        & (F.col("_to").isNull() | (at_date < F.col("_to"))),
        "left",
    ).drop("_vehicle", "_from", "_to")


def _fuel_price(facts: DataFrame, fuel_prices: DataFrame, day: str) -> DataFrame:
    prices = fuel_prices.select(
        F.col("region").alias("_price_region"),
        F.col("price_date").alias("_price_date"),
        F.col("price_per_litre_usd").alias("fuel_price_per_litre_usd"),
    )
    return facts.join(
        F.broadcast(prices),
        (F.col("region") == F.col("_price_region")) & (F.col(day) == F.col("_price_date")),
        "left",
    ).drop("_price_region", "_price_date")


def fact_trip(
    trips: DataFrame,
    idling: DataFrame,
    speeding: DataFrame,
    dimensions: dict[str, DataFrame],
    regions: DataFrame,
    fuel_prices: DataFrame,
) -> DataFrame:
    """One row per trip, with costs, idle and speeding totals, and the after-hours flag.

    Fuel is priced at the vehicle's region's price on the trip's local start day.
    A trip is after hours when it starts outside its client's operating hours.
    """
    idle = idling.filter(F.col("trip_id").isNotNull()).groupBy("trip_id").agg(
        F.sum("duration_seconds").alias("idle_events_seconds"),
        F.sum("fuel_wasted_l").alias("idle_fuel_l"),
    )
    fast = speeding.filter(F.col("trip_id").isNotNull()).groupBy("trip_id").agg(
        F.count("*").alias("speeding_events"),
        F.sum("duration_seconds").alias("speeding_seconds"),
    )
    zones = vehicle_time_zones(dimensions["dim_vehicle"], regions)

    rows = (
        trips.join(idle, "trip_id", "left")
        .join(fast, "trip_id", "left")
        .join(zones, "vehicle_id", "left")
        .withColumn("local_start_ts", local_time("start_ts"))
        .withColumn("local_start_date", F.to_date("local_start_ts"))
    )
    rows = _with_keys(rows, dimensions, F.col("local_start_date"))
    rows = _fuel_price(rows, fuel_prices, "local_start_date")

    hours = dimensions["dim_client"].select(
        F.col("client_key").alias("_client_key"), F.col("operating_hours")
    )
    rows = rows.join(F.broadcast(hours), F.col("client_key") == F.col("_client_key"), "left")

    return rows.select(
        "trip_id",
        "vehicle_key", "driver_key", "client_key", "device_key",
        date_key(F.col("local_start_date")).alias("start_date_key"),
        "vehicle_id", "driver_id", "client_id",
        "start_ts", "end_ts", "local_start_ts", "duration_seconds",
        "distance_km", "gps_distance_km", "fuel_used_l", "fuel_price_per_litre_usd",
        F.round(F.col("fuel_used_l") * F.col("fuel_price_per_litre_usd"), 2).alias("fuel_cost_usd"),
        "idle_seconds",
        F.coalesce("idle_fuel_l", F.lit(0.0)).alias("idle_fuel_l"),
        F.round(F.coalesce("idle_fuel_l", F.lit(0.0)) * F.col("fuel_price_per_litre_usd"), 2).alias("idle_cost_usd"),
        F.coalesce("speeding_events", F.lit(0)).alias("speeding_events"),
        F.coalesce("speeding_seconds", F.lit(0)).alias("speeding_seconds"),
        "harsh_brakes", "harsh_accelerations", "max_speed_kph", "average_speed_kph",
        is_after_hours(F.col("local_start_ts"), F.col("operating_hours")).alias("is_after_hours"),
        "is_complete", "out_of_sequence_readings",
    )


def _time_above(measurements: DataFrame, column: str, threshold: float, maximum_step_seconds: float) -> Column:
    """Seconds spent above a threshold: each reading counts until the next one, capped,
    so a device gap is not counted as time spent overheating."""
    return F.sum(
        F.when(F.col(column) > threshold, F.least(F.col("_step_seconds"), F.lit(maximum_step_seconds)))
    )


def fact_vehicle_day(
    measurements: DataFrame,
    trip_facts: DataFrame,
    diagnostics: DataFrame,
    refuels: DataFrame,
    geofence: DataFrame,
    heartbeats: DataFrame,
    dimensions: dict[str, DataFrame],
    regions: DataFrame,
    fuel_prices: DataFrame,
    warning_celsius: float,
    critical_celsius: float,
    maximum_step_seconds: float,
    heartbeat_minutes: int,
) -> DataFrame:
    """One row per vehicle per local day it reported or was expected to.

    Fault codes are counted two ways: raise events, and distinct codes. A code
    that flaps on and off raises many events but is still one problem, so
    distinct codes is the measure for "how many problems did this vehicle have".
    """
    zones = vehicle_time_zones(dimensions["dim_vehicle"], regions)

    def local_day(df: DataFrame, ts: str) -> DataFrame:
        return df.join(zones, "vehicle_id", "left").withColumn("day", F.to_date(local_time(ts)))

    by_vehicle = Window.partitionBy("vehicle_id").orderBy("event_ts", "measurement_id")
    readings = local_day(measurements, "event_ts").withColumn(
        "_step_seconds", F.lead("event_ts").over(by_vehicle).cast("double") - F.col("event_ts").cast("double")
    )
    health = readings.groupBy("vehicle_id", "day").agg(
        F.count("*").alias("readings"),
        F.max("engine_temperature_celsius").alias("max_engine_temperature_celsius"),
        F.coalesce(_time_above(readings, "engine_temperature_celsius", warning_celsius, maximum_step_seconds),
                   F.lit(0.0)).cast("long").alias("seconds_above_warning_temperature"),
        F.coalesce(_time_above(readings, "engine_temperature_celsius", critical_celsius, maximum_step_seconds),
                   F.lit(0.0)).cast("long").alias("seconds_above_critical_temperature"),
    )

    usage = trip_facts.groupBy("vehicle_id", F.to_date("local_start_ts").alias("day")).agg(
        F.count("*").alias("trips"),
        F.sum("distance_km").alias("distance_km"),
        F.sum("duration_seconds").alias("driving_seconds"),
        F.sum("idle_seconds").alias("idle_seconds"),
        F.sum("fuel_used_l").alias("fuel_used_l"),
        F.sum("fuel_cost_usd").alias("fuel_cost_usd"),
        F.sum("idle_cost_usd").alias("idle_cost_usd"),
        F.sum(F.col("is_after_hours").cast("int")).alias("after_hours_trips"),
        F.sum(F.when(F.col("is_after_hours"), F.col("distance_km"))).alias("after_hours_distance_km"),
    )
    faults = local_day(diagnostics.filter(F.col("event_type") == "fault_raised"), "event_ts").groupBy(
        "vehicle_id", "day"
    ).agg(
        F.count("*").alias("fault_raise_events"),
        F.countDistinct("fault_code").alias("distinct_fault_codes"),
        F.countDistinct(F.when(F.col("severity") == "high", F.col("fault_code"))).alias("distinct_high_severity_codes"),
    )
    fills = local_day(refuels, "end_ts").groupBy("vehicle_id", "day").agg(
        F.count("*").alias("refuels"), F.sum("litres_added").alias("litres_refuelled")
    )
    exits = local_day(geofence.filter(F.col("event_type") == "left_geofence"), "event_ts").groupBy(
        "vehicle_id", "day"
    ).agg(F.count("*").alias("geofence_exits"))
    silence = (
        local_day(heartbeats.filter(F.col("vehicle_id").isNotNull()), "bucket_start")
        .groupBy("vehicle_id", "day")
        .agg(F.sum(F.when(F.col("status") == "silent", heartbeat_minutes).otherwise(0)).alias("silent_minutes"))
    )

    days = health.select("vehicle_id", "day").unionByName(silence.select("vehicle_id", "day")).distinct()
    combined = days
    for part in (health, usage, faults, fills, exits, silence):
        combined = combined.join(part, ["vehicle_id", "day"], "left")
    combined = combined.join(zones.select("vehicle_id", "region"), "vehicle_id", "left")
    combined = _vehicle_client_at(combined, dimensions["dim_vehicle"], F.col("day"))
    combined = _with_keys(combined, dimensions, F.col("day"))
    combined = _fuel_price(combined, fuel_prices, "day")

    zero = ["trips", "distance_km", "driving_seconds", "idle_seconds", "fuel_used_l", "fuel_cost_usd",
            "idle_cost_usd", "after_hours_trips", "after_hours_distance_km", "fault_raise_events",
            "distinct_fault_codes", "distinct_high_severity_codes", "refuels", "litres_refuelled",
            "geofence_exits", "silent_minutes", "readings",
            "seconds_above_warning_temperature", "seconds_above_critical_temperature"]

    return combined.select(
        "vehicle_key", "client_key",
        date_key(F.col("day")).alias("date_key"),
        "vehicle_id", "client_id", F.col("day").alias("local_date"),
        *[F.coalesce(F.col(c), F.lit(0)).alias(c) for c in zero],
        "max_engine_temperature_celsius",
        "fuel_price_per_litre_usd",
        F.round(F.col("litres_refuelled") * F.col("fuel_price_per_litre_usd"), 2).alias("refuel_cost_usd"),
    )


def fact_driver_day(
    trip_facts: DataFrame,
    behavior: DataFrame,
    speeding: DataFrame,
    dimensions: dict[str, DataFrame],
    regions: DataFrame,
) -> DataFrame:
    """One row per driver per local day they drove.

    Readings with no fob, or an unregistered one, belong to the unknown driver,
    so their harsh driving is still counted for the vehicle and the client.
    """
    zones = vehicle_time_zones(dimensions["dim_vehicle"], regions)

    def driver_day(df: DataFrame, ts: str) -> DataFrame:
        return (
            df.join(zones, "vehicle_id", "left")
            .withColumn("day", F.to_date(local_time(ts)))
            .withColumn("driver_id", F.coalesce("driver_id", F.lit(UNKNOWN)))
        )

    driving = driver_day(trip_facts, "start_ts").groupBy("driver_id", "client_id", "day").agg(
        F.count("*").alias("trips"),
        F.sum("distance_km").alias("distance_km"),
        F.sum("duration_seconds").alias("driving_seconds"),
        F.sum("idle_seconds").alias("idle_seconds"),
        F.sum(F.col("is_after_hours").cast("int")).alias("after_hours_trips"),
    )
    harsh = driver_day(behavior, "event_ts").groupBy("driver_id", "client_id", "day").agg(
        F.sum(F.when(F.col("event_type") == "harsh_brake", 1).otherwise(0)).alias("harsh_brakes"),
        F.sum(F.when(F.col("event_type") == "harsh_acceleration", 1).otherwise(0)).alias("harsh_accelerations"),
    )
    fast = driver_day(speeding, "start_ts").groupBy("driver_id", "client_id", "day").agg(
        F.count("*").alias("speeding_events"), F.sum("duration_seconds").alias("speeding_seconds")
    )

    keys = ["driver_id", "client_id", "day"]
    days = driving.select(*keys).unionByName(harsh.select(*keys)).unionByName(fast.select(*keys)).distinct()
    combined = days.join(driving, keys, "left").join(harsh, keys, "left").join(fast, keys, "left")
    combined = _with_keys(combined, dimensions, F.col("day"))

    zero = ["trips", "distance_km", "driving_seconds", "idle_seconds", "after_hours_trips",
            "harsh_brakes", "harsh_accelerations", "speeding_events", "speeding_seconds"]
    return combined.select(
        "driver_key", "client_key",
        date_key(F.col("day")).alias("date_key"),
        "driver_id", "client_id", F.col("day").alias("local_date"),
        *[F.coalesce(F.col(c), F.lit(0)).alias(c) for c in zero],
    )