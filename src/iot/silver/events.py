"""Silver event tables: what changed, derived from vehicle_measurements.

A single reading cannot say "the engine started"; only the comparison with the
same vehicle's previous reading can. Every table here uses one of three patterns:

  change detection   compare a value with the previous reading (lag)
  gaps and islands   number consecutive readings where a condition holds,
                     then aggregate each numbered group
  bucket counting    sum what happened per time slot

These run in batch over the whole table, so a late reading is sorted into
place on the next run instead of being dropped.

Ordering by event time assumes the timestamp is right. A device whose clock
runs behind sends current values under an older timestamp, which looks like
the vehicle jumping ahead and back. flag_out_of_sequence finds those readings
by their odometer, which can only go up, and every event table leaves them out.
"""

from __future__ import annotations

from bisect import bisect_right

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0


def _by_vehicle() -> Window:
    # measurement_id breaks ties, so the order is the same on every run.
    return Window.partitionBy("vehicle_id").orderBy("event_ts", "measurement_id")


def _event_id(*parts: str | Column) -> Column:
    """Deterministic id: the same event gets the same id on every recompute."""
    return F.sha2(F.concat_ws("|", *[F.col(p) if isinstance(p, str) else p for p in parts]), 256)


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance between two points on the earth."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def longest_non_decreasing(values: list[float | None]) -> list[bool]:
    """Which values to keep so the kept ones never decrease, removing as few as possible.

    The classic longest non-decreasing subsequence, in O(n log n). Nulls are
    kept and not judged. When two answers are equally long, the one ending lower
    wins, so later readings decide between a jump ahead and the readings after it:

        100, 117, 101, 102   keeps 100, 101, 102   (117 jumped ahead)
        100, 117, 118, 101, 102, 103   keeps 100, 101, 102, 103 (two bad side by side)
        100,  95, 101        keeps 100, 101        (95 dipped)
    """
    tail_values: list[float] = []
    tail_positions: list[int] = []
    previous_in_chain = [-1] * len(values)

    for position, value in enumerate(values):
        if value is None:
            continue
        slot = bisect_right(tail_values, value)
        previous_in_chain[position] = tail_positions[slot - 1] if slot > 0 else -1
        if slot == len(tail_values):
            tail_values.append(value)
            tail_positions.append(position)
        else:
            tail_values[slot] = value
            tail_positions[slot] = position

    keep = [value is None for value in values]
    position = tail_positions[-1] if tail_positions else -1
    while position != -1:
        keep[position] = True
        position = previous_in_chain[position]
    return keep


@F.udf(ArrayType(BooleanType()))
def _keep_flags(odometers: list[float | None]) -> list[bool]:
    return longest_non_decreasing(list(odometers))


def flag_out_of_sequence(measurements: DataFrame) -> DataFrame:
    """Add out_of_sequence: readings that must be removed for the odometer to never run backwards.

    An odometer only goes up, so ordered by event time the readings should form
    a non-decreasing sequence. The largest set of readings that does is trusted;
    the rest are out of sequence. This removes as few readings as possible, and
    handles jumps ahead, dips, and several bad readings side by side.

    Readings are judged per vehicle per day, which keeps each group small at
    any table size. The cost: a bad reading just after midnight is not compared
    with the readings before midnight.
    """
    sequences = measurements.groupBy("vehicle_id", F.to_date("event_ts").alias("_day")).agg(
        F.sort_array(F.collect_list(F.struct("event_ts", "measurement_id", "odometer_km"))).alias("_readings")
    )
    flags = (
        sequences.select(
            F.col("_readings.measurement_id").alias("ids"),
            _keep_flags("_readings.odometer_km").alias("keep"),
        )
        .select(F.explode(F.arrays_zip("ids", "keep")).alias("_r"))
        .select(F.col("_r.ids").alias("measurement_id"), (~F.col("_r.keep")).alias("out_of_sequence"))
    )
    return measurements.join(flags, "measurement_id", "left").withColumn(
        "out_of_sequence", F.coalesce("out_of_sequence", F.lit(False))
    )


def _in_sequence(measurements: DataFrame) -> DataFrame:
    return flag_out_of_sequence(measurements).filter(~F.col("out_of_sequence")).drop("out_of_sequence")


def ignition_events(measurements: DataFrame) -> DataFrame:
    """One row each time a vehicle's engine switches on or off.

    Change detection: compare ignition_on with the previous reading. A vehicle's
    first reading has nothing to compare with, so it produces no event.
    Out-of-sequence readings are left out, or one would create a false switch on
    and off.
    """
    previous = F.lag("ignition_on").over(_by_vehicle())
    changes = _in_sequence(measurements).withColumn("previous_ignition_on", previous).filter(
        F.col("previous_ignition_on").isNotNull() & (F.col("ignition_on") != F.col("previous_ignition_on"))
    )
    event_type = F.when(F.col("ignition_on"), F.lit("ignition_on")).otherwise(F.lit("ignition_off"))
    return changes.select(
        _event_id("vehicle_id", F.col("event_ts").cast("string"), event_type).alias("event_id"),
        "vehicle_id",
        "client_id",
        "device_id",
        "driver_id",
        event_type.alias("event_type"),
        "event_ts",
        "latitude",
        "longitude",
        "odometer_km",
        "fuel_level_percent",
        "measurement_id",
    )


def vehicle_trips(measurements: DataFrame, vehicles: DataFrame, minimum_stop_seconds: float) -> DataFrame:
    """One row per journey.

    Gaps and islands over readings with the engine running:
      1. gap   = time since the previous running reading of the same vehicle
      2. a new trip starts when there is no previous one, or the gap is longer
         than minimum_stop_seconds (a short stop at a pump stays one trip)
      3. a running total of "new trip" flags numbers the trips
      4. each numbered group is aggregated into one row

    A trip is complete once the vehicle has been seen minimum_stop_seconds past
    its end. Until then it may still continue, and its row can change.

    Out-of-sequence readings are left out of every measure and counted in
    out_of_sequence_readings, so nothing is hidden.
    """
    flagged = flag_out_of_sequence(measurements)
    running = flagged.filter(F.col("ignition_on") & ~F.col("out_of_sequence"))
    window = _by_vehicle()

    previous_ts = F.lag("event_ts").over(window)
    gap_seconds = F.col("event_ts").cast("double") - previous_ts.cast("double")
    new_trip = previous_ts.isNull() | (gap_seconds > minimum_stop_seconds)

    # Distance and time to the next running reading in the same trip, for GPS
    # distance and idle time. The last reading of a trip contributes nothing.
    numbered = (
        running.withColumn("gap_seconds", gap_seconds)
        .withColumn("trip_number", F.sum(new_trip.cast("int")).over(window.rowsBetween(Window.unboundedPreceding, 0)))
    )
    in_trip = Window.partitionBy("vehicle_id", "trip_number").orderBy("event_ts", "measurement_id")
    steps = (
        numbered.withColumn("next_ts", F.lead("event_ts").over(in_trip))
        .withColumn("next_latitude", F.lead("latitude").over(in_trip))
        .withColumn("next_longitude", F.lead("longitude").over(in_trip))
        .withColumn("next_fuel_percent", F.lead("fuel_level_percent").over(in_trip))
        .withColumn("step_seconds", F.col("next_ts").cast("double") - F.col("event_ts").cast("double"))
        .withColumn(
            "step_km",
            F.coalesce(
                haversine_km(F.col("latitude"), F.col("longitude"), F.col("next_latitude"), F.col("next_longitude")),
                F.lit(0.0),
            ),
        )
        # Only drops count as fuel used; a rise is a refuel, which must not cancel them out.
        .withColumn(
            "step_fuel_used_percent",
            F.greatest(F.col("fuel_level_percent") - F.col("next_fuel_percent"), F.lit(0.0)),
        )
    )

    trips = steps.groupBy("vehicle_id", "trip_number").agg(
        F.first("client_id").alias("client_id"),
        F.min_by("device_id", "event_ts").alias("device_id"),
        F.min_by("driver_id", "event_ts").alias("driver_id"),
        F.min("event_ts").alias("start_ts"),
        F.max("event_ts").alias("end_ts"),
        F.count("*").alias("readings"),
        F.min_by("latitude", "event_ts").alias("start_latitude"),
        F.min_by("longitude", "event_ts").alias("start_longitude"),
        F.max_by("latitude", "event_ts").alias("end_latitude"),
        F.max_by("longitude", "event_ts").alias("end_longitude"),
        F.min_by("odometer_km", "event_ts").alias("start_odometer_km"),
        F.max_by("odometer_km", "event_ts").alias("end_odometer_km"),
        F.sum("step_km").alias("gps_distance_km"),
        F.coalesce(F.sum("step_fuel_used_percent"), F.lit(0.0)).alias("fuel_used_percent"),
        F.max("speed_kph").alias("max_speed_kph"),
        F.coalesce(F.sum(F.when(F.col("speed_kph") == 0, F.col("step_seconds"))), F.lit(0.0)).alias("idle_seconds"),
        F.sum("harsh_brake_count").alias("harsh_brakes"),
        F.sum("harsh_accel_count").alias("harsh_accelerations"),
    )

    last_seen = measurements.groupBy("vehicle_id").agg(F.max("event_ts").alias("vehicle_last_seen"))

    kept_trips = trips.filter(F.col("readings") >= 2)
    suspicious = flagged.filter(F.col("out_of_sequence")).select(
        F.col("vehicle_id").alias("s_vehicle_id"), F.col("event_ts").alias("s_event_ts")
    )
    skipped = (
        kept_trips.select("vehicle_id", "trip_number", "start_ts", "end_ts")
        .join(
            suspicious,
            (F.col("s_vehicle_id") == F.col("vehicle_id"))
            & F.col("s_event_ts").between(F.col("start_ts"), F.col("end_ts")),
        )
        .groupBy("vehicle_id", "trip_number")
        .agg(F.count("*").alias("out_of_sequence_readings"))
    )
    tank = vehicles.filter(F.col("__END_AT").isNull()).select("vehicle_id", "tank_capacity_l")

    duration = F.col("end_ts").cast("double") - F.col("start_ts").cast("double")
    distance = F.round(F.col("end_odometer_km") - F.col("start_odometer_km"), 3)

    return (
        kept_trips.join(skipped, ["vehicle_id", "trip_number"], "left")
        .join(last_seen, "vehicle_id", "left")
        .join(tank, "vehicle_id", "left")
        .select(
            _event_id("vehicle_id", F.col("start_ts").cast("string")).alias("trip_id"),
            "vehicle_id", "client_id", "device_id", "driver_id",
            "start_ts", "end_ts",
            duration.cast("long").alias("duration_seconds"),
            "start_latitude", "start_longitude", "end_latitude", "end_longitude",
            distance.alias("distance_km"),
            F.round("gps_distance_km", 3).alias("gps_distance_km"),
            F.round(F.col("fuel_used_percent") / 100 * F.col("tank_capacity_l"), 2).alias("fuel_used_l"),
            "max_speed_kph",
            F.when(duration > 0, F.round(distance / (duration / 3600), 1)).alias("average_speed_kph"),
            F.col("idle_seconds").cast("long").alias("idle_seconds"),
            "harsh_brakes", "harsh_accelerations", "readings",
            F.coalesce("out_of_sequence_readings", F.lit(0)).alias("out_of_sequence_readings"),
            (F.col("vehicle_last_seen").cast("double") >= F.col("end_ts").cast("double") + minimum_stop_seconds)
            .alias("is_complete"),
        )
    )