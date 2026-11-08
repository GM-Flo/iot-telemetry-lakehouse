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
        F.sort_array(F.collect_list(F.struct("event_ts", "measurement_id", "device_id", "odometer_km")))
        .alias("_readings")
    )
    # Flags go back to readings by (device_id, event_ts), the key AUTO CDC keeps
    # unique in vehicle_measurements. measurement_id comes from the vendor's
    # message id, and nothing outside our control is guaranteed unique.
    flags = (
        sequences.select(
            F.col("_readings.device_id").alias("devices"),
            F.col("_readings.event_ts").alias("times"),
            _keep_flags("_readings.odometer_km").alias("keep"),
        )
        .select(F.explode(F.arrays_zip("devices", "times", "keep")).alias("_r"))
        .select(
            F.col("_r.devices").alias("device_id"),
            F.col("_r.times").alias("event_ts"),
            (~F.col("_r.keep")).alias("out_of_sequence"),
        )
    )
    return measurements.join(flags, ["device_id", "event_ts"], "left").withColumn(
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


def _condition_periods(measurements: DataFrame, condition: Column, maximum_gap_seconds: float) -> DataFrame:
    """Gaps and islands for any condition: one row per unbroken period where it holds.

    A period starts at a reading where the condition holds and either did not
    hold at the previous reading, or the previous reading is more than
    maximum_gap_seconds back (the device was silent, so we cannot say what
    happened in between). The duration runs from the first to the last reading
    of the period, so it never claims more than was observed.
    """
    window = _by_vehicle()
    readings = _in_sequence(measurements).withColumn("_holds", F.coalesce(condition, F.lit(False)))

    previous_holds = F.lag("_holds").over(window)
    gap = F.col("event_ts").cast("double") - F.lag("event_ts").over(window).cast("double")
    starts = F.col("_holds") & (previous_holds.isNull() | ~previous_holds | (gap > maximum_gap_seconds))

    numbered = readings.withColumn(
        "_period", F.sum(starts.cast("int")).over(window.rowsBetween(Window.unboundedPreceding, 0))
    ).filter(F.col("_holds"))

    in_period = Window.partitionBy("vehicle_id", "_period").orderBy("event_ts", "measurement_id")
    steps = numbered.withColumn(
        "_fuel_drop_percent",
        F.greatest(F.col("fuel_level_percent") - F.lead("fuel_level_percent").over(in_period), F.lit(0.0)),
    )

    periods = steps.groupBy("vehicle_id", "_period").agg(
        F.first("client_id").alias("client_id"),
        F.min_by("device_id", "event_ts").alias("device_id"),
        F.min_by("driver_id", "event_ts").alias("driver_id"),
        F.min("event_ts").alias("start_ts"),
        F.max("event_ts").alias("end_ts"),
        F.min_by("latitude", "event_ts").alias("latitude"),
        F.min_by("longitude", "event_ts").alias("longitude"),
        F.count("*").alias("readings"),
        F.max("speed_kph").alias("max_speed_kph"),
        F.round(F.avg("speed_kph"), 1).alias("average_speed_kph"),
        F.coalesce(F.sum("_fuel_drop_percent"), F.lit(0.0)).alias("fuel_drop_percent"),
    )
    return periods.withColumn(
        "duration_seconds", (F.col("end_ts").cast("double") - F.col("start_ts").cast("double")).cast("long")
    )


def _with_trip(periods: DataFrame, trips: DataFrame, at: str = "start_ts") -> DataFrame:
    """Attach the trip a row happened in, by its `at` time. A row outside any trip keeps a null trip_id."""
    trip_ranges = trips.select(
        F.col("vehicle_id").alias("t_vehicle_id"),
        "trip_id",
        F.col("start_ts").alias("t_start_ts"),
        F.col("end_ts").alias("t_end_ts"),
    )
    return periods.join(
        trip_ranges,
        (F.col("vehicle_id") == F.col("t_vehicle_id"))
        & F.col(at).between(F.col("t_start_ts"), F.col("t_end_ts")),
        "left",
    ).drop("t_vehicle_id", "t_start_ts", "t_end_ts")


def idling_events(
    measurements: DataFrame,
    trips: DataFrame,
    vehicles: DataFrame,
    minimum_idle_seconds: float,
    maximum_gap_seconds: float,
) -> DataFrame:
    """One row per idle period: engine running, vehicle stationary, for at least minimum_idle_seconds.

    fuel_wasted_l counts only drops in fuel level during the period, converted
    with the vehicle's current tank size.
    """
    idle = F.col("ignition_on") & (F.col("speed_kph") == 0)
    periods = _condition_periods(measurements, idle, maximum_gap_seconds).filter(
        F.col("duration_seconds") >= minimum_idle_seconds
    )
    tank = vehicles.filter(F.col("__END_AT").isNull()).select("vehicle_id", "tank_capacity_l")

    return _with_trip(periods, trips).join(tank, "vehicle_id", "left").select(
        _event_id("vehicle_id", F.col("start_ts").cast("string"), F.lit("idling")).alias("event_id"),
        "trip_id", "vehicle_id", "client_id", "device_id", "driver_id",
        "start_ts", "end_ts", "duration_seconds", "latitude", "longitude",
        F.round(F.col("fuel_drop_percent") / 100 * F.col("tank_capacity_l"), 2).alias("fuel_wasted_l"),
        "readings",
    )


def speeding_events(
    measurements: DataFrame,
    trips: DataFrame,
    speed_limit_kph: float,
    minimum_speeding_seconds: float,
    maximum_gap_seconds: float,
) -> DataFrame:
    """One row per sustained overspeed: above speed_limit_kph for at least minimum_speeding_seconds.

    A short burst, such as overtaking, is not an event: this table can lead to a
    driver being warned, so it only records sustained speeding.
    """
    over = F.col("speed_kph") > speed_limit_kph
    periods = _condition_periods(measurements, over, maximum_gap_seconds).filter(
        F.col("duration_seconds") >= minimum_speeding_seconds
    )
    return _with_trip(periods, trips).select(
        _event_id("vehicle_id", F.col("start_ts").cast("string"), F.lit("speeding")).alias("event_id"),
        "trip_id", "vehicle_id", "client_id", "device_id", "driver_id",
        "start_ts", "end_ts", "duration_seconds", "latitude", "longitude",
        F.lit(speed_limit_kph).alias("speed_limit_kph"),
        "max_speed_kph", "average_speed_kph",
        F.round(F.col("max_speed_kph") - speed_limit_kph, 1).alias("max_over_limit_kph"),
        "readings",
    )


def _has_previous(window: Window) -> Column:
    return F.lag("event_ts").over(window).isNotNull()


def driver_change_events(measurements: DataFrame, trips: DataFrame) -> DataFrame:
    """One row each time a different driver's fob takes over a vehicle.

    The fob is only reported while the engine runs, so a gap with no fob is not
    a change. The comparison is with the last fob seen on this vehicle, however
    long ago: the same driver restarting after lunch is not a change.
    An unregistered fob still counts, with a null driver_id.
    """
    window = _by_vehicle()
    earlier = window.rowsBetween(Window.unboundedPreceding, -1)
    readings = _in_sequence(measurements).select(
        "*",
        F.last("driver_fob_id", ignorenulls=True).over(earlier).alias("previous_driver_fob_id"),
        F.last("driver_id", ignorenulls=True).over(earlier).alias("previous_driver_id"),
    )
    changes = readings.filter(
        F.col("driver_fob_id").isNotNull()
        & F.col("previous_driver_fob_id").isNotNull()
        & (F.col("driver_fob_id") != F.col("previous_driver_fob_id"))
    )
    return _with_trip(changes, trips, at="event_ts").select(
        _event_id("vehicle_id", F.col("event_ts").cast("string"), F.lit("driver_change")).alias("event_id"),
        "trip_id", "vehicle_id", "client_id", "device_id", "event_ts",
        "previous_driver_fob_id", "previous_driver_id",
        F.col("driver_fob_id").alias("new_driver_fob_id"),
        F.col("driver_id").alias("new_driver_id"),
        "latitude", "longitude",
    )


def diagnostic_events(measurements: DataFrame, trips: DataFrame, fault_codes: DataFrame) -> DataFrame:
    """One row each time a fault code appears or clears.

    A change from one code straight to another is two rows: the old one clears
    and the new one appears. A vehicle's first reading is not compared with
    anything, so a fault already active then has no "raised" row.
    """
    window = _by_vehicle()
    readings = _in_sequence(measurements).select(
        "*",
        F.lag("fault_code").over(window).alias("previous_fault_code"),
        _has_previous(window).alias("_has_previous"),
    ).filter(F.col("_has_previous") & ~F.col("fault_code").eqNullSafe(F.col("previous_fault_code")))

    raised = readings.filter(F.col("fault_code").isNotNull()).select(
        "*", F.lit("fault_raised").alias("event_type"), F.col("fault_code").alias("code")
    )
    cleared = readings.filter(F.col("previous_fault_code").isNotNull()).select(
        "*", F.lit("fault_cleared").alias("event_type"), F.col("previous_fault_code").alias("code")
    )
    codes = fault_codes.select(F.col("code"), "description", "severity")

    return (
        _with_trip(raised.unionByName(cleared), trips, at="event_ts")
        .join(F.broadcast(codes), "code", "left")
        .select(
            _event_id("vehicle_id", F.col("event_ts").cast("string"), "event_type", "code").alias("event_id"),
            "trip_id", "vehicle_id", "client_id", "device_id", "driver_id", "event_ts", "event_type",
            F.col("code").alias("fault_code"),
            F.coalesce("description", F.lit("unknown code")).alias("description"),
            F.coalesce("severity", F.lit("unknown")).alias("severity"),
            "latitude", "longitude", "odometer_km",
        )
    )


def geofence_events(measurements: DataFrame, trips: DataFrame, vehicles: DataFrame, regions: DataFrame) -> DataFrame:
    """One row each time a vehicle leaves its region's geofence, or comes back.

    The geofence is a circle around the region's depot. Each vehicle is judged
    against the region it belongs to now.
    """
    fence = (
        vehicles.filter(F.col("__END_AT").isNull())
        .select("vehicle_id", "region")
        .join(regions.select("region", "depot_latitude", "depot_longitude", "geofence_radius_km"), "region")
    )
    window = _by_vehicle()
    distance = haversine_km(F.col("latitude"), F.col("longitude"), F.col("depot_latitude"), F.col("depot_longitude"))
    readings = (
        _in_sequence(measurements)
        .join(F.broadcast(fence), "vehicle_id")
        .withColumn("distance_from_depot_km", F.round(distance, 2))
        .withColumn("_inside", F.col("distance_from_depot_km") <= F.col("geofence_radius_km"))
    )
    changes = readings.withColumn("_was_inside", F.lag("_inside").over(window)).filter(
        F.col("_was_inside").isNotNull() & (F.col("_inside") != F.col("_was_inside"))
    )
    event_type = F.when(F.col("_inside"), F.lit("returned_to_geofence")).otherwise(F.lit("left_geofence"))

    return _with_trip(changes, trips, at="event_ts").select(
        _event_id("vehicle_id", F.col("event_ts").cast("string"), event_type).alias("event_id"),
        "trip_id", "vehicle_id", "client_id", "device_id", "driver_id", "event_ts",
        event_type.alias("event_type"),
        "region", "geofence_radius_km", "distance_from_depot_km", "latitude", "longitude",
    )


def refuel_events(measurements: DataFrame, vehicles: DataFrame, minimum_increase_percent: float) -> DataFrame:
    """One row per refuel: the fuel level rising by at least minimum_increase_percent.

    Consecutive rising readings are one refuel, so a pump that fills over several
    readings is counted once. Smaller rises are sloshing or sensor noise.
    """
    window = _by_vehicle()
    readings = _in_sequence(measurements).select(
        "*",
        F.lag("fuel_level_percent").over(window).alias("_previous_fuel"),
        F.lag("event_ts").over(window).alias("_previous_ts"),
    )
    rising = readings.withColumn(
        "_rising", F.coalesce(F.col("fuel_level_percent") > F.col("_previous_fuel"), F.lit(False))
    )
    previous_rising = F.lag("_rising").over(window)
    starts = F.col("_rising") & (previous_rising.isNull() | ~previous_rising)

    fills = (
        rising.withColumn("_fill", F.sum(starts.cast("int")).over(window.rowsBetween(Window.unboundedPreceding, 0)))
        .filter(F.col("_rising"))
        .groupBy("vehicle_id", "_fill")
        .agg(
            F.first("client_id").alias("client_id"),
            F.min_by("device_id", "event_ts").alias("device_id"),
            F.min_by("driver_id", "event_ts").alias("driver_id"),
            F.min("_previous_ts").alias("start_ts"),
            F.max("event_ts").alias("end_ts"),
            F.min_by("_previous_fuel", "event_ts").alias("fuel_before_percent"),
            F.max_by("fuel_level_percent", "event_ts").alias("fuel_after_percent"),
            F.max_by("latitude", "event_ts").alias("latitude"),
            F.max_by("longitude", "event_ts").alias("longitude"),
        )
        .withColumn("added_percent", F.round(F.col("fuel_after_percent") - F.col("fuel_before_percent"), 2))
        .filter(F.col("added_percent") >= minimum_increase_percent)
    )
    tank = vehicles.filter(F.col("__END_AT").isNull()).select("vehicle_id", "tank_capacity_l")

    return fills.join(tank, "vehicle_id", "left").select(
        _event_id("vehicle_id", F.col("end_ts").cast("string"), F.lit("refuel")).alias("event_id"),
        "vehicle_id", "client_id", "device_id", "driver_id", "start_ts", "end_ts",
        "fuel_before_percent", "fuel_after_percent", "added_percent",
        F.round(F.col("added_percent") / 100 * F.col("tank_capacity_l"), 2).alias("litres_added"),
        F.to_date("end_ts").alias("refuel_date"),
        "latitude", "longitude",
    )


HARSH_EVENT_TYPES = {"harsh_brake": "harsh_brake_count", "harsh_acceleration": "harsh_accel_count"}


def driver_behavior_events(measurements: DataFrame, trips: DataFrame) -> DataFrame:
    """One row per harsh brake or harsh acceleration.

    Each reading counts the harsh events since the device's previous reading, so
    a reading with a count of 2 becomes two rows (event_index 1 and 2).

    Unlike the other tables, out-of-sequence readings are kept: their counts are
    real events, and dropping them would undercount a driver's harsh driving.
    The reading's time is not trustworthy, so such an event gets no trip_id and
    is marked timestamp_trusted = false. Its driver is still right: the fob
    comes with the reading's values, not its timestamp.
    """
    flagged = flag_out_of_sequence(measurements)
    per_type = [
        flagged.filter(F.col(column) > 0).select(
            "*",
            F.lit(event_type).alias("event_type"),
            F.explode(F.sequence(F.lit(1), F.col(column))).alias("event_index"),
        )
        for event_type, column in HARSH_EVENT_TYPES.items()
    ]
    events = per_type[0].unionByName(per_type[1])

    trusted = events.filter(~F.col("out_of_sequence"))
    untrusted = events.filter(F.col("out_of_sequence")).withColumn("trip_id", F.lit(None).cast("string"))
    with_trips = _with_trip(trusted, trips, at="event_ts").unionByName(untrusted)

    return with_trips.select(
        _event_id(
            "device_id", F.col("event_ts").cast("string"), "event_type", F.col("event_index").cast("string")
        ).alias("event_id"),
        "trip_id", "vehicle_id", "client_id", "device_id", "driver_id", "driver_fob_id",
        "event_ts", "event_type", "event_index", "speed_kph", "latitude", "longitude",
        (~F.col("out_of_sequence")).alias("timestamp_trusted"),
        "measurement_id",
    )


def _bucket_start(ts: Column, bucket_seconds: int) -> Column:
    epoch = F.unix_timestamp(ts)
    return F.timestamp_seconds(epoch - F.pmod(epoch, F.lit(bucket_seconds)))


def device_heartbeats(bronze_messages: DataFrame, device_assignments: DataFrame, bucket_minutes: int) -> DataFrame:
    """One row per device per time bucket: how many messages arrived, or silent.

    Built from bronze, not silver, because a device that sends only broken
    messages is still alive: liveness counts every message, valid or not. The
    device comes from the Event Hubs partition key, which the gateway sets to the
    device id on every message, including ones too broken to parse.

    Buckets run on arrival time, from each device's first message to the newest
    message of any device. A bucket with no message from a device is a silent
    row, so a device that stops reporting shows up instead of just disappearing.
    """
    bucket_seconds = bucket_minutes * 60
    arrivals = bronze_messages.filter(F.col("partition_key").isNotNull()).select(
        F.col("partition_key").alias("device_id"),
        _bucket_start(F.col("eventhub_enqueued_at"), bucket_seconds).alias("bucket_start"),
    )
    counts = arrivals.groupBy("device_id", "bucket_start").agg(F.count("*").alias("messages_received"))

    newest = arrivals.agg(F.max("bucket_start").alias("newest_bucket"))
    spine = (
        arrivals.groupBy("device_id")
        .agg(F.min("bucket_start").alias("first_bucket"))
        .crossJoin(newest)
        .select(
            "device_id",
            F.explode(
                F.sequence("first_bucket", "newest_bucket", F.expr(f"INTERVAL {bucket_minutes} MINUTES"))
            ).alias("bucket_start"),
        )
    )

    bucket_date = F.to_date("bucket_start")
    assignment = device_assignments.select(
        F.col("device_id").alias("a_device_id"), "vehicle_id", "client_id",
        F.col("valid_from").alias("a_valid_from"), F.col("valid_to").alias("a_valid_to"),
    )
    messages = F.coalesce(F.col("messages_received"), F.lit(0))

    return (
        spine.join(counts, ["device_id", "bucket_start"], "left")
        .join(
            F.broadcast(assignment),
            (F.col("device_id") == F.col("a_device_id"))
            & (bucket_date >= F.col("a_valid_from"))
            & (F.col("a_valid_to").isNull() | (bucket_date < F.col("a_valid_to"))),
            "left",
        )
        .select(
            "device_id", "vehicle_id", "client_id", "bucket_start",
            (F.col("bucket_start") + F.expr(f"INTERVAL {bucket_minutes} MINUTES")).alias("bucket_end"),
            messages.alias("messages_received"),
            F.when(messages > 0, F.lit("reporting")).otherwise(F.lit("silent")).alias("status"),
        )
    )