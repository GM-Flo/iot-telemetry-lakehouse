"""Matching: check each reading against the registry as it was at the reading's event time.

A device only claims who it is. Matching decides what is true: a reading that
passes takes vehicle_id, client_id and driver_id from the registry, never from
the device.

  1 unknown_device          device not in the registry at all             fail
  2 inactive_device         no assignment covering the event date         fail
  3 device_vin_mismatch     reported VIN is not the fitted vehicle's VIN  fail
  4 client_vendor_mismatch  reported account is not the vehicle's client,
                            or that client does not use this vendor      fail
  5 driver_unmatched        fob is not a driver of that client            warn

Every join is a left join, so no reading is lost: each one comes out passed,
warned or failed. A stream-static join matches each reading once, against the
registry as it is when the reading is processed. Readings quarantined because
the registry was behind are fixed by replaying them, not by this join.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.iot.silver.telemetry import STATUS_FAIL, STATUS_PASS, STATUS_WARN, to_quarantine

OPEN_START = 0


def as_of_ranges(history: DataFrame, key: str) -> DataFrame:
    """SCD type 2 rows with valid_from_version / valid_to_version for point-in-time joins.

    The first version of each key is made valid from the beginning of time. The
    registry history starts at our first snapshot, but a driver who appears in it
    existed before it: no earlier snapshot is not the same as no driver.
    """
    first = F.min("__START_AT").over(Window.partitionBy(key))
    return history.withColumn(
        "valid_from_version", F.when(F.col("__START_AT") == first, F.lit(OPEN_START)).otherwise(F.col("__START_AT"))
    ).withColumn("valid_to_version", F.col("__END_AT"))


def _valid_at(version: F.Column, prefix: str) -> F.Column:
    return (version >= F.col(f"{prefix}valid_from_version")) & (
        F.col(f"{prefix}valid_to_version").isNull() | (version < F.col(f"{prefix}valid_to_version"))
    )


def match_registry(
    readings: DataFrame,
    device_assignments: DataFrame,
    clients: DataFrame,
    drivers: DataFrame,
) -> DataFrame:
    """Readings that passed validity, with registry identity and a pass, warn or fail status.

    device_assignments carries business dates (valid_from inclusive, valid_to
    exclusive). clients and drivers are SCD type 2 tables built by AUTO CDC.
    """
    passed = readings.filter(F.col("record_status") == STATUS_PASS)
    reading_columns = [c for c in passed.columns if c not in ("record_status", "validity_error", "validity_detail")]

    event_date = F.to_date("event_ts")
    event_version = F.date_format("event_ts", "yyyyMMdd").cast("int")

    known_devices = device_assignments.select("device_id").distinct().withColumn("device_known", F.lit(True))

    assignment = device_assignments.select(
        F.col("device_id").alias("a_device_id"),
        F.col("vin").alias("registered_vin"),
        F.col("vehicle_id").alias("registered_vehicle_id"),
        F.col("client_id").alias("registered_client_id"),
        F.col("valid_from").alias("a_valid_from"),
        F.col("valid_to").alias("a_valid_to"),
    )
    client = as_of_ranges(clients, "client_id").select(
        F.col("client_id").alias("c_client_id"),
        F.col("vendor_id").alias("client_vendor_id"),
        F.col("valid_from_version").alias("c_valid_from_version"),
        F.col("valid_to_version").alias("c_valid_to_version"),
    )
    driver = as_of_ranges(drivers, "driver_id").select(
        F.col("driver_id").alias("registered_driver_id"),
        F.col("client_id").alias("d_client_id"),
        F.col("fob_id").alias("d_fob_id"),
        F.col("valid_from_version").alias("d_valid_from_version"),
        F.col("valid_to_version").alias("d_valid_to_version"),
    )

    joined = (
        passed.join(F.broadcast(known_devices), "device_id", "left")
        .join(
            F.broadcast(assignment),
            (F.col("device_id") == F.col("a_device_id"))
            & (event_date >= F.col("a_valid_from"))
            & (F.col("a_valid_to").isNull() | (event_date < F.col("a_valid_to"))),
            "left",
        )
        .join(
            F.broadcast(client),
            (F.col("registered_client_id") == F.col("c_client_id")) & _valid_at(event_version, "c_"),
            "left",
        )
        .join(
            F.broadcast(driver),
            (F.col("driver_fob_id") == F.col("d_fob_id"))
            & (F.col("d_client_id") == F.col("registered_client_id"))
            & _valid_at(event_version, "d_"),
            "left",
        )
    )

    unknown = F.col("device_known").isNull()
    inactive = F.col("a_device_id").isNull()
    wrong_vin = ~F.col("reported_vin").eqNullSafe(F.col("registered_vin"))
    wrong_client = ~F.col("reported_client_id").eqNullSafe(F.col("registered_client_id")) | ~F.col(
        "client_vendor_id"
    ).eqNullSafe(F.col("vendor_id"))

    match_error = (
        F.when(unknown, F.lit("unknown_device"))
        .when(inactive, F.lit("inactive_device"))
        .when(wrong_vin, F.lit("device_vin_mismatch"))
        .when(wrong_client, F.lit("client_vendor_mismatch"))
    )
    match_detail = (
        F.when(unknown, F.format_string("device %s is not in the registry", "device_id"))
        .when(inactive, F.format_string("device %s has no assignment covering %s", F.col("device_id"), event_date.cast("string")))
        .when(wrong_vin, F.format_string("reported VIN %s, device is fitted to VIN %s", "reported_vin", "registered_vin"))
        .when(
            wrong_client,
            F.format_string(
                "reported account %s on vendor %s, vehicle belongs to %s on vendor %s",
                "reported_client_id", "vendor_id", "registered_client_id", "client_vendor_id",
            ),
        )
    )
    driver_unmatched = F.col("driver_fob_id").isNotNull() & F.col("registered_driver_id").isNull()

    status = (
        F.when(match_error.isNotNull(), F.lit(STATUS_FAIL))
        .when(driver_unmatched, F.lit(STATUS_WARN))
        .otherwise(F.lit(STATUS_PASS))
    )
    failed = match_error.isNotNull()

    return joined.select(
        *reading_columns,
        F.when(~failed, F.col("registered_vehicle_id")).alias("vehicle_id"),
        F.when(~failed, F.col("registered_client_id")).alias("client_id"),
        F.when(~failed, F.col("registered_driver_id")).alias("driver_id"),
        match_error.alias("match_error"),
        match_detail.alias("match_detail"),
        F.when(~failed & driver_unmatched, F.lit("driver_unmatched")).alias("match_warning"),
        status.alias("record_status"),
    )


def quarantine_registry_mismatches(matched: DataFrame) -> DataFrame:
    """Readings the registry does not support. Replayed once the registry catches up."""
    return to_quarantine(
        matched.filter(F.col("record_status") == STATUS_FAIL),
        stage="matching",
        reason=F.col("match_error"),
        detail=F.col("match_detail"),
        raw_payload=F.lit(None),
        measurement_id=F.col("measurement_id"),
        reading_index=F.col("reading_index"),
    )