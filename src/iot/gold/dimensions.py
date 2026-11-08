"""Gold dimensions: who, what and when, with history, keyed for analytics.

Surrogate keys are hashes of the natural key and the version's start, not
identity numbers. Gold tables are rebuilt on every run, and a hash gives the
same key for the same data every time; an identity number could change, and
silently point facts and saved reports at the wrong row.

Each dimension has one "unknown" row. A fact that cannot be matched, such as
an unregistered driver's harsh brake, points to it rather than to null, so no
fact is lost when joined to its dimensions.
"""

from __future__ import annotations

import hashlib
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

UNKNOWN = "unknown"
# The registry's history starts at our first snapshot, but everything in it
# existed before: the first version of each row is valid from the beginning.
BEGINNING = date(1900, 1, 1)


def surrogate_key(*parts: str | Column) -> Column:
    return F.sha2(F.concat_ws("|", *[F.col(p) if isinstance(p, str) else p for p in parts]), 256)


# Computed in Python, so it exists before any Spark session does. It equals
# surrogate_key(lit("unknown")): sha2 of the same text.
UNKNOWN_KEY = hashlib.sha256(UNKNOWN.encode()).hexdigest()


def _version_date(column: str) -> Column:
    return F.to_date(F.col(column).cast("string"), "yyyyMMdd")


def _version_ranges(history: DataFrame, key: str) -> DataFrame:
    """SCD type 2 rows from AUTO CDC, with valid_from / valid_to as dates and is_current.

    __START_AT and __END_AT are snapshot versions (yyyymmdd). valid_to is
    exclusive and null for the current version.
    """
    first = F.min("__START_AT").over(Window.partitionBy(key))
    return history.select(
        "*",
        F.when(F.col("__START_AT") == first, F.lit(BEGINNING)).otherwise(_version_date("__START_AT")).alias("valid_from"),
        _version_date("__END_AT").alias("valid_to"),
        F.col("__END_AT").isNull().alias("is_current"),
    )


def with_unknown_member(dimension: DataFrame, key: str, natural_key: str) -> DataFrame:
    """Add the one row unmatched facts point to. Every other column is null."""
    special = {key: F.lit(UNKNOWN_KEY), natural_key: F.lit(UNKNOWN), "valid_from": F.lit(BEGINNING), "is_current": F.lit(True)}
    row = [
        special.get(c, F.lit(None)).cast(dimension.schema[c].dataType).alias(c)
        for c in dimension.columns
    ]
    return dimension.unionByName(dimension.sparkSession.range(1).select(*row))


def dim_client(clients: DataFrame, vendors: DataFrame) -> DataFrame:
    versions = _version_ranges(clients, "client_id")
    vendor_names = vendors.select("vendor_id", F.col("name").alias("vendor_name"))
    dimension = versions.join(vendor_names, "vendor_id", "left").select(
        surrogate_key("client_id", F.col("valid_from").cast("string")).alias("client_key"),
        "client_id",
        F.col("name").alias("client_name"),
        "vendor_id",
        "vendor_name",
        "region",
        "operating_hours",
        "valid_from",
        "valid_to",
        "is_current",
    )
    return with_unknown_member(dimension, "client_key", "client_id")


def dim_vehicle(vehicles: DataFrame) -> DataFrame:
    versions = _version_ranges(vehicles, "vehicle_id")
    dimension = versions.select(
        surrogate_key("vehicle_id", F.col("valid_from").cast("string")).alias("vehicle_key"),
        "vehicle_id",
        "vin",
        "make",
        "model",
        "model_year",
        "tank_capacity_l",
        "client_id",
        "region",
        "valid_from",
        "valid_to",
        "is_current",
    )
    return with_unknown_member(dimension, "vehicle_key", "vehicle_id")


def dim_driver(drivers: DataFrame) -> DataFrame:
    versions = _version_ranges(drivers, "driver_id")
    dimension = versions.select(
        surrogate_key("driver_id", F.col("valid_from").cast("string")).alias("driver_key"),
        "driver_id",
        F.col("name").alias("driver_name"),
        "fob_id",
        "client_id",
        "licensed_since",
        "valid_from",
        "valid_to",
        "is_current",
    )
    return with_unknown_member(dimension, "driver_key", "driver_id")


def dim_device(device_assignments: DataFrame) -> DataFrame:
    """One row per device fitting. The registry already holds this history with business dates."""
    dimension = device_assignments.select(
        surrogate_key("device_id", F.col("valid_from").cast("string")).alias("device_key"),
        "device_id",
        "vendor_id",
        "vehicle_id",
        "client_id",
        "firmware_version",
        "status",
        "valid_from",
        "valid_to",
        F.col("valid_to").isNull().alias("is_current"),
    )
    return with_unknown_member(dimension, "device_key", "device_id")


def dim_date_spanning(dates: DataFrame) -> DataFrame:
    """One row per calendar day from the earliest to the latest value of a single date column.

    date_key is yyyymmdd: stable, readable, and the usual convention for dates.
    Built without collecting to the driver, so it stays one Spark plan.
    """
    bounds = dates.agg(F.min(dates.columns[0]).alias("first"), F.max(dates.columns[0]).alias("last"))
    days = bounds.select(F.explode(F.sequence("first", "last", F.expr("INTERVAL 1 DAY"))).alias("date"))
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofmonth("date").alias("day_of_month"),
        F.date_format("date", "EEEE").alias("weekday_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def dim_date(spark: SparkSession, first: date, last: date) -> DataFrame:
    return dim_date_spanning(spark.createDataFrame([(first,), (last,)], "date date"))