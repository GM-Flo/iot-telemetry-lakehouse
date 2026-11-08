"""Gold dimensions, rebuilt from silver master data on every run."""

import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot.gold import dimensions as dims  # noqa: E402

CATALOG = spark.conf.get("catalog")
SILVER = f"{CATALOG}.silver"


@dp.materialized_view(comment="Fleet companies, one row per version, with their telematics vendor.")
@dp.expect_or_fail("has_key", "client_key IS NOT NULL")
def dim_client():
    return dims.dim_client(spark.read.table(f"{SILVER}.clients"), spark.read.table(f"{SILVER}.vendors"))


@dp.materialized_view(comment="Vehicles, one row per version.")
@dp.expect_or_fail("has_key", "vehicle_key IS NOT NULL")
def dim_vehicle():
    return dims.dim_vehicle(spark.read.table(f"{SILVER}.vehicles"))


@dp.materialized_view(comment="Drivers and their fobs, one row per version, including moves between clients.")
@dp.expect_or_fail("has_key", "driver_key IS NOT NULL")
def dim_driver():
    return dims.dim_driver(spark.read.table(f"{SILVER}.drivers"))


@dp.materialized_view(comment="Telematics devices, one row per fitting to a vehicle.")
@dp.expect_or_fail("has_key", "device_key IS NOT NULL")
def dim_device():
    return dims.dim_device(spark.read.table(f"{SILVER}.device_assignments"))


@dp.materialized_view(comment="Calendar days covering all readings and fuel prices. date_key is yyyymmdd.")
def dim_date():
    readings = spark.read.table(f"{SILVER}.vehicle_measurements").select(F.to_date("event_ts").alias("d"))
    prices = spark.read.table(f"{SILVER}.fuel_prices").select(F.col("price_date").alias("d"))
    return dims.dim_date_spanning(readings.unionByName(prices))