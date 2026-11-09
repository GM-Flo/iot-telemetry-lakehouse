"""Turn raw registry snapshots into master-data rows.

Bronze holds each registry file as one raw JSON string. These functions parse
that string with an explicit schema and explode each section (vehicles,
drivers, ...) into one row per entity, ready for the pipeline to store.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _struct(*fields: tuple[str, object]) -> StructType:
    return StructType([StructField(name, dtype) for name, dtype in fields])


SECTION_SCHEMAS: dict[str, StructType] = {
    "vendors": _struct(
        ("vendor_id", StringType()), ("name", StringType()), ("contract", StringType())),
    "clients": _struct(
        ("client_id", StringType()), ("vendor_id", StringType()), ("name", StringType()),
        ("region", StringType()), ("operating_hours", StringType())),
    "regions": _struct(
        ("region", StringType()), ("depot_latitude", DoubleType()),
        ("depot_longitude", DoubleType()), ("geofence_radius_km", DoubleType()),
        ("utc_offset_hours", IntegerType())),
    "vehicles": _struct(
        ("vehicle_id", StringType()), ("vin", StringType()), ("make", StringType()),
        ("model", StringType()), ("model_year", IntegerType()),
        ("tank_capacity_l", DoubleType()), ("client_id", StringType()), ("region", StringType())),
    "drivers": _struct(
        ("driver_id", StringType()), ("client_id", StringType()), ("name", StringType()),
        ("fob_id", StringType()), ("licensed_since", DateType())),
    "device_vehicle_assignments": _struct(
        ("device_id", StringType()), ("vendor_id", StringType()), ("vin", StringType()),
        ("vehicle_id", StringType()), ("client_id", StringType()),
        ("firmware_version", StringType()), ("valid_from", DateType()),
        ("valid_to", DateType()), ("status", StringType())),
    "fuel_prices_by_region": _struct(
        ("region", StringType()), ("price_date", DateType()),
        ("price_per_litre_usd", DoubleType())),
}

REGISTRY_SCHEMA = StructType([
    StructField(section, ArrayType(schema)) for section, schema in SECTION_SCHEMAS.items()
])


def snapshot_version(snapshot: date) -> int:
    """2026-11-07 -> 20261107. AUTO CDC orders snapshots by this number."""
    return int(snapshot.strftime("%Y%m%d"))


def version_to_date(version: int) -> date:
    text = str(version)
    return date(int(text[:4]), int(text[4:6]), int(text[6:]))


def latest_file_per_snapshot(bronze: DataFrame) -> DataFrame:
    """One file per snapshot date: the most recently ingested.

    If a corrected file for the same date is uploaded again, it replaces the
    earlier one rather than being treated as a separate snapshot.
    """
    newest_first = Window.partitionBy("snapshot_date").orderBy(F.col("ingested_at").desc())
    return (
        bronze.withColumn("_rank", F.row_number().over(newest_first))
        .filter("_rank = 1")
        .drop("_rank")
    )


def parse_registry(bronze: DataFrame) -> DataFrame:
    """Parse raw_content into a typed struct, keeping the snapshot date."""
    return bronze.select(
        "snapshot_date",
        F.from_json("raw_content", REGISTRY_SCHEMA).alias("registry"),
    )


def registry_section(parsed: DataFrame, section: str) -> DataFrame:
    """Explode one section into one row per entity.

    `explode` turns an array of N items into N rows. `item.*` then turns the
    struct's fields into ordinary columns.
    """
    if section not in SECTION_SCHEMAS:
        raise ValueError(f"Unknown registry section: {section}")
    return (
        parsed.select("snapshot_date", F.explode(f"registry.{section}").alias("item"))
        .select("snapshot_date", "item.*")
    )
