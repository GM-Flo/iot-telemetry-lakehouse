"""Bronze transformations for the telemetry stream and file-based sources.

Bronze stores data as received. These functions only add the metadata needed
to trace and order what arrived; they never parse the content itself.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

REGISTRY_FILE_PATTERN = r"fleet_registry_(\d{4}-\d{2}-\d{2})\.json$"


def to_bronze(df: DataFrame) -> DataFrame:
    """Shape a raw Kafka source into the raw_telemetry_messages contract.

    Bronze never parses. It keeps the payload as an opaque string alongside
    Kafka coordinates and an ingestion timestamp, which makes the layer
    replayable: a parser fix is a silver rebuild, not a re-ingest.
    """
    return df.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.col("key").cast("string").alias("partition_key"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("eventhub_enqueued_at"),
        F.current_timestamp().alias("ingested_at"),
    )


def snapshot_date_from_path(path: Column) -> Column:
    """The business date a registry file describes, taken from its file name.

    Never the upload time: re-uploading a corrected old file must not make it
    look like the newest snapshot. Returns null when the name does not match,
    which the caller must treat as an error.
    """
    extracted = F.regexp_extract(path, REGISTRY_FILE_PATTERN, 1)
    return F.when(extracted == "", F.lit(None).cast("date")).otherwise(F.to_date(extracted))


def to_registry_bronze(df: DataFrame) -> DataFrame:
    """Shape an Auto Loader whole-text read into the raw_registry_snapshots contract.

    Expects the `value` column and the `_metadata` struct that Auto Loader provides.
    """
    return df.select(
        F.col("value").alias("raw_content"),
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_modification_time").alias("file_modified_at"),
        snapshot_date_from_path(F.col("_metadata.file_path")).alias("snapshot_date"),
        F.current_timestamp().alias("ingested_at"),
    )
