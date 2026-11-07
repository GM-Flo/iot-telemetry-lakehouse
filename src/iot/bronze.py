"""Bronze transformations for file-based sources.

Bronze stores data as received. These functions only add the metadata needed
to trace and order what arrived; they never parse the content itself.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

REGISTRY_FILE_PATTERN = r"fleet_registry_(\d{4}-\d{2}-\d{2})\.json$"


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