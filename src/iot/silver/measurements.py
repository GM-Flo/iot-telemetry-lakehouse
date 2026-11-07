"""vehicle_measurements: one row per real reading, deduplicated.

Duplicates arrive three ways: the same message delivered twice (Event Hubs is
at-least-once), the same reading re-sent by a vendor in a new message, and
quarantined readings replayed after a fix. All three share the reading's
natural key, device and event time: a device cannot take two readings at the
same instant. measurement_id is kept for lineage but is not the key, because a
re-sent message has a new message_id.

When copies differ, the one that reached us last wins: it is the vendor's
latest word on that reading.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.iot.silver.telemetry import STATUS_FAIL

MEASUREMENT_KEYS = ["device_id", "event_ts"]

# Which copy is newer: when Event Hubs received it, then its position in the partition.
SEQUENCE_COLUMNS = ["eventhub_enqueued_at", "kafka_offset"]

DROPPED_COLUMNS = ["match_error", "match_detail", "reported_vin", "reported_client_id"]


def to_store(matched: DataFrame) -> DataFrame:
    """Readings that passed or only warned, without the columns that explained failures.

    reported_vin and reported_client_id are dropped: for a stored reading they
    equal the registry's values, which are kept as vehicle_id and client_id.
    """
    return matched.filter(F.col("record_status") != STATUS_FAIL).drop(*DROPPED_COLUMNS)


def latest_per_key(readings: DataFrame) -> DataFrame:
    """The copy AUTO CDC keeps for each key, computed in batch.

    The pipeline lets AUTO CDC do this as a streaming upsert. This batch version
    states the same rule so it can be tested, and checked against the table.
    """
    newest_first = Window.partitionBy(*MEASUREMENT_KEYS).orderBy(*[F.col(c).desc() for c in SEQUENCE_COLUMNS])
    return readings.withColumn("_rank", F.row_number().over(newest_first)).filter("_rank = 1").drop("_rank")