"""Operations: how the pipeline is doing, from data the pipeline already produces.

  pipeline_runs              one row per pipeline update, from the Lakeflow event log
  pipeline_flow_runs         one row per flow per update: rows written, expectation failures
  quarantine_summary_by_hour failures per hour of arrival, stage, reason and vendor
  silver_reconciliation      bronze recounted in batch against what the stream stored
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

TERMINAL_STATES = ("COMPLETED", "FAILED", "CANCELED")

EXPECTATIONS_SCHEMA = ArrayType(StructType([
    StructField("name", StringType()),
    StructField("dataset", StringType()),
    StructField("passed_records", LongType()),
    StructField("failed_records", LongType()),
]))


def _detail(path: str) -> F.Column:
    return F.get_json_object("details", path)


def pipeline_flow_runs(event_log: DataFrame) -> DataFrame:
    """Rows written and expectation failures per flow, per pipeline update.

    A flow reports progress several times per update, once per micro-batch, so
    row counts are summed. AUTO CDC flows report upserted rather than output rows.
    """
    progress = event_log.filter(F.col("event_type") == "flow_progress").select(
        F.col("origin.update_id").alias("update_id"),
        F.col("origin.flow_name").alias("flow_name"),
        "timestamp",
        _detail("$.flow_progress.status").alias("status"),
        F.coalesce(
            _detail("$.flow_progress.metrics.num_output_rows").cast("long"),
            _detail("$.flow_progress.metrics.num_upserted_rows").cast("long"),
        ).alias("rows_written"),
        F.aggregate(
            F.from_json(_detail("$.flow_progress.data_quality.expectations"), EXPECTATIONS_SCHEMA),
            F.lit(0).cast("long"),
            lambda total, e: total + F.coalesce(e["failed_records"], F.lit(0).cast("long")),
        ).alias("expectation_failures"),
    )
    return progress.groupBy("update_id", "flow_name").agg(
        F.max_by("status", "timestamp").alias("final_status"),
        F.coalesce(F.sum("rows_written"), F.lit(0)).alias("rows_written"),
        F.coalesce(F.sum("expectation_failures"), F.lit(0)).alias("expectation_failures"),
    )


def pipeline_runs(event_log: DataFrame) -> DataFrame:
    """One row per finished pipeline update. An update still running appears once it ends."""
    updates = (
        event_log.filter(F.col("event_type") == "update_progress")
        .groupBy(F.col("origin.update_id").alias("update_id"))
        .agg(
            F.min("timestamp").alias("started_at"),
            F.max("timestamp").alias("ended_at"),
            F.max_by(_detail("$.update_progress.state"), "timestamp").alias("final_state"),
        )
        .filter(F.col("final_state").isin(*TERMINAL_STATES))
    )

    flows = pipeline_flow_runs(event_log)
    quarantine = F.col("flow_name").contains("quarantine")
    totals = flows.groupBy("update_id").agg(
        F.count("*").alias("flows"),
        F.sum(F.when(F.col("final_status") == "FAILED", 1).otherwise(0)).alias("failed_flows"),
        F.sum(F.when(quarantine, F.col("rows_written")).otherwise(0)).alias("rows_quarantined"),
        F.sum(F.when(F.col("flow_name").endswith("vehicle_measurements"), F.col("rows_written")).otherwise(0))
        .alias("rows_to_vehicle_measurements"),
        F.sum("expectation_failures").alias("expectation_failures"),
    )

    return (
        updates.join(totals, "update_id", "left")
        .withColumn("duration_seconds", F.col("ended_at").cast("long") - F.col("started_at").cast("long"))
        .select(
            "update_id", "started_at", "ended_at", "duration_seconds", "final_state", "flows", "failed_flows",
            "rows_to_vehicle_measurements", "rows_quarantined", "expectation_failures",
        )
    )


def quarantine_summary_by_hour(*quarantines: DataFrame) -> DataFrame:
    """Failures grouped by the hour the data arrived, not the hour the pipeline ran.

    With a daily pipeline, grouping by processing time would put a 3 AM outage
    at 9 AM. Every quarantine table has the same columns, so they union directly.
    """
    combined = reduce(DataFrame.unionByName, quarantines)
    return combined.groupBy(
        F.date_trunc("hour", "eventhub_enqueued_at").alias("arrival_hour"),
        "quarantine_stage",
        "quarantine_reason",
        "vendor_id",
    ).agg(
        F.count("*").alias("quarantined"),
        F.sum(F.when(F.col("replayed"), 1).otherwise(0)).alias("replayed"),
        F.min("quarantine_detail").alias("example_detail"),
    )


READING_KEY = ["device_id", "event_ts"]


def _keys(df: DataFrame) -> DataFrame:
    return df.select(*READING_KEY).where(F.col("device_id").isNotNull() & F.col("event_ts").isNotNull())


def _count_without_key(df: DataFrame, name: str) -> DataFrame:
    missing = F.col("device_id").isNull() | F.col("event_ts").isNull()
    return df.agg(F.coalesce(F.sum(missing.cast("int")), F.lit(0)).alias(name))


def silver_reconciliation(
    bronze_messages: DataFrame,
    expected_readings: DataFrame,
    unparseable: DataFrame,
    contract_violations: DataFrame,
    measurements: DataFrame,
    invalid_values: DataFrame,
    registry_mismatches: DataFrame,
    content_columns: list[str],
) -> DataFrame:
    """One row comparing a batch recount of bronze with what the streaming flows stored.

    expected_readings is bronze put through parse, contract and normalize again,
    in batch, independent of checkpoints, joins and the AUTO CDC upsert, which is
    where readings get lost or doubled.

    Readings are compared by their identity, (device_id, event_ts), the key
    vehicle_measurements is kept unique on. Every expected identity must be
    stored or quarantined somewhere, and nothing may exist that was not expected:

      lost_readings          expected, but found in no table           must be 0
      unexpected_readings    found in a table, but never expected      must be 0
      duplicate_readings     extra copies of an identity, collapsed    information
      conflicting_duplicates identities whose copies differ in content information:
                             two different readings claiming the same
                             device and instant, e.g. a vendor sending
                             timestamps rounded to the second

    A reading missing its device or its time cannot be identified; validity
    quarantines it, so such readings are matched by count instead.
    """
    expected_keys = _keys(expected_readings).distinct()
    found = (
        _keys(measurements).unionByName(_keys(invalid_values)).unionByName(_keys(registry_mismatches)).distinct()
    )
    lost = expected_keys.join(found, READING_KEY, "left_anti").agg(F.count("*").alias("_lost"))
    unexpected = found.join(expected_keys, READING_KEY, "left_anti").agg(F.count("*").alias("_unexpected"))

    content = F.sha2(F.to_json(F.struct(*content_columns)), 256)
    copies = (
        expected_readings.where(F.col("device_id").isNotNull() & F.col("event_ts").isNotNull())
        .withColumn("_content", content)
        .groupBy(*READING_KEY)
        .agg(F.count("*").alias("_copies"), F.countDistinct("_content").alias("_versions"))
        .agg(
            F.coalesce(F.sum(F.col("_copies") - 1), F.lit(0)).alias("duplicate_readings"),
            F.coalesce(F.sum((F.col("_versions") > 1).cast("int")), F.lit(0)).alias("conflicting_duplicates"),
        )
    )
    unidentified = _count_without_key(expected_readings, "_expected_no_key").crossJoin(
        _count_without_key(invalid_values, "_quarantined_no_key")
    )

    def count(df: DataFrame, name: str) -> DataFrame:
        return df.agg(F.count("*").alias(name))

    parts = [
        count(bronze_messages, "bronze_messages"),
        count(unparseable, "unparseable_messages"),
        count(contract_violations, "contract_violation_messages"),
        count(expected_readings, "expected_readings"),
        count(measurements, "stored_readings"),
        count(invalid_values, "invalid_readings"),
        count(registry_mismatches, "mismatched_readings"),
        copies,
        lost,
        unexpected,
        unidentified,
    ]
    row = reduce(lambda left, right: left.crossJoin(right), parts)
    no_key_difference = F.col("_expected_no_key") - F.col("_quarantined_no_key")
    return row.select(
        "bronze_messages", "unparseable_messages", "contract_violation_messages",
        "expected_readings", "stored_readings", "invalid_readings", "mismatched_readings",
        "duplicate_readings", "conflicting_duplicates",
        (F.col("_lost") + F.greatest(no_key_difference, F.lit(0))).alias("lost_readings"),
        (F.col("_unexpected") + F.greatest(-no_key_difference, F.lit(0))).alias("unexpected_readings"),
        F.current_timestamp().alias("checked_at"),
    )
