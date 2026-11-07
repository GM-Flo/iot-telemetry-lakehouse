"""Silver transformations for vendor telemetry.

Each function is one stage of the silver flow, DataFrame in and DataFrame out:

  parse_envelope    raw text -> envelope columns, payload kept as JSON text
  (later stages)    contract check, normalize, validity, matching, deduplicate
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CORRUPT_RECORD = "_corrupt_record"

# Severity of a record, alongside the reason for it.
#   pass  clean
#   warn  usable but flagged, e.g. a reading whose driver fob is unknown
#   fail  unusable, routed to a quarantine table
STATUS_PASS = "pass"
STATUS_WARN = "warn"
STATUS_FAIL = "fail"

# payload is declared as a string on purpose. The four vendors send four
# different payload shapes, so no single schema fits them all. Declared as a
# string, Spark keeps the payload object as JSON text for a later, vendor-
# specific stage to parse.
ENVELOPE_SCHEMA = StructType([
    StructField("envelope_version", StringType()),
    StructField("message_id", StringType()),
    StructField("vendor_id", StringType()),
    StructField("contract", StringType()),
    StructField("received_at", StringType()),
    StructField("payload", StringType()),
    StructField(CORRUPT_RECORD, StringType()),
])

# PERMISSIVE mode does not return null for broken JSON. It returns a struct of
# nulls, which looks just like valid JSON with fields missing. Naming a corrupt-
# record column is what lets the two be told apart.
ENVELOPE_PARSE_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD,
}

BRONZE_TRACE_COLUMNS = [
    "raw_payload",
    "partition_key",
    "kafka_partition",
    "kafka_offset",
    "eventhub_enqueued_at",
    "ingested_at",
]


def parse_envelope(bronze: DataFrame, known_vendors: list[str]) -> DataFrame:
    """Parse the gateway envelope and say why a message cannot be used, if it cannot.

    Every bronze row comes out exactly once. `parse_error` is null for a usable
    message, or one reason, checked in this order:

      null_payload        bronze row has no text at all
      malformed_json      not JSON, or cut off part-way
      missing_message_id  no UUID, so the message cannot be traced or deduplicated
      missing_payload     envelope without the vendor's data
      unknown_vendor      a vendor we have no contract with
    """
    parsed = bronze.select(
        *BRONZE_TRACE_COLUMNS,
        F.from_json("raw_payload", ENVELOPE_SCHEMA, ENVELOPE_PARSE_OPTIONS).alias("envelope"),
    )

    envelope = F.col("envelope")
    parse_error = (
        F.when(F.col("raw_payload").isNull(), F.lit("null_payload"))
        .when(envelope.isNull() | envelope[CORRUPT_RECORD].isNotNull(), F.lit("malformed_json"))
        .when(envelope["message_id"].isNull(), F.lit("missing_message_id"))
        .when(envelope["payload"].isNull(), F.lit("missing_payload"))
        .when(
            envelope["vendor_id"].isNull() | ~envelope["vendor_id"].isin(known_vendors),
            F.lit("unknown_vendor"),
        )
    )

    return parsed.select(
        *BRONZE_TRACE_COLUMNS,
        envelope["envelope_version"].alias("envelope_version"),
        envelope["message_id"].alias("message_id"),
        envelope["vendor_id"].alias("vendor_id"),
        envelope["contract"].alias("contract"),
        envelope["received_at"].alias("received_at"),
        envelope["payload"].alias("payload"),
        parse_error.alias("parse_error"),
        # Every parse problem makes a message unusable, so here it is pass or fail.
        # Later stages also use warn: usable, but flagged.
        F.when(parse_error.isNull(), F.lit(STATUS_PASS)).otherwise(F.lit(STATUS_FAIL)).alias("record_status"),
    )


def quarantine_unparseable(parsed: DataFrame) -> DataFrame:
    """Messages that failed parsing, with everything needed to diagnose and replay them."""
    return parsed.filter(F.col("record_status") == STATUS_FAIL).select(
        "message_id",
        "vendor_id",
        "raw_payload",
        F.lit("parse").alias("quarantine_stage"),
        F.col("parse_error").alias("quarantine_reason"),
        "kafka_partition",
        "kafka_offset",
        "eventhub_enqueued_at",
        "ingested_at",
        F.current_timestamp().alias("quarantined_at"),
        F.lit(False).alias("replayed"),
    )