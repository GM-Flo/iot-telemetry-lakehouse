"""Silver transformations for vendor telemetry.

Each function is one stage of the silver flow, DataFrame in and DataFrame out:

  parse_envelope    raw text -> envelope columns, payload kept as JSON text
  check_contract    payload checked against the vendor's JSON Schema
  (later stages)    normalize, validity, matching, deduplicate

Each stage takes only the rows that passed the stage before it. Failed rows go
to that stage's quarantine table, so every bronze row ends up in exactly one place.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import Column, DataFrame
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


QUARANTINE_COLUMNS = [
    "message_id",
    "measurement_id",
    "reading_index",
    "device_id",
    "event_ts",
    "vendor_id",
    "raw_payload",
    "quarantine_stage",
    "quarantine_reason",
    "quarantine_detail",
    "kafka_partition",
    "kafka_offset",
    "eventhub_enqueued_at",
    "ingested_at",
    "quarantined_at",
    "replayed",
]


def to_quarantine(
    failed: DataFrame,
    stage: str,
    reason: Column,
    detail: Column,
    raw_payload: Column | None = None,
    measurement_id: Column | None = None,
    reading_index: Column | None = None,
    device_id: Column | None = None,
    event_ts: Column | None = None,
) -> DataFrame:
    """Shape failed rows the same way for every quarantine table.

    reason is a short fixed category for counting and alerting; detail is the
    free text a person reads when fixing one message. Message-level stages leave
    measurement_id, reading_index, device_id and event_ts empty; reading-level
    stages fill them, so a quarantined reading says which device and when, and
    can be counted by the same identity as a stored one.
    """
    return failed.select(
        "message_id",
        (measurement_id if measurement_id is not None else F.lit(None)).cast("string").alias("measurement_id"),
        (reading_index if reading_index is not None else F.lit(None)).cast("int").alias("reading_index"),
        (device_id if device_id is not None else F.lit(None)).cast("string").alias("device_id"),
        (event_ts if event_ts is not None else F.lit(None)).cast("timestamp").alias("event_ts"),
        "vendor_id",
        (raw_payload if raw_payload is not None else F.col("raw_payload")).cast("string").alias("raw_payload"),
        F.lit(stage).alias("quarantine_stage"),
        reason.alias("quarantine_reason"),
        detail.cast("string").alias("quarantine_detail"),
        "kafka_partition",
        "kafka_offset",
        "eventhub_enqueued_at",
        "ingested_at",
        F.current_timestamp().alias("quarantined_at"),
        F.lit(False).alias("replayed"),
    )


def quarantine_unparseable(parsed: DataFrame) -> DataFrame:
    """Messages that failed parsing, with everything needed to diagnose and replay them."""
    return to_quarantine(
        parsed.filter(F.col("record_status") == STATUS_FAIL),
        stage="parse",
        reason=F.col("parse_error"),
        detail=F.lit(None),
    )


def load_contract_schemas(contracts_dir: str | Path, contracts: list[str]) -> dict[str, dict]:
    """The JSON Schema for each accepted contract, read from contracts/<name>.json."""
    return {
        name: json.loads((Path(contracts_dir) / f"{name}.json").read_text(encoding="utf-8"))
        for name in contracts
    }


def _schema_violation_udf(schemas: dict[str, dict]):
    """A UDF returning null for a valid payload, or the first error as readable text.

    The schemas travel to the workers inside this function. Validators are built
    once per worker, on first use, not once per row.
    """
    validators: dict = {}

    @F.udf("string")
    def violation(contract: str | None, payload: str | None) -> str | None:
        from jsonschema import Draft202012Validator
        from jsonschema.exceptions import best_match

        if contract not in schemas or payload is None:
            return None
        if contract not in validators:
            validators[contract] = Draft202012Validator(schemas[contract])
        try:
            document = json.loads(payload)
        except ValueError:
            return "payload is not valid JSON"
        error = best_match(validators[contract].iter_errors(document))
        if error is None:
            return None
        return f"{error.json_path}: {error.message}"[:500]

    return violation


def check_contract(parsed: DataFrame, schemas: dict[str, dict], accepted: dict[str, str]) -> DataFrame:
    """Check each parsed message against the contract its vendor agreed to send.

    Only rows that passed parsing are checked. `contract_error` is null for a
    valid message, or one reason, checked in this order:

      unsupported_contract  contract not accepted, or claimed by the wrong vendor
      schema_violation      payload breaks the JSON Schema (wrong type, missing
                            field, extra field, bad pattern)

    A broken shape fails the whole message, even when only one AutoSense reading
    in it is at fault: the contract is about messages, and replays are per message.
    """
    contract_vendor = F.create_map(*[F.lit(x) for pair in accepted.items() for x in pair])[F.col("contract")]
    supported = contract_vendor.isNotNull() & (contract_vendor == F.col("vendor_id"))
    violation = _schema_violation_udf(schemas)("contract", "payload")

    checked = parsed.filter(F.col("record_status") == STATUS_PASS).withColumn("schema_violation", violation)

    contract_error = (
        F.when(~supported, F.lit("unsupported_contract"))
        .when(F.col("schema_violation").isNotNull(), F.lit("schema_violation"))
    )
    contract_detail = (
        F.when(~supported, F.format_string("vendor %s sent contract %s", "vendor_id", "contract"))
        .otherwise(F.col("schema_violation"))
    )

    return checked.select(
        *BRONZE_TRACE_COLUMNS,
        "envelope_version", "message_id", "vendor_id", "contract", "received_at", "payload",
        contract_error.alias("contract_error"),
        contract_detail.alias("contract_detail"),
        F.when(contract_error.isNull(), F.lit(STATUS_PASS)).otherwise(F.lit(STATUS_FAIL)).alias("record_status"),
    )


def quarantine_contract_violations(checked: DataFrame) -> DataFrame:
    return to_quarantine(
        checked.filter(F.col("record_status") == STATUS_FAIL),
        stage="contract",
        reason=F.col("contract_error"),
        detail=F.col("contract_detail"),
    )
