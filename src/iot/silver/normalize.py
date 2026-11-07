"""Normalize: four vendor payload shapes into one row per reading with standard fields.

What to read and which conversion to apply comes from vendor_field_mappings.csv.
How each conversion works lives here, as a fixed vocabulary of Spark expressions,
so the CSV can choose a conversion but never run code of its own.

Paths in the CSV:
  engine.rpm         nested field
  readings[].spd     field of each element of an array; the array is exploded,
                     so one message becomes one row per element
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from src.iot.silver.telemetry import STATUS_PASS

STANDARD_FIELDS: dict[str, str] = {
    "device_id": "string",
    "reported_vin": "string",
    "reported_client_id": "string",
    "driver_fob_id": "string",
    "event_ts": "timestamp",
    "sent_ts": "timestamp",
    "ignition_on": "boolean",
    "speed_kph": "double",
    "engine_rpm": "int",
    "engine_temperature_celsius": "double",
    "coolant_pressure_kpa": "double",
    "oil_pressure_kpa": "double",
    "battery_voltage": "double",
    "fuel_level_percent": "double",
    "odometer_km": "double",
    "latitude": "double",
    "longitude": "double",
    "heading_degrees": "double",
    "fault_code": "string",
    "harsh_brake_count": "int",
    "harsh_accel_count": "int",
}

KPH_PER_MPH = 1.609344
KM_PER_MILE = 1.609344
KPA_PER_PSI = 6.894757


def _number(value: Column) -> Column:
    return value.cast("double")


def _on_off(value: Column) -> Column:
    upper = F.upper(value)
    return F.when(upper == "ON", True).when(upper == "OFF", False)


def _one_zero(value: Column) -> Column:
    number = _number(value)
    return F.when(number == 1, True).when(number == 0, False)


# Every value arrives as JSON text (see payload_schema), so each conversion
# starts from a string column.
CONVERSIONS: dict[str, Callable[[Column], Column]] = {
    "none": lambda v: v,
    "iso_timestamp": lambda v: v.cast("timestamp"),
    "epoch_milliseconds": lambda v: (_number(v) / 1000).cast("timestamp"),
    "epoch_seconds": lambda v: _number(v).cast("timestamp"),
    # No time zone in the text; the vendor documents it as UTC.
    "naive_utc_timestamp": lambda v: F.concat(v, F.lit("Z")).cast("timestamp"),
    "mph_to_kph": lambda v: F.round(_number(v) * KPH_PER_MPH, 2),
    "fahrenheit_to_celsius": lambda v: F.round((_number(v) - 32) * 5 / 9, 1),
    "psi_to_kpa": lambda v: F.round(_number(v) * KPA_PER_PSI, 1),
    "miles_to_km": lambda v: F.round(_number(v) * KM_PER_MILE, 3),
    "on_off_to_boolean": _on_off,
    "one_zero_to_boolean": _one_zero,
    "first_array_element": lambda v: F.get(F.from_json(v, ArrayType(StringType())), 0),
}

ARRAY_MARKER = "[]"


def validate_mappings(contract: str, mappings: list[dict]) -> None:
    """Fail at pipeline start, before any data is read, if the configuration is wrong."""
    targets = [m["target_field"] for m in mappings]
    problems = []
    if missing := sorted(set(STANDARD_FIELDS) - set(targets)):
        problems.append(f"missing target fields {missing}")
    if unknown := sorted(set(targets) - set(STANDARD_FIELDS)):
        problems.append(f"unknown target fields {unknown}")
    if duplicated := sorted({t for t in targets if targets.count(t) > 1}):
        problems.append(f"target fields mapped twice {duplicated}")
    if bad := sorted({m["conversion"] for m in mappings} - set(CONVERSIONS)):
        problems.append(f"unknown conversions {bad}")
    arrays = {m["source_path"].split(ARRAY_MARKER)[0] for m in mappings if ARRAY_MARKER in m["source_path"]}
    if len(arrays) > 1:
        problems.append(f"more than one array to explode {sorted(arrays)}")
    if problems:
        raise ValueError(f"vendor_field_mappings for {contract}: " + "; ".join(problems))


def _add_path(fields: dict, parts: list[str]) -> None:
    head, rest = parts[0], parts[1:]
    if not rest:
        fields.setdefault(head, None)
    else:
        _add_path(fields.setdefault(head, {}), rest)


def _to_struct(fields: dict) -> StructType:
    return StructType([
        StructField(name, StringType() if child is None else _to_struct(child)) for name, child in fields.items()
    ])


def payload_schema(source_paths: list[str]) -> StructType:
    """A Spark schema holding exactly the fields the mappings read, every leaf as a string.

    Reading leaves as strings keeps the JSON text of each value ("12.5", "true",
    '["P0128"]'), so all conversions start from the same type and do their own casting.
    """
    message_fields: dict = {}
    element_fields: dict = {}
    array_name = None
    for path in source_paths:
        if ARRAY_MARKER in path:
            array_name, element_path = path.split(ARRAY_MARKER + ".")
            _add_path(element_fields, element_path.split("."))
        else:
            _add_path(message_fields, path.split("."))

    schema = _to_struct(message_fields)
    if array_name:
        schema = schema.add(array_name, ArrayType(_to_struct(element_fields)))
    return schema


def _source_column(path: str) -> Column:
    if ARRAY_MARKER in path:
        return F.col("reading." + path.split(ARRAY_MARKER + ".")[1])
    return F.col("p." + path)


def _cast(value: Column, dtype: str) -> Column:
    # Through double first, so "2150.0" still becomes the integer 2150.
    if dtype == "int":
        return value.cast("double").cast("int")
    return value.cast(dtype)


TRACE_COLUMNS = [
    "message_id",
    "vendor_id",
    "contract",
    "kafka_partition",
    "kafka_offset",
    "eventhub_enqueued_at",
    "ingested_at",
]


def _normalize_contract(messages: DataFrame, contract: str, mappings: list[dict]) -> DataFrame:
    paths = [m["source_path"] for m in mappings]
    parsed = messages.filter(F.col("contract") == contract).withColumn(
        "p", F.from_json("payload", payload_schema(paths))
    )

    array_paths = [p for p in paths if ARRAY_MARKER in p]
    if array_paths:
        array_name = array_paths[0].split(ARRAY_MARKER)[0]
        rows = parsed.select("*", F.posexplode(f"p.{array_name}").alias("reading_index", "reading"))
    else:
        rows = parsed.withColumn("reading_index", F.lit(0))

    by_target = {m["target_field"]: m for m in mappings}
    fields = []
    for name, dtype in STANDARD_FIELDS.items():
        mapping = by_target[name]
        convert = CONVERSIONS[mapping["conversion"]]
        fields.append(_cast(convert(_source_column(mapping["source_path"])), dtype).alias(name))
    return rows.select(*TRACE_COLUMNS, "reading_index", F.col("received_at").cast("timestamp").alias("received_at"), *fields)


def normalize(checked: DataFrame, mappings_by_contract: dict[str, list[dict]]) -> DataFrame:
    """One row per reading, with the standard fields, for every message that passed its contract.

    measurement_id is derived from message_id and the reading's position in the
    message, so replaying the same message produces the same ids.
    """
    for contract, mappings in mappings_by_contract.items():
        validate_mappings(contract, mappings)

    passed = checked.filter(F.col("record_status") == STATUS_PASS)
    per_contract = [_normalize_contract(passed, c, m) for c, m in sorted(mappings_by_contract.items())]

    readings = per_contract[0]
    for other in per_contract[1:]:
        readings = readings.unionByName(other)

    measurement_id = F.sha2(F.concat_ws("|", "message_id", F.col("reading_index").cast("string")), 256)
    return readings.select(measurement_id.alias("measurement_id"), "*")