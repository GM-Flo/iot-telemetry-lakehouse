"""Normalize: four vendor payload shapes into one row per reading with standard fields.

What to read and which conversion to apply comes from vendor_field_mappings.csv.
How each conversion works lives here, as a fixed vocabulary of Spark expressions,
so the CSV can choose a conversion but never run code of its own.

Paths in the CSV:
  engine.rpm         nested field
  readings[].spd     field of each element of an array; the array is exploded,
                     so one message becomes one row per element

The query has the same shape whatever the number of contracts: one stream in,
one stream out, every field a CASE on the contract. A streaming checkpoint
records the shape of its query, so a design with one branch per contract would
break every downstream checkpoint each time a vendor adds a contract version.
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


def _source_column(path: str, payload: str) -> Column:
    """The column a mapping path points to, inside the parsed payload column `payload`.

    An array path reads the element at reading_index, so every message, batched
    or not, is handled by the same expression.
    """
    if ARRAY_MARKER not in path:
        return F.col(f"{payload}.{path}")
    array_name, element_path = path.split(ARRAY_MARKER + ".")
    value = F.col(f"{payload}.{array_name}").getItem(F.col("reading_index"))
    for part in element_path.split("."):
        value = value.getField(part)
    return value


def _case(branches: list[tuple[Column, Column]]) -> Column:
    """CASE WHEN condition THEN value ... with no ELSE: null when no branch matches."""
    expression = F.when(*branches[0])
    for condition, value in branches[1:]:
        expression = expression.when(condition, value)
    return expression


def _array_name(paths: list[str]) -> str | None:
    arrays = {p.split(ARRAY_MARKER)[0] for p in paths if ARRAY_MARKER in p}
    return arrays.pop() if arrays else None


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


def normalize(checked: DataFrame, mappings_by_contract: dict[str, list[dict]]) -> DataFrame:
    """One row per reading, with the standard fields, for every message that passed its contract.

    1. parse: each message's payload with its own contract's schema, into one
       column per contract; only the matching contract's column is filled
    2. explode: one row per reading, reading_index 0..n-1, where n is 1 for a
       single-reading message and the array's size for a batched one
    3. convert: each standard field is a CASE on the contract, applying that
       contract's mapping and conversion

    measurement_id is derived from message_id and the reading's position in the
    message, so replaying the same message produces the same ids.
    """
    for contract, mappings in mappings_by_contract.items():
        validate_mappings(contract, mappings)

    contracts = sorted(mappings_by_contract)
    payload_columns = {c: f"_payload_{i}" for i, c in enumerate(contracts)}
    is_contract = {c: F.col("contract") == c for c in contracts}

    passed = checked.filter(F.col("record_status") == STATUS_PASS)
    parsed = passed.select(
        "*",
        *[
            F.when(
                is_contract[c],
                F.from_json("payload", payload_schema([m["source_path"] for m in mappings_by_contract[c]])),
            ).alias(payload_columns[c])
            for c in contracts
        ],
    )

    def readings_in_message(contract: str) -> Column:
        array_name = _array_name([m["source_path"] for m in mappings_by_contract[contract]])
        return F.size(F.col(f"{payload_columns[contract]}.{array_name}")) if array_name else F.lit(1)

    reading_count = _case([(is_contract[c], readings_in_message(c)) for c in contracts])
    indexes = F.when(reading_count > 0, F.sequence(F.lit(0), reading_count - 1)).otherwise(
        F.array().cast("array<int>")
    )
    rows = parsed.withColumn("reading_index", F.explode(indexes))

    def standard_value(contract: str, name: str, dtype: str) -> Column:
        mapping = next(m for m in mappings_by_contract[contract] if m["target_field"] == name)
        source = _source_column(mapping["source_path"], payload_columns[contract])
        return _cast(CONVERSIONS[mapping["conversion"]](source), dtype)

    fields = [
        _case([(is_contract[c], standard_value(c, name, dtype)) for c in contracts]).alias(name)
        for name, dtype in STANDARD_FIELDS.items()
    ]

    measurement_id = F.sha2(F.concat_ws("|", "message_id", F.col("reading_index").cast("string")), 256)
    return rows.select(
        measurement_id.alias("measurement_id"),
        *TRACE_COLUMNS,
        "reading_index",
        F.col("received_at").cast("timestamp").alias("received_at"),
        *fields,
    )