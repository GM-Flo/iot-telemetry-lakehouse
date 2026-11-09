"""Validity: can this reading be true?

Checked per reading, after normalize, so one faulty reading in an AutoSense batch
fails alone. This stage only removes readings that are physically impossible.
Real but alarming values, such as an engine at 120 °C, pass: detecting those is
the business purpose of the platform, and happens later as anomalies.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.iot.silver.telemetry import STATUS_FAIL, STATUS_PASS, to_quarantine

REQUIRED_FIELDS = ["device_id", "event_ts"]

# A device clock may run a little ahead of the clocks that receive its messages.
MAX_CLOCK_SKEW_MINUTES = 5


def _checks(limits: dict[str, tuple[float, float]], max_clock_skew_minutes: int) -> list[tuple[str, Column, Column]]:
    """(reason, failed condition, detail) in the order they are reported."""
    missing = [F.col(c).isNull() for c in REQUIRED_FIELDS]
    missing_names = F.concat_ws(", ", *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED_FIELDS])

    lat, lon = F.col("latitude"), F.col("longitude")

    # Compared with when the device sent it and when Event Hubs received it,
    # never with processing time, so replaying old data does not look like the future.
    received_by = F.least("sent_ts", "eventhub_enqueued_at")
    in_future = F.col("event_ts") > received_by + F.expr(f"INTERVAL {max_clock_skew_minutes} MINUTES")

    out_of_range = [
        (
            (F.col(field) < low) | (F.col(field) > high),
            F.format_string(f"{field}=%s outside [{low:g}, {high:g}]", F.col(field).cast("string")),
        )
        for field, (low, high) in limits.items()
    ]

    return [
        ("missing_required_value", _any(missing), F.concat(F.lit("missing "), missing_names)),
        ("gps_null_island", (lat == 0) & (lon == 0), F.lit("latitude and longitude are both 0")),
        (
            "coordinates_out_of_range",
            (F.abs(lat) > 90) | (F.abs(lon) > 180),
            F.format_string("latitude=%s longitude=%s", lat.cast("string"), lon.cast("string")),
        ),
        (
            "event_in_future",
            in_future,
            F.format_string(
                "event_ts %s is after it was received at %s",
                F.col("event_ts").cast("string"),
                received_by.cast("string"),
            ),
        ),
        ("sensor_out_of_range", _any([c for c, _ in out_of_range]), F.concat_ws("; ", *[F.when(c, d) for c, d in out_of_range])),
    ]


def _any(conditions: list[Column]) -> Column:
    combined = F.lit(False)
    for condition in conditions:
        combined = combined | F.coalesce(condition, F.lit(False))
    return combined


def check_validity(
    readings: DataFrame,
    limits: dict[str, tuple[float, float]],
    max_clock_skew_minutes: int = MAX_CLOCK_SKEW_MINUTES,
) -> DataFrame:
    """Add validity_error (the first failed rule), validity_detail (every failed rule) and record_status.

    A null value is missing, not invalid, so it fails no range check. Only the
    required fields must be present.
    """
    unknown = sorted(set(limits) - set(readings.columns))
    if unknown:
        raise ValueError(f"validity_limits names fields that readings do not have: {unknown}")

    checks = _checks(limits, max_clock_skew_minutes)
    failed = [(reason, F.coalesce(condition, F.lit(False)), detail) for reason, condition, detail in checks]

    error = F.lit(None).cast("string")
    for reason, condition, _ in reversed(failed):
        error = F.when(condition, F.lit(reason)).otherwise(error)
    detail = F.concat_ws(" | ", *[F.when(condition, d) for _, condition, d in failed])

    return readings.select(
        "*",
        error.alias("validity_error"),
        F.when(error.isNotNull(), detail).alias("validity_detail"),
    ).withColumn(
        "record_status", F.when(F.col("validity_error").isNull(), F.lit(STATUS_PASS)).otherwise(F.lit(STATUS_FAIL))
    )


def quarantine_invalid_values(validated: DataFrame) -> DataFrame:
    """Readings that cannot be true. Only the failed reading is quarantined, not its whole message.

    raw_payload stays empty: the reading is one part of a message, and the whole
    message is in bronze at kafka_partition / kafka_offset.
    """
    return to_quarantine(
        validated.filter(F.col("record_status") == STATUS_FAIL),
        stage="validity",
        reason=F.col("validity_error"),
        detail=F.col("validity_detail"),
        raw_payload=F.lit(None),
        measurement_id=F.col("measurement_id"),
        reading_index=F.col("reading_index"),
        device_id=F.col("device_id"),
        event_ts=F.col("event_ts"),
    )