"""Pure DataFrame transformations for the telemetry medallion layers.

Every function here is DataFrame -> DataFrame with no I/O, no table names and
no Spark session construction. The notebooks read, call these, and write. That
split is what makes the logic testable against small hand-built fixtures
instead of requiring a cluster and live Event Hubs traffic.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Physical thresholds. Tests reference these rather than hardcoding numbers, so
# a threshold change cannot silently invalidate the suite.
ENGINE_TEMP_WARN_C = 105.0
ENGINE_TEMP_CRIT_C = 115.0
ENGINE_TEMP_FLOOR_C = -40.0
ENGINE_TEMP_CEILING_C = 200.0

COOLANT_PRESSURE_MIN_KPA = 70.0
OIL_PRESSURE_MIN_KPA = 150.0
BATTERY_VOLTAGE_MIN_V = 12.0
SPEED_MAX_KPH = 130.0
RPM_REDLINE = 5000

DEFAULT_WATERMARK = "10 minutes"
DEFAULT_WINDOW = "5 minutes"


CORRUPT_RECORD_FIELD = "_corrupt_record"

PARSE_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD_FIELD,
}


def telemetry_schema() -> StructType:
    """Schema the producer emits.

    Numeric fields are read as strings first and cast explicitly in silver, so
    a payload carrying "ninety-four" for engine_temp_c lands in quarantine with
    a usable reason rather than silently nulling the column.

    The trailing corrupt-record field matters more than it looks. In PERMISSIVE
    mode from_json does not return null for malformed JSON: it returns a struct
    with every field nulled, which is indistinguishable from valid JSON that
    happens to be missing fields. Declaring this column is what lets the two be
    told apart.
    """
    return StructType([
        StructField("vin", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("emitted_at", StringType()),
        StructField("make", StringType()),
        StructField("model", StringType()),
        StructField("model_year", IntegerType()),
        StructField("fleet_region", StringType()),
        StructField("ignition_on", BooleanType()),
        StructField("speed_kph", StringType()),
        StructField("engine_rpm", StringType()),
        StructField("engine_temp_c", StringType()),
        StructField("coolant_pressure_kpa", StringType()),
        StructField("oil_pressure_kpa", StringType()),
        StructField("battery_voltage", StringType()),
        StructField("fuel_level_pct", StringType()),
        StructField("odometer_km", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("heading_deg", StringType()),
        StructField("dtc_code", StringType()),
        StructField(CORRUPT_RECORD_FIELD, StringType()),
    ])


def to_bronze(df: DataFrame) -> DataFrame:
    """Shape a raw Kafka source into the bronze contract.

    Bronze never parses. It keeps the payload as an opaque string alongside
    Kafka coordinates and an ingestion timestamp, which makes the layer
    infinitely replayable: a schema fix is a silver rebuild, not a re-ingest.
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


def parse_payload(df: DataFrame, schema: StructType | None = None) -> DataFrame:
    """Attach a parsed struct and a parse-failure reason to bronze rows."""
    schema = schema or telemetry_schema()

    parsed = df.withColumn(
        "_parsed", F.from_json(F.col("raw_payload"), schema, PARSE_OPTIONS)
    )

    return parsed.withColumn(
        "_parse_error",
        F.when(F.col("raw_payload").isNull(), F.lit("null_payload"))
        .when(F.col("_parsed").isNull(), F.lit("malformed_json"))
        .when(
            F.col(f"_parsed.{CORRUPT_RECORD_FIELD}").isNotNull(),
            F.lit("malformed_json"),
        )
        .when(F.col("_parsed.vin").isNull(), F.lit("missing_vin"))
        .when(F.col("_parsed.event_ts").isNull(), F.lit("missing_event_ts"))
        .when(
            F.col("_parsed.engine_temp_c").isNotNull()
            & F.col("_parsed.engine_temp_c").cast("double").isNull(),
            F.lit("uncastable_engine_temp"),
        )
        .otherwise(F.lit(None).cast("string")),
    )


def split_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition parsed bronze into (clean, quarantined).

    Rows that fail parsing are routed, not dropped. The quarantine table keeps
    the original payload so a failure can be diagnosed and replayed after a fix.
    """
    clean = df.filter(F.col("_parse_error").isNull())

    quarantined = df.filter(F.col("_parse_error").isNotNull()).select(
        F.col("raw_payload"),
        F.col("_parse_error").alias("quarantine_reason"),
        F.col("source_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("eventhub_enqueued_at"),
        F.col("ingested_at"),
        F.current_timestamp().alias("_quarantined_at"),
    )

    return clean, quarantined


def _num(field: str, target: str = "double") -> Column:
    return F.col(f"_parsed.{field}").cast(target)


def to_silver(df: DataFrame) -> DataFrame:
    """Flatten, cast and enrich clean parsed rows.

    Expects the output of parse_payload filtered through split_quarantine.
    """
    return df.select(
        F.col("_parsed.vin").alias("vin"),
        F.col("_parsed.vehicle_id").alias("vehicle_id"),
        F.to_timestamp(F.col("_parsed.event_ts")).alias("event_ts"),
        F.to_timestamp(F.col("_parsed.emitted_at")).alias("emitted_at"),
        F.col("_parsed.make").alias("make"),
        F.col("_parsed.model").alias("model"),
        F.col("_parsed.model_year").alias("model_year"),
        F.col("_parsed.fleet_region").alias("fleet_region"),
        F.col("_parsed.ignition_on").alias("ignition_on"),
        _num("speed_kph").alias("speed_kph"),
        _num("engine_rpm", "int").alias("engine_rpm"),
        _num("engine_temp_c").alias("engine_temp_c"),
        _num("coolant_pressure_kpa").alias("coolant_pressure_kpa"),
        _num("oil_pressure_kpa").alias("oil_pressure_kpa"),
        _num("battery_voltage").alias("battery_voltage"),
        _num("fuel_level_pct").alias("fuel_level_pct"),
        _num("odometer_km").alias("odometer_km"),
        _num("latitude").alias("latitude"),
        _num("longitude").alias("longitude"),
        _num("heading_deg").alias("heading_deg"),
        F.col("_parsed.dtc_code").alias("dtc_code"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("eventhub_enqueued_at"),
        F.col("ingested_at"),
    ).withColumn(
        "event_date", F.to_date(F.col("event_ts"))
    ).withColumn(
        # Producer-side lateness: how long after the event occurred was it
        # transmitted. Measured against transmission time, not processing time,
        # so the flag is replay-invariant: comparing to ingested_at would mark
        # every row of a historical backfill as late, which is useless.
        # emitted_at is the producer's own claim and survives a file replay;
        # eventhub_enqueued_at is the broker's and is trusted when present.
        "event_lag_seconds",
        F.unix_timestamp(F.coalesce(F.col("eventhub_enqueued_at"), F.col("emitted_at")))
        - F.unix_timestamp(F.col("event_ts")),
    ).withColumn(
        # Pipeline-side lag: how long the row sat before we processed it.
        "ingest_lag_seconds",
        F.unix_timestamp(F.col("ingested_at")) - F.unix_timestamp(F.col("eventhub_enqueued_at")),
    ).withColumn(
        "is_late_arrival", F.col("event_lag_seconds") > F.lit(60)
    )


def filter_implausible(df: DataFrame) -> DataFrame:
    """Drop rows whose sensor values are physically impossible.

    Deliberately narrow: only values outside the sensor's own operating range.
    A hot engine is an anomaly to be scored, not bad data to be discarded.
    """
    return df.filter(
        F.col("engine_temp_c").between(ENGINE_TEMP_FLOOR_C, ENGINE_TEMP_CEILING_C)
        & F.col("speed_kph").between(0, SPEED_MAX_KPH * 1.5)
        & (F.col("engine_rpm") >= 0)
        & F.col("vin").isNotNull()
        & F.col("event_ts").isNotNull()
    )


def deduplicate(df: DataFrame, watermark: str = DEFAULT_WATERMARK) -> DataFrame:
    """Drop duplicates on the natural key within the watermark window.

    At-least-once delivery from Event Hubs means a retried batch can replay
    events. dropDuplicatesWithinWatermark bounds the state store, unlike a
    plain dropDuplicates which grows without limit on a stream.
    """
    bounded = df.withWatermark("event_ts", watermark)
    try:
        return bounded.dropDuplicatesWithinWatermark(["vin", "event_ts"])
    except AttributeError:
        # Spark < 3.5
        return bounded.dropDuplicates(["vin", "event_ts"])


def flag_anomalies(df: DataFrame) -> DataFrame:
    """Add per-reading anomaly flags and a composite severity score.

    Flags are independent booleans so each can be aggregated and charted on its
    own; the score is a weighted sum for ranking vehicles by urgency.
    """
    overheat_warn = F.col("engine_temp_c") > ENGINE_TEMP_WARN_C
    overheat_crit = F.col("engine_temp_c") > ENGINE_TEMP_CRIT_C
    running = F.coalesce(F.col("ignition_on"), F.lit(False))

    out = (
        df.withColumn("is_overheating", overheat_warn)
        .withColumn("is_critical_temp", overheat_crit)
        .withColumn(
            "is_coolant_pressure_low",
            running & (F.col("coolant_pressure_kpa") < COOLANT_PRESSURE_MIN_KPA),
        )
        .withColumn(
            "is_oil_pressure_low",
            running & (F.col("oil_pressure_kpa") < OIL_PRESSURE_MIN_KPA),
        )
        .withColumn(
            "is_battery_low", F.col("battery_voltage") < BATTERY_VOLTAGE_MIN_V
        )
        .withColumn("is_overspeed", F.col("speed_kph") > SPEED_MAX_KPH)
        .withColumn("is_redline", F.col("engine_rpm") > RPM_REDLINE)
        .withColumn("has_dtc", F.col("dtc_code").isNotNull())
    )

    # Weights are chosen so that a vehicle tripping every flag scores exactly
    # 100, which lets the score be read as percent severity. is_overheating and
    # is_critical_temp are mutually exclusive by construction, so the ceiling
    # cannot be exceeded.
    score = (
        F.when(F.col("is_critical_temp"), F.lit(45)).otherwise(F.lit(0))
        + F.when(F.col("is_overheating") & ~F.col("is_critical_temp"), F.lit(20)).otherwise(F.lit(0))
        + F.when(F.col("is_oil_pressure_low"), F.lit(25)).otherwise(F.lit(0))
        + F.when(F.col("is_coolant_pressure_low"), F.lit(10)).otherwise(F.lit(0))
        + F.when(F.col("is_battery_low"), F.lit(5)).otherwise(F.lit(0))
        + F.when(F.col("is_redline"), F.lit(5)).otherwise(F.lit(0))
        + F.when(F.col("has_dtc"), F.lit(10)).otherwise(F.lit(0))
    )

    return out.withColumn("anomaly_score", F.least(score, F.lit(100)))


def windowed_telemetry(
    df: DataFrame,
    window_duration: str = DEFAULT_WINDOW,
    watermark: str = DEFAULT_WATERMARK,
) -> DataFrame:
    """Aggregate scored silver rows into fixed event-time windows per vehicle.

    The watermark bounds aggregation state and defines how late an event may be
    and still be folded into its window. Late events beyond it are dropped by
    Spark rather than silently corrupting a closed window.
    """
    return (
        df.withWatermark("event_ts", watermark)
        .groupBy(
            F.window(F.col("event_ts"), window_duration).alias("w"),
            F.col("vehicle_id"),
            F.col("vin"),
        )
        .agg(
            F.count("*").alias("reading_count"),
            F.avg("engine_temp_c").alias("avg_engine_temp_c"),
            F.max("engine_temp_c").alias("max_engine_temp_c"),
            F.min("engine_temp_c").alias("min_engine_temp_c"),
            F.stddev_samp("engine_temp_c").alias("stddev_engine_temp_c"),
            F.avg("speed_kph").alias("avg_speed_kph"),
            F.max("speed_kph").alias("max_speed_kph"),
            F.avg("engine_rpm").alias("avg_engine_rpm"),
            F.min("coolant_pressure_kpa").alias("min_coolant_pressure_kpa"),
            F.min("oil_pressure_kpa").alias("min_oil_pressure_kpa"),
            F.min("battery_voltage").alias("min_battery_voltage"),
            F.min("fuel_level_pct").alias("end_fuel_level_pct"),
            F.max("odometer_km").alias("end_odometer_km"),
            F.min("odometer_km").alias("start_odometer_km"),
            F.sum(F.col("is_overheating").cast("int")).alias("overheating_readings"),
            F.sum(F.col("is_critical_temp").cast("int")).alias("critical_temp_readings"),
            F.sum(F.col("is_oil_pressure_low").cast("int")).alias("low_oil_pressure_readings"),
            F.sum(F.col("is_late_arrival").cast("int")).alias("late_arrival_readings"),
            F.max("anomaly_score").alias("max_anomaly_score"),
            F.avg("anomaly_score").alias("avg_anomaly_score"),
            F.collect_set("dtc_code").alias("dtc_codes"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "*",
        )
        .drop("w")
        .withColumn("event_date", F.to_date(F.col("window_start")))
        .withColumn(
            "distance_km",
            F.round(F.col("end_odometer_km") - F.col("start_odometer_km"), 2),
        )
        .withColumn(
            "overheating_rate",
            F.round(F.col("overheating_readings") / F.col("reading_count"), 4),
        )
    )


def vehicle_dimension(df: DataFrame) -> DataFrame:
    """Latest descriptive attributes per VIN, ready for an SCD2 merge.

    Emits one row per VIN carrying a hash of the tracked attributes. The
    notebook feeds this into MERGE / APPLY CHANGES INTO, which is what makes the
    dimension slowly-changing rather than overwritten.
    """
    latest = Window.partitionBy("vin").orderBy(F.col("event_ts").desc())
    ranked = df.withColumn("_rn", F.row_number().over(latest))

    return (
        ranked.filter(F.col("_rn") == 1)
        .select(
            "vin",
            "vehicle_id",
            "make",
            "model",
            "model_year",
            "fleet_region",
            F.col("odometer_km").alias("latest_odometer_km"),
            F.col("event_ts").alias("effective_from"),
        )
        .withColumn(
            "attribute_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("make"), F.lit("")),
                    F.coalesce(F.col("model"), F.lit("")),
                    F.coalesce(F.col("model_year").cast("string"), F.lit("")),
                    F.coalesce(F.col("fleet_region"), F.lit("")),
                ),
                256,
            ),
        )
    )


def fleet_health_summary(df: DataFrame) -> DataFrame:
    """Rank vehicles by sustained anomaly severity.

    Input is the windowed gold table. Ranking on windows rather than raw
    readings means a single bad sensor spike cannot put a healthy vehicle at the
    top of the list.
    """
    return (
        df.groupBy("vehicle_id", "vin")
        .agg(
            F.count("*").alias("window_count"),
            F.avg("avg_engine_temp_c").alias("mean_engine_temp_c"),
            F.max("max_engine_temp_c").alias("peak_engine_temp_c"),
            F.sum("critical_temp_readings").alias("total_critical_readings"),
            F.sum("overheating_readings").alias("total_overheating_readings"),
            F.avg("max_anomaly_score").alias("mean_window_anomaly_score"),
            F.sum("distance_km").alias("total_distance_km"),
        )
        .withColumn(
            "health_status",
            F.when(F.col("total_critical_readings") > 0, F.lit("critical"))
            .when(F.col("total_overheating_readings") > 0, F.lit("degraded"))
            .otherwise(F.lit("healthy")),
        )
        .orderBy(F.col("mean_window_anomaly_score").desc())
    )