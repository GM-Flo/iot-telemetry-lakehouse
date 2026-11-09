from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Spark launches worker processes with "python3" by default, which does not
# exist on Windows. Pin workers to the interpreter running the tests (the venv).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BASE_TS = datetime(2026, 11, 4, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("iot-transforms-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        # The default 1 GB is too small for a session of 400 tests, some of which
        # simulate hours of fleet data.
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True, scope="module")
def _release_cached_data():
    """Free what a test file cached once it finishes, so it cannot starve the files after it."""
    yield
    session = SparkSession.getActiveSession()
    if session is not None:
        session.catalog.clearCache()


def _bronze_schema() -> StructType:
    return StructType([
        StructField("raw_payload", StringType()),
        StructField("partition_key", StringType()),
        StructField("source_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("eventhub_enqueued_at", TimestampType()),
        StructField("ingested_at", TimestampType()),
    ])


def reading(
    vin: str = "1HGBH41JXMN109186",
    vehicle_id: str = "VEH-0001",
    offset_seconds: int = 0,
    engine_temp_c: float = 90.0,
    speed_kph: float = 60.0,
    engine_rpm: int = 2200,
    coolant_pressure_kpa: float = 110.0,
    oil_pressure_kpa: float = 300.0,
    battery_voltage: float = 14.1,
    fuel_level_pct: float = 70.0,
    odometer_km: float = 100_000.0,
    ignition_on: bool = True,
    dtc_code: str | None = None,
    make: str = "Toyota",
    model: str = "Camry",
    model_year: int = 2021,
    fleet_region: str = "houston-north",
    emitted_offset_seconds: int | None = None,
) -> dict:
    """One well-formed telemetry reading with overridable fields."""
    emitted = BASE_TS + timedelta(
        seconds=offset_seconds if emitted_offset_seconds is None else emitted_offset_seconds
    )
    return {
        "vin": vin,
        "vehicle_id": vehicle_id,
        "event_ts": (BASE_TS + timedelta(seconds=offset_seconds)).isoformat(),
        "emitted_at": emitted.isoformat(),
        "make": make,
        "model": model,
        "model_year": model_year,
        "fleet_region": fleet_region,
        "ignition_on": ignition_on,
        "speed_kph": speed_kph,
        "engine_rpm": engine_rpm,
        "engine_temp_c": engine_temp_c,
        "coolant_pressure_kpa": coolant_pressure_kpa,
        "oil_pressure_kpa": oil_pressure_kpa,
        "battery_voltage": battery_voltage,
        "fuel_level_pct": fuel_level_pct,
        "odometer_km": odometer_km,
        "latitude": 29.86,
        "longitude": -95.56,
        "heading_deg": 180.0,
        "dtc_code": dtc_code,
    }


@pytest.fixture
def make_bronze(spark):
    """Build a bronze-shaped DataFrame from raw payload strings.

    Accepts dicts (serialized to JSON) or strings (passed through verbatim, so
    malformed payloads survive into the DataFrame).
    """

    def _build(
        payloads,
        ingested_offset_seconds: int = 30,
        processed_offset_seconds: int | None = None,
    ):
        """
        ingested_offset_seconds  -> eventhub_enqueued_at, when Event Hubs received it
        processed_offset_seconds -> ingested_at, when the pipeline read it
                                    (defaults to the same instant; set it far
                                    ahead to simulate a historical replay)
        """
        if processed_offset_seconds is None:
            processed_offset_seconds = (
                ingested_offset_seconds if ingested_offset_seconds is not None else 0
            )

        enqueued = (
            None if ingested_offset_seconds is None
            else BASE_TS + timedelta(seconds=ingested_offset_seconds)
        )

        rows = []
        for i, payload in enumerate(payloads):
            if payload is None:
                body = None
            elif isinstance(payload, str):
                body = payload
            else:
                body = json.dumps(payload)
            rows.append((
                body,
                None,
                "vehicle-telemetry",
                i % 4,
                1000 + i,
                enqueued,
                BASE_TS + timedelta(seconds=processed_offset_seconds),
            ))
        return spark.createDataFrame(rows, schema=_bronze_schema())

    return _build


@pytest.fixture
def silver_of(make_bronze):
    """bronze payloads -> clean, cast, scored silver rows."""
    from src.iot import transforms as T

    def _build(
        payloads,
        ingested_offset_seconds: int = 30,
        processed_offset_seconds: int | None = None,
    ):
        bronze = make_bronze(payloads, ingested_offset_seconds, processed_offset_seconds)
        clean, _ = T.split_quarantine(T.parse_payload(bronze))
        return T.flag_anomalies(T.filter_implausible(T.to_silver(clean)))

    return _build