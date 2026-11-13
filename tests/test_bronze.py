"""Tests for the bronze ingestion helpers: the telemetry stream and registry files."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.iot.bronze import to_bronze, to_registry_bronze

LANDING = "dbfs:/Volumes/iot_dev/bronze/landing/registry"


@pytest.fixture
def auto_loader_rows(spark):
    """Rows shaped like an Auto Loader whole-text read, including _metadata."""

    def _build(*file_names):
        rows = [
            ('{"vendors": []}', (f"{LANDING}/{name}", datetime(2026, 11, 7, 3, 55)))
            for name in file_names
        ]
        return spark.createDataFrame(
            rows,
            "value string, _metadata struct<file_path: string, file_modification_time: timestamp>",
        )

    return _build


class TestRegistryBronze:
    def test_snapshot_date_comes_from_file_name(self, auto_loader_rows):
        row = to_registry_bronze(auto_loader_rows("fleet_registry_2026-11-03.json")).collect()[0]

        assert row["snapshot_date"] == date(2026, 11, 3)

    def test_snapshot_date_ignores_upload_time(self, auto_loader_rows):
        """A corrected old file uploaded today keeps its own date."""
        row = to_registry_bronze(auto_loader_rows("fleet_registry_2026-10-15.json")).collect()[0]

        assert row["file_modified_at"].date() == date(2026, 11, 7)
        assert row["snapshot_date"] == date(2026, 10, 15)

    @pytest.mark.parametrize("bad_name", [
        "registry_final_v2.json",
        "fleet_registry_2026-11-07.json.bak",
        "fleet_registry_07-11-2026.json",
    ])
    def test_unrecognised_file_name_gives_null_date(self, auto_loader_rows, bad_name):
        row = to_registry_bronze(auto_loader_rows(bad_name)).collect()[0]

        assert row["snapshot_date"] is None

    def test_content_stays_raw(self, auto_loader_rows):
        row = to_registry_bronze(auto_loader_rows("fleet_registry_2026-11-07.json")).collect()[0]

        assert row["raw_content"] == '{"vendors": []}'
        assert row["source_file_path"].endswith("fleet_registry_2026-11-07.json")
        assert row["ingested_at"] is not None


class TestTelemetryBronze:
    def test_bronze_keeps_payload_opaque(self, spark):
        """Bronze must not parse. Replayability depends on it."""
        kafka_like = spark.createDataFrame(
            [(b'{"vin":"AAA"}', b"key1", "vehicle-telemetry", 0, 42)],
            "value binary, key binary, topic string, partition int, offset long",
        ).withColumn("timestamp", F.current_timestamp())

        bronze = to_bronze(kafka_like)
        row = bronze.collect()[0]

        assert row["raw_payload"] == '{"vin":"AAA"}'
        assert row["partition_key"] == "key1"
        assert row["kafka_offset"] == 42
        assert row["ingested_at"] is not None
        assert "vin" not in bronze.columns
