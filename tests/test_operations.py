"""Tests for operations tables, using event log rows shaped like Lakeflow's."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from src.iot.operations import (
    pipeline_flow_runs,
    pipeline_runs,
    quarantine_summary_by_hour,
    silver_reconciliation,
)
from src.iot.silver.telemetry import QUARANTINE_COLUMNS

T0 = datetime(2026, 11, 7, 9, 0, tzinfo=timezone.utc)
EVENT_LOG_SCHEMA = "origin struct<update_id:string,flow_name:string>, event_type string, timestamp timestamp, details string"


def update_event(update_id, seconds, state):
    return ((update_id, None), "update_progress", T0 + timedelta(seconds=seconds),
            json.dumps({"update_progress": {"state": state}}))


def flow_event(update_id, flow, seconds, status, rows=None, upserted=None, failed=None):
    progress = {"status": status, "metrics": {}}
    if rows is not None:
        progress["metrics"]["num_output_rows"] = rows
    if upserted is not None:
        progress["metrics"]["num_upserted_rows"] = upserted
    if failed is not None:
        progress["data_quality"] = {"expectations": [
            {"name": "check", "dataset": flow, "passed_records": 10, "failed_records": failed},
        ]}
    return ((update_id, flow), "flow_progress", T0 + timedelta(seconds=seconds), json.dumps({"flow_progress": progress}))


@pytest.fixture
def event_log(spark):
    rows = [
        update_event("u1", 0, "INITIALIZING"),
        update_event("u1", 10, "RUNNING"),
        flow_event("u1", "quarantine_unparseable_messages", 20, "RUNNING", rows=15),
        flow_event("u1", "quarantine_unparseable_messages", 30, "COMPLETED", rows=10),
        flow_event("u1", "vehicle_measurements", 40, "COMPLETED", upserted=3643),
        flow_event("u1", "device_assignments", 45, "COMPLETED", rows=30, failed=2),
        update_event("u1", 300, "COMPLETED"),
        update_event("u2", 400, "RUNNING"),
        flow_event("u2", "vehicle_measurements", 420, "RUNNING", upserted=5),
    ]
    return spark.createDataFrame(rows, EVENT_LOG_SCHEMA)


class TestPipelineFlowRuns:
    def test_rows_are_summed_across_micro_batches(self, event_log):
        flows = {r["flow_name"]: r for r in pipeline_flow_runs(event_log).filter("update_id = 'u1'").collect()}
        assert flows["quarantine_unparseable_messages"]["rows_written"] == 25
        assert flows["quarantine_unparseable_messages"]["final_status"] == "COMPLETED"

    def test_auto_cdc_flows_report_upserted_rows(self, event_log):
        flows = {r["flow_name"]: r for r in pipeline_flow_runs(event_log).filter("update_id = 'u1'").collect()}
        assert flows["vehicle_measurements"]["rows_written"] == 3643

    def test_expectation_failures_are_counted(self, event_log):
        flows = {r["flow_name"]: r for r in pipeline_flow_runs(event_log).filter("update_id = 'u1'").collect()}
        assert flows["device_assignments"]["expectation_failures"] == 2
        assert flows["vehicle_measurements"]["expectation_failures"] == 0


class TestPipelineRuns:
    def test_only_finished_updates_appear(self, event_log):
        assert [r["update_id"] for r in pipeline_runs(event_log).collect()] == ["u1"]

    def test_run_summary(self, event_log):
        run = pipeline_runs(event_log).first()
        assert run["final_state"] == "COMPLETED"
        assert run["duration_seconds"] == 300
        assert run["flows"] == 3
        assert run["failed_flows"] == 0
        assert run["rows_to_vehicle_measurements"] == 3643
        assert run["rows_quarantined"] == 25
        assert run["expectation_failures"] == 2


def quarantine_rows(spark, rows):
    schema = (
        "message_id string, measurement_id string, reading_index int, device_id string, event_ts timestamp, "
        "vendor_id string, raw_payload string, "
        "quarantine_stage string, quarantine_reason string, quarantine_detail string, kafka_partition int, "
        "kafka_offset long, eventhub_enqueued_at timestamp, ingested_at timestamp, quarantined_at timestamp, "
        "replayed boolean"
    )
    assert [c.split()[0] for c in schema.split(", ")] == QUARANTINE_COLUMNS
    return spark.createDataFrame(
        [("m", None, None, None, None, vendor, None, stage, reason, None, 0, 1, arrived, arrived,
          T0 + timedelta(days=1), replayed)
         for vendor, stage, reason, arrived, replayed in rows],
        schema,
    )


class TestQuarantineSummary:
    def test_grouped_by_arrival_hour_not_processing_time(self, spark):
        three_am = datetime(2026, 11, 7, 3, 15, tzinfo=timezone.utc)
        parse = quarantine_rows(spark, [("fpl", "parse", "malformed_json", three_am, False)])
        validity = quarantine_rows(spark, [
            ("fpl", "validity", "gps_null_island", three_am, False),
            ("fpl", "validity", "gps_null_island", three_am + timedelta(minutes=30), True),
        ])
        summary = quarantine_summary_by_hour(parse, validity)
        rows = summary.withColumn("hour_utc", F.hour("arrival_hour")).collect()
        by_reason = {r["quarantine_reason"]: r for r in rows}

        assert {r["hour_utc"] for r in rows} == {3}
        assert by_reason["gps_null_island"]["quarantined"] == 2
        assert by_reason["gps_null_island"]["replayed"] == 1
        assert by_reason["malformed_json"]["quarantine_stage"] == "parse"


class TestSilverReconciliation:
    """Readings are compared by identity, (device_id, event_ts), in every table."""

    @pytest.fixture
    def run(self, spark):
        def readings(rows):
            return spark.createDataFrame(rows, "device_id string, event_ts long, speed double")

        def _run(expected, stored=(), invalid=(), mismatched=()):
            messages = spark.createDataFrame([(i,) for i in range(10)], "x int")
            return silver_reconciliation(
                messages, readings(expected), messages.limit(2), messages.limit(1),
                readings(stored), readings(invalid), readings(mismatched), content_columns=["speed"],
            ).first()

        return _run

    def test_balanced_when_every_reading_is_somewhere(self, run):
        result = run(
            expected=[("A", 1, 50.0), ("A", 2, 50.0), ("B", 1, 50.0), (None, 9, 50.0)],
            stored=[("A", 1, 50.0), ("A", 2, 50.0)],
            mismatched=[("B", 1, 50.0)],
            invalid=[(None, 9, 50.0)],
        )
        assert (result["lost_readings"], result["unexpected_readings"]) == (0, 0)
        assert result["expected_readings"] == 4

    def test_a_lost_reading_shows_up(self, run):
        result = run(expected=[("A", 1, 50.0), ("A", 2, 50.0)], stored=[("A", 1, 50.0)])
        assert (result["lost_readings"], result["unexpected_readings"]) == (1, 0)

    def test_an_invented_reading_shows_up(self, run):
        result = run(expected=[("A", 1, 50.0)], stored=[("A", 1, 50.0), ("Z", 7, 50.0)])
        assert (result["lost_readings"], result["unexpected_readings"]) == (0, 1)

    def test_both_copies_of_a_duplicate_in_quarantine_are_not_double_counted(self, run):
        """The bug that made unaccounted_readings -3: append-only quarantine keeps both copies."""
        result = run(expected=[("A", 1, 50.0), ("A", 1, 50.0)], mismatched=[("A", 1, 50.0), ("A", 1, 50.0)])
        assert (result["lost_readings"], result["unexpected_readings"]) == (0, 0)
        assert result["duplicate_readings"] == 1

    def test_identical_and_conflicting_duplicates_are_told_apart(self, run):
        result = run(
            expected=[("A", 1, 50.0), ("A", 1, 50.0), ("B", 1, 40.0), ("B", 1, 90.0)],
            stored=[("A", 1, 50.0), ("B", 1, 90.0)],
        )
        assert result["duplicate_readings"] == 2
        assert result["conflicting_duplicates"] == 1
        assert (result["lost_readings"], result["unexpected_readings"]) == (0, 0)

    def test_a_reading_without_identity_is_matched_by_count(self, run):
        missing = run(expected=[(None, 1, 50.0), (None, 2, 50.0)], invalid=[(None, 1, 50.0)])
        extra = run(expected=[(None, 1, 50.0)], invalid=[(None, 1, 50.0), (None, 2, 50.0)])
        assert (missing["lost_readings"], missing["unexpected_readings"]) == (1, 0)
        assert (extra["lost_readings"], extra["unexpected_readings"]) == (0, 1)