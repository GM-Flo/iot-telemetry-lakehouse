"""Tests for the rules behind vehicle_measurements: what is stored, and which copy wins."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.iot.silver.measurements import MEASUREMENT_KEYS, latest_per_key, to_store

NOW = datetime(2026, 11, 6, 18, 0, tzinfo=timezone.utc)
SCHEMA = (
    "measurement_id string, message_id string, device_id string, event_ts timestamp, "
    "eventhub_enqueued_at timestamp, kafka_offset long, reported_vin string, reported_client_id string, "
    "vehicle_id string, client_id string, fault_code string, match_error string, match_detail string, "
    "match_warning string, record_status string"
)


@pytest.fixture
def readings(spark):
    def _build(*rows):
        defaults = {
            "measurement_id": "m", "message_id": "msg", "device_id": "TMX-000001", "event_ts": NOW,
            "eventhub_enqueued_at": NOW + timedelta(seconds=5), "kafka_offset": 1,
            "reported_vin": "V", "reported_client_id": "C001", "vehicle_id": "C001-V001", "client_id": "C001",
            "fault_code": None, "match_error": None, "match_detail": None, "match_warning": None,
            "record_status": "pass",
        }
        columns = [c.split()[0] for c in SCHEMA.split(", ")]
        return spark.createDataFrame([tuple({**defaults, **r}[c] for c in columns) for r in rows], SCHEMA)

    return _build


class TestToStore:
    def test_pass_and_warn_are_stored_fail_is_not(self, readings):
        df = readings(
            {"measurement_id": "a", "record_status": "pass"},
            {"measurement_id": "b", "record_status": "warn", "match_warning": "driver_unmatched"},
            {"measurement_id": "c", "record_status": "fail", "match_error": "unknown_device"},
        )
        assert sorted(r["measurement_id"] for r in to_store(df).collect()) == ["a", "b"]

    def test_failure_and_device_claim_columns_are_dropped(self, readings):
        columns = to_store(readings({})).columns
        for dropped in ("match_error", "match_detail", "reported_vin", "reported_client_id"):
            assert dropped not in columns
        assert {"vehicle_id", "client_id", "match_warning", "record_status"} <= set(columns)


class TestLatestPerKey:
    def test_same_message_delivered_twice_keeps_one(self, readings):
        df = readings({"kafka_offset": 1}, {"kafka_offset": 2})
        assert latest_per_key(df).count() == 1

    def test_resent_reading_with_new_message_id_keeps_one(self, readings):
        """measurement_id differs, so it cannot be the key. device and event time can."""
        df = readings(
            {"measurement_id": "m1", "message_id": "msg-1"},
            {"measurement_id": "m2", "message_id": "msg-2", "eventhub_enqueued_at": NOW + timedelta(hours=5)},
        )
        assert [r["measurement_id"] for r in latest_per_key(df).collect()] == ["m2"]

    def test_copy_received_last_wins(self, readings):
        df = readings(
            {"fault_code": "P0300", "eventhub_enqueued_at": NOW + timedelta(days=1)},
            {"fault_code": None, "eventhub_enqueued_at": NOW + timedelta(seconds=5)},
        )
        assert latest_per_key(df).first()["fault_code"] == "P0300"

    def test_offset_breaks_a_tie_in_receive_time(self, readings):
        df = readings({"kafka_offset": 9, "fault_code": "late"}, {"kafka_offset": 3, "fault_code": "early"})
        assert latest_per_key(df).first()["fault_code"] == "late"

    def test_different_instants_are_different_readings(self, readings):
        df = readings({"event_ts": NOW}, {"event_ts": NOW + timedelta(seconds=30)})
        assert latest_per_key(df).count() == 2

    def test_key_is_device_and_event_time(self):
        assert MEASUREMENT_KEYS == ["device_id", "event_ts"]
