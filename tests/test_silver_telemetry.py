"""Tests for silver telemetry stages, fed with real producer messages.

Corrupted inputs come from the producer's own corrupt() function, so these tests
check silver against exactly the faults the producer injects in production runs.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone

import pytest

from src.iot.silver.telemetry import parse_envelope, quarantine_unparseable
from src.producer.fleet import Fleet
from src.producer.produce import corrupt
from src.producer.registry import VENDORS, select_clients
from src.producer.vendors import Gateway

KNOWN_VENDORS = sorted(VENDORS)
NOW = datetime(2026, 11, 6, 18, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def envelopes():
    """Real gateway envelopes: a few ticks of the dev fleet, all four vendors."""
    fleet = Fleet(select_clients(1), seed=11, glitch_fraction=0)
    gateway = Gateway(random.Random(11))
    out = []
    for tick in range(3):
        now = NOW + timedelta(seconds=30 * tick)
        fleet.advance(30, now=now)
        out += [e for _, e in gateway.messages(fleet.readings(now=now, late_fraction=0), now)]
    return out


@pytest.fixture
def bronze_of(spark):
    def _build(raw_payloads):
        rows = [
            (raw, "KEY", i % 4, 100 + i, NOW, NOW + timedelta(seconds=5))
            for i, raw in enumerate(raw_payloads)
        ]
        return spark.createDataFrame(
            rows,
            "raw_payload string, partition_key string, kafka_partition int, kafka_offset long, "
            "eventhub_enqueued_at timestamp, ingested_at timestamp",
        )

    return _build


def parse(bronze_of, raws):
    return parse_envelope(bronze_of(raws), KNOWN_VENDORS).collect()


class TestParseEnvelope:
    def test_valid_messages_from_every_vendor_parse(self, bronze_of, envelopes):
        rows = parse(bronze_of, [json.dumps(e) for e in envelopes])

        assert all(r["parse_error"] is None for r in rows)
        assert {r["vendor_id"] for r in rows} == set(KNOWN_VENDORS)

    def test_payload_stays_as_json_text(self, bronze_of, envelopes):
        """Declared as a string, the payload object comes back as its JSON text."""
        original = next(e for e in envelopes if e["vendor_id"] == "gtk")
        row = parse(bronze_of, [json.dumps(original)])[0]

        assert isinstance(row["payload"], str)
        assert json.loads(row["payload"]) == original["payload"]

    def test_message_id_is_the_uuid(self, bronze_of, envelopes):
        row = parse(bronze_of, [json.dumps(envelopes[0])])[0]
        assert row["message_id"] == envelopes[0]["message_id"]

    def test_trace_columns_survive(self, bronze_of, envelopes):
        row = parse(bronze_of, [json.dumps(envelopes[0])])[0]

        assert row["kafka_offset"] == 100
        assert row["raw_payload"] == json.dumps(envelopes[0])

    @pytest.mark.parametrize("raw, reason", [
        (None, "null_payload"),
        ('{"envelope_version": "1.0", "message_id": "ab', "malformed_json"),
        ("vendor=tmx;id=123", "malformed_json"),
        ("", "malformed_json"),
    ])
    def test_unreadable_messages_get_a_reason(self, bronze_of, raw, reason):
        assert parse(bronze_of, [raw])[0]["parse_error"] == reason

    def test_missing_payload(self, bronze_of, envelopes):
        broken = {k: v for k, v in envelopes[0].items() if k != "payload"}
        assert parse(bronze_of, [json.dumps(broken)])[0]["parse_error"] == "missing_payload"

    def test_missing_message_id(self, bronze_of, envelopes):
        broken = {k: v for k, v in envelopes[0].items() if k != "message_id"}
        assert parse(bronze_of, [json.dumps(broken)])[0]["parse_error"] == "missing_message_id"

    def test_unknown_vendor(self, bronze_of, envelopes):
        broken = {**envelopes[0], "vendor_id": "zzz"}
        assert parse(bronze_of, [json.dumps(broken)])[0]["parse_error"] == "unknown_vendor"

    def test_missing_vendor_is_unknown(self, bronze_of, envelopes):
        """isin() on a null gives null, not false, so a missing vendor needs its own check."""
        broken = {k: v for k, v in envelopes[0].items() if k != "vendor_id"}
        assert parse(bronze_of, [json.dumps(broken)])[0]["parse_error"] == "unknown_vendor"

    def test_every_row_comes_out_exactly_once(self, bronze_of, envelopes):
        """Nothing silently dropped: the start of reconciliation."""
        raws = [json.dumps(e) for e in envelopes] + ["garbage", None]
        assert len(parse(bronze_of, raws)) == len(raws)

    def test_producer_corruptions_are_caught_or_left_for_the_contract_stage(self, bronze_of, envelopes):
        """Some corruption hides inside a valid envelope. Parse must not pass it off as clean
        unless the damage is inside the payload, which the contract stage checks next."""
        rng = random.Random(5)
        raws = [corrupt(e, rng) for e in envelopes]
        rows = parse(bronze_of, raws)

        for raw, row in zip(raws, rows, strict=True):
            if row["parse_error"] is None:
                payload = json.loads(row["payload"])
                assert "n/a" in json.dumps(payload), raw


class TestRecordStatus:
    def test_clean_message_passes_and_broken_message_fails(self, bronze_of, envelopes):
        rows = parse(bronze_of, [json.dumps(envelopes[0]), "garbage"])

        assert [r["record_status"] for r in rows] == ["pass", "fail"]

    def test_status_and_reason_always_agree(self, bronze_of, envelopes):
        rng = random.Random(9)
        raws = [json.dumps(e) for e in envelopes] + [corrupt(e, rng) for e in envelopes]
        for row in parse(bronze_of, raws):
            assert (row["record_status"] == "fail") == (row["parse_error"] is not None)


class TestQuarantineUnparseable:
    def test_only_failed_rows_are_quarantined(self, bronze_of, envelopes):
        raws = [json.dumps(e) for e in envelopes[:5]] + ["garbage", None]
        quarantined = quarantine_unparseable(parse_envelope(bronze_of(raws), KNOWN_VENDORS)).collect()

        assert len(quarantined) == 2
        assert {r["quarantine_reason"] for r in quarantined} == {"malformed_json", "null_payload"}

    def test_quarantine_row_keeps_what_a_replay_needs(self, bronze_of):
        row = quarantine_unparseable(parse_envelope(bronze_of(["garbage"]), KNOWN_VENDORS)).collect()[0]

        assert row["raw_payload"] == "garbage"
        assert row["quarantine_stage"] == "parse"
        assert row["kafka_offset"] == 100
        assert row["quarantined_at"] is not None
        assert row["replayed"] is False

    def test_reconciliation_holds(self, bronze_of, envelopes):
        """passed + quarantined == bronze. Nothing disappears."""
        rng = random.Random(3)
        raws = [json.dumps(e) for e in envelopes] + [corrupt(e, rng) for e in envelopes[:20]]
        parsed = parse_envelope(bronze_of(raws), KNOWN_VENDORS)

        passed = parsed.filter("record_status = 'pass'").count()
        quarantined = quarantine_unparseable(parsed).count()
        assert passed + quarantined == len(raws)