"""Contract and behaviour tests for the multi-vendor producer. Pure Python, no Spark.

Contract tests check that every message the producer emits conforms to the
JSON Schema its envelope claims. If a formatter drifts from its contract, these
fail before anything reaches Event Hubs.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from src.producer.fleet import Fleet
from src.producer.produce import corrupt
from src.producer.registry import CLIENTS, UTC_OFFSET_HOURS, VENDORS, is_operating, select_clients
from src.producer.vendors import (
    GTK_TIME_FORMAT,
    KM_PER_MILE,
    KPA_PER_PSI,
    KPH_PER_MPH,
    Gateway,
    format_fpl,
    format_gtk,
)

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
# 18:00 UTC is midday in Texas, so every fleet is on duty.
NOW = datetime(2026, 11, 6, 18, 0, 0, tzinfo=timezone.utc)


def validator(name: str) -> Draft202012Validator:
    schema = json.loads((CONTRACTS / f"{name}.json").read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def simulate(clients_per_vendor=None, ticks=9, seed=7, late_fraction=0.05):
    """Run the fleet for a few ticks and return every gateway message."""
    fleet = Fleet(select_clients(clients_per_vendor), seed=seed, glitch_fraction=0)
    gateway = Gateway(random.Random(seed))
    messages = []
    for tick in range(ticks):
        now = NOW + timedelta(seconds=30 * tick)
        fleet.advance(30, now=now)
        messages += gateway.messages(fleet.readings(now=now, late_fraction=late_fraction), now)
    messages += gateway.flush(NOW + timedelta(seconds=30 * ticks))
    return fleet, messages


@pytest.fixture(scope="module")
def prod_messages():
    return simulate()[1]


class TestRegistry:
    def test_ids_are_unique(self):
        assert len({c.client_id for c in CLIENTS}) == len(CLIENTS)

    def test_every_client_belongs_to_a_known_vendor(self):
        assert all(c.vendor_id in VENDORS for c in CLIENTS)

    def test_dev_profile_takes_one_client_per_vendor(self):
        dev = select_clients(1)
        assert len(dev) == len(VENDORS)
        assert {c.vendor_id for c in dev} == set(VENDORS)

    def test_prod_profile_takes_every_client(self):
        assert select_clients(None) == CLIENTS


class TestFleet:
    def test_vehicle_and_device_ids_follow_conventions(self):
        fleet = Fleet(select_clients(), seed=1)
        for v in fleet.vehicles:
            assert v.vehicle_id.startswith(f"{v.client.client_id}-V")
            assert v.device_id.startswith(f"{v.vendor_id.upper()}-")

    def test_ids_unique_across_fleet(self):
        fleet = Fleet(select_clients(), seed=1)
        assert len({v.vin for v in fleet.vehicles}) == len(fleet.vehicles)
        assert len({v.device_id for v in fleet.vehicles}) == len(fleet.vehicles)

    @pytest.mark.parametrize("clients_per_vendor", [1, None])
    def test_every_vendor_has_a_failing_vehicle(self, clients_per_vendor):
        """Each vendor format must carry at least one known anomaly to detect."""
        fleet = Fleet(select_clients(clients_per_vendor), seed=3)
        assert {v.vendor_id for v in fleet.vehicles if v.degrading} == set(VENDORS)

    def test_same_seed_same_fleet(self):
        a = Fleet(select_clients(), seed=11).registry_export()
        b = Fleet(select_clients(), seed=11).registry_export()
        assert a == b

    def test_registry_export_covers_every_vehicle(self):
        fleet = Fleet(select_clients(1), seed=1)
        export = fleet.registry_export()
        assert len(export["vehicles"]) == len(fleet.vehicles)
        assert {v["vendor_id"] for v in export["vendors"]} == set(VENDORS)


class TestContracts:
    def test_every_contract_is_a_valid_schema(self):
        for name in ["envelope.v1", *[v.contract for v in VENDORS.values()]]:
            validator(name)

    def test_every_envelope_conforms(self, prod_messages):
        v = validator("envelope.v1")
        for _, envelope in prod_messages:
            v.validate(envelope)

    @pytest.mark.parametrize("vendor_id", sorted(VENDORS))
    def test_every_payload_conforms_to_its_contract(self, prod_messages, vendor_id):
        v = validator(VENDORS[vendor_id].contract)
        payloads = [e["payload"] for _, e in prod_messages if e["vendor_id"] == vendor_id]

        assert payloads, f"no {vendor_id} messages produced"
        for payload in payloads:
            v.validate(payload)

    def test_envelope_contract_matches_vendor(self, prod_messages):
        for _, e in prod_messages:
            assert e["contract"] == VENDORS[e["vendor_id"]].contract

    def test_contract_rejects_unknown_fields(self, prod_messages):
        """additionalProperties: false is what makes schema drift detectable."""
        payload = next(e["payload"] for _, e in prod_messages if e["vendor_id"] == "tmx")
        drifted = {**payload, "tire_pressure_kpa": 220.0}
        assert not validator("tmx.telemetry.v1").is_valid(drifted)

    def test_message_ids_unique(self, prod_messages):
        ids = [e["message_id"] for _, e in prod_messages]
        assert len(ids) == len(set(ids))


class TestUnitConversions:
    """Silver will reverse these. Tests pin the forward direction exactly."""

    @pytest.fixture
    def reading(self):
        fleet = Fleet(select_clients(1), seed=5, glitch_fraction=0)
        fleet.advance(30, now=NOW)
        return next(r for r in fleet.readings(now=NOW, late_fraction=0) if r["vendor_id"] == "fpl")

    def test_fpl_imperial_converts_back_to_metric(self, reading):
        p = format_fpl(reading)

        assert p["speedMph"] * KPH_PER_MPH == pytest.approx(reading["speed_kph"], abs=0.1)
        assert (p["engineTempF"] - 32) * 5 / 9 == pytest.approx(reading["engine_temp_c"], abs=0.1)
        assert p["oilPressurePsi"] * KPA_PER_PSI == pytest.approx(reading["oil_pressure_kpa"], abs=0.1)
        assert p["odometerMiles"] * KM_PER_MILE == pytest.approx(reading["odometer_km"], abs=0.02)

    def test_fpl_epoch_ms_round_trips(self, reading):
        ts = datetime.fromtimestamp(format_fpl(reading)["timestamp"] / 1000, tz=timezone.utc)
        assert abs((ts - reading["event_ts"]).total_seconds()) < 0.001

    def test_gtk_timestamp_has_no_zone_and_is_utc(self):
        fleet = Fleet(select_clients(1), seed=5, glitch_fraction=0)
        fleet.advance(30, now=NOW)
        r = next(x for x in fleet.readings(now=NOW, late_fraction=0) if x["vendor_id"] == "gtk")
        parsed = datetime.strptime(format_gtk(r)["time"], GTK_TIME_FORMAT)

        assert parsed.tzinfo is None
        assert parsed.replace(tzinfo=timezone.utc) == r["event_ts"].replace(microsecond=0)


class TestAutoSenseBatching:
    def test_batches_respect_size_limit(self, prod_messages):
        for _, e in prod_messages:
            if e["vendor_id"] == "ats":
                assert 1 <= len(e["payload"]["readings"]) <= 3

    def test_no_readings_lost_in_batching(self):
        fleet, messages = simulate(clients_per_vendor=None, ticks=7)
        ats_vehicles = sum(1 for v in fleet.transmitting_devices if v.vendor_id == "ats")
        batched = sum(len(e["payload"]["readings"]) for _, e in messages if e["vendor_id"] == "ats")

        assert batched == ats_vehicles * 7

    def test_late_reading_keeps_its_own_event_time_inside_a_batch(self):
        _, messages = simulate(late_fraction=1.0, ticks=3)
        batch = next(e["payload"] for _, e in messages if e["vendor_id"] == "ats")
        assert all(r["t"] < batch["sent"] for r in batch["readings"])


class TestPartitioning:
    def test_partition_key_is_device_id(self, prod_messages):
        for key, e in prod_messages:
            p = e["payload"]
            device = p.get("device_id") or p.get("deviceId") or p.get("dev") or p["device"]["id"]
            assert key == device


class TestEnvironmentGuard:
    CONN = ("Endpoint=sb://evhns-x.servicebus.windows.net/;SharedAccessKeyName=producer-send;"
            "SharedAccessKey=abc=;EntityPath=vehicle-telemetry-{env}")

    def test_refuses_dev_profile_against_prod_hub(self, monkeypatch):
        from src.producer import produce

        monkeypatch.setenv("EVENTHUB_CONN_STR", self.CONN.format(env="prod"))
        assert produce.main(["--env", "dev"]) == 2

    def test_refuses_bare_key(self, monkeypatch):
        from src.producer import produce

        monkeypatch.setenv("EVENTHUB_CONN_STR", "f85ctRnzZW65+ua9HUP0ipARysUKo8MQh+AEhGzaPxk=")
        assert produce.main(["--env", "dev"]) == 2

    def test_registry_export_needs_no_credentials(self, monkeypatch, tmp_path):
        from src.producer import produce

        monkeypatch.delenv("EVENTHUB_CONN_STR", raising=False)
        out = tmp_path / "registry.json"
        assert produce.main(["--env", "prod", "--export-registry", str(out)]) == 0
        assert len(json.loads(out.read_text())["vehicles"]) == sum(c.vehicle_count for c in CLIENTS)


class TestCorruption:
    def test_every_corruption_breaks_parsing_or_a_contract(self, prod_messages):
        rng = random.Random(0)
        envelope_v = validator("envelope.v1")

        for _, envelope in prod_messages[:200]:
            text = corrupt(envelope, rng)
            try:
                broken = json.loads(text)
            except json.JSONDecodeError:
                continue

            if not envelope_v.is_valid(broken):
                continue
            contract = validator(VENDORS[broken["vendor_id"]].contract)
            assert not contract.is_valid(broken["payload"]), text


def run_hours(fleet: Fleet, hours: float, start: datetime, step: float = 60.0) -> list[dict]:
    """Advance the fleet and collect canonical readings, without the gateway."""
    readings = []
    for tick in range(int(hours * 3600 / step)):
        now = start + timedelta(seconds=tick * step)
        fleet.advance(step, now=now)
        readings += fleet.readings(now=now, late_fraction=0)
    return readings


class TestIntegrationFaults:
    @pytest.mark.parametrize("clients_per_vendor", [1, None])
    def test_all_four_faults_present_in_both_profiles(self, clients_per_vendor):
        fleet = Fleet(select_clients(clients_per_vendor), seed=2)
        assert sorted(fleet.integration_issues.values()) == [
            "device_not_yet_registered",
            "device_reports_wrong_account",
            "device_reports_wrong_vin",
            "retired_device_still_transmitting",
        ]

    def _fleet_and_registry(self, **kwargs):
        fleet = Fleet(select_clients(1), seed=2, glitch_fraction=0)
        return fleet, fleet.registry_export(**kwargs)

    def test_retired_device_still_transmits_but_registry_says_retired(self):
        fleet, registry = self._fleet_and_registry()
        ghost = fleet.ghost_devices[0]
        record = next(a for a in registry["device_vehicle_assignments"] if a["device_id"] == ghost.device_id)

        assert record["status"] == "retired"
        assert record["valid_to"] is not None
        assert ghost in fleet.transmitting_devices

    def test_wrong_vin_device_reports_a_vin_the_registry_does_not_pair_with_it(self):
        fleet, registry = self._fleet_and_registry()
        vehicle_id = next(k for k, v in fleet.integration_issues.items() if v == "device_reports_wrong_vin")
        v = next(x for x in fleet.vehicles if x.vehicle_id == vehicle_id)
        paired = {a["vin"] for a in registry["device_vehicle_assignments"] if a["device_id"] == v.device_id}

        assert v.reading(NOW, NOW)["vin"] not in paired

    def test_unregistered_device_appears_only_after_late_registration(self):
        fleet, before = self._fleet_and_registry()
        _, after = self._fleet_and_registry(include_late_registrations=True)
        device = next(iter(fleet.unregistered_devices))

        assert device not in {a["device_id"] for a in before["device_vehicle_assignments"]}
        assert device in {a["device_id"] for a in after["device_vehicle_assignments"]}

    def test_wrong_account_belongs_to_another_vendor(self):
        fleet, registry = self._fleet_and_registry()
        vehicle_id = next(k for k, v in fleet.integration_issues.items() if v == "device_reports_wrong_account")
        v = next(x for x in fleet.vehicles if x.vehicle_id == vehicle_id)
        reported = v.reading(NOW, NOW)["client_id"]
        owner = next(c for c in registry["clients"] if c["client_id"] == reported)

        assert owner["vendor_id"] != v.vendor_id


class TestRegistryExport:
    def test_assignment_periods_never_overlap_per_vehicle(self):
        registry = Fleet(select_clients(), seed=4).registry_export(include_late_registrations=True)
        by_vehicle: dict[str, list[dict]] = {}
        for a in registry["device_vehicle_assignments"]:
            by_vehicle.setdefault(a["vehicle_id"], []).append(a)

        for vehicle_id, rows in by_vehicle.items():
            rows.sort(key=lambda a: a["valid_from"])
            assert sum(1 for a in rows if a["status"] == "active") == 1, vehicle_id
            for earlier, later in pairwise(rows):
                assert earlier["valid_to"] is not None
                assert earlier["valid_to"] <= later["valid_from"], vehicle_id

    def test_some_vehicles_have_device_history(self):
        registry = Fleet(select_clients(), seed=4).registry_export()
        retired = [a for a in registry["device_vehicle_assignments"] if a["status"] == "retired"]
        assert len(retired) >= 3

    def test_reference_lookups_exported(self):
        registry = Fleet(select_clients(1), seed=4).registry_export()

        assert len(registry["fuel_prices_by_region"]) == 61 * 4
        assert {f["code"] for f in registry["fault_code_descriptions"]} >= {"P0128", "P0300"}
        assert all(v["tank_capacity_l"] > 0 for v in registry["vehicles"])


class TestBehaviour:
    """Statistical but seeded: each signal a gold table depends on must exist."""

    def test_aggressive_drivers_brake_harshly_more_often(self):
        fleet = Fleet(select_clients(), seed=8, glitch_fraction=0)
        readings = [r for r in run_hours(fleet, 4, NOW) if r["driver_fob_id"]]
        by_fob = fleet.driver_by_fob

        def rate(style):
            rows = [r for r in readings if by_fob[r["driver_fob_id"]].style == style]
            return sum(r["harsh_brake_count"] for r in rows) / len(rows)

        assert rate("aggressive") > 2 * rate("calm")

    def test_daytime_fleets_park_overnight(self):
        fleet = Fleet(select_clients(1), seed=8, glitch_fraction=0)
        night = datetime(2026, 11, 6, 9, 0, tzinfo=timezone.utc)  # 03:00 in Texas
        readings = run_hours(fleet, 1, night)
        daytime = {v.vehicle_id for v in fleet.vehicles
                   if v.client.operating_hours != "24x7" and not v.home_driver.misuse_prone}
        always_on = {v.vehicle_id for v in fleet.vehicles if v.client.operating_hours == "24x7"}

        assert not any(r["ignition_on"] for r in readings if r["vehicle_id"] in daytime)
        assert any(r["ignition_on"] for r in readings if r["vehicle_id"] in always_on)

    def test_vehicles_stay_inside_their_region_unless_misused(self):
        fleet = Fleet(select_clients(), seed=8, glitch_fraction=0)
        run_hours(fleet, 8, NOW)
        for v in fleet.vehicles:
            if not v.home_driver.misuse_prone:
                assert v.distance_from_depot_km() < 50 * 1.1, v.vehicle_id

    def test_fuel_burns_and_tanks_get_refilled(self):
        fleet = Fleet(select_clients(1), seed=8, glitch_fraction=0)
        readings = run_hours(fleet, 12, NOW, step=120)
        by_device: dict[str, list[float]] = {}
        for r in readings:
            by_device.setdefault(r["device_id"], []).append(r["fuel_level_pct"])

        refuels = sum(1 for levels in by_device.values()
                      for a, b in pairwise(levels) if b - a > 20)
        burned = sum(1 for levels in by_device.values() if min(levels) < levels[0])

        assert burned >= len(by_device) // 2
        assert refuels >= 1

    def test_idling_happens(self):
        fleet = Fleet(select_clients(), seed=8, glitch_fraction=0)
        readings = run_hours(fleet, 2, NOW)
        idle = [r for r in readings if r["ignition_on"] and r["speed_kph"] == 0]
        assert len(idle) > 0.03 * len(readings)


class TestDrivers:
    def test_pool_covers_every_shift_plus_a_spare(self):
        fleet = Fleet(select_clients(), seed=8)
        for client in fleet.clients:
            vehicles = sum(1 for v in fleet.vehicles if v.client is client)
            drivers = sum(1 for d in fleet.drivers if d.client_id == client.client_id)
            shifts = 3 if client.operating_hours == "24x7" else 2
            assert drivers == vehicles * shifts + 1

    def test_driver_ids_and_fobs_unique(self):
        fleet = Fleet(select_clients(), seed=8)
        assert len({d.driver_id for d in fleet.drivers}) == len(fleet.drivers)
        assert len({d.fob_id for d in fleet.drivers}) == len(fleet.drivers)

    def test_fob_reported_only_while_engine_runs(self):
        fleet = Fleet(select_clients(1), seed=8, glitch_fraction=0)
        for r in run_hours(fleet, 1, NOW):
            assert (r["driver_fob_id"] is not None) == r["ignition_on"]

    def test_drivers_change_at_shift_boundaries(self):
        fleet = Fleet(select_clients(1), seed=8, glitch_fraction=0)
        start = datetime(2026, 11, 6, 19, 0, tzinfo=timezone.utc)  # 13:00 local: shift change
        readings = run_hours(fleet, 2, start - timedelta(hours=1))
        fobs: dict[str, set[str]] = {}
        for r in readings:
            if r["driver_fob_id"]:
                fobs.setdefault(r["vehicle_id"], set()).add(r["driver_fob_id"])
        assert any(len(f) > 1 for f in fobs.values())

    def test_unregistered_temp_drivers_appear_over_time(self):
        fleet = Fleet(select_clients(), seed=8, glitch_fraction=0)
        run_hours(fleet, 48, NOW, step=600)
        registered = {d["fob_id"] for d in fleet.registry_export()["drivers"]}

        assert fleet.temp_drivers
        assert all(t.fob_id not in registered for t in fleet.temp_drivers)

    def test_after_hours_misuse_is_by_the_daytime_driver(self):
        # Three nights, not one: a single night has a real chance of no misuse trip,
        # which made this test depend on luck. Three nights make that chance negligible.
        fleet = Fleet(select_clients(1), seed=8, glitch_fraction=0)
        night = datetime(2026, 11, 6, 2, 0, tzinfo=timezone.utc)  # 20:00 local, shifts just ended
        readings = run_hours(fleet, 72, night, step=300)
        client_of = {v.vehicle_id: v.client for v in fleet.vehicles}
        misuse = {d.fob_id for d in fleet.drivers if d.misuse_prone}

        def local_hour(r):
            return (r["event_ts"] + timedelta(hours=UTC_OFFSET_HOURS)).hour

        after_hours = [r for r in readings
                       if r["ignition_on"] and not is_operating(client_of[r["vehicle_id"]], local_hour(r))]

        assert after_hours
        assert {r["driver_fob_id"] for r in after_hours} <= misuse

    def test_registry_exports_drivers_and_regions(self):
        registry = Fleet(select_clients(), seed=8).registry_export()
        assert len(registry["drivers"]) == sum(
            c.vehicle_count * (3 if c.operating_hours == "24x7" else 2) + 1 for c in CLIENTS)
        assert {r["region"] for r in registry["regions"]} == {"houston", "austin", "dallas", "san-antonio"}


class TestGlitches:
    def test_every_glitch_is_one_of_the_three_kinds(self):
        fleet = Fleet(select_clients(1), seed=9, glitch_fraction=1.0)
        fleet.advance(30, now=NOW)
        for r in fleet.readings(now=NOW, late_fraction=0):
            null_island = r["latitude"] == 0.0 and r["longitude"] == 0.0
            future = r["event_ts"] > r["emitted_at"]
            spike = r["engine_temp_c"] == 999.9
            assert null_island or future or spike

    def test_glitched_readings_still_pass_their_contract(self):
        """Glitches are validity failures, not contract failures. Silver must tell them apart."""
        fleet = Fleet(select_clients(1), seed=9, glitch_fraction=1.0)
        gateway = Gateway(random.Random(9), ats_batch_size=1)
        fleet.advance(30, now=NOW)
        for _, e in gateway.messages(fleet.readings(now=NOW, late_fraction=0), NOW):
            validator(VENDORS[e["vendor_id"]].contract).validate(e["payload"])