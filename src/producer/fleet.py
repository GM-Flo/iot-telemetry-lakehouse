"""Stateful vehicle fleet simulator.

Telemetry is generated from persistent per-vehicle state rather than independent
random draws, so downstream windowed aggregates and anomaly scores have real
shape: odometers only increase, engine temperature drifts with load, and a
designated subset of vehicles degrades over the run.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from faker import Faker

MAKES = {
    "Toyota": ["Camry", "Corolla", "RAV4", "Hilux"],
    "Ford": ["F-150", "Transit", "Escape", "Ranger"],
    "Honda": ["Civic", "Accord", "CR-V"],
    "Chevrolet": ["Silverado", "Equinox", "Malibu"],
    "Nissan": ["Altima", "Rogue", "Frontier"],
}

DTC_CODES = [
    "P0128",  # coolant thermostat below regulating temperature
    "P0300",  # random cylinder misfire
    "P0420",  # catalyst efficiency below threshold
    "P0171",  # system too lean
    "P0442",  # evaporative leak, small
    "P0562",  # system voltage low
]

# Houston. Keeps generated coordinates plausible for an auto-repair fleet.
DEPOT_LAT, DEPOT_LON = 29.8601, -95.5637

NOMINAL_ENGINE_TEMP_C = 90.0
MAX_ENGINE_TEMP_C = 125.0


@dataclass
class Vehicle:
    """One vehicle's identity and mutable physical state."""

    vehicle_id: str
    vin: str
    make: str
    model: str
    model_year: int
    fleet_region: str

    odometer_km: float
    engine_temp_c: float = NOMINAL_ENGINE_TEMP_C
    coolant_pressure_kpa: float = 110.0
    rpm: int = 800
    speed_kph: float = 0.0
    fuel_level_pct: float = 100.0
    battery_voltage: float = 14.1
    oil_pressure_kpa: float = 310.0

    lat: float = DEPOT_LAT
    lon: float = DEPOT_LON
    heading_deg: float = 0.0
    trip_target_kph: float = 0.0

    ignition_on: bool = True
    degrading: bool = False
    degradation_max_offset_c: float = 0.0
    degradation_tau_hours: float = 3.0
    active_dtc: str | None = None

    _ticks: int = field(default=0, repr=False)
    _elapsed_s: float = field(default=0.0, repr=False)

    @property
    def thermal_offset_c(self) -> float:
        """Heat the cooling fault has added so far.

        Asymptotic in elapsed simulated time rather than tick count, so a
        degrading vehicle follows the same curve whether it is replayed in a
        6-hour backfill or streamed live at a 5-second interval.
        """
        if not self.degrading:
            return 0.0
        hours = self._elapsed_s / 3600.0
        return self.degradation_max_offset_c * (
            1.0 - math.exp(-hours / max(0.1, self.degradation_tau_hours))
        )

    def tick(self, rng: random.Random, seconds: float) -> None:
        """Advance physical state by `seconds` of simulated time."""
        self._ticks += 1
        self._elapsed_s += seconds

        if self.ignition_on and rng.random() < 0.02:
            self.ignition_on = False
        elif not self.ignition_on and rng.random() < 0.25:
            self.ignition_on = True

        if not self.ignition_on:
            self.rpm = 0
            self.speed_kph = 0.0
            self.engine_temp_c = max(25.0, self.engine_temp_c - 4.0)
            self.oil_pressure_kpa = 0.0
            self.battery_voltage = round(rng.uniform(12.2, 12.7), 2)
            return

        # Mean-revert toward a trip target rather than random-walking from zero,
        # so engine load reaches realistic values within a few ticks.
        if self.trip_target_kph == 0.0 or rng.random() < 0.04:
            self.trip_target_kph = rng.choice([
                rng.uniform(0, 15),     # traffic or depot
                rng.uniform(30, 55),    # city
                rng.uniform(55, 85),    # suburban
                rng.uniform(95, 115),   # highway
            ])

        self.speed_kph = round(
            max(0.0, min(130.0,
                0.75 * self.speed_kph + 0.25 * self.trip_target_kph + rng.gauss(0, 3))),
            1,
        )

        load = self.speed_kph / 120.0
        self.rpm = int(max(700, min(5200, 800 + load * 3400 + rng.gauss(0, 180))))

        distance_km = self.speed_kph * (seconds / 3600.0)
        self.odometer_km = round(self.odometer_km + distance_km, 2)

        if distance_km > 0:
            self.heading_deg = (self.heading_deg + rng.gauss(0, 15)) % 360
            rad = math.radians(self.heading_deg)
            self.lat = round(self.lat + (distance_km / 111.0) * math.cos(rad), 6)
            self.lon = round(
                self.lon
                + (distance_km / (111.0 * math.cos(math.radians(self.lat)))) * math.sin(rad),
                6,
            )

        thermal_target = NOMINAL_ENGINE_TEMP_C + load * 12.0 + self.thermal_offset_c

        self.engine_temp_c = round(
            min(MAX_ENGINE_TEMP_C, 0.85 * self.engine_temp_c + 0.15 * thermal_target
                + rng.gauss(0, 0.8)),
            1,
        )

        self.coolant_pressure_kpa = round(
            95.0 + (self.engine_temp_c - NOMINAL_ENGINE_TEMP_C) * 1.8 + rng.gauss(0, 3), 1
        )
        self.oil_pressure_kpa = round(
            200.0 + (self.rpm / 5200.0) * 250.0 + rng.gauss(0, 12), 1
        )
        self.battery_voltage = round(rng.uniform(13.8, 14.4), 2)

        self.fuel_level_pct = round(max(0.0, self.fuel_level_pct - distance_km * 0.12), 2)
        if self.fuel_level_pct <= 2.0:
            self.fuel_level_pct = 100.0

        if self.engine_temp_c > 115.0 and self.active_dtc is None and rng.random() < 0.3:
            self.active_dtc = "P0128"
        elif self.active_dtc is None and rng.random() < 0.004:
            self.active_dtc = rng.choice(DTC_CODES)
        elif self.active_dtc is not None and rng.random() < 0.05:
            self.active_dtc = None

    def reading(self, event_ts: datetime, emitted_at: datetime | None = None) -> dict:
        """Serialize current state as one telemetry event.

        event_ts is device time, which a buffering or reconnecting device can
        backdate. emitted_at is when the producer actually transmitted. Keeping
        both is what makes late arrival measurable downstream without relying on
        broker metadata.
        """
        return {
            "vin": self.vin,
            "vehicle_id": self.vehicle_id,
            "event_ts": event_ts.isoformat(),
            "emitted_at": (emitted_at or event_ts).isoformat(),
            "make": self.make,
            "model": self.model,
            "model_year": self.model_year,
            "fleet_region": self.fleet_region,
            "ignition_on": self.ignition_on,
            "speed_kph": self.speed_kph,
            "engine_rpm": self.rpm,
            "engine_temp_c": self.engine_temp_c,
            "coolant_pressure_kpa": self.coolant_pressure_kpa,
            "oil_pressure_kpa": self.oil_pressure_kpa,
            "battery_voltage": self.battery_voltage,
            "fuel_level_pct": self.fuel_level_pct,
            "odometer_km": self.odometer_km,
            "latitude": self.lat,
            "longitude": self.lon,
            "heading_deg": round(self.heading_deg, 1),
            "dtc_code": self.active_dtc,
        }


class Fleet:
    """A fixed set of vehicles advanced in lockstep."""

    def __init__(
        self,
        size: int = 25,
        seed: int | None = 42,
        degrading_fraction: float = 0.12,
    ) -> None:
        self.rng = random.Random(seed)
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)

        regions = ["houston-north", "houston-west", "houston-south", "austin"]
        self.vehicles: list[Vehicle] = []

        for i in range(size):
            make = self.rng.choice(list(MAKES))
            vehicle = Vehicle(
                vehicle_id=f"VEH-{i + 1:04d}",
                vin=self.faker.vin() if hasattr(self.faker, "vin") else self._fake_vin(),
                make=make,
                model=self.rng.choice(MAKES[make]),
                model_year=self.rng.randint(2012, 2025),
                fleet_region=self.rng.choice(regions),
                odometer_km=round(self.rng.uniform(5_000, 280_000), 2),
                lat=round(DEPOT_LAT + self.rng.gauss(0, 0.05), 6),
                lon=round(DEPOT_LON + self.rng.gauss(0, 0.05), 6),
                heading_deg=self.rng.uniform(0, 360),
            )

            if self.rng.random() < degrading_fraction:
                vehicle.degrading = True
                vehicle.degradation_max_offset_c = self.rng.uniform(22.0, 34.0)
                vehicle.degradation_tau_hours = self.rng.uniform(1.5, 4.0)

            self.vehicles.append(vehicle)

    def _fake_vin(self) -> str:
        alphabet = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
        return "".join(self.rng.choice(alphabet) for _ in range(17))

    def advance(self, seconds: float) -> None:
        for vehicle in self.vehicles:
            vehicle.tick(self.rng, seconds)

    def readings(
        self,
        now: datetime | None = None,
        late_fraction: float = 0.05,
        late_delay_minutes: int = 10,
    ) -> list[dict]:
        """Emit one reading per vehicle.

        A `late_fraction` share carry an event_ts pushed into the past, so the
        silver layer's watermark handling has a late-arrival path to exercise.
        """
        now = now or datetime.now(timezone.utc)
        events = []

        for vehicle in self.vehicles:
            event_ts = now
            if self.rng.random() < late_fraction:
                event_ts = now - timedelta(
                    minutes=self.rng.uniform(late_delay_minutes * 0.5, late_delay_minutes * 1.5)
                )
            events.append(vehicle.reading(event_ts, emitted_at=now))

        return events

    @property
    def degrading_ids(self) -> list[str]:
        return [v.vehicle_id for v in self.vehicles if v.degrading]