"""Stateful vehicle fleet simulator.

Telemetry is generated from persistent state rather than independent random draws,
so every gold-layer question has a real signal to find:

  vehicle health     a known set of vehicles develops a cooling fault and overheats
  driver safety      each driver has a style; aggressive drivers brake and accelerate
                     harshly and speed, whichever vehicle they are in
  cost/utilization   fuel burns with distance, speed and idling; tanks are refilled;
                     idle-prone drivers leave engines running
  security           daytime fleets park overnight, except when a misuse-prone driver
                     takes their vehicle out after hours, sometimes out of the region
  registry checks    four deliberate integration faults, unregistered temporary
                     drivers, and occasional sensor glitches

Readings come out in one canonical, metric shape. Vendor formats are applied
afterwards by vendors.py, the way a real vendor platform would.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from src.producer.registry import (
    BASE_FUEL_PRICE_PER_LITRE,
    FAULT_CODES,
    GEOFENCE_RADIUS_KM,
    INTEGRATION_ISSUES,
    REGION_DEPOTS,
    UTC_OFFSET_HOURS,
    VENDORS,
    Client,
    is_operating,
    shift_slot,
    shifts_per_day,
)

MODELS = {
    "Toyota": {
        "Camry": 60, "Corolla": 50, "RAV4": 55, "Hilux": 80,
        "Yaris": 42, "Highlander": 68, "Tacoma": 80, "4Runner": 87,
        "Prius": 43, "Corolla Cross": 47, "Land Cruiser": 87,
    },
    "Ford": {
        "F-150": 98, "Transit": 95, "Escape": 57, "Ranger": 80,
        "Explorer": 68, "Mustang": 61, "Edge": 68, "Expedition": 106,
        "Focus": 52, "Maverick": 52, "Fusion": 62,
    },
    "Honda": {
        "Civic": 47, "Accord": 56, "CR-V": 57,
        "Fit": 40, "Pilot": 74, "HR-V": 50, "Odyssey": 73, "Ridgeline": 74,
    },
    "Chevrolet": {
        "Silverado": 91, "Equinox": 56, "Malibu": 59,
        "Tahoe": 98, "Suburban": 98, "Colorado": 80, "Traverse": 83,
        "Trax": 48, "Cruze": 52,
    },
    "Nissan": {
        "Altima": 61, "Rogue": 55, "Frontier": 80,
        "Sentra": 50, "Versa": 41, "Pathfinder": 74, "Murano": 72, "Kicks": 41,
    },
    "Hyundai": {
        "Elantra": 47, "Tucson": 54, "Santa Fe": 67, "Sonata": 60,
        "Kona": 47, "Accent": 43,
    },
    "Kia": {
        "Rio": 45, "Forte": 50, "Sportage": 54, "Sorento": 67,
        "Seltos": 50, "Telluride": 71,
    },
    "Volkswagen": {
        "Golf": 50, "Jetta": 50, "Tiguan": 58, "Passat": 66, "Atlas": 70,
    },
    "Subaru": {
        "Impreza": 50, "Crosstrek": 63, "Forester": 63, "Outback": 63, "WRX": 60,
    },
    "Mazda": {
        "Mazda3": 51, "Mazda6": 62, "CX-30": 51, "CX-5": 56, "CX-9": 74,
    },
    "Jeep": {
        "Wrangler": 70, "Cherokee": 60, "Compass": 51, "Grand Cherokee": 87,
    },
    "Ram": {
        "1500": 98, "ProMaster": 91,
    },
    "GMC": {
        "Sierra": 91, "Terrain": 56, "Acadia": 73, "Yukon": 98,
    },
    "Dodge": {
        "Charger": 70, "Challenger": 70, "Durango": 93,
    },
    "BMW": {
        "3 Series": 59, "5 Series": 68, "X3": 65, "X5": 80,
    },
    "Mercedes-Benz": {
        "C-Class": 66, "E-Class": 66, "GLC": 66,
    },
    "Lexus": {
        "ES": 60, "NX": 60, "RX": 65,
    },
}

FIRST_NAMES = [
    "Alex", "Jordan", "Sam", "Taylor", "Morgan", "Casey", "Jamie", "Riley", "Avery", "Quinn",
    "Drew", "Reese", "Rowan", "Parker", "Logan", "Hayden", "Emerson", "Finley", "Sage", "Blake",
    "Cameron", "Dakota", "Skyler", "Peyton", "Harper", "Kendall", "Marley", "Ellis", "Jesse", "Devon",
    "Charlie", "Robin", "Shay", "Tatum", "Lane", "Remy", "Kai", "Noel", "Arden", "Micah",
    "Ayesha", "Omar", "Priya", "Mateo", "Lucia", "Hiro", "Amara", "Dmitri", "Fatima", "Tomas",
]

LAST_NAMES = [
    "Garcia", "Nguyen", "Patel", "Okafor", "Kowalski", "Haddad", "Silva", "Novak", "Tanaka",
    "Reyes", "Moreau", "Larsen", "Mensah", "Ibrahim", "Fischer", "Costa", "Sato", "Walsh",
    "Khan", "Rossi", "Andersson", "Oliveira", "Petrov", "Kim", "Delgado", "Murphy", "Hassan", "Schmidt",
    "Ali", "Bianchi", "Dubois", "Eriksen", "Fernandez", "Gupta", "Hoffman", "Ivanov", "Jensen", "Kaplan",
    "Lopez", "Mbeki", "Nakamura", "Ortiz", "Popov", "Quintero", "Rahman", "Santos", "Takahashi", "Underwood",
]

DTC_CODES = [f["code"] for f in FAULT_CODES]
VIN_ALPHABET = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"

NOMINAL_ENGINE_TEMP_C = 90.0
MAX_ENGINE_TEMP_C = 125.0

DRIVER_STYLES = {
    #              weight  speed_bias  harsh/h  overspeed  fuel_factor
    "calm":       (0.50,   -5.0,       0.3,     0.00,      0.95),
    "normal":     (0.35,    0.0,       1.0,     0.02,      1.00),
    "aggressive": (0.15,   12.0,       4.5,     0.25,      1.18),
}

SPARE_COVERS_SHIFT_PROBABILITY = 0.10
TEMP_DRIVER_PROBABILITY = 0.03

IDLE_BURN_LITRES_PER_HOUR = 0.9
REFUEL_BELOW_PCT = 15.0

# Fixed so the registry is identical whenever it is exported.
REGISTRY_ERA = date(2026, 10, 1)


def _poisson(rng: random.Random, lam: float) -> int:
    """Knuth's method; lam is small here, so this is cheap."""
    if lam <= 0:
        return 0
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= threshold:
            return k
        k += 1


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


@dataclass
class Driver:
    driver_id: str
    client_id: str
    name: str
    fob_id: str
    licensed_since: date
    style: str = "normal"
    idle_prone: bool = False
    misuse_prone: bool = False
    registered: bool = True


@dataclass
class Vehicle:
    """One vehicle, the device fitted to it, who is driving, and mutable physical state."""

    vehicle_id: str
    vin: str
    make: str
    model: str
    model_year: int
    tank_capacity_l: float
    client: Client
    device_id: str
    firmware_version: str
    device_installed_on: date
    odometer_km: float
    depot: tuple[float, float]

    # Drivers rostered to this vehicle, one per shift. The first is its daytime driver.
    shift_drivers: list[Driver] = field(default_factory=list)
    driver: Driver | None = None

    # What the device claims, when it is misconfigured. None means it tells the truth.
    reported_vin: str | None = None
    reported_client_id: str | None = None

    engine_temp_c: float = NOMINAL_ENGINE_TEMP_C
    coolant_pressure_kpa: float = 110.0
    rpm: int = 800
    speed_kph: float = 0.0
    fuel_level_pct: float = 100.0
    battery_voltage: float = 14.1
    oil_pressure_kpa: float = 310.0
    lat: float = 0.0
    lon: float = 0.0
    heading_deg: float = 0.0
    trip_target_kph: float = 0.0

    ignition_on: bool = True
    degrading: bool = False
    degradation_max_offset_c: float = 0.0
    degradation_tau_hours: float = 3.0
    active_dtc: str | None = None

    harsh_brake_count: int = 0
    harsh_accel_count: int = 0
    rogue_ticks_remaining: int = 0

    _elapsed_s: float = field(default=0.0, repr=False)
    _last_slot: int | None = field(default=None, repr=False)

    @property
    def vendor_id(self) -> str:
        return self.client.vendor_id

    @property
    def home_driver(self) -> Driver | None:
        return self.shift_drivers[0] if self.shift_drivers else None

    @property
    def thermal_offset_c(self) -> float:
        """Heat the cooling fault has added so far, asymptotic in simulated time."""
        if not self.degrading:
            return 0.0
        hours = self._elapsed_s / 3600.0
        return self.degradation_max_offset_c * (
            1.0 - math.exp(-hours / max(0.1, self.degradation_tau_hours))
        )

    def distance_from_depot_km(self) -> float:
        return _haversine_km(self.lat, self.lon, *self.depot)

    def _traits(self) -> tuple[str, bool, bool]:
        if self.driver is None:
            return "normal", False, False
        return self.driver.style, self.driver.idle_prone, self.driver.misuse_prone

    def _should_be_running(self, rng: random.Random, local_hour: int, seconds: float) -> bool:
        if is_operating(self.client, local_hour):
            return True
        if self.rogue_ticks_remaining > 0:
            return True
        # Misuse: the daytime driver takes the vehicle out after hours,
        # roughly once or twice a night for a misuse-prone driver.
        _, _, misuse_prone = self._traits()
        if misuse_prone and rng.random() < 1.5 * seconds / (10 * 3600):
            self.rogue_ticks_remaining = int(rng.uniform(40, 90) * 60 / max(seconds, 1))
            self.heading_deg = rng.uniform(0, 360)
            return True
        return False

    def tick(self, rng: random.Random, seconds: float, now: datetime) -> None:
        """Advance physical state by `seconds` of simulated time ending at `now`."""
        self._elapsed_s += seconds
        self.harsh_brake_count = 0
        self.harsh_accel_count = 0
        local_hour = (now + timedelta(hours=UTC_OFFSET_HOURS)).hour

        off_duty = not self._should_be_running(rng, local_hour, seconds)
        short_stop = self.ignition_on and self.rogue_ticks_remaining == 0 and rng.random() < 0.02
        restart = not self.ignition_on and rng.random() < 0.25

        if off_duty or short_stop:
            self.ignition_on = False
        elif restart:
            self.ignition_on = True

        if not self.ignition_on:
            self.rpm = 0
            self.speed_kph = 0.0
            self.engine_temp_c = round(max(25.0, self.engine_temp_c - 4.0), 1)
            self.oil_pressure_kpa = 0.0
            self.coolant_pressure_kpa = round(max(0.0, self.coolant_pressure_kpa - 8.0), 1)
            self.battery_voltage = round(rng.uniform(12.2, 12.7), 2)
            return

        style, idle_prone, _ = self._traits()
        _, speed_bias, harsh_per_hour, overspeed_p, fuel_factor = DRIVER_STYLES[style]

        if self.trip_target_kph == 0.0 or rng.random() < 0.04:
            if idle_prone and rng.random() < 0.35:
                self.trip_target_kph = 0.0001  # engine running, vehicle stationary
            else:
                target = rng.choice([
                    rng.uniform(0, 15),
                    rng.uniform(30, 55),
                    rng.uniform(55, 85),
                    rng.uniform(95, 115),
                ])
                if target > 90 and rng.random() < overspeed_p:
                    target = rng.uniform(125, 145)
                self.trip_target_kph = max(0.0001, target + speed_bias)

        previous_speed = self.speed_kph
        self.speed_kph = round(
            max(0.0, min(150.0,
                0.75 * self.speed_kph + 0.25 * self.trip_target_kph + rng.gauss(0, 3))),
            1,
        )
        if self.trip_target_kph < 1 and self.speed_kph < 3:
            self.speed_kph = 0.0

        moving_fraction = 1.0 if self.speed_kph > 5 else 0.2
        lam = harsh_per_hour * moving_fraction * seconds / 3600.0
        self.harsh_brake_count = _poisson(rng, lam)
        self.harsh_accel_count = _poisson(rng, lam * 0.8)
        if self.speed_kph - previous_speed > 25:
            self.harsh_accel_count += 1
        if previous_speed - self.speed_kph > 25:
            self.harsh_brake_count += 1

        load = self.speed_kph / 120.0
        self.rpm = int(max(700, min(5200, 800 + load * 3400 + rng.gauss(0, 180))))

        distance_km = self.speed_kph * (seconds / 3600.0)
        self.odometer_km = round(self.odometer_km + distance_km, 2)

        if distance_km > 0:
            self._steer(rng)
            rad = math.radians(self.heading_deg)
            self.lat = round(self.lat + (distance_km / 111.0) * math.cos(rad), 6)
            self.lon = round(
                self.lon
                + (distance_km / (111.0 * math.cos(math.radians(self.lat)))) * math.sin(rad),
                6,
            )

        if self.rogue_ticks_remaining > 0:
            self.rogue_ticks_remaining -= 1

        thermal_target = NOMINAL_ENGINE_TEMP_C + load * 12.0 + self.thermal_offset_c
        self.engine_temp_c = round(
            min(MAX_ENGINE_TEMP_C,
                0.85 * self.engine_temp_c + 0.15 * thermal_target + rng.gauss(0, 0.8)),
            1,
        )
        self.coolant_pressure_kpa = round(
            95.0 + (self.engine_temp_c - NOMINAL_ENGINE_TEMP_C) * 1.8 + rng.gauss(0, 3), 1
        )
        self.oil_pressure_kpa = round(200.0 + (self.rpm / 5200.0) * 250.0 + rng.gauss(0, 12), 1)
        self.battery_voltage = round(rng.uniform(13.8, 14.4), 2)

        self._burn_fuel(rng, distance_km, seconds, fuel_factor)

        if self.engine_temp_c > 115.0 and self.active_dtc is None and rng.random() < 0.3:
            self.active_dtc = "P0128"
        elif self.active_dtc is None and rng.random() < 0.001:
            self.active_dtc = rng.choice(DTC_CODES)
        elif self.active_dtc is not None and rng.random() < 0.05:
            self.active_dtc = None

    def _steer(self, rng: random.Random) -> None:
        """Wander, but turn back toward the depot near the region edge.

        A rogue trip holds its heading, which is what carries a misused vehicle
        across the geofence.
        """
        if self.rogue_ticks_remaining > 0:
            self.heading_deg = (self.heading_deg + rng.gauss(0, 3)) % 360
            return

        if self.distance_from_depot_km() > GEOFENCE_RADIUS_KM * 0.7:
            dlat = self.depot[0] - self.lat
            dlon = (self.depot[1] - self.lon) * math.cos(math.radians(self.lat))
            self.heading_deg = (math.degrees(math.atan2(dlon, dlat)) + rng.gauss(0, 20)) % 360
        else:
            self.heading_deg = (self.heading_deg + rng.gauss(0, 15)) % 360

    def _burn_fuel(self, rng: random.Random, distance_km: float, seconds: float, factor: float) -> None:
        base_l_per_100km = 7.0 + self.tank_capacity_l * 0.06
        speed_penalty = 1.0 + max(0.0, self.speed_kph - 90) / 100.0
        litres = distance_km * base_l_per_100km / 100.0 * speed_penalty * factor
        if self.speed_kph == 0:
            litres += IDLE_BURN_LITRES_PER_HOUR * seconds / 3600.0

        self.fuel_level_pct = round(
            max(0.0, self.fuel_level_pct - litres / self.tank_capacity_l * 100.0), 2
        )
        if self.fuel_level_pct < REFUEL_BELOW_PCT and rng.random() < 0.3:
            self.fuel_level_pct = round(rng.uniform(92.0, 100.0), 2)

    def reading(self, event_ts: datetime, emitted_at: datetime) -> dict:
        """Current state as one canonical reading (metric units, datetimes).

        Identity fields are what the device reports, which for a misconfigured
        device is not the truth. The driver fob is only present while the engine
        runs: drivers log in at start-up and out at shut-down.
        """
        fob = self.driver.fob_id if (self.ignition_on and self.driver) else None
        return {
            "vendor_id": self.vendor_id,
            "client_id": self.reported_client_id or self.client.client_id,
            "device_id": self.device_id,
            "firmware_version": self.firmware_version,
            "vehicle_id": self.vehicle_id,
            "vin": self.reported_vin or self.vin,
            "driver_fob_id": fob,
            "event_ts": event_ts,
            "emitted_at": emitted_at,
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
            "harsh_brake_count": self.harsh_brake_count,
            "harsh_accel_count": self.harsh_accel_count,
        }


class Fleet:
    """Every vehicle and driver belonging to the selected clients, advanced in lockstep."""

    def __init__(
        self,
        clients: list[Client],
        seed: int | None = 42,
        extra_degrading_fraction: float = 0.05,
        glitch_fraction: float = 0.003,
    ) -> None:
        self.rng = random.Random(seed)
        self.clients = clients
        self.glitch_fraction = glitch_fraction
        self.vehicles: list[Vehicle] = []
        self.drivers: list[Driver] = []
        self.temp_drivers: list[Driver] = []
        self.ghost_devices: list[Vehicle] = []
        self._ghost_twins: dict[int, Vehicle] = {}
        self.assignments: list[dict] = []
        self.unregistered_devices: set[str] = set()
        self.integration_issues: dict[str, str] = {}
        self._spares: dict[str, list[Driver]] = {}

        for client in clients:
            for n in range(1, client.vehicle_count + 1):
                self.vehicles.append(self._new_vehicle(client, n))

        self._hire_drivers()
        self._assign_degradation(extra_degrading_fraction)
        self._build_assignment_history()
        self._inject_integration_issues()

    def _device_id(self, vendor_id: str) -> str:
        return f"{vendor_id.upper()}-{self.rng.getrandbits(24):06X}"

    def _fob_id(self) -> str:
        return f"FOB-{self.rng.getrandbits(32):08X}"

    def _new_vehicle(self, client: Client, n: int) -> Vehicle:
        make = self.rng.choice(list(MODELS))
        model = self.rng.choice(list(MODELS[make]))
        depot = REGION_DEPOTS[client.region]
        return Vehicle(
            vehicle_id=f"{client.client_id}-V{n:03d}",
            vin="".join(self.rng.choice(VIN_ALPHABET) for _ in range(17)),
            make=make,
            model=model,
            model_year=self.rng.randint(2012, 2025),
            tank_capacity_l=float(MODELS[make][model]),
            client=client,
            device_id=self._device_id(client.vendor_id),
            firmware_version=VENDORS[client.vendor_id].firmware,
            device_installed_on=REGISTRY_ERA - timedelta(days=self.rng.randint(120, 900)),
            odometer_km=round(self.rng.uniform(5_000, 280_000), 2),
            depot=depot,
            lat=round(depot[0] + self.rng.gauss(0, 0.05), 6),
            lon=round(depot[1] + self.rng.gauss(0, 0.05), 6),
            heading_deg=self.rng.uniform(0, 360),
            fuel_level_pct=round(self.rng.uniform(40, 100), 2),
        )

    def _new_driver(self, client: Client, n: int, registered: bool = True) -> Driver:
        styles = list(DRIVER_STYLES)
        weights = [DRIVER_STYLES[s][0] for s in styles]
        return Driver(
            driver_id=f"{client.client_id}-D{n:03d}" if registered else f"{client.client_id}-TEMP",
            client_id=client.client_id,
            name=f"{self.rng.choice(FIRST_NAMES)} {self.rng.choice(LAST_NAMES)}",
            fob_id=self._fob_id(),
            licensed_since=REGISTRY_ERA - timedelta(days=self.rng.randint(400, 9000)),
            style=self.rng.choices(styles, weights)[0],
            idle_prone=self.rng.random() < 0.15,
            registered=registered,
        )

    def _hire_drivers(self) -> None:
        """One driver per vehicle per shift, plus one spare per client.

        At least one daytime driver per daytime fleet is misuse-prone, so
        after-hours and geofence alerts have something to find in both profiles.
        """
        for client in self.clients:
            fleet_vehicles = [v for v in self.vehicles if v.client is client]
            shifts = shifts_per_day(client)
            pool = [self._new_driver(client, n)
                    for n in range(1, len(fleet_vehicles) * shifts + 2)]
            self.drivers.extend(pool)

            for i, vehicle in enumerate(fleet_vehicles):
                vehicle.shift_drivers = pool[i * shifts:(i + 1) * shifts]
                vehicle.driver = vehicle.home_driver
            self._spares[client.client_id] = pool[len(fleet_vehicles) * shifts:]

            if client.operating_hours != "24x7":
                self.rng.choice(fleet_vehicles).home_driver.misuse_prone = True

    def _assign_degradation(self, extra_fraction: float) -> None:
        """At least one failing vehicle per vendor, plus a few at random.

        Silver converts four unit systems; a conversion bug in any one would hide
        that vendor's overheating vehicle, so each vendor carries a known failure.
        """
        by_vendor: dict[str, list[Vehicle]] = {}
        for v in self.vehicles:
            by_vendor.setdefault(v.vendor_id, []).append(v)

        chosen = {id(self.rng.choice(group)) for group in by_vendor.values()}
        for v in self.vehicles:
            if id(v) in chosen or self.rng.random() < extra_fraction:
                v.degrading = True
                v.degradation_max_offset_c = self.rng.uniform(22.0, 34.0)
                v.degradation_tau_hours = self.rng.uniform(1.5, 4.0)

    def _assignment(self, v: Vehicle, device_id: str, start: date, end: date | None) -> dict:
        return {
            "device_id": device_id,
            "vendor_id": v.vendor_id,
            "vin": v.vin,
            "vehicle_id": v.vehicle_id,
            "client_id": v.client.client_id,
            "firmware_version": v.firmware_version,
            "valid_from": start.isoformat(),
            "valid_to": end.isoformat() if end else None,
            "status": "active" if end is None else "retired",
        }

    def _build_assignment_history(self) -> None:
        """Current device per vehicle, plus some earlier devices that were replaced.

        Replaced devices are what make device assignments a slowly changing dimension.
        """
        for v in self.vehicles:
            if self.rng.random() < 0.2:
                swapped_on = v.device_installed_on
                earlier_install = swapped_on - timedelta(days=self.rng.randint(200, 700))
                old_device = self._device_id(v.vendor_id)
                self.assignments.append(self._assignment(v, old_device, earlier_install, swapped_on))
            self.assignments.append(self._assignment(v, v.device_id, v.device_installed_on, None))

    def _inject_integration_issues(self) -> None:
        for client in self.clients:
            issue = INTEGRATION_ISSUES.get(client.client_id)
            if not issue:
                continue
            v = next(x for x in self.vehicles if x.client is client)
            self.integration_issues[v.vehicle_id] = issue

            if issue == "retired_device_still_transmitting":
                # The previous device was taken out of service but left powered in
                # the vehicle, so two devices report for one VIN.
                retired_id = self._device_id(v.vendor_id)
                swapped_on = v.device_installed_on
                self.assignments = [
                    a for a in self.assignments
                    if not (a["vehicle_id"] == v.vehicle_id and a["status"] == "retired")
                ]
                self.assignments.append(self._assignment(
                    v, retired_id, swapped_on - timedelta(days=400), swapped_on))
                ghost = Vehicle(**{
                    **{f: getattr(v, f) for f in v.__dataclass_fields__ if not f.startswith("_")},
                    "device_id": retired_id,
                    "degrading": False,
                })
                self.ghost_devices.append(ghost)
                self._ghost_twins[id(ghost)] = v

            elif issue == "device_reports_wrong_vin":
                last = v.vin[-1]
                v.reported_vin = v.vin[:-1] + ("A" if last != "A" else "B")

            elif issue == "device_not_yet_registered":
                self.unregistered_devices.add(v.device_id)

            elif issue == "device_reports_wrong_account":
                other = next(c for c in self.clients if c.vendor_id != v.vendor_id)
                v.reported_client_id = other.client_id

    def _roster(self, now: datetime) -> None:
        """Put the right driver in each vehicle for the shift on duty.

        On a shift change the rostered driver takes over, except that a spare
        sometimes covers, and occasionally an unregistered temporary driver does.
        Off duty, the vehicle's daytime driver holds it (and may misuse it).
        """
        local_hour = (now + timedelta(hours=UTC_OFFSET_HOURS)).hour

        for v in self.vehicles:
            if not is_operating(v.client, local_hour):
                v.driver = v.home_driver
                v._last_slot = None
                continue

            slot = shift_slot(v.client, local_hour)
            if slot == v._last_slot:
                continue
            v._last_slot = slot

            roll = self.rng.random()
            spares = self._spares.get(v.client.client_id, [])
            if roll < TEMP_DRIVER_PROBABILITY:
                temp = self._new_driver(v.client, 0, registered=False)
                self.temp_drivers.append(temp)
                v.driver = temp
            elif roll < TEMP_DRIVER_PROBABILITY + SPARE_COVERS_SHIFT_PROBABILITY and spares:
                v.driver = self.rng.choice(spares)
            else:
                v.driver = v.shift_drivers[slot]

        for ghost in self.ghost_devices:
            ghost.driver = self._ghost_twins[id(ghost)].driver

    @property
    def transmitting_devices(self) -> list[Vehicle]:
        return self.vehicles + self.ghost_devices

    @property
    def driver_by_fob(self) -> dict[str, Driver]:
        return {d.fob_id: d for d in self.drivers + self.temp_drivers}

    def advance(self, seconds: float, now: datetime | None = None) -> None:
        now = now or datetime.now(timezone.utc)
        self._roster(now)
        for v in self.transmitting_devices:
            v.tick(self.rng, seconds, now)

    def _glitch(self, reading: dict) -> dict:
        """A sensor or clock fault: valid JSON, valid contract, invalid values."""
        kind = self.rng.choice(["gps_null_island", "clock_ahead", "sensor_spike"])
        if kind == "gps_null_island":
            reading["latitude"], reading["longitude"] = 0.0, 0.0
        elif kind == "clock_ahead":
            reading["event_ts"] = reading["event_ts"] + timedelta(hours=self.rng.uniform(2, 6))
        else:
            reading["engine_temp_c"] = 999.9
        return reading

    def readings(
        self,
        now: datetime | None = None,
        late_fraction: float = 0.05,
        late_delay_minutes: int = 10,
    ) -> list[dict]:
        """One canonical reading per transmitting device.

        A `late_fraction` share carry an event_ts pushed into the past, and a
        `glitch_fraction` share carry a sensor or clock fault.
        """
        now = now or datetime.now(timezone.utc)
        out = []

        for v in self.transmitting_devices:
            event_ts = now
            if self.rng.random() < late_fraction:
                event_ts = now - timedelta(
                    minutes=self.rng.uniform(late_delay_minutes * 0.5, late_delay_minutes * 1.5)
                )
            reading = v.reading(event_ts, emitted_at=now)
            if self.rng.random() < self.glitch_fraction:
                reading = self._glitch(reading)
            out.append(reading)

        return out

    @property
    def degrading_ids(self) -> list[str]:
        return [v.vehicle_id for v in self.vehicles if v.degrading]

    @property
    def misuse_prone_drivers(self) -> list[str]:
        return [d.driver_id for d in self.drivers if d.misuse_prone]

    def registry_export(self, include_late_registrations: bool = False, today: date | None = None) -> dict:
        """Reference data for the master-data and lookup tables.

        Devices awaiting registration are left out unless include_late_registrations
        is set, which models the registry being corrected after their data arrived.
        Temporary drivers are never registered.
        """
        today = today or datetime.now(timezone.utc).date()
        assignments = [
            a for a in self.assignments
            if include_late_registrations or a["device_id"] not in self.unregistered_devices
        ]

        prices = []
        price_rng = random.Random(f"fuel-{today.isoformat()}")
        for region, base in BASE_FUEL_PRICE_PER_LITRE.items():
            price = base
            for days_ago in range(60, -1, -1):
                price = max(0.5, price + price_rng.gauss(0, 0.006))
                prices.append({
                    "region": region,
                    "price_date": (today - timedelta(days=days_ago)).isoformat(),
                    "price_per_litre_usd": round(price, 3),
                })

        used_regions = {c.region for c in self.clients}

        return {
            "vendors": [
                {"vendor_id": v.vendor_id, "name": v.name, "contract": v.contract}
                for v in VENDORS.values()
                if any(c.vendor_id == v.vendor_id for c in self.clients)
            ],
            "clients": [
                {
                    "client_id": c.client_id,
                    "vendor_id": c.vendor_id,
                    "name": c.name,
                    "region": c.region,
                    "operating_hours": c.operating_hours,
                }
                for c in self.clients
            ],
            "regions": [
                {
                    "region": region,
                    "depot_latitude": lat,
                    "depot_longitude": lon,
                    "geofence_radius_km": GEOFENCE_RADIUS_KM,
                    "utc_offset_hours": UTC_OFFSET_HOURS,
                }
                for region, (lat, lon) in REGION_DEPOTS.items()
                if region in used_regions
            ],
            "vehicles": [
                {
                    "vehicle_id": v.vehicle_id,
                    "vin": v.vin,
                    "make": v.make,
                    "model": v.model,
                    "model_year": v.model_year,
                    "tank_capacity_l": v.tank_capacity_l,
                    "client_id": v.client.client_id,
                    "region": v.client.region,
                }
                for v in self.vehicles
            ],
            "drivers": [
                {
                    "driver_id": d.driver_id,
                    "client_id": d.client_id,
                    "name": d.name,
                    "fob_id": d.fob_id,
                    "licensed_since": d.licensed_since.isoformat(),
                }
                for d in self.drivers
            ],
            "device_vehicle_assignments": assignments,
            "fuel_prices_by_region": prices,
            "fault_code_descriptions": FAULT_CODES,
        }