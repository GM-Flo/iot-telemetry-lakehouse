"""Who sends us data: telematics vendors, their fleet-company clients, and regions.

Vendors make the tracking devices and run the platform the devices report to.
Clients are fleet companies that bought a vendor's devices for their vehicles.
We sit downstream of all four vendors and receive every client's telemetry.

All names are fictional.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Vendor:
    vendor_id: str
    name: str
    contract: str
    firmware: str


@dataclass(frozen=True)
class Client:
    client_id: str
    vendor_id: str
    name: str
    region: str
    vehicle_count: int
    operating_hours: str  # "24x7", or "HH-HH" in local time


VENDORS: dict[str, Vendor] = {
    v.vendor_id: v
    for v in [
        Vendor("tmx", "TelemaxPro", "tmx.telemetry.v1", "tmx-os 3.8.2"),
        Vendor("fpl", "FleetPulse", "fpl.telemetry.v1", "fp-fw 4.2.1"),
        Vendor("gtk", "GeoTrak", "gtk.telemetry.v1", "gtk 2.11.0"),
        Vendor("ats", "AutoSense", "ats.telemetry.v1", "as-1.9.7"),
    ]
}

CLIENTS: list[Client] = [
    Client("C001", "tmx", "Bluebonnet Courier", "houston", 6, "06-20"),
    Client("C002", "tmx", "Harbor Point Plumbing", "houston", 4, "06-20"),
    Client("C003", "fpl", "Pecan Street Logistics", "austin", 7, "24x7"),
    Client("C004", "fpl", "Cypress Creek Cabs", "houston", 5, "24x7"),
    Client("C005", "fpl", "Prairie Wind Delivery", "dallas", 4, "06-20"),
    Client("C006", "gtk", "Gulfline Freight", "houston", 6, "24x7"),
    Client("C007", "gtk", "Copperfield HVAC", "san-antonio", 3, "06-20"),
    Client("C008", "ats", "Riverbend Movers", "san-antonio", 5, "06-20"),
    Client("C009", "ats", "Ironwood Equipment Rentals", "austin", 4, "06-20"),
]

# Every region is in Texas, US Central Time. A fixed UTC-6 offset is a deliberate
# simplification: it ignores daylight saving, which does not matter for a simulation.
UTC_OFFSET_HOURS = -6

REGION_DEPOTS: dict[str, tuple[float, float]] = {
    "houston": (29.8601, -95.5637),
    "austin": (30.2672, -97.7431),
    "dallas": (32.7767, -96.7970),
    "san-antonio": (29.4241, -98.4936),
}

# How far from its depot a vehicle may operate before it counts as leaving its region.
GEOFENCE_RADIUS_KM = 50.0

# Retail fuel price per litre, USD, around which daily prices fluctuate.
BASE_FUEL_PRICE_PER_LITRE = {
    "houston": 0.74,
    "austin": 0.77,
    "dallas": 0.75,
    "san-antonio": 0.73,
}

FAULT_CODES = [
    {"code": "P0128", "description": "Coolant thermostat below regulating temperature", "severity": "high"},
    {"code": "P0300", "description": "Random or multiple cylinder misfire", "severity": "high"},
    {"code": "P0420", "description": "Catalyst system efficiency below threshold", "severity": "medium"},
    {"code": "P0171", "description": "System too lean, bank 1", "severity": "medium"},
    {"code": "P0442", "description": "Evaporative emission system leak, small", "severity": "low"},
    {"code": "P0562", "description": "System voltage low", "severity": "medium"},
]

# One deliberate integration fault per vendor, each on that vendor's first client so the
# dev profile carries all four. Each one exercises a different registry check in silver.
INTEGRATION_ISSUES: dict[str, str] = {
    "C001": "retired_device_still_transmitting",
    "C003": "device_reports_wrong_vin",
    "C006": "device_not_yet_registered",
    "C008": "device_reports_wrong_account",
}


def select_clients(clients_per_vendor: int | None = None) -> list[Client]:
    """All clients, or the first N of each vendor.

    The dev profile uses one client per vendor: every vendor format and every
    integration issue still flows, at roughly half the volume of prod.
    """
    if clients_per_vendor is None:
        return list(CLIENTS)

    selected: list[Client] = []
    seen: dict[str, int] = {}
    for client in CLIENTS:
        if seen.get(client.vendor_id, 0) < clients_per_vendor:
            selected.append(client)
            seen[client.vendor_id] = seen.get(client.vendor_id, 0) + 1
    return selected


def is_operating(client: Client, local_hour: int) -> bool:
    if client.operating_hours == "24x7":
        return True
    start, end = (int(h) for h in client.operating_hours.split("-"))
    return start <= local_hour < end


def shifts_per_day(client: Client) -> int:
    """24/7 fleets run three 8-hour shifts; daytime fleets split the day in two."""
    return 3 if client.operating_hours == "24x7" else 2


def shift_slot(client: Client, local_hour: int) -> int:
    """Which shift is on duty at a local hour: 0-2 for 24/7 fleets, 0-1 for daytime."""
    if client.operating_hours == "24x7":
        return ((local_hour - 6) % 24) // 8
    start, end = (int(h) for h in client.operating_hours.split("-"))
    return 0 if local_hour < (start + end) // 2 else 1