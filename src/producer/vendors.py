"""Vendor payload formats and the ingestion gateway envelope.

Each telematics vendor sends the same physical facts in its own shape:

  tmx  TelemaxPro   flat JSON, metric units, ISO-8601 timestamps
  fpl  FleetPulse   camelCase, imperial units (mph, °F, psi, miles), epoch milliseconds
  gtk  GeoTrak      nested objects, metric, "YYYY-MM-DD HH:MM:SS" timestamps with no zone
  ats  AutoSense    batches several readings per message, terse keys, epoch seconds

Our ingestion gateway wraps every vendor payload in a common envelope that
identifies the vendor and contract. That is what lands on Event Hubs and in
bronze. Silver is responsible for turning four dialects into one schema.
"""

from __future__ import annotations

import math
import random
import uuid
import zlib
from datetime import datetime, timezone

from src.producer.registry import VENDORS

ENVELOPE_VERSION = "1.0"

KPH_PER_MPH = 1.609344
KM_PER_MILE = 1.609344
KPA_PER_PSI = 6.894757

GTK_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def c_to_f(celsius: float) -> float:
    return round(celsius * 9.0 / 5.0 + 32.0, 1)


def _epoch_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def format_tmx(r: dict) -> dict:
    return {
        "device_id": r["device_id"],
        "vin": r["vin"],
        "account_id": r["client_id"],
        "driver_fob_id": r["driver_fob_id"],
        "event_ts": r["event_ts"].isoformat(),
        "sent_ts": r["emitted_at"].isoformat(),
        "ignition_on": r["ignition_on"],
        "speed_kph": r["speed_kph"],
        "engine_rpm": r["engine_rpm"],
        "engine_temp_c": r["engine_temp_c"],
        "coolant_pressure_kpa": r["coolant_pressure_kpa"],
        "oil_pressure_kpa": r["oil_pressure_kpa"],
        "battery_voltage": r["battery_voltage"],
        "fuel_level_pct": r["fuel_level_pct"],
        "odometer_km": r["odometer_km"],
        "latitude": r["latitude"],
        "longitude": r["longitude"],
        "heading_deg": r["heading_deg"],
        "dtc_code": r["dtc_code"],
        "harsh_brake_count": r["harsh_brake_count"],
        "harsh_accel_count": r["harsh_accel_count"],
    }


def format_fpl(r: dict) -> dict:
    return {
        "deviceId": r["device_id"],
        "vin": r["vin"],
        "accountId": r["client_id"],
        "driverFobId": r["driver_fob_id"],
        "timestamp": _epoch_ms(r["event_ts"]),
        "sentAt": _epoch_ms(r["emitted_at"]),
        "ignition": "ON" if r["ignition_on"] else "OFF",
        "speedMph": round(r["speed_kph"] / KPH_PER_MPH, 1),
        "rpm": r["engine_rpm"],
        "engineTempF": c_to_f(r["engine_temp_c"]),
        "coolantPressurePsi": round(r["coolant_pressure_kpa"] / KPA_PER_PSI, 2),
        "oilPressurePsi": round(r["oil_pressure_kpa"] / KPA_PER_PSI, 2),
        "batteryVolts": r["battery_voltage"],
        "fuelPct": r["fuel_level_pct"],
        "odometerMiles": round(r["odometer_km"] / KM_PER_MILE, 2),
        "lat": r["latitude"],
        "lng": r["longitude"],
        "heading": r["heading_deg"],
        "faultCodes": [r["dtc_code"]] if r["dtc_code"] else [],
        "harshBrakeEvents": r["harsh_brake_count"],
        "harshAccelEvents": r["harsh_accel_count"],
    }


FPL_V2_CONTRACT = "fpl.telemetry.v2"


def _iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _ambient_temp_c(ts: datetime) -> float:
    """Outside air temperature: coolest before dawn, warmest mid-afternoon (local UTC-6)."""
    local_hour = (ts.hour - 6) % 24 + ts.minute / 60
    return round(22.0 + 7.0 * math.sin((local_hour - 9) / 24 * 2 * math.pi), 1)


def format_fpl_v2(r: dict) -> dict:
    """FleetPulse firmware 2: metric units, nested engine and location, ISO 8601 timestamps,
    and a new outside-temperature field."""
    return {
        "deviceId": r["device_id"],
        "vin": r["vin"],
        "accountId": r["client_id"],
        "driverFobId": r["driver_fob_id"],
        "timestamp": _iso_utc(r["event_ts"]),
        "sentAt": _iso_utc(r["emitted_at"]),
        "ignitionOn": r["ignition_on"],
        "speedKph": r["speed_kph"],
        "engine": {
            "rpm": r["engine_rpm"],
            "tempC": r["engine_temp_c"],
            "coolantPressureKpa": r["coolant_pressure_kpa"],
            "oilPressureKpa": r["oil_pressure_kpa"],
        },
        "batteryVolts": r["battery_voltage"],
        "fuelPct": r["fuel_level_pct"],
        "odometerKm": r["odometer_km"],
        "location": {"lat": r["latitude"], "lng": r["longitude"], "heading": r["heading_deg"]},
        "faultCodes": [r["dtc_code"]] if r["dtc_code"] else [],
        "harshBrakeEvents": r["harsh_brake_count"],
        "harshAccelEvents": r["harsh_accel_count"],
        "ambientTempC": _ambient_temp_c(r["event_ts"]),
    }


def is_upgraded(device_id: str, share: float) -> bool:
    """Whether a device runs the new firmware. Decided by the device id alone, so a
    device stays upgraded across producer runs, and a larger share only adds devices."""
    return zlib.crc32(device_id.encode()) % 1000 < share * 1000


def format_gtk(r: dict) -> dict:
    return {
        "device": {"id": r["device_id"], "fw": r["firmware_version"]},
        "asset": {"vin": r["vin"], "customer": r["client_id"]},
        "driver": {"fob": r["driver_fob_id"]} if r["driver_fob_id"] else None,
        "time": r["event_ts"].strftime(GTK_TIME_FORMAT),
        "sent": r["emitted_at"].strftime(GTK_TIME_FORMAT),
        "position": {
            "lat": r["latitude"],
            "lon": r["longitude"],
            "bearing": r["heading_deg"],
            "speed_kmh": r["speed_kph"],
        },
        "engine": {
            "on": r["ignition_on"],
            "rpm": r["engine_rpm"],
            "coolant_temp_c": r["engine_temp_c"],
            "coolant_kpa": r["coolant_pressure_kpa"],
            "oil_kpa": r["oil_pressure_kpa"],
            "fuel_pct": r["fuel_level_pct"],
            "odo_km": r["odometer_km"],
        },
        "electrical": {"battery_v": r["battery_voltage"]},
        "events": {"harsh_brake": r["harsh_brake_count"], "harsh_accel": r["harsh_accel_count"]},
        "dtc": r["dtc_code"],
    }


def format_ats(batch: list[dict]) -> dict:
    """One message carrying several readings from the same device."""
    first = batch[0]
    return {
        "dev": first["device_id"],
        "vin": first["vin"],
        "acct": first["client_id"],
        "fw": first["firmware_version"],
        "sent": round(batch[-1]["emitted_at"].timestamp(), 3),
        "readings": [
            {
                "t": round(r["event_ts"].timestamp(), 3),
                "fob": r["driver_fob_id"],
                "ign": 1 if r["ignition_on"] else 0,
                "spd": r["speed_kph"],
                "rpm": r["engine_rpm"],
                "tmp": r["engine_temp_c"],
                "clp": r["coolant_pressure_kpa"],
                "oil": r["oil_pressure_kpa"],
                "bat": r["battery_voltage"],
                "ful": r["fuel_level_pct"],
                "odo": r["odometer_km"],
                "lat": r["latitude"],
                "lon": r["longitude"],
                "hdg": r["heading_deg"],
                "dtc": r["dtc_code"],
                "hb": r["harsh_brake_count"],
                "ha": r["harsh_accel_count"],
            }
            for r in batch
        ],
    }


SINGLE_READING_FORMATTERS = {
    "tmx": format_tmx,
    "fpl": format_fpl,
    "gtk": format_gtk,
}


class Gateway:
    """Formats canonical readings per vendor and wraps them in the envelope.

    AutoSense devices buffer readings and transmit them in batches, so their
    readings are held here until a batch fills.
    """

    def __init__(self, rng: random.Random, ats_batch_size: int = 3, fpl_v2_share: float = 0.0) -> None:
        self.rng = rng
        self.ats_batch_size = ats_batch_size
        self.fpl_v2_share = fpl_v2_share
        self._ats_buffers: dict[str, list[dict]] = {}

    @staticmethod
    def _message_id() -> str:
        # True randomness, never the seeded generator: a seed would repeat the same
        # ids on every producer run, and a message id must be unique forever.
        return str(uuid.uuid4())

    def envelope(self, vendor_id: str, payload: dict, received_at: datetime, contract: str | None = None) -> dict:
        return {
            "envelope_version": ENVELOPE_VERSION,
            "message_id": self._message_id(),
            "vendor_id": vendor_id,
            "contract": contract or VENDORS[vendor_id].contract,
            "received_at": received_at.isoformat(),
            "payload": payload,
        }

    def messages(self, readings: list[dict], received_at: datetime) -> list[tuple[str, dict]]:
        """(partition_key, envelope) pairs ready to serialize.

        The partition key is the device ID, so each device's messages stay in
        order within one Event Hubs partition.
        """
        out: list[tuple[str, dict]] = []

        for r in readings:
            vendor_id = r["vendor_id"]

            if vendor_id == "ats":
                buffer = self._ats_buffers.setdefault(r["device_id"], [])
                buffer.append(r)
                if len(buffer) >= self.ats_batch_size:
                    out.append((r["device_id"],
                                self.envelope("ats", format_ats(buffer), received_at)))
                    self._ats_buffers[r["device_id"]] = []
                continue

            if vendor_id == "fpl" and is_upgraded(r["device_id"], self.fpl_v2_share):
                out.append((r["device_id"],
                            self.envelope("fpl", format_fpl_v2(r), received_at, contract=FPL_V2_CONTRACT)))
                continue

            payload = SINGLE_READING_FORMATTERS[vendor_id](r)
            out.append((r["device_id"], self.envelope(vendor_id, payload, received_at)))

        return out

    def flush(self, received_at: datetime) -> list[tuple[str, dict]]:
        """Emit partially filled AutoSense batches, e.g. on shutdown."""
        out = [
            (device_id, self.envelope("ats", format_ats(buffer), received_at))
            for device_id, buffer in self._ats_buffers.items()
            if buffer
        ]
        self._ats_buffers = {}
        return out
