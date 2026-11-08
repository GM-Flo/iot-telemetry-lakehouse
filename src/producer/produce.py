"""Stream simulated multi-vendor vehicle telemetry to Azure Event Hubs.

Four telematics vendors, nine fleet-company clients, one message format per
vendor, every message wrapped in the ingestion gateway envelope.

Profiles:
  dev   one client per vendor (24 vehicles), 15 s interval: small, sporadic
  prod  every client (44 vehicles), 5 s interval: full fleet, continuous

Modes:
  live      continuous emission, until Ctrl+C
  backfill  dense historical burst, so gold windows have shape immediately

The target Event Hub is read from the connection string's EntityPath, so the
same command sends to dev or prod depending only on which string is loaded.

Usage:
  python src/producer/produce.py --env dev
  python src/producer/produce.py --env prod --mode backfill --backfill-hours 6
  python src/producer/produce.py --env dev --dry-run
  python src/producer/produce.py --env prod --export-registry fleet_registry.json
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.producer.fleet import Fleet  # noqa: E402
from src.producer.registry import select_clients  # noqa: E402
from src.producer.vendors import Gateway  # noqa: E402

try:
    from azure.eventhub import EventData, EventHubProducerClient
    from azure.eventhub.exceptions import EventHubError

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    EventData = EventHubProducerClient = None
    EventHubError = Exception

LOG = logging.getLogger("producer")

PROFILES = {
    "dev": {"clients_per_vendor": 1, "interval": 15.0},
    "prod": {"clients_per_vendor": None, "interval": 5.0},
}

MAX_BACKOFF_SECONDS = 60.0
SHUTDOWN = False


def _handle_signal(signum, _frame) -> None:
    global SHUTDOWN
    LOG.info("signal %s received, finishing current batch then exiting", signum)
    SHUTDOWN = True


class TelemetrySink:
    """Event Hubs sender with reconnect, or a stdout sink in dry-run mode.

    Accepts (partition_key, body) pairs where body is already a string, so
    deliberately malformed payloads reach the hub intact.
    """

    def __init__(self, conn_str: str | None, dry_run: bool) -> None:
        self.conn_str = conn_str
        self.dry_run = dry_run
        self._client = None
        self.sent = 0
        self.failed = 0

    def _connect(self):
        if self._client is None:
            self._client = EventHubProducerClient.from_connection_string(conn_str=self.conn_str)
            LOG.info("connected to event hub %s", self._client.eventhub_name)
        return self._client

    def _reset(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
        self._client = None

    def send(self, items: list[tuple[str, str]]) -> None:
        if not items:
            return

        if self.dry_run:
            for _, body in items:
                print(body)
            self.sent += len(items)
            return

        grouped: dict[str, list[str]] = {}
        for key, body in items:
            grouped.setdefault(key, []).append(body)

        backoff = 1.0
        while True:
            try:
                client = self._connect()
                for key, bodies in grouped.items():
                    batch = client.create_batch(partition_key=key)
                    for body in bodies:
                        batch.add(EventData(body))
                    client.send_batch(batch)
                self.sent += len(items)
                return
            except (EventHubError, OSError, ValueError) as exc:
                self.failed += len(items)
                if SHUTDOWN:
                    LOG.warning("dropping batch during shutdown: %s", type(exc).__name__)
                    return
                LOG.warning("send failed (%s: %s), reconnecting in %.0fs",
                            type(exc).__name__, exc, backoff)
                self._reset()
                time.sleep(backoff)
                backoff = min(MAX_BACKOFF_SECONDS, backoff * 2)

    def close(self) -> None:
        self._reset()


def _first_numeric_path(node, path=()):
    """Path to the first numeric leaf in a nested payload, for type corruption."""
    if isinstance(node, dict):
        for key, value in node.items():
            found = _first_numeric_path(value, (*path, key))
            if found:
                return found
    elif isinstance(node, list) and node:
        return _first_numeric_path(node[0], (*path, 0))
    elif isinstance(node, (int, float)) and not isinstance(node, bool):
        return path
    return None


def corrupt(envelope: dict, rng: random.Random) -> str:
    """Return a deliberately malformed message, in one of several realistic ways."""
    mode = rng.choice(["truncated", "not_json", "missing_payload", "unknown_vendor", "bad_type"])
    message = json.loads(json.dumps(envelope))

    if mode == "truncated":
        text = json.dumps(message)
        return text[: rng.randint(20, max(21, len(text) // 2))]
    if mode == "not_json":
        return f"vendor={message['vendor_id']};id={message['message_id']}"
    if mode == "missing_payload":
        message.pop("payload", None)
        return json.dumps(message)
    if mode == "unknown_vendor":
        message["vendor_id"] = "zzz"
        message["contract"] = "zzz.telemetry.v1"
        return json.dumps(message)

    path = _first_numeric_path(message["payload"])
    if path:
        target = message["payload"]
        for step in path[:-1]:
            target = target[step]
        target[path[-1]] = "n/a"
    return json.dumps(message)


def build_items(
    fleet: Fleet,
    gateway: Gateway,
    now: datetime,
    late_fraction: float,
    malformed_fraction: float,
    rng: random.Random,
) -> list[tuple[str, str]]:
    readings = fleet.readings(now=now, late_fraction=late_fraction)
    items: list[tuple[str, str]] = []

    for key, envelope in gateway.messages(readings, received_at=now):
        if rng.random() < malformed_fraction:
            items.append((key, corrupt(envelope, rng)))
        else:
            items.append((key, json.dumps(envelope)))

    return items


def describe(fleet: Fleet, args) -> None:
    LOG.info("[%s] %d clients, %d vehicles, %d transmitting devices, interval %.0fs",
             args.env, len(fleet.clients), len(fleet.vehicles),
             len(fleet.transmitting_devices), args.interval)
    LOG.info("[%s] %d registered drivers", args.env, len(fleet.drivers))
    LOG.info("[%s] overheating by design: %s", args.env, ", ".join(fleet.degrading_ids))
    LOG.info("[%s] after-hours misuse by design: %s", args.env, ", ".join(fleet.misuse_prone_drivers))
    for vehicle_id, issue in fleet.integration_issues.items():
        LOG.info("[%s] integration fault by design: %s -> %s", args.env, vehicle_id, issue)


STATE_SAVE_EVERY_TICKS = 20


def save_state(fleet, args, at: datetime) -> None:
    if args.dry_run:
        return
    fleet.save_state(args.state_file, saved_at=at)


def run_live(sink, fleet, gateway, args, rng) -> None:
    describe(fleet, args)

    started = time.monotonic()
    last_report = 0
    tick = 0
    now = datetime.now(timezone.utc)

    while not SHUTDOWN:
        loop_start = time.monotonic()

        now = datetime.now(timezone.utc)
        fleet.advance(args.interval, now=now)
        sink.send(build_items(fleet, gateway, now,
                              args.late_fraction, args.malformed_fraction, rng))
        tick += 1
        if tick % STATE_SAVE_EVERY_TICKS == 0:
            save_state(fleet, args, now)

        if sink.sent - last_report >= 500:
            last_report = sink.sent
            LOG.info("sent=%d failed=%d uptime=%.1fmin",
                     sink.sent, sink.failed, (time.monotonic() - started) / 60)

        elapsed = time.monotonic() - loop_start
        if elapsed < args.interval and not SHUTDOWN:
            time.sleep(args.interval - elapsed)

    save_state(fleet, args, now)


def backfill_start(args) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=args.backfill_hours)


def run_backfill(sink, fleet, gateway, args, rng) -> None:
    step = args.backfill_interval
    ticks = int((args.backfill_hours * 3600) / step)
    start = backfill_start(args)
    now = start

    LOG.info("backfill: %.1fh at %.0fs resolution, %d ticks", args.backfill_hours, step, ticks)
    describe(fleet, args)

    for tick in range(ticks):
        if SHUTDOWN:
            break
        now = start + timedelta(seconds=tick * step)
        fleet.advance(step, now=now)
        sink.send(build_items(fleet, gateway, now,
                              args.late_fraction, args.malformed_fraction, rng))
        if tick and tick % 100 == 0:
            LOG.info("backfill %d/%d ticks, sent=%d", tick, ticks, sink.sent)
            save_state(fleet, args, now)

    save_state(fleet, args, now)


def resume_fleet(fleet, args) -> bool:
    """Continue the fleet from its saved state, if there is one. False means: do not start.

    Each run otherwise rebuilds the fleet from the seed, putting every vehicle
    back at its first odometer reading and position. Real vehicles keep their
    odometer between our runs, so the producer must too.
    """
    path = Path(args.state_file)
    if args.fresh_fleet or not path.exists():
        LOG.info("[%s] starting a new fleet%s", args.env,
                 "" if args.dry_run else f"; its state will be saved to {path}")
        return True

    starts_at = backfill_start(args) if args.mode == "backfill" else datetime.now(timezone.utc)
    try:
        saved_at = fleet.load_state(path, now=starts_at)
    except ValueError as error:
        LOG.error("%s", error)
        return False

    if starts_at < saved_at:
        LOG.error("%s was saved at %s, but this run would start earlier, at %s, and rewrite "
                  "history that was already sent. Use fewer --backfill-hours, or --fresh-fleet.",
                  path, saved_at.isoformat(timespec="seconds"), starts_at.isoformat(timespec="seconds"))
        return False

    LOG.info("[%s] continuing the fleet saved at %s (%s ago)", args.env,
             saved_at.isoformat(timespec="seconds"), str(starts_at - saved_at).split(".")[0])
    return True


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-vendor vehicle telemetry producer")

    parser.add_argument("--env", choices=sorted(PROFILES), default="dev",
                        help="profile: fleet size and default interval")
    parser.add_argument("--mode", choices=["live", "backfill"], default="live")
    parser.add_argument("--dry-run", action="store_true",
                        help="print messages instead of sending; needs no Azure credentials")
    parser.add_argument("--export-registry", metavar="PATH",
                        help="write reference data (vendors, clients, regions, vehicles, "
                             "drivers, device assignments, fuel prices, fault codes) as JSON, then exit")
    parser.add_argument("--include-late-registrations", action="store_true",
                        help="with --export-registry: include devices that were registered "
                             "after their data started arriving")

    parser.add_argument("--seed", type=int, default=42, help="-1 for a non-deterministic fleet")
    parser.add_argument("--state-file", help="where the fleet's state is kept between runs "
                                             "(default: producer_state_<env>.json)")
    parser.add_argument("--fresh-fleet", action="store_true",
                        help="ignore the saved state and start the fleet from the seed again")
    parser.add_argument("--interval", type=float, help="override the profile's live interval")
    parser.add_argument("--backfill-hours", type=float, default=6.0)
    parser.add_argument("--backfill-interval", type=float, default=30.0)
    parser.add_argument("--late-fraction", type=float, default=0.05)
    parser.add_argument("--malformed-fraction", type=float, default=0.01)
    parser.add_argument("--glitch-fraction", type=float, default=0.003,
                        help="share of readings with a sensor or clock fault")
    parser.add_argument("--ats-batch-size", type=int, default=3)
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args(argv)
    if args.interval is None:
        args.interval = PROFILES[args.env]["interval"]
    if args.state_file is None:
        args.state_file = f"producer_state_{args.env}.json"
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("azure").setLevel(logging.WARNING)

    seed = None if args.seed == -1 else args.seed
    rng = random.Random(seed)
    fleet = Fleet(select_clients(PROFILES[args.env]["clients_per_vendor"]),
                  seed=seed, glitch_fraction=args.glitch_fraction)

    if args.export_registry:
        export = fleet.registry_export(include_late_registrations=args.include_late_registrations)
        Path(args.export_registry).write_text(json.dumps(export, indent=2))
        LOG.info("wrote %d vehicles and %d device assignments to %s",
                 len(export["vehicles"]), len(export["device_vehicle_assignments"]),
                 args.export_registry)
        return 0

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn_str = os.environ.get("EVENTHUB_CONN_STR")
    if not args.dry_run:
        if not conn_str or "EntityPath=" not in conn_str:
            LOG.error("EVENTHUB_CONN_STR must be a hub-level send connection string "
                      "(it contains EntityPath=vehicle-telemetry-<env>).")
            return 2
        # Guard against the classic mistake: dev data sent to the prod hub.
        if not conn_str.rstrip(";").endswith(f"EntityPath=vehicle-telemetry-{args.env}"):
            LOG.error("--env %s does not match the hub in EVENTHUB_CONN_STR. "
                      "Load the %s connection string or change --env.", args.env, args.env)
            return 2
        if not AZURE_AVAILABLE:
            LOG.error("azure-eventhub not installed: pip install -r src/producer/requirements.txt")
            return 2

    if not resume_fleet(fleet, args):
        return 2

    gateway = Gateway(rng, ats_batch_size=args.ats_batch_size)
    sink = TelemetrySink(conn_str, args.dry_run)

    try:
        if args.mode == "backfill":
            run_backfill(sink, fleet, gateway, args, rng)
        else:
            run_live(sink, fleet, gateway, args, rng)
        sink.send([(k, json.dumps(e)) for k, e in gateway.flush(datetime.now(timezone.utc))])
    finally:
        sink.close()
        LOG.info("stopped. sent=%d failed=%d", sink.sent, sink.failed)

    return 0


if __name__ == "__main__":
    sys.exit(main())