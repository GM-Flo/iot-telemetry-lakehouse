"""Stream simulated vehicle telemetry to Azure Event Hubs.

Modes:
  live      continuous emission at --interval, intended to run for days
  backfill  dense historical burst, so gold-layer windows have shape even if
            live uptime is patchy
  dry-run   print to stdout, no Azure connection required

Partition key is vehicle_id, which preserves per-vehicle ordering inside a
partition and spreads the fleet evenly across them.

Usage:
  export EVENTHUB_CONN_STR="$(terraform -chdir=infra output -raw producer_connection_string)"
  python src/producer/produce.py --mode live --fleet-size 25 --interval 5
  python src/producer/produce.py --mode backfill --backfill-hours 6
  python src/producer/produce.py --dry-run --fleet-size 3
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

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fleet import Fleet  # noqa: E402

try:
    from azure.eventhub import EventData, EventHubProducerClient
    from azure.eventhub.exceptions import EventHubError

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    EventData = EventHubProducerClient = None
    EventHubError = Exception

LOG = logging.getLogger("producer")

MAX_BACKOFF_SECONDS = 60.0
SHUTDOWN = False


def _handle_signal(signum, _frame) -> None:
    global SHUTDOWN
    LOG.info("signal %s received, finishing current batch then exiting", signum)
    SHUTDOWN = True


class TelemetrySink:
    """Event Hubs sender with reconnect, or a stdout sink in dry-run mode.

    Accepts (partition_key, body) pairs where body is already a string. Sending
    pre-serialized bodies is what lets deliberately malformed payloads reach the
    hub intact instead of being re-serialized into valid JSON.
    """

    def __init__(self, conn_str: str | None, eventhub_name: str, dry_run: bool) -> None:
        self.conn_str = conn_str
        self.eventhub_name = eventhub_name
        self.dry_run = dry_run
        self._client = None
        self.sent = 0
        self.failed = 0

    def _connect(self):
        if self._client is None:
            self._client = EventHubProducerClient.from_connection_string(
                conn_str=self.conn_str, eventhub_name=self.eventhub_name
            )
            LOG.info("connected to event hub %s", self.eventhub_name)
        return self._client

    def _reset(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
        self._client = None

    def send(self, items: list[tuple[str, str]]) -> None:
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
                LOG.warning(
                    "send failed (%s: %s), reconnecting in %.0fs",
                    type(exc).__name__, exc, backoff,
                )
                self._reset()
                time.sleep(backoff)
                backoff = min(MAX_BACKOFF_SECONDS, backoff * 2)

    def close(self) -> None:
        self._reset()


def corrupt(event: dict, rng: random.Random) -> str:
    """Return a deliberately malformed payload.

    Silver routes rows that fail from_json into a quarantine table. Without
    malformed input that table stays empty, so a small share of traffic is
    corrupted on purpose, in several distinct ways.
    """
    mode = rng.choice(["truncated", "bad_type", "missing_vin", "not_json", "null_fields"])
    payload = dict(event)

    if mode == "truncated":
        return json.dumps(payload)[: rng.randint(20, 60)]
    if mode == "bad_type":
        payload["engine_temp_c"] = "ninety-four"
        payload["engine_rpm"] = "idle"
        return json.dumps(payload)
    if mode == "missing_vin":
        payload.pop("vin", None)
        return json.dumps(payload)
    if mode == "not_json":
        return f"vin={payload.get('vin')};temp={payload.get('engine_temp_c')}"

    payload["event_ts"] = None
    payload["vin"] = None
    return json.dumps(payload)


def build_batch(
    fleet: Fleet,
    now: datetime,
    late_fraction: float,
    malformed_fraction: float,
    rng: random.Random,
) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []

    for reading in fleet.readings(now=now, late_fraction=late_fraction):
        key = reading["vehicle_id"]
        if rng.random() < malformed_fraction:
            items.append((key, corrupt(reading, rng)))
        else:
            items.append((key, json.dumps(reading)))

    return items


def run_live(sink: TelemetrySink, fleet: Fleet, args, rng: random.Random) -> None:
    LOG.info(
        "live mode: %d vehicles, %.1fs interval, %d degrading (%s)",
        len(fleet.vehicles),
        args.interval,
        len(fleet.degrading_ids),
        ", ".join(fleet.degrading_ids[:5]) or "none",
    )

    started = time.monotonic()
    report_every = max(1, len(fleet.vehicles) * 20)
    last_report = 0

    while not SHUTDOWN:
        loop_start = time.monotonic()

        fleet.advance(args.interval)
        sink.send(build_batch(
            fleet, datetime.now(timezone.utc),
            args.late_fraction, args.malformed_fraction, rng,
        ))

        if sink.sent - last_report >= report_every:
            last_report = sink.sent
            LOG.info(
                "sent=%d failed=%d uptime=%.1fmin",
                sink.sent, sink.failed, (time.monotonic() - started) / 60,
            )

        elapsed = time.monotonic() - loop_start
        if elapsed < args.interval and not SHUTDOWN:
            time.sleep(args.interval - elapsed)


def run_backfill(sink: TelemetrySink, fleet: Fleet, args, rng: random.Random) -> None:
    step = args.backfill_interval
    ticks = int((args.backfill_hours * 3600) / step)
    start = datetime.now(timezone.utc) - timedelta(hours=args.backfill_hours)

    LOG.info(
        "backfill: %.1fh at %.0fs resolution = %d ticks, ~%d events",
        args.backfill_hours, step, ticks, ticks * len(fleet.vehicles),
    )

    for tick in range(ticks):
        if SHUTDOWN:
            break

        fleet.advance(step)
        sink.send(build_batch(
            fleet, start + timedelta(seconds=tick * step),
            args.late_fraction, args.malformed_fraction, rng,
        ))

        if tick and tick % 50 == 0:
            LOG.info("backfill %d/%d ticks, sent=%d", tick, ticks, sink.sent)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Vehicle telemetry producer")

    parser.add_argument("--mode", choices=["live", "backfill"], default="live")
    parser.add_argument("--dry-run", action="store_true",
                        help="print to stdout instead of sending; needs no Azure credentials")

    parser.add_argument("--fleet-size", type=int, default=25)
    parser.add_argument("--seed", type=int, default=42,
                        help="set to -1 for a non-deterministic fleet")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="seconds between emissions in live mode")

    parser.add_argument("--backfill-hours", type=float, default=6.0)
    parser.add_argument("--backfill-interval", type=float, default=30.0,
                        help="simulated seconds between historical readings")

    parser.add_argument("--late-fraction", type=float, default=0.05,
                        help="share of events timestamped in the past")
    parser.add_argument("--malformed-fraction", type=float, default=0.01,
                        help="share of events emitted as unparseable payloads")

    parser.add_argument("--eventhub-name",
                        default=os.environ.get("EVENTHUB_NAME", "vehicle-telemetry"))
    parser.add_argument("--log-level", default="INFO")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stderr,
    )

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn_str = os.environ.get("EVENTHUB_CONN_STR")

    if not args.dry_run:
        if not AZURE_AVAILABLE:
            LOG.error("azure-eventhub not installed. pip install -r src/producer/requirements.txt")
            return 2
        if not conn_str:
            LOG.error(
                "EVENTHUB_CONN_STR is not set. Get it with:\n"
                '  export EVENTHUB_CONN_STR="$(terraform -chdir=infra output -raw producer_connection_string)"'
            )
            return 2

    seed = None if args.seed == -1 else args.seed
    rng = random.Random(seed)
    fleet = Fleet(size=args.fleet_size, seed=seed)
    sink = TelemetrySink(conn_str, args.eventhub_name, args.dry_run)

    try:
        if args.mode == "backfill":
            run_backfill(sink, fleet, args, rng)
        else:
            run_live(sink, fleet, args, rng)
    finally:
        sink.close()
        LOG.info("stopped. sent=%d failed=%d", sink.sent, sink.failed)

    return 0


if __name__ == "__main__":
    sys.exit(main())