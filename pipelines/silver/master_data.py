"""Silver master data from registry snapshots.

Declarative: each function says what a table is; the pipeline decides order,
creates the tables and manages progress.

  vehicles, drivers, clients    SCD type 2, built by AUTO CDC from snapshots.
                                The registry sends only today's values, so the
                                history of changes must be built here.
  device_assignments            The registry already carries history
                                (valid_from / valid_to); stored as received.
  vendors, regions, fuel_prices Current values from the latest snapshot.
"""

import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("bundle.sourcePath"))

from src.iot.silver import master_data as md  # noqa: E402

CATALOG = spark.conf.get("catalog")
BRONZE_REGISTRY = f"{CATALOG}.bronze.raw_registry_snapshots"


def _snapshots():
    return md.latest_file_per_snapshot(spark.read.table(BRONZE_REGISTRY))


def _latest_section(section: str):
    latest = _snapshots().orderBy(F.col("snapshot_date").desc()).limit(1)
    return md.registry_section(md.parse_registry(latest), section)


def _snapshot_loader(section: str):
    """Feed AUTO CDC one snapshot at a time, oldest unprocessed first.

    AUTO CDC passes the last version it processed (None on the first run) and
    asks for the next one. Returning None means it is up to date.
    """

    def next_snapshot_and_version(latest_version):
        dates = [r.snapshot_date for r in _snapshots().select("snapshot_date").collect()]
        pending = sorted(
            v for v in map(md.snapshot_version, dates)
            if latest_version is None or v > latest_version
        )
        if not pending:
            return None

        version = pending[0]
        snapshot = _snapshots().filter(F.col("snapshot_date") == md.version_to_date(version))
        rows = md.registry_section(md.parse_registry(snapshot), section).drop("snapshot_date")
        return rows, version

    return next_snapshot_and_version


HISTORY_TRACKED = {
    "vehicles": ("vehicles", "vehicle_id", "One row per vehicle version. __START_AT and __END_AT give each version's validity."),
    "drivers": ("drivers", "driver_id", "One row per driver version, including moves between clients."),
    "clients": ("clients", "client_id", "One row per fleet-company version."),
}

for table, (section, key, comment) in HISTORY_TRACKED.items():
    dp.create_streaming_table(name=table, comment=comment)
    dp.create_auto_cdc_from_snapshot_flow(
        target=table,
        source=_snapshot_loader(section),
        keys=[key],
        stored_as_scd_type=2,
    )


@dp.materialized_view(
    name="device_assignments",
    comment="Which device was fitted to which vehicle, and when. History as provided by the registry.",
)
@dp.expect("has_valid_from", "valid_from IS NOT NULL")
@dp.expect("retired_rows_have_an_end", "status <> 'retired' OR valid_to IS NOT NULL")
@dp.expect("active_rows_are_open", "status <> 'active' OR valid_to IS NULL")
def device_assignments():
    return _latest_section("device_vehicle_assignments")


@dp.materialized_view(name="vendors", comment="Telematics vendors, current values.")
def vendors():
    return _latest_section("vendors")


@dp.materialized_view(name="regions", comment="Operating regions: depot location, geofence radius, UTC offset.")
def regions():
    return _latest_section("regions")


@dp.materialized_view(name="fuel_prices", comment="Daily fuel price per litre per region.")
@dp.expect("positive_price", "price_per_litre_usd > 0")
def fuel_prices():
    return _latest_section("fuel_prices_by_region")
