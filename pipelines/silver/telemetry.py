"""Silver telemetry: from raw bronze messages towards vehicle_measurements.

Built one stage at a time. So far:
  parsed_messages                   temporary view: envelope parsed, every row kept
  quarantine_unparseable_messages   messages that could not be parsed at all
  contract_checked_messages         temporary view: parsed messages checked against their contract
  quarantine_contract_violations    messages that break their vendor's contract
  normalized_readings               temporary view: one row per reading, 21 standard fields
  validated_readings                temporary view: readings checked for impossible values
  quarantine_invalid_values         readings that cannot be true (only that reading, not its message)
  matched_readings                  temporary view: readings checked against the registry at event time
  quarantine_registry_mismatches    readings the registry does not support
  measurements_to_store             temporary view: readings that passed or only warned
  vehicle_measurements              one row per real reading, deduplicated by AUTO CDC
"""

import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot import reference as ref  # noqa: E402
from src.iot.silver import matching as match  # noqa: E402
from src.iot.silver import measurements as meas  # noqa: E402
from src.iot.silver import normalize as norm  # noqa: E402
from src.iot.silver import telemetry as tel  # noqa: E402
from src.iot.silver import validity as val  # noqa: E402

CATALOG = spark.conf.get("catalog")
BRONZE_MESSAGES = f"{CATALOG}.bronze.raw_telemetry_messages"

KNOWN_VENDORS = ref.known_vendors(f"{ROOT}/reference")
ACCEPTED_CONTRACTS = ref.accepted_contracts(f"{ROOT}/reference")
CONTRACT_SCHEMAS = tel.load_contract_schemas(f"{ROOT}/contracts", list(ACCEPTED_CONTRACTS))
FIELD_MAPPINGS = ref.field_mappings_by_contract(f"{ROOT}/reference")
VALIDITY_LIMITS = ref.validity_limits(f"{ROOT}/reference")


@dp.temporary_view(comment="Every bronze message with its envelope parsed and a pass/fail status.")
def parsed_messages():
    return tel.parse_envelope(spark.readStream.table(BRONZE_MESSAGES), KNOWN_VENDORS)


@dp.table(
    name="quarantine_unparseable_messages",
    comment="Messages that are not JSON, have no message_id or payload, or name an unknown vendor.",
)
def quarantine_unparseable_messages():
    return tel.quarantine_unparseable(spark.readStream.table("parsed_messages"))


@dp.temporary_view(comment="Parsed messages checked against their vendor's JSON Schema contract.")
def contract_checked_messages():
    return tel.check_contract(spark.readStream.table("parsed_messages"), CONTRACT_SCHEMAS, ACCEPTED_CONTRACTS)


@dp.table(
    name="quarantine_contract_violations",
    comment="Messages whose payload breaks the contract, or whose contract the vendor may not send.",
)
def quarantine_contract_violations():
    return tel.quarantine_contract_violations(spark.readStream.table("contract_checked_messages"))


@dp.temporary_view(comment="One row per reading from every vendor, with the 21 standard fields in standard units.")
def normalized_readings():
    return norm.normalize(spark.readStream.table("contract_checked_messages"), FIELD_MAPPINGS)


@dp.temporary_view(comment="Normalized readings checked for values that cannot be true.")
def validated_readings():
    return val.check_validity(spark.readStream.table("normalized_readings"), VALIDITY_LIMITS)


@dp.table(
    name="quarantine_invalid_values",
    comment="Readings with impossible values: GPS at 0,0, clock in the future, sensor outside its physical range.",
)
def quarantine_invalid_values():
    return val.quarantine_invalid_values(spark.readStream.table("validated_readings"))


@dp.temporary_view(comment="Valid readings with vehicle, client and driver taken from the registry, or a mismatch reason.")
def matched_readings():
    # Stream-static join: each micro-batch of readings is matched against the
    # registry tables as they are at that moment.
    return match.match_registry(
        spark.readStream.table("validated_readings"),
        device_assignments=spark.read.table("device_assignments"),
        clients=spark.read.table("clients"),
        drivers=spark.read.table("drivers"),
    )


@dp.table(
    name="quarantine_registry_mismatches",
    comment="Readings from unknown or retired devices, or reporting a VIN or account the registry does not support.",
)
def quarantine_registry_mismatches():
    return match.quarantine_registry_mismatches(spark.readStream.table("matched_readings"))


@dp.temporary_view(comment="Matched readings that passed or only warned, ready to store.")
def measurements_to_store():
    return meas.to_store(spark.readStream.table("matched_readings"))


dp.create_streaming_table(
    name="vehicle_measurements",
    comment="One validated, matched reading per device and event time. Duplicates and re-sends collapse into one row.",
)

# Upsert on the natural key: a duplicate overwrites its row instead of adding
# one, however late it arrives. The copy received last wins.
dp.create_auto_cdc_flow(
    target="vehicle_measurements",
    source="measurements_to_store",
    keys=meas.MEASUREMENT_KEYS,
    sequence_by=F.struct(*meas.SEQUENCE_COLUMNS),
    stored_as_scd_type=1,
)