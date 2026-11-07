"""Silver telemetry: from raw bronze messages towards vehicle_measurements.

Built one stage at a time. So far:
  parsed_messages                   temporary view: envelope parsed, every row kept
  quarantine_unparseable_messages   messages that could not be parsed at all
"""

import sys

from pyspark import pipelines as dp

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot import reference as ref  # noqa: E402
from src.iot.silver import telemetry as tel  # noqa: E402

CATALOG = spark.conf.get("catalog")
BRONZE_MESSAGES = f"{CATALOG}.bronze.raw_telemetry_messages"
KNOWN_VENDORS = ref.known_vendors(f"{ROOT}/reference")


@dp.temporary_view(comment="Every bronze message with its envelope parsed and a pass/fail status.")
def parsed_messages():
    return tel.parse_envelope(spark.readStream.table(BRONZE_MESSAGES), KNOWN_VENDORS)


@dp.table(
    name="quarantine_unparseable_messages",
    comment="Messages that are not JSON, have no message_id or payload, or name an unknown vendor.",
)
def quarantine_unparseable_messages():
    return tel.quarantine_unparseable(spark.readStream.table("parsed_messages"))