"""Operations tables for the silver pipeline, written to <catalog>.operations.

Computed inside the pipeline, on the cluster that is already running, so
checking the pipeline's health costs no SQL warehouse time.
"""

import sys

from pyspark import pipelines as dp

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot import operations as ops  # noqa: E402
from src.iot import reference as ref  # noqa: E402
from src.iot.silver import normalize as norm  # noqa: E402
from src.iot.silver import telemetry as tel  # noqa: E402

CATALOG = spark.conf.get("catalog")
OPERATIONS = f"{CATALOG}.operations"
EVENT_LOG = f"{OPERATIONS}.{spark.conf.get('event_log_name')}"
BRONZE_MESSAGES = f"{CATALOG}.bronze.raw_telemetry_messages"

ACCEPTED_CONTRACTS = ref.accepted_contracts(f"{ROOT}/reference")


@dp.materialized_view(
    name=f"{OPERATIONS}.pipeline_runs",
    comment="One row per finished update of the silver pipeline. The update in progress appears once it ends.",
)
def pipeline_runs():
    return ops.pipeline_runs(spark.read.table(EVENT_LOG))


@dp.materialized_view(
    name=f"{OPERATIONS}.pipeline_flow_runs",
    comment="Rows written and expectation failures per flow, per pipeline update.",
)
def pipeline_flow_runs():
    return ops.pipeline_flow_runs(spark.read.table(EVENT_LOG))


@dp.materialized_view(
    name=f"{OPERATIONS}.quarantine_summary_by_hour",
    comment="Quarantined rows per hour of arrival, stage, reason and vendor, across all four quarantine tables.",
)
def quarantine_summary_by_hour():
    return ops.quarantine_summary_by_hour(
        spark.read.table("quarantine_unparseable_messages"),
        spark.read.table("quarantine_contract_violations"),
        spark.read.table("quarantine_invalid_values"),
        spark.read.table("quarantine_registry_mismatches"),
    )


@dp.materialized_view(
    name=f"{OPERATIONS}.silver_reconciliation",
    comment="Bronze recounted in batch against what silver stored and quarantined, reading by reading. "
    "lost_readings and unexpected_readings must be 0.",
)
@dp.expect("no_reading_lost", "lost_readings = 0")
@dp.expect("no_reading_invented", "unexpected_readings = 0")
def silver_reconciliation():
    bronze = spark.read.table(BRONZE_MESSAGES)
    checked = tel.check_contract(
        tel.parse_envelope(bronze, ref.known_vendors(f"{ROOT}/reference")),
        tel.load_contract_schemas(f"{ROOT}/contracts", list(ACCEPTED_CONTRACTS)),
        ACCEPTED_CONTRACTS,
    )
    expected = norm.normalize(checked, ref.field_mappings_by_contract(f"{ROOT}/reference"))

    return ops.silver_reconciliation(
        bronze_messages=bronze,
        expected_readings=expected,
        unparseable=spark.read.table("quarantine_unparseable_messages"),
        contract_violations=spark.read.table("quarantine_contract_violations"),
        measurements=spark.read.table("vehicle_measurements"),
        invalid_values=spark.read.table("quarantine_invalid_values"),
        registry_mismatches=spark.read.table("quarantine_registry_mismatches"),
        content_columns=list(norm.STANDARD_FIELDS),
    )
