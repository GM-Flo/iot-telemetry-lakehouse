"""Reference (lookup) tables, loaded from the seed CSV files in the repository.

Written to <catalog>.reference, not silver: lookups are configuration, kept in
their own schema so they are easy to find and to permission separately.
"""

import sys

from pyspark import pipelines as dp

ROOT = spark.conf.get("bundle.sourcePath")
sys.path.append(ROOT)

from src.iot import reference as ref  # noqa: E402

CATALOG = spark.conf.get("catalog")
REFERENCE_DIR = f"{ROOT}/reference"

DESCRIPTIONS = {
    "fault_codes": "Diagnostic trouble code meaning and severity.",
    "anomaly_thresholds": "Limits such as overheating temperature, held as data rather than code.",
    "validity_limits": "Physical range of each sensor; values outside it are faults, not events.",
    "vendor_contract_versions": "Contract versions each vendor may send, and which is current.",
    "vendor_field_mappings": "Vendor field path to standard field, with the unit conversion to apply.",
}


def _define(name: str) -> None:
    @dp.materialized_view(name=f"{CATALOG}.reference.{name}", comment=DESCRIPTIONS[name])
    def _table():
        return ref.load_reference_table(spark, REFERENCE_DIR, name)


for table_name in ref.REFERENCE_TABLES:
    _define(table_name)
