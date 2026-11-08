"""Reference (lookup) data: seed CSV files in the repository's reference/ folder.

The files are the source of truth and change through pull requests. The
pipeline loads each one into <catalog>.reference.<name> so that SQL users and
later pipeline stages can join to it.
"""

from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Column name -> Spark type. Files are read as text, then cast to these types,
# so a bad value (for example "abc" as a threshold) shows up as null.
REFERENCE_TABLES: dict[str, dict[str, str]] = {
    "fault_codes": {
        "code": "string", "description": "string", "severity": "string",
    },
    "anomaly_thresholds": {
        "threshold_name": "string", "value": "double", "unit": "string", "description": "string",
    },
    "vendor_contract_versions": {
        "vendor_id": "string", "contract": "string", "version": "int",
        "status": "string", "supported_from": "date", "notes": "string",
    },
    "validity_limits": {
        "field": "string", "minimum": "double", "maximum": "double", "unit": "string", "reason": "string",
    },
    "vendor_field_mappings": {
        "vendor_id": "string", "contract_version": "int", "target_field": "string",
        "source_path": "string", "conversion": "string",
    },
}

ACCEPTED_CONTRACT_STATUSES = ("current", "supported")


def read_rows(path: str | Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_reference_table(spark: SparkSession, reference_dir: str | Path, name: str) -> DataFrame:
    """One seed file as a typed DataFrame, with columns in the declared order."""
    columns = REFERENCE_TABLES[name]
    rows = read_rows(Path(reference_dir) / f"{name}.csv")

    missing = set(columns) - set(rows[0]) if rows else set(columns)
    if missing:
        raise ValueError(f"{name}.csv is missing columns: {sorted(missing)}")

    text = spark.createDataFrame(
        [tuple(row[c] for c in columns) for row in rows],
        ", ".join(f"{c} string" for c in columns),
    )
    return text.select(*[F.col(c).cast(t).alias(c) for c, t in columns.items()])


def known_vendors(reference_dir: str | Path) -> list[str]:
    """Vendors with at least one contract we currently accept."""
    rows = read_rows(Path(reference_dir) / "vendor_contract_versions.csv")
    return sorted({r["vendor_id"] for r in rows if r["status"] in ACCEPTED_CONTRACT_STATUSES})


def accepted_contracts(reference_dir: str | Path) -> dict[str, str]:
    """Contract name -> the vendor allowed to send it, for contracts we currently accept."""
    rows = read_rows(Path(reference_dir) / "vendor_contract_versions.csv")
    return {r["contract"]: r["vendor_id"] for r in rows if r["status"] in ACCEPTED_CONTRACT_STATUSES}


def field_mappings_by_contract(reference_dir: str | Path) -> dict[str, list[dict[str, str]]]:
    """For each accepted contract, the rows of vendor_field_mappings.csv that apply to it."""
    versions = read_rows(Path(reference_dir) / "vendor_contract_versions.csv")
    mappings = read_rows(Path(reference_dir) / "vendor_field_mappings.csv")
    return {
        v["contract"]: [
            m for m in mappings if m["vendor_id"] == v["vendor_id"] and m["contract_version"] == v["version"]
        ]
        for v in versions
        if v["status"] in ACCEPTED_CONTRACT_STATUSES
    }


def validity_limits(reference_dir: str | Path) -> dict[str, tuple[float, float]]:
    """Standard field -> (minimum, maximum) a working sensor can report."""
    rows = read_rows(Path(reference_dir) / "validity_limits.csv")
    return {r["field"]: (float(r["minimum"]), float(r["maximum"])) for r in rows}


def thresholds(reference_dir: str | Path) -> dict[str, float]:
    """Threshold name -> value, from anomaly_thresholds.csv."""
    rows = read_rows(Path(reference_dir) / "anomaly_thresholds.csv")
    return {r["threshold_name"]: float(r["value"]) for r in rows}