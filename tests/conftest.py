from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Spark launches worker processes with "python3" by default, which does not
# exist on Windows. Pin workers to the interpreter running the tests (the venv).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("iot-lakehouse-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        # The default 1 GB is too small for a session of 400 tests, some of which
        # simulate hours of fleet data.
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True, scope="module")
def _release_cached_data():
    """Free what a test file cached once it finishes, so it cannot starve the files after it."""
    yield
    session = SparkSession.getActiveSession()
    if session is not None:
        session.catalog.clearCache()
