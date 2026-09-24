"""
PHASE 10 - Integration tests: the real Spark logic, on in-memory DataFrames.

They run INSIDE the Spark image (`make test-integration`), with the exact
PySpark the jobs use. No MinIO, no Kafka, no metastore: the jobs keep their
DataFrame logic apart from their I/O, and that is what is tested here.

Outside the Spark image PySpark is absent, and this folder is simply not
collected - so `pytest tests/` stays usable in the lightweight container.
"""

from __future__ import annotations

import logging

import pytest

try:
    import pyspark  # noqa: F401
except ImportError:
    collect_ignore_glob = ["test_*.py"]


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        # Overrides spark-defaults: no cluster, no Hive metastore.
        .master("local[2]")
        .appName("lakehouse-integration-tests")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    # py4j logs its shutdown at INFO, after pytest has closed the captured
    # stdout the job loggers write to: a harmless but noisy traceback.
    logging.getLogger("py4j").setLevel(logging.WARNING)
