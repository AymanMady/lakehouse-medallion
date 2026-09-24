"""
Pipeline monitoring (phase 11).

A quality report that is only printed dies with the container's log. To answer
"is the data getting worse?" you need the history, so every run leaves a trace
in the lake itself, as Delta tables any SQL client can query:

    monitoring.quality_runs   one row per Silver entity per run
                              read / valid / quarantined / duplicates, the
                              ratio, the threshold, and the count per rule
    monitoring.metrics        one row per (layer, subject, metric) per run
                              a long, generic format: Gold row counts, rows
                              lost in joins... adding a metric needs no DDL

Both are APPEND-only: overwriting them would erase exactly the history they
exist to keep. A run that FAILS its quality gate is recorded too - it is the
run you most want to find afterwards.

The run id ties together what separate jobs record. Airflow passes its own
run_id; a manual run gets a timestamped one.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.common import config
from spark.common.data_quality import QualityReport

QUALITY_RUNS = "quality_runs"
METRICS = "metrics"

QUALITY_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
    StructField("dataset", StringType(), False),
    StructField("rows_read", LongType(), False),
    StructField("rows_valid", LongType(), False),
    StructField("rows_invalid", LongType(), False),
    StructField("rows_duplicated", LongType(), False),
    StructField("valid_ratio", DoubleType(), False),
    StructField("threshold", DoubleType(), False),
    StructField("passed", BooleanType(), False),
    StructField("failures_by_rule", MapType(StringType(), LongType()), True),
])

METRIC_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
    StructField("layer", StringType(), False),
    StructField("subject", StringType(), False),
    StructField("metric", StringType(), False),
    StructField("value", DoubleType(), False),
])


def run_id() -> str:
    """The id shared by every job of one pipeline run."""
    return (os.getenv("PIPELINE_RUN_ID")
            or f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}")


def _append(spark: SparkSession, rows: list[tuple], schema: StructType, table: str) -> None:
    if not rows:
        return
    path = config.table_path("monitoring", table)
    (spark.createDataFrame(rows, schema)
     .write.format("delta").mode("append").save(path))
    spark.sql(f"CREATE DATABASE IF NOT EXISTS monitoring "
              f"LOCATION '{config.MONITORING}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS monitoring.{table} "
              f"USING DELTA LOCATION '{path}'")


def record_quality(spark: SparkSession, report: QualityReport, threshold: float,
                   run: str | None = None) -> None:
    now = datetime.now(timezone.utc)
    row = (
        run or run_id(), now, report.dataset,
        report.rows_read, report.rows_valid, report.rows_invalid,
        report.rows_duplicated, float(report.valid_ratio), float(threshold),
        report.valid_ratio >= threshold,
        {rule: int(count) for rule, count in report.failures.items()},
    )
    _append(spark, [row], QUALITY_SCHEMA, QUALITY_RUNS)


def record_metrics(spark: SparkSession, layer: str,
                   values: dict[tuple[str, str], float], run: str | None = None) -> None:
    """values: {(subject, metric): value}, e.g. {("fact_orders", "row_count"): 9815}."""
    now = datetime.now(timezone.utc)
    rid = run or run_id()
    rows = [(rid, now, layer, subject, metric, float(value))
            for (subject, metric), value in values.items()]
    _append(spark, rows, METRIC_SCHEMA, METRICS)
