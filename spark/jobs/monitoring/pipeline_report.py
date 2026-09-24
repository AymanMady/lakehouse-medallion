"""
PHASE 11 - Pipeline health report, and the alerts that go with it.

Reads what the jobs recorded in monitoring.* and answers four questions:

    1. QUALITY    did every Silver entity pass its gate in the latest run?
    2. DRIFT      did a valid ratio drop sharply compared to the previous run?
    3. VOLUME     did a Gold table shrink sharply compared to the previous run?
    4. FRESHNESS  when did Bronze last receive data?

The point is comparison. A valid ratio of 96% says nothing on its own; 96%
when it was 99.5% yesterday is an incident. Every check below is therefore
relative to the previous run, except freshness, which is relative to now.

    spark-submit spark/jobs/monitoring/pipeline_report.py
    spark-submit spark/jobs/monitoring/pipeline_report.py --fail-on-alert
"""

from __future__ import annotations

import argparse
import sys
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.generator.schemas import TOPICS
from spark.common import config
from spark.common.monitoring import METRICS, QUALITY_RUNS
from spark.common.session import get_logger, get_spark

LOG = get_logger("monitor")


def _load(spark, layer: str, table: str) -> DataFrame | None:
    path = config.table_path(layer, table)
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def _last_two_runs(df: DataFrame) -> list[str]:
    """Run ids, most recent first. Ordered by WHEN they were recorded, not by
    name: a manual run id and an Airflow run id do not sort together."""
    rows = (df.groupBy("run_id").agg(F.max("recorded_at").alias("at"))
            .orderBy(F.col("at").desc()).limit(2).collect())
    return [r["run_id"] for r in rows]


def check_quality(quality: DataFrame, max_ratio_drop: float) -> list[str]:
    alerts: list[str] = []
    runs = _last_two_runs(quality)
    latest = {r["dataset"]: r for r in quality.filter(F.col("run_id") == runs[0]).collect()}
    previous = ({r["dataset"]: r for r in quality.filter(F.col("run_id") == runs[1]).collect()}
                if len(runs) > 1 else {})

    LOG.info(f"QUALITY  latest run {runs[0]}"
             + (f"  (compared with {runs[1]})" if previous else "  (no previous run)"))
    LOG.info(f"  {'dataset':<13} {'read':>8} {'valid':>8} {'quar.':>6} "
             f"{'ratio':>8} {'Δ pts':>7}  top rule")
    for dataset in sorted(latest, key=lambda d: list(TOPICS).index(d) if d in TOPICS else 99):
        row = latest[dataset]
        before = previous.get(dataset)
        delta = (row["valid_ratio"] - before["valid_ratio"]) * 100 if before else None
        failures = {k: v for k, v in (row["failures_by_rule"] or {}).items() if v}
        top = max(failures.items(), key=lambda kv: kv[1]) if failures else None
        LOG.info(f"  {dataset:<13} {row['rows_read']:>8,} {row['rows_valid']:>8,} "
                 f"{row['rows_invalid']:>6,} {row['valid_ratio']:>8.2%} "
                 f"{(f'{delta:+.2f}' if delta is not None else '-'):>7}  "
                 f"{f'{top[0]} ({top[1]:,})' if top else '-'}")

        if not row["passed"]:
            alerts.append(f"{dataset}: failed its quality gate "
                          f"({row['valid_ratio']:.2%} < {row['threshold']:.2%})")
        if delta is not None and -delta > max_ratio_drop * 100:
            alerts.append(f"{dataset}: valid ratio dropped {-delta:.2f} pts "
                          f"since the previous run")
    return alerts


def check_volume(metrics: DataFrame, max_row_drop: float) -> list[str]:
    alerts: list[str] = []
    gold = metrics.filter((F.col("layer") == "gold"))
    runs = _last_two_runs(gold)
    if not runs:
        return alerts

    def values(run: str) -> dict[tuple[str, str], float]:
        return {(r["subject"], r["metric"]): r["value"]
                for r in gold.filter(F.col("run_id") == run).collect()}

    latest = values(runs[0])
    previous = values(runs[1]) if len(runs) > 1 else {}

    LOG.info(f"VOLUME   gold, latest run {runs[0]}")
    for (subject, metric), value in sorted(latest.items()):
        before = previous.get((subject, metric))
        change = ((value - before) / before) if before else None
        LOG.info(f"  {subject:<18} {metric:<24} {value:>14,.2f}"
                 + (f"  ({change:+.1%})" if change is not None else ""))
        if metric == "row_count" and change is not None and -change > max_row_drop:
            alerts.append(f"gold.{subject}: {-change:.1%} fewer rows than the previous run")
    if latest.get(("fact_order_items", "lines_lost_in_joins"), 0) > 0:
        LOG.info("  (lines lost in joins are orphans quarantined upstream, not a Gold bug)")
    return alerts


def check_freshness(spark, max_age_hours: float) -> list[str]:
    alerts: list[str] = []
    LOG.info("FRESHNESS bronze, last ingestion")
    for entity in TOPICS:
        bronze = _load(spark, "bronze", entity)
        if bronze is None:
            alerts.append(f"bronze.{entity}: table does not exist")
            continue
        # Computed IN Spark, never on collected timestamps: collect() converts
        # them to the driver's local time zone (TZ of the container), not the
        # session's UTC, and the age comes out wrong by the offset.
        last = F.max("ingestion_timestamp")
        row = bronze.agg(
            F.date_format(last, "yyyy-MM-dd HH:mm").alias("last"),
            ((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(last)) / 3600)
            .alias("age"),
        ).first()
        if row["last"] is None:
            alerts.append(f"bronze.{entity}: table is empty")
            continue
        age = row["age"]
        LOG.info(f"  {entity:<13} {row['last']} UTC  ({age:,.1f} h ago)")
        if age > max_age_hours:
            alerts.append(f"bronze.{entity}: no data for {age:,.1f} h "
                          f"(limit {max_age_hours:g} h)")
    return alerts


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 11 - pipeline health report")
    p.add_argument("--max-ratio-drop", type=float, default=0.05,
                   help="alert when a valid ratio falls by more than this (0.05 = 5 pts)")
    p.add_argument("--max-row-drop", type=float, default=0.20,
                   help="alert when a Gold table loses more than this share of its rows")
    p.add_argument("--max-age-hours", type=float, default=26,
                   help="alert when Bronze has received nothing for this long")
    p.add_argument("--fail-on-alert", action="store_true",
                   help="exit 1 when an alert fires (what the Airflow task uses)")
    args = p.parse_args(argv)

    spark = get_spark("monitoring-pipeline-report")

    quality = _load(spark, "monitoring", QUALITY_RUNS)
    metrics = _load(spark, "monitoring", METRICS)
    if quality is None:
        LOG.info("no run recorded yet in monitoring.quality_runs - run the pipeline first")
        spark.stop()
        return 0

    alerts = check_quality(quality, args.max_ratio_drop)
    if metrics is not None:
        alerts += check_volume(metrics, args.max_row_drop)
    alerts += check_freshness(spark, args.max_age_hours)

    if alerts:
        LOG.warning(f"ALERTS   {len(alerts)}")
        for alert in alerts:
            LOG.warning(f"  - {alert}")
    else:
        LOG.info("ALERTS   none - the pipeline is healthy")

    spark.stop()
    return 1 if (alerts and args.fail_on_alert) else 0


if __name__ == "__main__":
    sys.exit(main())
