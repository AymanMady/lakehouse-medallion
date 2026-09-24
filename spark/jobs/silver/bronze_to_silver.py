"""
PHASE 5 & 6 - BRONZE -> SILVER, with quarantine.

THE RULE OF THE SILVER LAYER: a Silver row is a row whose validity can be
proven.

The nine steps, in order - and the order matters:
    1. read Bronze (everything is text)
    2. parse the payload JSON
    3. deduplicate on event_id      technical duplicates: the same message twice
    4. cast to the target types     a failed cast becomes NULL, and is caught
    5. repair                       cosmetic anomalies
    6. validate                     the rules
    7. check referential integrity  orphans
    8. deduplicate on business key  keep the most recent version
    9. write, and quarantine the rest

Deduplicating on event_id BEFORE casting is deliberate: a duplicate that fails
to cast would otherwise be counted twice in the quality report.

    spark-submit spark/jobs/silver/bronze_to_silver.py
    spark-submit spark/jobs/silver/bronze_to_silver.py --entities orders --min-valid-ratio 0.9
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.generator.schemas import BUSINESS_KEYS, ENTITIES, FOREIGN_KEYS
from spark.common import config
from spark.common.data_quality import (
    ERROR_COL,
    QualityReport,
    apply_rules,
    assert_quality,
    count_failures,
    log_report,
    split_valid_invalid,
)
from spark.common.monitoring import record_quality
from spark.common.schemas import payload_schema, silver_schema
from spark.common.session import get_logger, get_spark
from spark.jobs.silver.rules import repair, rules_for

LOG = get_logger("silver")

# Order matters: a table is only usable as a foreign key reference once it
# exists in Silver.
ORDER = ["customers", "products", "orders", "order_items", "payments", "shipments"]


def parse_payload(bronze: DataFrame, entity: str) -> DataFrame:
    """Bronze text -> one column per field, still as text."""
    return (
        bronze
        .withColumn("_p", F.from_json("payload", payload_schema(entity)))
        .select("event_id", "occurred_at", "ingestion_timestamp", "payload", "_p.*")
    )


def cast_types(df: DataFrame, entity: str) -> DataFrame:
    """Text -> target types. A cast that fails yields NULL, without raising.

    That is what we want in a pipeline: one unreadable value must not bring
    the job down. The `_missing` rules then turn those NULLs into an explicit,
    counted rejection.
    """
    schema = silver_schema(entity)
    casted = []
    for field in schema.fields:
        column = F.col(field.name)
        if field.dataType.typeName() == "timestamp":
            # Two accepted formats, tried in order. to_timestamp returns NULL
            # instead of raising when neither matches.
            column = F.coalesce(
                F.to_timestamp(column, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                F.to_timestamp(column, "yyyy-MM-dd HH:mm:ss"),
            )
        elif field.dataType.typeName() == "date":
            column = F.to_date(column, "yyyy-MM-dd")
        else:
            column = column.cast(field.dataType)
        casted.append(column.alias(field.name))
    return df.select("event_id", "occurred_at", "ingestion_timestamp", "payload",
                     "_corruption", *casted)


def check_foreign_keys(df: DataFrame, entity: str, silver: dict[str, DataFrame]) -> DataFrame:
    """Flag orphan rows with a `fk_orphan_<field>` error.

    A LEFT ANTI JOIN answers "which rows of the left side have NO match on the
    right". It is the cheapest way to measure orphans - and measuring them
    matters, because rejecting rows in a dimension ORPHANS the facts that
    referenced it. That cascade is invisible unless you look for it.
    """
    errors = F.col(ERROR_COL)
    for fk_field, ref_entity, ref_key in FOREIGN_KEYS.get(entity, []):
        reference = silver.get(ref_entity)
        if reference is None:
            continue
        orphans = (df.select(fk_field).distinct()
                   .join(reference.select(F.col(ref_key).alias(fk_field)),
                         on=fk_field, how="left_anti")
                   .withColumn("_orphan", F.lit(True)))
        df = df.join(orphans, on=fk_field, how="left")
        errors = F.when(F.col("_orphan").isNotNull(),
                        F.array_union(errors, F.array(F.lit(f"fk_orphan_{fk_field}")))
                        ).otherwise(errors)
        df = df.withColumn(ERROR_COL, errors).drop("_orphan")
        errors = F.col(ERROR_COL)
    return df


def deduplicate(df: DataFrame, key: str, order_by: str) -> tuple[DataFrame, int]:
    """Keep the most recent row per business key.

    Not `dropDuplicates([key])`, which keeps an arbitrary row: when the same
    entity is emitted twice with different values, the LATEST one is the one
    that reflects reality.
    """
    from pyspark.sql.window import Window

    before = df.count()
    window = Window.partitionBy(key).orderBy(F.col(order_by).desc_nulls_last())
    deduped = (df.withColumn("_rn", F.row_number().over(window))
               .filter(F.col("_rn") == 1).drop("_rn"))
    after = deduped.count()
    return deduped, before - after


def write_silver(valid: DataFrame, entity: str, spark) -> None:
    """Idempotent write: MERGE on the business key.

    Re-running the job must not duplicate anything. With an append, a second
    run would double every table. The MERGE makes the write replayable, which
    is what turns "the job ran twice" from an incident into a non-event.
    """
    key = BUSINESS_KEYS[entity]
    path = config.table_path("silver", entity)
    columns = list(ENTITIES[entity]) + ["_ingested_at"]
    out = valid.select(*ENTITIES[entity]).withColumn("_ingested_at", F.current_timestamp())

    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)
        (target.alias("t")
         .merge(out.alias("s"), f"t.{key} = s.{key}")
         .whenMatchedUpdate(set={c: f"s.{c}" for c in columns})
         .whenNotMatchedInsertAll()
         .execute())
    else:
        out.write.format("delta").mode("overwrite").save(path)

    spark.sql(f"CREATE TABLE IF NOT EXISTS silver.{entity} "
              f"USING DELTA LOCATION '{path}'")


def write_quarantine(invalid: DataFrame, entity: str, spark) -> None:
    """The rejects, with their reason and their original payload.

    A quarantine that only keeps the payload tells you something failed, never
    what. Keeping both is what makes it possible to decide whether the data is
    wrong or the rule is too strict - and to replay it once the source is fixed.
    """
    path = config.table_path("quarantine", entity)
    out = invalid.select(
        "event_id",
        F.lit(entity).alias("entity"),
        F.col(ERROR_COL).alias("rejection_reasons"),
        F.col("_corruption").alias("injected_corruption"),
        F.col("payload").alias("raw_payload"),
        F.col("occurred_at"),
        F.current_timestamp().alias("quarantined_at"),
    )
    (out.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(path))
    spark.sql(f"CREATE TABLE IF NOT EXISTS quarantine.{entity} "
              f"USING DELTA LOCATION '{path}'")


def annotate(parsed: DataFrame, entity: str,
             silver: dict[str, DataFrame]) -> tuple[DataFrame, int]:
    """Steps 3 to 7: every row, valid or not, with its list of violated rules.

    Pure DataFrame work, no I/O - which is what lets the integration tests run
    the real Silver logic on an in-memory DataFrame.
    Returns (annotated rows, technical duplicates removed).
    """
    # 3. technical duplicates: the SAME message delivered twice.
    deduped_events, dup_events = deduplicate(parsed, "event_id", "ingestion_timestamp")

    typed = cast_types(deduped_events, entity)
    repaired = repair(entity, typed)

    annotated = apply_rules(repaired, rules_for(entity))
    annotated = check_foreign_keys(annotated, entity, silver)
    return annotated, dup_events


def process(spark, entity: str, silver: dict[str, DataFrame],
            min_ratio: float) -> QualityReport:
    bronze = spark.read.format("delta").load(config.table_path("bronze", entity))

    parsed = parse_payload(bronze, entity)
    rows_read = parsed.count()

    rules = rules_for(entity)
    annotated, dup_events = annotate(parsed, entity, silver)
    annotated.persist()

    valid, invalid = split_valid_invalid(annotated)

    # 8. business duplicates: the same entity emitted twice with different
    # values - keep the latest version.
    key = BUSINESS_KEYS[entity]
    valid, dup_business = deduplicate(valid, key, "occurred_at")
    # The same rejected entity re-emitted by every run is ONE problem, not N:
    # left alone, the quarantine would grow with each run while Silver does
    # not, and the valid ratio would drift down on unchanged data. A reject
    # with no business key cannot be matched to anything, so all are kept.
    keyed, dup_rejected = deduplicate(invalid.filter(F.col(key).isNotNull()),
                                      key, "occurred_at")
    invalid = keyed.unionByName(invalid.filter(F.col(key).isNull()))

    report = QualityReport(
        dataset=entity,
        rows_read=rows_read,
        rows_valid=valid.count(),
        rows_invalid=invalid.count(),
        rows_duplicated=dup_events + dup_business + dup_rejected,
        failures=count_failures(invalid, rules),
    )
    # The orphan counts come from the FK step, not from `rules`.
    orphan_counts = (invalid.select(F.explode(ERROR_COL).alias("rule"))
                     .filter(F.col("rule").startswith("fk_orphan_"))
                     .groupBy("rule").count().collect())
    for row in orphan_counts:
        report.failures[row["rule"]] = row["count"]

    write_silver(valid, entity, spark)
    write_quarantine(invalid, entity, spark)

    log_report(LOG, report)
    # Recorded BEFORE the gate: a run that fails it is the one you will want
    # to find in the history afterwards.
    record_quality(spark, report, min_ratio)
    assert_quality(report, min_ratio, LOG)

    silver[entity] = spark.read.format("delta").load(config.table_path("silver", entity))
    annotated.unpersist()
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phases 5-6 - Bronze -> Silver")
    p.add_argument("--entities", nargs="+", default=ORDER, choices=ORDER)
    p.add_argument("--min-valid-ratio", type=float, default=0.80,
                   help="below this, the pipeline stops")
    args = p.parse_args(argv)

    spark = get_spark("silver-bronze-to-silver")
    LOG.info(f"Bronze -> Silver | {config.describe()} | "
             f"min valid ratio {args.min_valid_ratio:.0%}")

    silver: dict[str, DataFrame] = {}
    # Always process in dependency order, even for a subset: a foreign key can
    # only be checked against a table that already exists.
    ordered = [e for e in ORDER if e in args.entities]
    for entity in ordered:
        process(spark, entity, silver, args.min_valid_ratio)

    LOG.info("  Silver layer ready.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
