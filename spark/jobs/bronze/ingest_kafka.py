"""
PHASE 4 - Kafka -> BRONZE (Delta on MinIO).

THE RULE OF THE BRONZE LAYER: never alter the source data.

Bronze is append-only and everything stays text. It is not a formatting
decision, it is what makes the pipeline replayable: a cleaning rule ALWAYS
changes, and when it does you want to re-run Silver and Gold without asking
the source for the data again.

What Bronze adds - and it is only metadata, never a correction:
    payload              the raw JSON of the record, untouched
    raw_value            the whole Kafka message, for absolute fidelity
    kafka_*              partition, offset, timestamp: where it came from
    ingestion_timestamp  when it entered the lake
    ingestion_date       the partition column

    spark-submit spark/jobs/bronze/ingest_kafka.py --mode batch
    spark-submit spark/jobs/bronze/ingest_kafka.py --mode stream --duration 120
"""

from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.generator.schemas import TOPICS
from spark.common import config
from spark.common.schemas import ENVELOPE_FIELDS
from spark.common.session import get_logger, get_spark

LOG = get_logger("bronze")


def to_bronze(raw: DataFrame, entity: str) -> DataFrame:
    """Kafka envelope -> Bronze columns. No cast, no cleaning, no filter."""
    envelope = [
        F.get_json_object(F.col("value_str"), f"$.{field}").alias(field)
        for field in ENVELOPE_FIELDS
    ]
    return (
        raw.withColumn("value_str", F.col("value").cast("string"))
        .select(
            *envelope,
            # get_json_object on an object returns its JSON TEXT. That is what
            # keeps Bronze raw while still letting Silver parse it later.
            F.get_json_object(F.col("value_str"), "$.payload").alias("payload"),
            F.col("value_str").alias("raw_value"),
            F.lit(entity).alias("source_entity"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
        # Partitioning by ingestion date, not by event date: Bronze is
        # organised by WHEN IT ARRIVED, which is what makes "re-run yesterday's
        # ingestion" a directory-level operation.
        .withColumn("ingestion_date", F.to_date("ingestion_timestamp"))
    )


def ingest_batch(spark, entity: str, starting: str) -> int:
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICS[entity])
        .option("startingOffsets", starting)
        .load()
    )
    bronze = to_bronze(raw, entity)
    count = bronze.count()
    if count == 0:
        LOG.info(f"  {entity:<14} nothing to ingest")
        return 0

    (bronze.write.format("delta")
     .mode("append")
     .partitionBy("ingestion_date")
     .option("mergeSchema", "true")
     .save(config.table_path("bronze", entity)))
    LOG.info(f"  {entity:<14} {count:>8,} events appended")
    return count


def ingest_stream(spark, entity: str, starting: str):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICS[entity])
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        # Caps a micro-batch. Without it, a job restarting after an outage
        # swallows the whole backlog at once and runs out of memory.
        .option("maxOffsetsPerTrigger", 20_000)
        .load()
    )
    return (
        to_bronze(raw, entity)
        .writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        # The checkpoint holds the consumed Kafka offsets. It is what makes a
        # restart resume instead of re-ingesting everything.
        .option("checkpointLocation", config.checkpoint_path(f"bronze_{entity}"))
        .queryName(f"bronze-{entity}")
        .trigger(processingTime="10 seconds")
        .start()
    )


def register(spark, entity: str) -> None:
    """Declare the Delta path as a catalog table.

    Without this, `SELECT * FROM bronze.orders` fails and everyone has to know
    the s3a:// path - including dbt.
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS bronze.{entity} "
        f"USING DELTA LOCATION '{config.table_path('bronze', entity)}'"
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 4 - Kafka -> Bronze")
    p.add_argument("--entities", nargs="+", default=sorted(TOPICS), choices=sorted(TOPICS))
    p.add_argument("--mode", choices=["batch", "stream"], default="batch")
    p.add_argument("--from-beginning", action="store_true", default=True)
    p.add_argument("--latest", dest="from_beginning", action="store_false",
                   help="only read what arrives from now on")
    p.add_argument("--duration", type=int, default=0,
                   help="stream mode: stop after N seconds")
    args = p.parse_args(argv)

    spark = get_spark("bronze-ingest-kafka")
    starting = "earliest" if args.from_beginning else "latest"
    LOG.info(f"Kafka -> Bronze | {config.describe()} | mode={args.mode} from={starting}")

    if args.mode == "batch":
        total = 0
        for entity in args.entities:
            total += ingest_batch(spark, entity, starting)
            register(spark, entity)
        LOG.info(f"  {'TOTAL':<14} {total:>8,} events in Bronze")
        spark.stop()
        return 0

    queries = [ingest_stream(spark, entity, starting) for entity in args.entities]
    LOG.info(f"  {len(queries)} streaming query(ies) running")
    deadline = time.monotonic() + args.duration if args.duration else None
    try:
        for query in queries:
            if deadline:
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    query.awaitTermination(remaining)
            else:
                query.awaitTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        for entity in args.entities:
            register(spark, entity)
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
