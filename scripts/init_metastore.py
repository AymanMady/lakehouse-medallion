"""
=============================================================================
 One-shot Hive Metastore bootstrap.

 Registers the medallion databases (plus `monitoring`) in the Hive catalog, so that
 `SELECT * FROM silver.orders` works in Spark SQL and in dbt.

 The metastore TABLES are NOT created here: PostgreSQL loads the official Hive
 2.3.0 DDL at first boot (docker/postgres/init/02-init-hive-metastore.sh).
 Letting Spark auto-create them instead is a trap — DataNucleus creates tables
 lazily (you later hit "Required table missing: TBL_PRIVS") and two Spark apps
 starting together deadlock PostgreSQL.

 This script therefore also acts as a guard: it fails loudly if the schema is
 missing, instead of hanging.
=============================================================================
"""
import os
import sys

from pyspark.sql import SparkSession

BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")

# catalog database -> physical location in the lake
DATABASES = {
    "bronze": f"s3a://{BUCKET}/bronze",
    "silver": f"s3a://{BUCKET}/silver",
    "gold": f"s3a://{BUCKET}/gold",
    "quarantine": f"s3a://{BUCKET}/quarantine",
    "monitoring": f"s3a://{BUCKET}/monitoring",
}


def main() -> int:
    print("[metastore-init] registering medallion databases in the catalog")

    spark = (
        SparkSession.builder.appName("metastore-init")
        .master("local[1]")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    for name, location in DATABASES.items():
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {name} LOCATION '{location}'")
        print(f"[metastore-init]   database {name:<11} -> {location}")

    existing = sorted(r.namespace for r in spark.sql("SHOW DATABASES").collect())
    print(f"[metastore-init] catalog now contains: {', '.join(existing)}")

    spark.stop()
    print("[metastore-init] OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
