"""
=============================================================================
 Phase 1 smoke test — proves the whole storage stack is wired correctly.

 It verifies, in one run:
   1. a SparkSession can be created with the Delta extensions loaded
   2. Spark can WRITE a Delta table to MinIO through s3a://
   3. Spark can READ it back
   4. Delta ACID features work: UPDATE + time travel (VERSION AS OF 0)
   5. the Hive Metastore (PostgreSQL) registers a database and a table

 Run with:  make smoke
=============================================================================
"""
import os
import sys

from pyspark.sql import SparkSession

BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
TABLE_PATH = f"s3a://{BUCKET}/bronze/_smoke_test"

PASSED, FAILED = [], []


def check(label, fn):
    try:
        detail = fn()
        PASSED.append(label)
        print(f"  [OK]   {label}" + (f" -> {detail}" if detail else ""))
    except Exception as exc:  # noqa: BLE001 - smoke test reports every failure
        FAILED.append(label)
        print(f"  [FAIL] {label}\n         {type(exc).__name__}: {exc}")


def main() -> int:
    print("\nLakehouse smoke test")
    print("-" * 60)

    spark = (
        SparkSession.builder.appName("phase1-smoke-test")
        .master(os.getenv("SMOKE_MASTER", "local[2]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print(f"  Spark {spark.version} | writing to {TABLE_PATH}\n")

    def t_delta_loaded():
        ext = spark.conf.get("spark.sql.extensions", "")
        assert "DeltaSparkSessionExtension" in ext, f"extensions = {ext!r}"
        return "DeltaSparkSessionExtension active"

    def t_write():
        df = spark.createDataFrame(
            [(1, "FR", 120.0), (2, "DE", 80.5), (3, "US", 240.25)],
            "order_id INT, country STRING, total_amount DOUBLE",
        )
        df.write.format("delta").mode("overwrite").save(TABLE_PATH)
        return "3 rows written as Delta on MinIO"

    def t_read():
        n = spark.read.format("delta").load(TABLE_PATH).count()
        assert n == 3, f"expected 3 rows, got {n}"
        return "3 rows read back"

    def t_acid_update():
        from delta.tables import DeltaTable

        DeltaTable.forPath(spark, TABLE_PATH).update(
            condition="order_id = 1", set={"total_amount": "999.0"}
        )
        v = (
            spark.read.format("delta")
            .load(TABLE_PATH)
            .where("order_id = 1")
            .collect()[0]["total_amount"]
        )
        assert v == 999.0, f"update did not apply (value = {v})"
        return "UPDATE applied (ACID)"

    def t_time_travel():
        old = (
            spark.read.format("delta")
            .option("versionAsOf", 0)
            .load(TABLE_PATH)
            .where("order_id = 1")
            .collect()[0]["total_amount"]
        )
        assert old == 120.0, f"version 0 should still hold 120.0, got {old}"
        return "VERSION AS OF 0 still returns the original value"

    def t_metastore():
        spark.sql("CREATE DATABASE IF NOT EXISTS smoke_db")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS smoke_db.orders "
            f"USING DELTA LOCATION '{TABLE_PATH}'"
        )
        rows = spark.sql("SELECT count(*) AS n FROM smoke_db.orders").collect()
        assert rows[0]["n"] == 3
        return "table registered in the PostgreSQL-backed Hive Metastore"

    check("Delta extensions loaded", t_delta_loaded)
    check("Write Delta to MinIO (s3a)", t_write)
    check("Read Delta from MinIO", t_read)
    check("Delta UPDATE (ACID)", t_acid_update)
    check("Delta time travel", t_time_travel)
    check("Hive Metastore on PostgreSQL", t_metastore)

    # cleanup
    try:
        spark.sql("DROP TABLE IF EXISTS smoke_db.orders")
        spark.sql("DROP DATABASE IF EXISTS smoke_db")
    except Exception:  # noqa: BLE001
        pass
    spark.stop()

    print("-" * 60)
    print(f"  {len(PASSED)} passed, {len(FAILED)} failed\n")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
