"""
PHASE 7 - SILVER -> GOLD: the star schema.

THE RULE OF THE GOLD LAYER: business vocabulary only. Nobody querying Gold
should have to know what a Kafka offset or a quarantine is.

A star schema is two kinds of table:
    DIMENSIONS  who / what / when   - textual, small, one row per entity
    FACTS       what happened       - numeric, large, one row per event

Why this shape rather than one wide flat table? Because a dimension changes
independently of the facts: renaming a category must not mean rewriting
millions of order lines. And because a BI tool can filter on a dimension
without scanning the fact table.

    spark-submit spark/jobs/gold/build_star_schema.py
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.generator.schemas import REVENUE_STATUSES
from spark.common import config
from spark.common.session import get_logger, get_spark

LOG = get_logger("gold")

TABLES = ["dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items"]


# ---------------------------------------------------------------------------
#  Dimensions
# ---------------------------------------------------------------------------

def dim_date(spark, start: str, end: str) -> DataFrame:
    """A generated calendar, not one derived from the data.

    Deriving it from the facts would leave holes: a day with no order would
    simply not exist, and "revenue per day" would silently skip it instead of
    showing a zero. A calendar dimension has no holes by construction.
    """
    return (
        spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), "
                  f"interval 1 day)) AS date_day")
        .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date_day"))
        .withColumn("quarter", F.quarter("date_day"))
        .withColumn("month", F.month("date_day"))
        .withColumn("month_name", F.date_format("date_day", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("date_day"))
        .withColumn("day_of_month", F.dayofmonth("date_day"))
        .withColumn("day_of_week", F.dayofweek("date_day"))
        .withColumn("day_name", F.date_format("date_day", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_day").isin([1, 7]))
        .withColumn("year_month", F.date_format("date_day", "yyyy-MM"))
        .select("date_key", "date_day", "year", "quarter", "month", "month_name",
                "week_of_year", "day_of_month", "day_of_week", "day_name",
                "is_weekend", "year_month")
    )


def dim_customer(customers: DataFrame) -> DataFrame:
    return (
        customers.select(
            F.col("customer_id").alias("customer_key"),
            F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
            "email", "country", "city", "segment",
            F.col("signup_date").alias("signup_date"),
            F.date_format("signup_date", "yyyy-MM").alias("signup_cohort"),
            # A derived attribute belongs in the dimension: computing it in
            # every query is how two dashboards end up disagreeing.
            F.datediff(F.current_date(), F.col("signup_date")).alias("tenure_days"),
            F.col("email").isNotNull().alias("is_contactable"),
        )
    )


def dim_product(products: DataFrame) -> DataFrame:
    margin = F.col("unit_price") - F.col("unit_cost")
    return (
        products.select(
            F.col("product_id").alias("product_key"),
            "product_name", "category", "subcategory", "supplier",
            "unit_price", "unit_cost",
            F.round(margin, 2).alias("unit_margin"),
            F.round(F.when(F.col("unit_price") > 0, margin / F.col("unit_price"))
                    .otherwise(F.lit(None)) * 100, 2).alias("margin_pct"),
            "is_active",
        )
    )


# ---------------------------------------------------------------------------
#  Facts
# ---------------------------------------------------------------------------

def fact_orders(orders: DataFrame, items: DataFrame) -> DataFrame:
    """One row per order. The grain is the order.

    Revenue counts ONLY the statuses in REVENUE_STATUSES. A cancelled order
    still exists and keeps its quantity - it simply brings in nothing. That is
    not the same as excluding it: it still counts as an order.
    """
    # NOTE on rounding, because the two fact tables differ by a few cents:
    # here the sum is rounded ONCE, while fact_order_items rounds EACH line
    # (a line amount is what appears on an invoice). Both are correct at their
    # own grain, and neither is a bug - but a consistency test across the two
    # must allow a small tolerance rather than demand exact equality.
    per_order = items.groupBy("order_id").agg(
        F.count("*").alias("line_count"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum(F.col("quantity") * F.col("unit_price")
                      * (1 - F.col("discount_pct"))), 2).alias("computed_amount"),
    )
    return (
        orders.join(per_order, "order_id", "left")
        .select(
            F.col("order_id").alias("order_key"),
            F.col("customer_id").alias("customer_key"),
            F.date_format("order_ts", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("order_ts"),
            "status", "channel", "currency",
            F.coalesce("line_count", F.lit(0)).alias("line_count"),
            F.coalesce("total_quantity", F.lit(0)).alias("total_quantity"),
            F.col("total_amount").alias("declared_amount"),
            F.coalesce("computed_amount", F.lit(0.0)).alias("computed_amount"),
            F.col("status").isin(list(REVENUE_STATUSES)).alias("is_revenue"),
        )
        .withColumn("revenue",
                    F.when(F.col("is_revenue"), F.col("computed_amount"))
                    .otherwise(F.lit(0.0)))
        # The gap between what the source declared and what the lines add up
        # to. It should be zero; where it is not, something upstream is wrong,
        # and a fact table that hides it is a fact table nobody can audit.
        .withColumn("amount_gap",
                    F.round(F.abs(F.coalesce("declared_amount", F.lit(0.0))
                                  - F.col("computed_amount")), 2))
    )


def fact_order_items(items: DataFrame, orders: DataFrame,
                     products: DataFrame) -> tuple[DataFrame, dict]:
    """One row per order line. The finest grain of the model.

    AN INNER JOIN IS A FILTER IN DISGUISE. Lines whose order or product is
    absent from Silver disappear here - silently, without an error. It is the
    number one cause of numbers that do not add up, so the loss is measured
    and returned rather than ignored.
    """
    before = items.count()
    joined = (
        items
        .join(orders.select("order_id", "customer_id", "order_ts", "status", "currency"),
              "order_id", "inner")
        .join(products.select("product_id", "unit_cost", "category"),
              "product_id", "inner")
    )
    after = joined.count()

    gross = F.col("quantity") * F.col("unit_price")
    net = gross * (1 - F.col("discount_pct"))
    fact = (
        joined.select(
            F.col("order_item_id").alias("order_item_key"),
            F.col("order_id").alias("order_key"),
            F.col("product_id").alias("product_key"),
            F.col("customer_id").alias("customer_key"),
            F.date_format("order_ts", "yyyyMMdd").cast("int").alias("date_key"),
            "category", "status", "currency", "quantity",
            "unit_price", "discount_pct",
            F.round(gross, 2).alias("gross_amount"),
            F.round(net, 2).alias("net_amount"),
            F.round(F.col("quantity") * F.col("unit_cost"), 2).alias("cost_amount"),
            F.col("status").isin(list(REVENUE_STATUSES)).alias("is_revenue"),
        )
        .withColumn("revenue",
                    F.when(F.col("is_revenue"), F.col("net_amount")).otherwise(F.lit(0.0)))
        .withColumn("margin", F.round(F.col("revenue") - F.when(
            F.col("is_revenue"), F.col("cost_amount")).otherwise(F.lit(0.0)), 2))
    )
    return fact, {"rows_in": before, "rows_out": after, "lost": before - after}


# ---------------------------------------------------------------------------

def write(df: DataFrame, name: str, spark, partition_by: str | None = None) -> int:
    path = config.table_path("gold", name)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.{name} USING DELTA LOCATION '{path}'")
    count = df.count()
    LOG.info(f"  {name:<18} {count:>8,} rows")
    return count


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 7 - Silver -> Gold star schema")
    p.add_argument("--calendar-start", default=None,
                   help="dim_date start (default: earliest order)")
    p.add_argument("--calendar-end", default=None)
    args = p.parse_args(argv)

    spark = get_spark("gold-star-schema")
    LOG.info(f"Silver -> Gold | {config.describe()}")

    read = lambda name: spark.read.format("delta").load(  # noqa: E731
        config.table_path("silver", name))
    customers, products = read("customers"), read("products")
    orders, items = read("orders"), read("order_items")

    bounds = orders.select(F.min("order_ts").alias("lo"), F.max("order_ts").alias("hi")).first()
    start = args.calendar_start or bounds["lo"].strftime("%Y-%m-%d")
    end = args.calendar_end or bounds["hi"].strftime("%Y-%m-%d")
    LOG.info(f"  calendar {start} -> {end}")

    write(dim_date(spark, start, end), "dim_date", spark)
    write(dim_customer(customers), "dim_customer", spark)
    write(dim_product(products), "dim_product", spark)
    write(fact_orders(orders, items), "fact_orders", spark)

    fact_items, loss = fact_order_items(items, orders, products)
    write(fact_items, "fact_order_items", spark)
    if loss["lost"]:
        LOG.info(f"  inner joins dropped {loss['lost']:,} of {loss['rows_in']:,} lines "
                 f"({loss['lost'] / loss['rows_in']:.2%}): their order or product "
                 f"is absent from Silver")

    LOG.info("  Gold layer ready.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
