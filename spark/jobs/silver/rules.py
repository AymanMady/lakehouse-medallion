"""
Quality rules per entity (phase 6).

Two policies, never to be confused:

    REPAIR  a cosmetic anomaly -> the row stays, the field is fixed
            broken email -> NULL, missing country -> "Unknown"

    REJECT  the row is unusable -> quarantine, with the reason
            quantity <= 0, unreadable date, missing primary key

The criterion is business, not technical: an order whose customer email is
broken still counts towards revenue. An order with no date does not.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.generator.schemas import (
    CHANNELS,
    COUNTRIES,
    CURRENCIES,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    SEGMENTS,
    SHIPMENT_STATUSES,
)
from spark.common.data_quality import Rule

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"


def _not_null(field: str) -> Rule:
    return Rule(f"{field}_missing", F.col(field).isNotNull())


def _in_set(field: str, values) -> Rule:
    # A NULL is handled by the missing_ rule, so an absent value must not be
    # rejected twice here.
    return Rule(f"{field}_unknown",
                F.col(field).isNull() | F.col(field).isin(list(values)))


# ---------------------------------------------------------------------------
#  REPAIR - applied before validation
# ---------------------------------------------------------------------------

def repair(entity: str, df: DataFrame) -> DataFrame:
    """Fix what can be fixed without inventing a fact."""
    if entity == "customers":
        return (df
                # An unparseable email becomes NULL rather than being kept as
                # garbage: downstream, "unknown" is workable, "jean.dupont.com"
                # is a landmine.
                .withColumn("email",
                            F.when(F.col("email").rlike(EMAIL_PATTERN),
                                   F.lower(F.trim("email"))))
                # An unknown country is REPAIRED, not rejected: the customer is
                # still a real customer and their orders still count. Only the
                # attribute is unusable, so it becomes "Unknown" rather than
                # letting "Atlantis" reach a dashboard.
                .withColumn("country",
                            F.when(F.initcap(F.trim("country")).isin(list(COUNTRIES)),
                                   F.initcap(F.trim("country")))
                            .otherwise(F.lit("Unknown")))
                .withColumn("city", F.initcap(F.trim("city")))
                .withColumn("segment", F.coalesce(F.lower(F.trim("segment")),
                                                  F.lit("consumer"))))
    if entity == "products":
        return (df
                .withColumn("category", F.initcap(F.trim("category")))
                .withColumn("supplier", F.trim("supplier"))
                .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True))))
    if entity == "orders":
        return (df
                .withColumn("status", F.lower(F.trim("status")))
                # A missing channel is not worth rejecting an order for.
                .withColumn("channel", F.coalesce(F.lower(F.trim("channel")),
                                                 F.lit("unknown")))
                .withColumn("currency", F.upper(F.trim("currency"))))
    if entity == "order_items":
        # No discount recorded means no discount.
        return df.withColumn("discount_pct", F.coalesce(F.col("discount_pct"), F.lit(0.0)))
    if entity == "payments":
        return (df
                .withColumn("method", F.coalesce(F.lower(F.trim("method")),
                                                 F.lit("unknown")))
                .withColumn("status", F.lower(F.trim("status"))))
    if entity == "shipments":
        return (df
                .withColumn("status", F.lower(F.trim("status")))
                .withColumn("carrier", F.coalesce(F.trim("carrier"), F.lit("Unknown"))))
    return df


# ---------------------------------------------------------------------------
#  REJECT - the rules
# ---------------------------------------------------------------------------

def rules_for(entity: str) -> list[Rule]:
    if entity == "customers":
        return [
            _not_null("customer_id"),
            _not_null("signup_date"),
            _in_set("segment", SEGMENTS),
            # A signup in the future is a clock or a mapping problem, and it
            # would poison every cohort analysis.
            Rule("signup_date_in_future", F.col("signup_date") <= F.current_date()),
        ]
    if entity == "products":
        return [
            _not_null("product_id"),
            _not_null("unit_price"),
            Rule("unit_price_not_positive", F.col("unit_price") > 0),
            Rule("unit_cost_negative", F.col("unit_cost") >= 0),
            # Selling below cost happens; a cost above the price on EVERY unit
            # is a data error, not a promotion.
            Rule("cost_above_price", F.col("unit_cost") <= F.col("unit_price")),
            Rule("category_empty", F.length(F.trim("category")) > 0),
        ]
    if entity == "orders":
        return [
            _not_null("order_id"),
            _not_null("customer_id"),
            # The cast failed if the source string was unreadable: this is
            # where an unparseable date is caught.
            _not_null("order_ts"),
            _in_set("status", ORDER_STATUSES),
            _in_set("channel", CHANNELS + ("unknown",)),
            _in_set("currency", CURRENCIES),
            Rule("total_amount_negative", F.col("total_amount") >= 0),
            Rule("order_ts_in_future", F.col("order_ts") <= F.current_timestamp()),
        ]
    if entity == "order_items":
        return [
            _not_null("order_item_id"),
            _not_null("order_id"),
            _not_null("product_id"),
            _not_null("quantity"),
            Rule("quantity_not_positive", F.col("quantity") > 0),
            Rule("unit_price_negative", F.col("unit_price") >= 0),
            Rule("discount_out_of_range",
                 (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 1)),
        ]
    if entity == "payments":
        return [
            _not_null("payment_id"),
            _not_null("order_id"),
            _not_null("amount"),
            Rule("amount_negative", F.col("amount") >= 0),
            _in_set("method", PAYMENT_METHODS + ("unknown",)),
            _in_set("status", PAYMENT_STATUSES),
        ]
    if entity == "shipments":
        return [
            _not_null("shipment_id"),
            _not_null("order_id"),
            _in_set("status", SHIPMENT_STATUSES),
            # Delivered before it shipped: physically impossible, and it would
            # produce negative delivery times in the Gold layer.
            Rule("delivered_before_shipped",
                 F.col("delivered_ts").isNull()
                 | (F.col("delivered_ts") >= F.col("shipped_ts"))),
        ]
    return []
