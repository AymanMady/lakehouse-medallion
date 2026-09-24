"""PHASE 10 - The star schema (spark/jobs/gold/build_star_schema.py)."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from spark.jobs.gold.build_star_schema import (
    dim_date,
    dim_product,
    fact_order_items,
    fact_orders,
)

ORDERS = "order_id LONG, customer_id LONG, order_ts TIMESTAMP, status STRING, " \
         "channel STRING, currency STRING, total_amount DOUBLE"
ITEMS = "order_item_id LONG, order_id LONG, product_id LONG, quantity INT, " \
        "unit_price DOUBLE, discount_pct DOUBLE"
PRODUCTS = "product_id LONG, product_name STRING, category STRING, subcategory STRING, " \
           "supplier STRING, unit_price DOUBLE, unit_cost DOUBLE, is_active BOOLEAN"


@pytest.fixture
def orders(spark):
    ts = datetime(2026, 3, 14, 10, 30)
    return spark.createDataFrame([
        (1, 10, ts, "delivered", "web", "EUR", 190.0),
        (2, 10, ts, "cancelled", "web", "EUR", 50.0),
        (3, 11, ts, "paid", "web", "EUR", 999.0),       # declared total is wrong
    ], ORDERS)


@pytest.fixture
def items(spark):
    return spark.createDataFrame([
        (1, 1, 100, 2, 50.0, 0.0),
        (2, 1, 101, 1, 100.0, 0.1),
        (3, 2, 100, 1, 50.0, 0.0),
        (4, 3, 100, 1, 50.0, 0.0),
        (5, 3, 999, 1, 10.0, 0.0),                       # product absent from Silver
    ], ITEMS)


@pytest.fixture
def products(spark):
    return spark.createDataFrame([
        (100, "Mug", "Home", "Kitchen", "Acme", 50.0, 20.0, True),
        (101, "Lamp", "Home", "Lighting", "Acme", 100.0, 60.0, True),
    ], PRODUCTS)


def test_the_calendar_has_no_holes(spark):
    days = dim_date(spark, "2024-02-27", "2024-03-02").orderBy("date_day").collect()
    assert [d["date_day"] for d in days] == [
        date(2024, 2, 27), date(2024, 2, 28), date(2024, 2, 29),
        date(2024, 3, 1), date(2024, 3, 2)]
    assert days[2]["date_key"] == 20240229
    assert [d["is_weekend"] for d in days] == [False, False, False, False, True]


def test_revenue_is_zero_for_a_cancelled_order_which_still_counts(orders, items):
    facts = {r["order_key"]: r for r in fact_orders(orders, items).collect()}
    assert len(facts) == 3, "a cancelled order is still an order"
    assert facts[2]["revenue"] == 0.0
    assert facts[2]["total_quantity"] == 1
    assert facts[1]["revenue"] == pytest.approx(190.0)


def test_the_gap_between_declared_and_computed_amounts_is_exposed(orders, items):
    facts = {r["order_key"]: r for r in fact_orders(orders, items).collect()}
    assert facts[1]["amount_gap"] == 0.0
    assert facts[3]["amount_gap"] == pytest.approx(939.0)


def test_lines_dropped_by_the_inner_joins_are_measured(orders, items, products):
    fact, loss = fact_order_items(items, orders, products)
    assert loss == {"rows_in": 5, "rows_out": 4, "lost": 1}
    assert 5 not in [r["order_item_key"] for r in fact.collect()]


def test_line_margin_only_counts_revenue_lines(orders, items, products):
    fact, _ = fact_order_items(items, orders, products)
    lines = {r["order_item_key"]: r for r in fact.collect()}
    # 1 lamp at 100 with 10% off, cost 60 -> revenue 90, margin 30
    assert lines[2]["revenue"] == pytest.approx(90.0)
    assert lines[2]["margin"] == pytest.approx(30.0)
    # cancelled: no revenue, and no margin either
    assert lines[3]["revenue"] == 0.0
    assert lines[3]["margin"] == 0.0


def test_product_margin_is_derived_in_the_dimension(products):
    rows = {r["product_key"]: r for r in dim_product(products).collect()}
    assert rows[100]["unit_margin"] == 30.0
    assert rows[100]["margin_pct"] == 60.0
