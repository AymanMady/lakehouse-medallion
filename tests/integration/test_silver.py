"""
PHASE 10 - The Silver layer, end to end, on generated events.

The generator marks every record it corrupts with `_corruption`. That trace is
what makes this test possible: it runs the REAL Silver steps (parse, dedupe,
cast, repair, validate, foreign keys) and checks that each injected defect is
caught by the rule meant to catch it - not merely that "something" rejected
the row, which could hide a rule that never fires.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from ingestion.generator.generate import generate
from ingestion.generator.schemas import BUSINESS_KEYS
from spark.common.data_quality import ERROR_COL, split_valid_invalid
from spark.jobs.silver.bronze_to_silver import ORDER, annotate, deduplicate, parse_payload

# corruption injected by the generator -> rule that must fire
REJECTED_BY = {
    ("customers", "future_signup"): "signup_date_in_future",
    ("products", "negative_price"): "unit_price_not_positive",
    ("products", "cost_above_price"): "cost_above_price",
    ("products", "empty_category"): "category_empty",
    ("orders", "unknown_currency"): "currency_unknown",
    ("orders", "unknown_status"): "status_unknown",
    ("orders", "unparseable_date"): "order_ts_missing",
    ("orders", "negative_total"): "total_amount_negative",
    ("orders", "orphan_customer"): "fk_orphan_customer_id",
    ("order_items", "zero_quantity"): "quantity_not_positive",
    ("order_items", "negative_quantity"): "quantity_not_positive",
    ("order_items", "discount_above_one"): "discount_out_of_range",
    ("order_items", "orphan_product"): "fk_orphan_product_id",
    ("payments", "unknown_method"): "method_unknown",
    ("payments", "negative_amount"): "amount_negative",
    ("payments", "orphan_order"): "fk_orphan_order_id",
    ("shipments", "delivered_before_shipped"): "delivered_before_shipped",
    ("shipments", "unknown_status"): "status_unknown",
}

# Cosmetic defects: the row is REPAIRED and stays in Silver.
REPAIRED = {
    ("customers", "email_without_at"): ("email", None),
    ("customers", "unknown_country"): ("country", "Unknown"),
}


def _as_bronze(spark, events: list[dict]):
    """What Bronze holds: envelope columns, and the payload as raw JSON text."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return spark.createDataFrame(
        [(e["event_id"], e["occurred_at"], now, json.dumps(e["payload"]))
         for e in events],
        "event_id STRING, occurred_at STRING, ingestion_timestamp TIMESTAMP, payload STRING",
    )


@pytest.fixture(scope="module")
def silver_run(spark):
    """Every entity through the real Silver steps, in dependency order."""
    events = generate(customers=500, products=300, orders=2_000, seed=11,
                      null_rate=0.05, duplicate_rate=0.05, invalid_rate=0.10)
    silver, results = {}, {}
    for entity in ORDER:
        parsed = parse_payload(_as_bronze(spark, events[entity]), entity)
        annotated, duplicates = annotate(parsed, entity, silver)
        annotated = annotated.cache()
        valid, _ = split_valid_invalid(annotated)
        valid, _ = deduplicate(valid, BUSINESS_KEYS[entity], "occurred_at")
        silver[entity] = valid.cache()
        results[entity] = {
            "events": events[entity],
            "rows": annotated.collect(),
            "duplicates": duplicates,
        }
    return results


@pytest.mark.parametrize(("entity", "corruption"), sorted(REJECTED_BY))
def test_each_injected_defect_is_caught_by_its_own_rule(silver_run, entity, corruption):
    rule = REJECTED_BY[(entity, corruption)]
    hits = [r for r in silver_run[entity]["rows"] if r["_corruption"] == corruption]
    if not hits:
        pytest.skip(f"the generator drew no {corruption} for this seed")
    missed = [r["event_id"] for r in hits if rule not in r[ERROR_COL]]
    assert not missed, f"{len(missed)}/{len(hits)} {corruption} rows escaped {rule}"


@pytest.mark.parametrize(("entity", "corruption"), sorted(REPAIRED))
def test_cosmetic_defects_are_repaired_not_rejected(silver_run, entity, corruption):
    field, expected = REPAIRED[(entity, corruption)]
    hits = [r for r in silver_run[entity]["rows"] if r["_corruption"] == corruption]
    if not hits:
        pytest.skip(f"the generator drew no {corruption} for this seed")
    for row in hits:
        assert row[ERROR_COL] == [], f"{row['event_id']} rejected: {row[ERROR_COL]}"
        assert row[field] == expected


@pytest.mark.parametrize("entity", ["customers", "products"])
def test_an_uncorrupted_row_is_never_rejected(silver_run, entity):
    """Including the rows where the generator blanked a nullable field: a
    missing phone number is not a reason to lose a customer."""
    rejected = [(r["event_id"], r[ERROR_COL]) for r in silver_run[entity]["rows"]
                if r["_corruption"] is None and r[ERROR_COL]]
    assert not rejected, rejected[:5]


@pytest.mark.parametrize("entity", ["orders", "order_items", "payments", "shipments"])
def test_an_uncorrupted_fact_is_only_ever_rejected_as_an_orphan(silver_run, entity):
    """A clean order can still be quarantined - when the customer it points at
    was itself rejected. That cascade is legitimate; any other reason is not."""
    for row in silver_run[entity]["rows"]:
        if row["_corruption"] is None:
            unexpected = [e for e in row[ERROR_COL] if not e.startswith("fk_orphan_")]
            assert not unexpected, (row["event_id"], unexpected)


@pytest.mark.parametrize("entity", ORDER)
def test_technical_duplicates_are_collapsed(silver_run, entity):
    result = silver_run[entity]
    distinct = len({e["event_id"] for e in result["events"]})
    assert len(result["rows"]) == distinct
    assert result["duplicates"] == len(result["events"]) - distinct


def test_every_quarantined_row_says_why(silver_run):
    for entity in ORDER:
        for row in silver_run[entity]["rows"]:
            if row["_corruption"] in {c for (e, c) in REJECTED_BY if e == entity}:
                assert row[ERROR_COL], f"{entity} {row['event_id']} has no reason"


def test_types_are_cast_to_the_silver_contract(silver_run):
    row = next(r for r in silver_run["orders"]["rows"] if not r[ERROR_COL])
    assert isinstance(row["order_id"], int)
    assert isinstance(row["total_amount"], float)
    assert isinstance(row["order_ts"], datetime)
