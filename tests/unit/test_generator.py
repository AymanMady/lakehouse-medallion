"""PHASE 10 - Tests of the generator. No Spark, no Kafka."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from ingestion.generator.generate import generate, wrap, write_jsonl
from ingestion.generator.schemas import BUSINESS_KEYS, ENTITIES


@pytest.fixture(scope="module")
def dataset():
    """A small but complete dataset, with every kind of corruption."""
    return generate(customers=200, products=50, orders=800, seed=7,
                    null_rate=0.05, duplicate_rate=0.05, invalid_rate=0.05)


@pytest.fixture(scope="module")
def clean(dataset):
    """Only the records the generator did not corrupt, deduplicated.

    Dropping the duplicates matters: a duplicated line event would be counted
    twice and the order total would look doubled. That is exactly the
    phenomenon the Silver layer deduplicates away, so the test has to do the
    same before it can compare anything.
    """
    out = {}
    for entity, events in dataset.items():
        seen: set[str] = set()
        rows = []
        for event in events:
            if event["event_id"] in seen:
                continue
            seen.add(event["event_id"])
            payload = event["payload"]
            if "_corruption" in payload or any(v is None for v in payload.values()):
                continue
            rows.append(payload)
        out[entity] = rows
    return out


def test_the_same_seed_gives_the_same_dataset():
    """Without reproducibility, two pipeline runs cannot be compared: you
    never know whether a difference came from your code or from the data."""
    a = generate(customers=50, products=10, orders=100, seed=1,
                 null_rate=0.02, duplicate_rate=0.02, invalid_rate=0.02)
    b = generate(customers=50, products=10, orders=100, seed=1,
                 null_rate=0.02, duplicate_rate=0.02, invalid_rate=0.02)
    # event_id and occurred_at are random by design, so compare the payloads.
    for entity in a:
        assert [e["payload"] for e in a[entity]] == [e["payload"] for e in b[entity]]


def test_different_seeds_give_different_datasets():
    a = generate(customers=50, products=10, orders=100, seed=1,
                 null_rate=0, duplicate_rate=0, invalid_rate=0)
    b = generate(customers=50, products=10, orders=100, seed=2,
                 null_rate=0, duplicate_rate=0, invalid_rate=0)
    assert [e["payload"] for e in a["orders"]] != [e["payload"] for e in b["orders"]]


def test_every_entity_is_generated(dataset):
    assert set(dataset) == set(ENTITIES)
    for entity, events in dataset.items():
        assert events, f"{entity} is empty"


def test_the_payload_carries_exactly_the_contracted_fields(dataset):
    for entity, events in dataset.items():
        expected = set(ENTITIES[entity])
        for event in events[:50]:
            actual = set(event["payload"]) - {"_corruption"}
            assert actual == expected, f"{entity}: {actual ^ expected}"


def test_the_envelope_is_well_formed(dataset):
    event = dataset["orders"][0]
    assert event["event_id"].startswith("evt_")
    assert event["entity"] == "orders"
    assert event["occurred_at"].endswith("Z"), "timestamps must say they are UTC"
    assert isinstance(event["payload"], dict)


def test_duplicates_are_exact_copies(dataset):
    """A technical duplicate carries the SAME event_id. That is what makes it
    deduplicable - and what distinguishes it from a genuine second business
    event about the same order."""
    events = dataset["orders"]
    counts = Counter(e["event_id"] for e in events)
    duplicated = [eid for eid, n in counts.items() if n > 1]
    assert duplicated, "no duplicate was injected"

    by_id = {}
    for event in events:
        by_id.setdefault(event["event_id"], []).append(event)
    for eid in duplicated[:20]:
        copies = by_id[eid]
        assert all(c["payload"] == copies[0]["payload"] for c in copies)


def test_corruptions_are_injected_and_traced(dataset):
    """Every corruption leaves a _corruption marker, which is what lets a test
    check that the rule that fired is the RIGHT one.

    Checked on the large entities only: corruption is drawn per record, so a
    50-row entity can legitimately receive none, and asserting otherwise would
    make the suite flaky.
    """
    for entity in ("orders", "order_items", "payments"):
        corruptions = Counter(e["payload"].get("_corruption")
                              for e in dataset[entity]
                              if e["payload"].get("_corruption"))
        assert corruptions, f"{entity}: no corruption injected"
        assert len(corruptions) > 1, f"{entity}: only one kind of corruption"


def test_nulls_are_injected_only_where_a_null_is_plausible(dataset):
    """A missing primary key is a different problem from a missing phone
    number, and the generator must not conflate them."""
    for entity, events in dataset.items():
        key = BUSINESS_KEYS[entity]
        missing_keys = [e for e in events
                        if e["payload"].get(key) is None
                        and "_corruption" not in e["payload"]]
        assert not missing_keys, f"{entity}: business key nulled without a corruption"


def test_the_order_total_matches_the_sum_of_its_lines():
    """The total is COMPUTED from the lines, never drawn at random.

    If it were random, the Gold layer's consistency check would be comparing
    two unrelated numbers and would never catch anything.

    Checked on an UNCORRUPTED dataset: filtering the corrupted lines out of a
    corrupted one leaves orders with only part of their lines, whose total then
    legitimately differs from the partial sum.
    """
    events = generate(customers=200, products=50, orders=800, seed=7,
                      null_rate=0, duplicate_rate=0, invalid_rate=0)
    totals = {e["payload"]["order_id"]: e["payload"]["total_amount"]
              for e in events["orders"]}
    summed: dict[int, float] = {}
    for item in (e["payload"] for e in events["order_items"]):
        summed.setdefault(item["order_id"], 0.0)
        summed[item["order_id"]] += (
            item["quantity"] * item["unit_price"] * (1 - item["discount_pct"]))

    compared = 0
    for order_id, total in totals.items():
        if order_id not in summed:
            continue
        compared += 1
        assert abs(total - round(summed[order_id], 2)) < 0.02, order_id
    assert compared > 100, "not enough clean orders to make the check meaningful"


def test_clean_orders_reference_a_customer_that_exists(clean):
    known = {c["customer_id"] for c in clean["customers"]}
    orders = clean["orders"]
    orphans = [o for o in orders if o["customer_id"] not in known]
    # Some orphans are legitimate: the referenced customer may itself have
    # been corrupted. What matters is that they stay a small minority.
    assert len(orphans) < 0.2 * len(orders)


def test_a_payment_exists_only_for_an_order_that_was_paid(dataset):
    """Business rule: a pending order has not been paid, so it emits no
    payment event."""
    statuses = {o["payload"]["order_id"]: o["payload"]["status"]
                for o in dataset["orders"]}
    for payment in dataset["payments"][:500]:
        order_id = payment["payload"]["order_id"]
        if order_id in statuses and "_corruption" not in payment["payload"]:
            assert statuses[order_id] != "pending"


def test_writing_produces_one_readable_jsonl_per_entity(dataset, tmp_path):
    written = write_jsonl(dataset, tmp_path)
    assert set(written) == set(ENTITIES)
    for entity, count in written.items():
        lines = (tmp_path / f"{entity}.jsonl").read_text().splitlines()
        assert len(lines) == count
        assert json.loads(lines[0])["entity"] == entity


def test_wrap_gives_each_event_a_distinct_identity():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    a = wrap("orders", {"order_id": 1}, now)
    b = wrap("orders", {"order_id": 1}, now)
    assert a["event_id"] != b["event_id"]
