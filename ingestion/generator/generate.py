"""
PHASE 2 - Synthetic e-commerce event generator.

Produces six related entities (customers, products, orders, order_items,
payments, shipments) that hold together: an order points at a customer that
exists, its items at products that exist.

And then it breaks a controlled share of them on purpose. A clean dataset
proves nothing: Silver would have nothing to clean, the quarantine nothing to
isolate, and the data quality rules would pass for the wrong reason.

    python3 -m ingestion.generator.generate --orders 50000
    python3 -m ingestion.generator.generate --preset small --out data/raw
"""

from __future__ import annotations

import argparse
import json
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from faker import Faker

from ingestion.generator.schemas import (
    CATEGORIES,
    CHANNELS,
    COUNTRIES,
    CURRENCIES,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    SEGMENTS,
    SHIPMENT_STATUSES,
    event_type_for,
)

PRESETS = {
    "tiny": {"customers": 200, "products": 60, "orders": 1_000},
    "small": {"customers": 1_000, "products": 200, "orders": 10_000},
    "medium": {"customers": 5_000, "products": 800, "orders": 50_000},
    "large": {"customers": 20_000, "products": 2_000, "orders": 500_000},
}

# The window over which orders are spread. Two years gives the Gold layer
# something to aggregate by month and by quarter.
HISTORY_DAYS = 730


# ---------------------------------------------------------------------------
#  Envelope
# ---------------------------------------------------------------------------

def _iso(moment: datetime) -> str:
    """ISO 8601 in UTC, milliseconds, explicit Z.

    Always UTC: a pipeline that mixes time zones produces wrong time windows,
    and the bug only shows up when the clocks change.
    """
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def wrap(entity: str, payload: dict[str, Any], occurred_at: datetime) -> dict[str, Any]:
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:16]}",
        "event_type": event_type_for(entity),
        "entity": entity,
        "occurred_at": _iso(occurred_at),
        "source": "generator",
        "payload": payload,
    }


# ---------------------------------------------------------------------------
#  Corruption
#
#  Three families, because they are handled differently downstream:
#    null      -> a missing value; Silver repairs or rejects depending on the field
#    duplicate -> the SAME event twice; deduplication's job
#    invalid   -> a value that breaks the contract; the quarantine's job
# ---------------------------------------------------------------------------

CORRUPTIONS = {
    "customers": [
        ("email_without_at", lambda r: r | {"email": str(r["email"]).replace("@", ".")}),
        ("unknown_country", lambda r: r | {"country": "Atlantis"}),
        ("future_signup", lambda r: r | {"signup_date": "2099-01-01"}),
    ],
    "products": [
        ("negative_price", lambda r: r | {"unit_price": -abs(r["unit_price"])}),
        ("cost_above_price", lambda r: r | {"unit_cost": r["unit_price"] * 3}),
        ("empty_category", lambda r: r | {"category": ""}),
    ],
    "orders": [
        ("unknown_currency", lambda r: r | {"currency": "XXX"}),
        ("unknown_status", lambda r: r | {"status": "en_cours"}),
        ("unparseable_date", lambda r: r | {"order_ts": "not-a-date"}),
        ("negative_total", lambda r: r | {"total_amount": -abs(r["total_amount"])}),
        ("orphan_customer", lambda r: r | {"customer_id": 9_999_999}),
    ],
    "order_items": [
        ("zero_quantity", lambda r: r | {"quantity": 0}),
        ("negative_quantity", lambda r: r | {"quantity": -2}),
        ("discount_above_one", lambda r: r | {"discount_pct": 1.5}),
        ("orphan_product", lambda r: r | {"product_id": 9_999_999}),
    ],
    "payments": [
        ("unknown_method", lambda r: r | {"method": "barter"}),
        ("negative_amount", lambda r: r | {"amount": -abs(r["amount"])}),
        ("orphan_order", lambda r: r | {"order_id": 9_999_999}),
    ],
    "shipments": [
        ("delivered_before_shipped", lambda r: r | {"delivered_ts": r["shipped_ts"],
                                                    "shipped_ts": r["delivered_ts"]}),
        ("unknown_status", lambda r: r | {"status": "perdu"}),
    ],
}

# Fields that may legitimately be missing in the real world. Corrupting a
# primary key would be a different problem, so it is not done here.
NULLABLE = {
    "customers": ["email", "phone", "country", "city", "segment"],
    "products": ["subcategory", "supplier"],
    "orders": ["channel", "currency"],
    "order_items": ["discount_pct"],
    "payments": ["method"],
    "shipments": ["carrier", "delivered_ts"],
}


def _corrupt(entity: str, record: dict, rng: random.Random) -> dict:
    name, mutate = rng.choice(CORRUPTIONS[entity])
    broken = mutate(dict(record))
    # A trace, so a test can check that the rule which fired is the right one.
    broken["_corruption"] = name
    return broken


def _blank(entity: str, record: dict, rng: random.Random) -> dict:
    field = rng.choice(NULLABLE[entity])
    return dict(record) | {field: None}


# ---------------------------------------------------------------------------
#  Entities
# ---------------------------------------------------------------------------

def build_customers(n: int, fake: Faker, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        first, last = fake.first_name(), fake.last_name()
        rows.append({
            "customer_id": i,
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}@{fake.free_email_domain()}".lower(),
            "phone": fake.msisdn()[:12],
            "country": rng.choice(COUNTRIES),
            "city": fake.city(),
            "signup_date": (_anchor().date()
                            - timedelta(days=rng.randint(0, HISTORY_DAYS + 365))
                            ).isoformat(),
            "segment": rng.choices(SEGMENTS, weights=[80, 15, 5])[0],
        })
    return rows


def build_products(n: int, fake: Faker, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        category = rng.choice(list(CATEGORIES))
        price = round(rng.uniform(5, 2500), 2)
        rows.append({
            "product_id": i,
            "product_name": fake.catch_phrase()[:60],
            "category": category,
            "subcategory": rng.choice(CATEGORIES[category]),
            "unit_price": price,
            # A margin between 15% and 55%: the Gold layer can compute one.
            "unit_cost": round(price * rng.uniform(0.45, 0.85), 2),
            "supplier": fake.company()[:40],
            "is_active": rng.random() > 0.08,
        })
    return rows


def _anchor() -> datetime:
    """The reference instant the order history hangs from.

    Truncated to midnight UTC on purpose. Using datetime.now() directly made
    the SAME SEED produce a different dataset on every run - the timestamps
    drifted by milliseconds - which silently breaks the reproducibility the
    whole project relies on to compare two pipeline runs. A unit test caught
    it. Anchoring to midnight keeps the data recent and makes it reproducible
    for the whole day.
    """
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def build_orders(n: int, n_customers: int, rng: random.Random) -> list[dict]:
    now = _anchor()
    rows = []
    for i in range(1, n + 1):
        moment = now - timedelta(
            days=rng.randint(0, HISTORY_DAYS),
            seconds=rng.randint(0, 86_399),
        )
        rows.append({
            "order_id": i,
            # 80/20: a fifth of the customers place four orders out of five.
            # A uniform draw would make the RFM analysis degenerate, and a raw
            # Pareto draw sends most orders to customer #1, which is worse.
            "customer_id": (rng.randint(1, max(1, n_customers // 5))
                            if rng.random() < 0.8
                            else rng.randint(1, n_customers)),
            "order_ts": _iso(moment),
            "status": rng.choices(ORDER_STATUSES, weights=[8, 20, 18, 42, 8, 4])[0],
            "channel": rng.choices(CHANNELS, weights=[45, 40, 10, 5])[0],
            "currency": rng.choices(CURRENCIES, weights=[55, 20, 12, 8, 5])[0],
            "total_amount": 0.0,   # filled in once the items are known
            "_ts": moment,
        })
    return rows


def build_order_items(orders: list[dict], products: list[dict],
                      rng: random.Random) -> list[dict]:
    """Items, and the order total derived from them.

    The total is COMPUTED, not drawn at random: it has to match the sum of the
    lines, otherwise the Gold layer's consistency test would be checking
    nothing at all.
    """
    rows = []
    item_id = 1
    by_id = {p["product_id"]: p for p in products}
    for order in orders:
        total = 0.0
        for _ in range(rng.choices([1, 2, 3, 4, 5], weights=[45, 28, 15, 8, 4])[0]):
            product = by_id[rng.randint(1, len(products))]
            quantity = rng.choices([1, 2, 3, 5, 10], weights=[60, 22, 10, 5, 3])[0]
            discount = round(rng.choices([0.0, 0.05, 0.10, 0.20],
                                         weights=[70, 15, 10, 5])[0], 2)
            price = product["unit_price"]
            rows.append({
                "order_item_id": item_id,
                "order_id": order["order_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": price,
                "discount_pct": discount,
            })
            total += quantity * price * (1 - discount)
            item_id += 1
        order["total_amount"] = round(total, 2)
    return rows


def build_payments(orders: list[dict], rng: random.Random) -> list[dict]:
    rows = []
    payment_id = 1
    for order in orders:
        if order["status"] == "pending":
            continue   # not paid yet: no payment event
        moment = order["_ts"] + timedelta(minutes=rng.randint(1, 240))
        rows.append({
            "payment_id": payment_id,
            "order_id": order["order_id"],
            "method": rng.choices(PAYMENT_METHODS, weights=[50, 30, 12, 8])[0],
            "amount": order["total_amount"],
            "paid_ts": _iso(moment),
            "status": ("refunded" if order["status"] == "returned"
                       else rng.choices(PAYMENT_STATUSES[:3], weights=[10, 88, 2])[0]),
        })
        payment_id += 1
    return rows


def build_shipments(orders: list[dict], rng: random.Random) -> list[dict]:
    rows = []
    shipment_id = 1
    for order in orders:
        if order["status"] in ("pending", "paid", "cancelled"):
            continue   # nothing shipped yet
        shipped = order["_ts"] + timedelta(hours=rng.randint(4, 96))
        delivered = shipped + timedelta(hours=rng.randint(12, 240))
        status = ("delivered" if order["status"] == "delivered"
                  else rng.choice(SHIPMENT_STATUSES))
        rows.append({
            "shipment_id": shipment_id,
            "order_id": order["order_id"],
            "carrier": rng.choice(["DHL", "Aramex", "La Poste", "Local Courier"]),
            "shipped_ts": _iso(shipped),
            "delivered_ts": _iso(delivered) if status == "delivered" else None,
            "status": status,
        })
        shipment_id += 1
    return rows


# ---------------------------------------------------------------------------
#  Assembly
# ---------------------------------------------------------------------------

def generate(*, customers: int, products: int, orders: int, seed: int,
             null_rate: float, duplicate_rate: float,
             invalid_rate: float) -> dict[str, list[dict]]:
    """Return {entity: [event, ...]}, corruptions included."""
    rng = random.Random(seed)
    fake = Faker("fr_FR")
    Faker.seed(seed)

    customer_rows = build_customers(customers, fake, rng)
    product_rows = build_products(products, fake, rng)
    order_rows = build_orders(orders, customers, rng)
    item_rows = build_order_items(order_rows, product_rows, rng)
    payment_rows = build_payments(order_rows, rng)
    shipment_rows = build_shipments(order_rows, rng)

    for order in order_rows:
        order.pop("_ts")

    raw = {
        "customers": customer_rows,
        "products": product_rows,
        "orders": order_rows,
        "order_items": item_rows,
        "payments": payment_rows,
        "shipments": shipment_rows,
    }

    # occurred_at is when the event is EMITTED, which really is now. It is the
    # one field that legitimately differs between two runs of the same seed.
    now = datetime.now(timezone.utc)
    events: dict[str, list[dict]] = {}
    for entity, rows in raw.items():
        out: list[dict] = []
        for row in rows:
            record = row
            if rng.random() < invalid_rate:
                record = _corrupt(entity, record, rng)
            elif rng.random() < null_rate:
                record = _blank(entity, record, rng)

            event = wrap(entity, record, now)
            out.append(event)
            # A duplicate is the SAME event, event_id included. That is what
            # makes it deduplicable - and what distinguishes it from a genuine
            # second business event.
            if rng.random() < duplicate_rate:
                out.append(dict(event))
        events[entity] = out
    return events


def write_jsonl(events: dict[str, list[dict]], out_dir: Path) -> dict[str, int]:
    """One JSON Lines file per entity - the format Spark reads natively."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for entity, rows in events.items():
        path = out_dir / f"{entity}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        written[entity] = len(rows)
    return written


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 2 - generate the e-commerce dataset")
    p.add_argument("--preset", choices=sorted(PRESETS), default=None)
    p.add_argument("--customers", type=int, default=_env_int("GEN_NUM_CUSTOMERS", 5_000))
    p.add_argument("--products", type=int, default=_env_int("GEN_NUM_PRODUCTS", 800))
    p.add_argument("--orders", type=int, default=_env_int("GEN_NUM_ORDERS", 50_000))
    p.add_argument("--seed", type=int, default=_env_int("GEN_SEED", 42))
    p.add_argument("--null-rate", type=float, default=_env_float("GEN_NULL_RATE", 0.03))
    p.add_argument("--duplicate-rate", type=float,
                   default=_env_float("GEN_DUPLICATE_RATE", 0.02))
    p.add_argument("--invalid-rate", type=float,
                   default=_env_float("GEN_INVALID_RATE", 0.02))
    p.add_argument("--out", type=Path, default=Path("/opt/lakehouse/data/raw"))
    args = p.parse_args(argv)

    if args.preset:
        preset = PRESETS[args.preset]
        args.customers, args.products, args.orders = (
            preset["customers"], preset["products"], preset["orders"])

    print(f"  generating: {args.customers:,} customers, {args.products:,} products, "
          f"{args.orders:,} orders (seed {args.seed})")
    print(f"  corruption: null {args.null_rate:.0%} | duplicates "
          f"{args.duplicate_rate:.0%} | invalid {args.invalid_rate:.0%}")

    started = datetime.now(timezone.utc)
    events = generate(
        customers=args.customers, products=args.products, orders=args.orders,
        seed=args.seed, null_rate=args.null_rate,
        duplicate_rate=args.duplicate_rate, invalid_rate=args.invalid_rate,
    )
    written = write_jsonl(events, args.out)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()

    total = sum(written.values())
    print(f"\n  {'entity':<14} {'events':>10}")
    print("  " + "-" * 26)
    for entity, count in written.items():
        print(f"  {entity:<14} {count:>10,}")
    print("  " + "-" * 26)
    print(f"  {'TOTAL':<14} {total:>10,}   in {elapsed:.1f}s "
          f"({total / max(elapsed, 0.001):,.0f} ev/s)")
    print(f"\n  written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
