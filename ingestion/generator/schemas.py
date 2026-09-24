"""
THE DOMAIN CONTRACT (phase 2).

Defined once, here, and imported by everything else: the generator, the Kafka
producer, the Spark jobs and the tests. Duplicating it would guarantee that
the copies drift apart.

Types are written as plain strings ("long", "double", "timestamp") rather than
Spark types on purpose: this module must stay importable without PySpark, so
that the generator and the unit tests run in a lightweight container.
spark/common/schemas.py turns them into StructTypes.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
#  Entities
#
#  The declared type is the SILVER type — what the field must become once it
#  is clean. Bronze keeps everything as text (see spark/jobs/bronze): a raw
#  layer that already casts has already lost the ability to replay.
# ---------------------------------------------------------------------------
ENTITIES: dict[str, dict[str, str]] = {
    "customers": {
        "customer_id": "long",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "phone": "string",
        "country": "string",
        "city": "string",
        "signup_date": "date",
        "segment": "string",
    },
    "products": {
        "product_id": "long",
        "product_name": "string",
        "category": "string",
        "subcategory": "string",
        "unit_price": "double",
        "unit_cost": "double",
        "supplier": "string",
        "is_active": "boolean",
    },
    "orders": {
        "order_id": "long",
        "customer_id": "long",
        "order_ts": "timestamp",
        "status": "string",
        "channel": "string",
        "currency": "string",
        "total_amount": "double",
    },
    "order_items": {
        "order_item_id": "long",
        "order_id": "long",
        "product_id": "long",
        "quantity": "int",
        "unit_price": "double",
        "discount_pct": "double",
    },
    "payments": {
        "payment_id": "long",
        "order_id": "long",
        "method": "string",
        "amount": "double",
        "paid_ts": "timestamp",
        "status": "string",
    },
    "shipments": {
        "shipment_id": "long",
        "order_id": "long",
        "carrier": "string",
        "shipped_ts": "timestamp",
        "delivered_ts": "timestamp",
        "status": "string",
    },
}

# Business key of each entity: what makes a row unique, and therefore what
# deduplication is done on in Silver.
BUSINESS_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
}

# Foreign keys, checked in Silver. An orphan row is not deleted: it is
# quarantined with its reason.
FOREIGN_KEYS: dict[str, list[tuple[str, str, str]]] = {
    "orders": [("customer_id", "customers", "customer_id")],
    "order_items": [("order_id", "orders", "order_id"),
                    ("product_id", "products", "product_id")],
    "payments": [("order_id", "orders", "order_id")],
    "shipments": [("order_id", "orders", "order_id")],
}

# The Kafka topic per entity is the entity name itself (see
# scripts/create_topics.sh).
TOPICS: dict[str, str] = {name: name for name in ENTITIES}

# ---------------------------------------------------------------------------
#  Reference data — the closed sets a value has to belong to
# ---------------------------------------------------------------------------
CURRENCIES = ("MRU", "EUR", "USD", "MAD", "XOF")
ORDER_STATUSES = ("pending", "paid", "shipped", "delivered", "cancelled", "returned")
PAYMENT_METHODS = ("card", "mobile_money", "bank_transfer", "cash_on_delivery")
PAYMENT_STATUSES = ("authorised", "captured", "failed", "refunded")
SHIPMENT_STATUSES = ("label_created", "in_transit", "delivered", "lost", "returned")
CHANNELS = ("web", "mobile_app", "marketplace", "phone")
SEGMENTS = ("consumer", "business", "vip")
COUNTRIES = ("Mauritania", "Senegal", "Morocco", "Mali", "France", "Spain")
CATEGORIES = {
    "Electronics": ("Phones", "Laptops", "Audio"),
    "Home": ("Kitchen", "Furniture", "Lighting"),
    "Fashion": ("Shoes", "Bags", "Clothing"),
    "Grocery": ("Beverages", "Snacks", "Staples"),
}

# Only the statuses below count as revenue. A cancelled order still exists and
# keeps its quantity; it simply brings in nothing.
REVENUE_STATUSES = ("paid", "shipped", "delivered")

# ---------------------------------------------------------------------------
#  Event envelope
#
#  Kafka carries the envelope, not the bare record. The envelope is what makes
#  the message traceable and deduplicable:
#    event_id    technical id of THIS message  -> deduplication
#    occurred_at when the fact happened        -> event time
#    entity      which table it belongs to     -> routing
#  The business id (order_id...) lives in the payload, and must not be confused
#  with event_id: one order legitimately emits several events.
# ---------------------------------------------------------------------------
ENVELOPE_FIELDS = ("event_id", "event_type", "entity", "occurred_at", "source", "payload")


def event_type_for(entity: str) -> str:
    """`orders` -> `order_created`. One event family per entity here."""
    singular = entity[:-1] if entity.endswith("s") else entity
    return f"{singular}_created"
