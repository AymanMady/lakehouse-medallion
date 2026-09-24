"""
PHASE 10 - Tests of the domain contract.

Pure Python: neither Spark nor Kafka. They run in a second and therefore
actually get run, which is the only property that makes a test suite useful.

What is checked here is not the code but the CONSISTENCY of the contract
itself: every other module derives from it, so a contradiction in it is a
contradiction everywhere.
"""

from __future__ import annotations

import pytest

from ingestion.generator.schemas import (
    BUSINESS_KEYS,
    ENTITIES,
    ENVELOPE_FIELDS,
    FOREIGN_KEYS,
    REVENUE_STATUSES,
    ORDER_STATUSES,
    TOPICS,
    event_type_for,
)

KNOWN_TYPES = {"string", "int", "long", "double", "boolean", "date", "timestamp"}


def test_every_entity_has_a_business_key():
    assert set(BUSINESS_KEYS) == set(ENTITIES)


def test_the_business_key_is_a_field_of_its_entity():
    for entity, key in BUSINESS_KEYS.items():
        assert key in ENTITIES[entity], f"{entity}: {key} is not one of its fields"


def test_every_declared_type_is_one_spark_can_build():
    """A typo here would only surface at runtime, inside a Spark job."""
    for entity, fields in ENTITIES.items():
        for field, kind in fields.items():
            assert kind in KNOWN_TYPES, f"{entity}.{field}: unknown type {kind!r}"


@pytest.mark.parametrize("entity", sorted(FOREIGN_KEYS))
def test_every_foreign_key_points_at_something_that_exists(entity):
    for field, ref_entity, ref_key in FOREIGN_KEYS[entity]:
        assert field in ENTITIES[entity], f"{entity}.{field} does not exist"
        assert ref_entity in ENTITIES, f"{ref_entity} is not an entity"
        assert ref_key in ENTITIES[ref_entity], f"{ref_entity}.{ref_key} does not exist"


def test_a_foreign_key_references_the_business_key_of_its_target():
    """Referencing a non-key column would let one row match several."""
    for entity, links in FOREIGN_KEYS.items():
        for _field, ref_entity, ref_key in links:
            assert ref_key == BUSINESS_KEYS[ref_entity], (
                f"{entity} references {ref_entity}.{ref_key}, "
                f"which is not its business key")


def test_every_entity_has_a_topic():
    assert set(TOPICS) == set(ENTITIES)


def test_revenue_statuses_are_real_order_statuses():
    """A revenue status that no order can ever have would silently zero out
    the whole revenue figure."""
    assert set(REVENUE_STATUSES) <= set(ORDER_STATUSES)


def test_cancelled_is_not_a_revenue_status():
    assert "cancelled" not in REVENUE_STATUSES
    assert "returned" not in REVENUE_STATUSES


def test_event_type_is_derived_from_the_entity():
    assert event_type_for("orders") == "order_created"
    assert event_type_for("order_items") == "order_item_created"


def test_the_envelope_carries_the_payload_and_its_identity():
    for field in ("event_id", "entity", "occurred_at", "payload"):
        assert field in ENVELOPE_FIELDS
