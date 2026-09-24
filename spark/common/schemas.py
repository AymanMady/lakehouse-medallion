"""
Spark schemas, derived from the domain contract (phase 4).

The contract lives in ingestion/generator/schemas.py as plain strings, so it
stays importable without PySpark. Here we turn it into StructTypes.

Why an EXPLICIT schema rather than inference:
  1. in streaming, inference is impossible - the stream is infinite and empty
     at startup, so there is nothing to sample;
  2. inference is unstable - a micro-batch where every amount is a round
     number gives LongType, the next one DoubleType, and the schema changes
     mid-flight;
  3. an explicit schema is a CONTRACT - if the producer starts sending
     total_amount as text, the column becomes NULL and we SEE it, instead of
     silently propagating wrong data.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingestion.generator.schemas import ENTITIES

_SPARK_TYPES = {
    "string": StringType(),
    "int": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def silver_schema(entity: str) -> StructType:
    """The typed schema: what the entity must look like once it is clean."""
    return StructType([
        StructField(field, _SPARK_TYPES[kind], nullable=True)
        for field, kind in ENTITIES[entity].items()
    ])


def payload_schema(entity: str) -> StructType:
    """The schema used to parse the Bronze payload.

    EVERYTHING IS A STRING here, deliberately. Parsing with the target types
    would let from_json turn a malformed value into NULL, and we could no
    longer tell "absent" from "present but unparseable" - which are two
    different data quality problems with two different owners.
    Casting happens in Silver, where it is measured.
    """
    fields = [StructField(field, StringType(), nullable=True) for field in ENTITIES[entity]]
    # The trace left by the generator when it corrupts a record on purpose.
    # It lets the tests check that the rule which fired is the right one.
    fields.append(StructField("_corruption", StringType(), nullable=True))
    return StructType(fields)


# The envelope is NOT parsed with from_json in Bronze. `payload` is a nested
# OBJECT, and from_json cannot hand it back as text: declaring it StringType
# yields NULL. Bronze therefore uses get_json_object("$.payload"), which
# returns the raw JSON text of the object untouched - which is precisely what
# a raw layer owes you.
ENVELOPE_FIELDS = ("event_id", "event_type", "entity", "occurred_at", "source")
