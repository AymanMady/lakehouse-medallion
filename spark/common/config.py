"""
Where things live, and how to reach them (phases 4 to 7).

One module so that no job hardcodes a bucket name or an endpoint. Everything
comes from the environment, with the docker-compose defaults as fallbacks.
"""

from __future__ import annotations

import os

BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# The medallion layers, as physical locations in the lake.
BRONZE = f"s3a://{BUCKET}/bronze"
SILVER = f"s3a://{BUCKET}/silver"
GOLD = f"s3a://{BUCKET}/gold"
QUARANTINE = f"s3a://{BUCKET}/quarantine"
CHECKPOINTS = f"s3a://{BUCKET}/_checkpoints"

# The catalog databases registered by scripts/init_metastore.py. Registering a
# Delta path as a table is what makes `SELECT * FROM silver.orders` work in
# Spark SQL and in dbt, instead of forcing everyone to know the s3a:// path.
DATABASES = {"bronze": BRONZE, "silver": SILVER, "gold": GOLD, "quarantine": QUARANTINE}

LOCAL_RAW = os.getenv("LAKEHOUSE_RAW_DIR", "/opt/lakehouse/data/raw")


def table_path(layer: str, name: str) -> str:
    return f"{DATABASES[layer]}/{name}"


def checkpoint_path(job: str) -> str:
    return f"{CHECKPOINTS}/{job}"


def describe() -> str:
    return f"bucket={BUCKET} kafka={KAFKA_BOOTSTRAP}"
