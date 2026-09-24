"""
SparkSession factory (phases 4 to 7).

Delta, S3A and the Hive Metastore are already configured in
docker/spark/conf/spark-defaults.conf.template, rendered at container start.
This module only adds what belongs to a job, and gives every job the same
logging.
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import SparkSession

_CONFIGURED = False


def get_logger(stage: str) -> logging.Logger:
    """Logger prefixed by the pipeline stage: [BRONZE], [SILVER], [GOLD]...

    The point is that the OUTPUT of a job tells the story of what happened.
    A job that prints nothing is a job nobody can operate.
    """
    global _CONFIGURED
    name = f"[{stage.upper()}]"
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(message)s",
                                               datefmt="%H:%M:%S"))
        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        _CONFIGURED = True
    return logging.getLogger(name)


def get_spark(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name).enableHiveSupport()

    # local[*] when a job is run standalone (tests, one-off), the cluster
    # otherwise. spark-defaults already points at the master, so we only
    # override when explicitly asked.
    master = os.getenv("SPARK_JOB_MASTER")
    if master:
        builder = builder.master(master)

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
