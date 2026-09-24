"""
PHASE 9 - The pipeline DAG.

Airflow NEVER runs Spark or dbt in its own process. Every step is launched as
a SIBLING container through the DockerOperator, which is why the Airflow image
carries no JVM, no Spark and no dbt.

What that buys:
  * a small, fast Airflow image;
  * each job running in exactly the runtime it was built and tested for;
  * the same shape as production, where this becomes KubernetesPodOperator.

What it costs, and it is worth knowing: the DockerOperator talks to the HOST's
docker daemon through the mounted socket. Every path it mounts is therefore
resolved BY THE HOST, not inside the Airflow container - hence
LAKEHOUSE_HOST_DIR. Mounting /opt/airflow/... here would silently mount the
wrong thing.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_DIR = os.environ["LAKEHOUSE_HOST_DIR"]
NETWORK = os.getenv("COMPOSE_NETWORK", "lakehouse-medallion")

SPARK_IMAGE = "lakehouse-medallion/spark:3.5.9"
PYTHON_IMAGE = "lakehouse-medallion/python-tools:3.12"
DBT_IMAGE = "lakehouse-medallion/dbt:1.9"

# Passed through to every launched container. Reading them from the
# scheduler's own environment keeps the credentials in one place (.env) rather
# than duplicated into the DAG.
SHARED_ENV = {
    key: os.environ[key]
    for key in (
        "S3_ENDPOINT", "LAKEHOUSE_BUCKET", "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_DEFAULT_REGION",
        "KAFKA_BOOTSTRAP_SERVERS", "POSTGRES_HOST", "POSTGRES_PORT",
        "POSTGRES_USER", "POSTGRES_PASSWORD", "METASTORE_DB", "TZ",
    )
    if key in os.environ
}

# `environment` is a templated field of the DockerOperator: every Spark job of
# one DAG run records its monitoring rows under the same run id.
SPARK_ENV = {**SHARED_ENV, "PYTHONPATH": "/opt/lakehouse", "SPARK_MODE": "client",
             "PIPELINE_RUN_ID": "{{ run_id }}"}


def _mount(source: str, target: str, read_only: bool = False) -> Mount:
    return Mount(source=f"{HOST_DIR}/{source}", target=target,
                 type="bind", read_only=read_only)


def docker_task(task_id: str, image: str, command, mounts, environment,
                **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        mounts=mounts,
        environment=environment,
        network_mode=NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        # The DockerOperator mounts a temp directory by default, which fails
        # when the daemon is on the host and the path is not: the host cannot
        # see the Airflow container's /tmp.
        mount_tmp_dir=False,
        # Without this, the task log only shows the exit code and you have to
        # go hunting in `docker logs` for what actually went wrong.
        retrieve_output=False,
        tty=False,
        **kwargs,
    )


def spark_task(task_id: str, script: str, *args) -> DockerOperator:
    return docker_task(
        task_id=task_id,
        image=SPARK_IMAGE,
        # local[2] rather than the standalone cluster: the job is launched as
        # a one-off container, so its driver would have to be reachable by the
        # executors. Running it self-contained is simpler and, at this volume,
        # no slower.
        command=["/opt/spark/bin/spark-submit", "--master", "local[2]",
                 f"/opt/lakehouse/{script}", *args],
        mounts=[
            _mount("spark", "/opt/lakehouse/spark", read_only=True),
            _mount("ingestion", "/opt/lakehouse/ingestion", read_only=True),
            _mount("scripts", "/opt/lakehouse/scripts", read_only=True),
            _mount("data", "/opt/lakehouse/data"),
        ],
        environment=SPARK_ENV,
    )


def dbt_task(task_id: str, command: str) -> DockerOperator:
    return docker_task(
        task_id=task_id,
        image=DBT_IMAGE,
        command=["dbt", command, "--profiles-dir", "/opt/dbt",
                 "--project-dir", "/opt/dbt"],
        mounts=[_mount("dbt", "/opt/dbt")],
        environment={
            **SHARED_ENV,
            "DBT_PROFILES_DIR": "/opt/dbt",
            "DBT_SPARK_HOST": "spark-thrift",
            "DBT_SPARK_PORT": "10000",
            "DBT_SPARK_SCHEMA": "gold",
        },
        # dbt writes target/ and logs/ into the bind-mounted project, so it
        # must run as the host user rather than the image's own uid.
        user=f"{os.getenv('HOST_UID', '1000')}:{os.getenv('HOST_GID', '1000')}",
    )


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # A task that hangs forever is worse than a task that fails: it holds a
    # slot and nobody gets alerted.
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Generate -> Kafka -> Bronze -> Silver -> Gold -> dbt marts -> health",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "medallion", "delta"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    generate = docker_task(
        task_id="generate_events",
        image=PYTHON_IMAGE,
        command=["python3", "-m", "ingestion.generator.generate",
                 "--preset", "small", "--out", "/opt/lakehouse/data/raw"],
        mounts=[_mount("ingestion", "/opt/lakehouse/ingestion", read_only=True),
                _mount("data", "/opt/lakehouse/data")],
        environment={**SHARED_ENV, "PYTHONPATH": "/opt/lakehouse"},
        user=f"{os.getenv('HOST_UID', '1000')}:{os.getenv('HOST_GID', '1000')}",
    )

    publish = docker_task(
        task_id="publish_to_kafka",
        image=PYTHON_IMAGE,
        command=["python3", "-m", "ingestion.producers.kafka_producer",
                 "--source", "/opt/lakehouse/data/raw"],
        mounts=[_mount("ingestion", "/opt/lakehouse/ingestion", read_only=True),
                _mount("data", "/opt/lakehouse/data", read_only=True)],
        environment={**SHARED_ENV, "PYTHONPATH": "/opt/lakehouse"},
    )

    bronze = spark_task("bronze_ingest",
                        "spark/jobs/bronze/ingest_kafka.py", "--mode", "batch")

    # --min-valid-ratio is the guard rail: below it the task FAILS, on purpose.
    # A pipeline that always succeeds is one that checks nothing.
    silver = spark_task("silver_transform",
                        "spark/jobs/silver/bronze_to_silver.py",
                        "--min-valid-ratio", "0.80")

    gold = spark_task("gold_star_schema", "spark/jobs/gold/build_star_schema.py")

    dbt_run = dbt_task("dbt_run", "run")
    dbt_test = dbt_task("dbt_test", "test")

    # Phase 11: compares this run with the previous one (quality drift, Gold
    # volume, Bronze freshness) and fails the task when an alert fires.
    monitor = spark_task("pipeline_health", "spark/jobs/monitoring/pipeline_report.py",
                         "--fail-on-alert")

    end = EmptyOperator(task_id="end")

    (start >> generate >> publish >> bronze >> silver >> gold
     >> dbt_run >> dbt_test >> monitor >> end)
