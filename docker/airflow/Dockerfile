# =============================================================================
#  Airflow 2.10.5 on Python 3.12 — orchestration only.
# -----------------------------------------------------------------------------
#  Design choice: Airflow NEVER runs Spark or dbt in-process. It launches them
#  as sibling containers through the DockerOperator (Docker socket mounted).
#  Benefits:
#    * the Airflow image stays small (no JVM, no Spark, no dbt)
#    * each job runs with exactly the runtime it was built for
#    * this is the real-world pattern (KubernetesPodOperator in production)
# =============================================================================
FROM apache/airflow:2.10.5-python3.12

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
