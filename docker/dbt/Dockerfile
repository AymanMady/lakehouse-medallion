# =============================================================================
#  dbt-core + dbt-spark  — SQL transformation layer (Silver -> Gold)
# -----------------------------------------------------------------------------
#  dbt executes NOTHING itself: it compiles Jinja+SQL and sends it over a
#  Thrift (HiveServer2) connection to the Spark Thrift Server, which runs the
#  query on the Spark cluster against our Delta tables.
#
#  The [PyHive] extra pulls `PyHive[hive_pure_sasl]`, a pure-Python SASL
#  implementation -> no libsasl2-dev / gcc needed in the image.
# =============================================================================
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/opt/dbt \
    DBT_PROJECT_DIR=/opt/dbt

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends git curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*

COPY docker/dbt/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm -f /tmp/requirements.txt

RUN useradd --create-home --shell /bin/bash --uid 1001 dbtuser \
 && mkdir -p /opt/dbt && chown -R dbtuser:dbtuser /opt/dbt

WORKDIR /opt/dbt
USER dbtuser
CMD ["bash"]
