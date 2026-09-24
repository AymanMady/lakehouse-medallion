# =============================================================================
#  Spark 3.5.9 + Delta Lake 3.3.2 + S3A (MinIO) + Kafka  — on Python 3.12
# -----------------------------------------------------------------------------
#  Why build from python:3.12 instead of the official apache/spark image?
#    -> apache/spark ships Ubuntu 22.04 => Python 3.10. We want 3.12 everywhere.
#    -> We download the official Spark tarball, so we get the FULL distribution
#       (bin/ + sbin/ + jars/), unlike `pip install pyspark`.
#
#  All JARs are baked into the image (no `--packages` at runtime):
#    * reproducible builds       * works offline
#    * no Ivy resolution delay on every spark-submit
# =============================================================================
FROM python:3.12-slim-bookworm

ARG SPARK_VERSION=3.5.9
ARG HADOOP_PROFILE=hadoop3
ARG SCALA_VERSION=2.12

# --- Version lock -------------------------------------------------------------
#  Verified against the Spark 3.5.9 parent POM:
#     hadoop.version = 3.3.4 | kafka.version = 3.4.1 | commons-pool2 = 2.11.1
#  Delta 3.3.x is the release line built for Spark 3.5.x.
#  Changing ONE of these without the others causes NoSuchMethodError at runtime.
ARG DELTA_VERSION=3.3.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG POSTGRES_JDBC_VERSION=42.7.4

ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# ---------------------------- System packages --------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl ca-certificates gettext-base procps tini bash; \
    rm -rf /var/lib/apt/lists/*

# ------------------------------ Apache Spark ---------------------------------
#  dlcdn is the fast Apache CDN but only keeps CURRENT releases; archive.apache
#  .org has everything but is heavily throttled. Try the CDN, fall back.
RUN set -eux; \
    CDN="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz"; \
    ARCHIVE="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz"; \
    curl -fsSL --retry 3 --retry-delay 5 -o /tmp/spark.tgz "${CDN}" \
      || curl -fsSL --retry 3 --retry-delay 5 -o /tmp/spark.tgz "${ARCHIVE}"; \
    mkdir -p "${SPARK_HOME}"; \
    tar -xzf /tmp/spark.tgz -C "${SPARK_HOME}" --strip-components=1; \
    rm -f /tmp/spark.tgz; \
    rm -rf "${SPARK_HOME}/examples" "${SPARK_HOME}/data"

# Expose Spark's bundled PySpark to the system interpreter without hardcoding
# the py4j version (it changes between Spark releases).
RUN set -eux; \
    SITE="$(python3 -c 'import site; print(site.getsitepackages()[0])')"; \
    { echo "${SPARK_HOME}/python"; \
      ls -d "${SPARK_HOME}"/python/lib/py4j-*-src.zip; } > "${SITE}/spark.pth"; \
    python3 -c "import pyspark, py4j; print('pyspark', pyspark.__version__, '| py4j ok')"

# --------------------------- Lakehouse JAR bundle ----------------------------
#  delta-spark / delta-storage : Delta Lake table format
#  hadoop-aws + aws-sdk-bundle : the s3a:// filesystem (MinIO, S3)
#  spark-sql-kafka + token-provider + kafka-clients + commons-pool2 :
#                                Structured Streaming source/sink for Kafka
#  postgresql                  : JDBC -> Hive Metastore + analytics serving
RUN set -eux; \
    cd "${SPARK_HOME}/jars"; \
    M=https://repo1.maven.org/maven2; \
    for URL in \
      "${M}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
      "${M}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "${M}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "${M}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "${M}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${M}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${M}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "${M}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "${M}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    ; do curl -fsSL --retry 3 --retry-delay 3 -O "${URL}"; done; \
    echo "--- sanity check: hadoop version bundled in Spark ---"; \
    ls hadoop-client-api-*.jar hadoop-aws-*.jar

# ----------------------------- Python packages -------------------------------
#  --no-deps on delta-spark: its only dependency is pyspark, already provided
#  by the tarball above.
COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN set -eux; \
    pip install --no-cache-dir --upgrade pip; \
    pip install --no-cache-dir -r /tmp/requirements.txt; \
    pip install --no-cache-dir --no-deps "delta-spark==${DELTA_VERSION}"; \
    rm -f /tmp/requirements.txt; \
    python3 -c "import delta; print('delta python package ok')"

# ------------------------------ Configuration --------------------------------
COPY docker/spark/conf/spark-defaults.conf.template ${SPARK_HOME}/conf/
COPY docker/spark/conf/log4j2.properties            ${SPARK_HOME}/conf/
COPY docker/spark/entrypoint.sh                     /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

#  The worker writes executor scratch data to $SPARK_HOME/work and daemon logs
#  to $SPARK_HOME/logs; both must be writable by the non-root runtime user.
RUN useradd --create-home --shell /bin/bash --uid 1001 spark \
    && mkdir -p /opt/lakehouse /tmp/spark-events \
                ${SPARK_HOME}/work ${SPARK_HOME}/logs \
    && chown -R spark:spark \
         ${SPARK_HOME}/conf ${SPARK_HOME}/work ${SPARK_HOME}/logs \
         /opt/lakehouse /tmp/spark-events

WORKDIR /opt/lakehouse
USER spark

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD ["bash"]
