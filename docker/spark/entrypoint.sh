#!/usr/bin/env bash
# =============================================================================
#  Spark container entrypoint
# -----------------------------------------------------------------------------
#  1. Renders spark-defaults.conf from the template using the environment
#     (so credentials never live in a file committed to Git).
#  2. Dispatches on $SPARK_MODE: master | worker | thrift | anything else.
# =============================================================================
set -euo pipefail

: "${SPARK_HOME:=/opt/spark}"
: "${SPARK_MODE:=client}"
: "${SPARK_MASTER_URL:=spark://spark-master:7077}"
: "${SPARK_DRIVER_MEMORY:=2g}"
: "${SPARK_EXECUTOR_MEMORY:=2g}"
: "${S3_ENDPOINT:=http://minio:9000}"
: "${LAKEHOUSE_BUCKET:=lakehouse}"
: "${POSTGRES_HOST:=postgres}"
: "${POSTGRES_PORT:=5432}"
: "${METASTORE_DB:=metastore}"
# Only the one-shot metastore-init service sets this to "true".
: "${METASTORE_AUTO_CREATE:=false}"

TEMPLATE="${SPARK_HOME}/conf/spark-defaults.conf.template"
TARGET="${SPARK_HOME}/conf/spark-defaults.conf"

if [[ -f "${TEMPLATE}" ]]; then
  export SPARK_MASTER_URL SPARK_DRIVER_MEMORY SPARK_EXECUTOR_MEMORY \
         S3_ENDPOINT LAKEHOUSE_BUCKET POSTGRES_HOST POSTGRES_PORT \
         METASTORE_DB METASTORE_AUTO_CREATE POSTGRES_USER POSTGRES_PASSWORD \
         AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  envsubst < "${TEMPLATE}" > "${TARGET}"
  echo "[entrypoint] rendered ${TARGET}"
fi

echo "[entrypoint] SPARK_MODE=${SPARK_MODE}"

case "${SPARK_MODE}" in
  master)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
        --host "$(hostname)" --port 7077 --webui-port 8080
    ;;

  worker)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
        --webui-port 8081 "${SPARK_MASTER_URL}"
    ;;

  thrift)
    # Spark Thrift Server = a JDBC/ODBC endpoint (HiveServer2 protocol) that
    # exposes the Spark SQL catalog on port 10000. This is how dbt talks to
    # our Delta tables.
    exec "${SPARK_HOME}/bin/spark-submit" \
        --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
        --name "Lakehouse Thrift Server" \
        --master "${SPARK_MASTER_URL}" \
        --conf spark.sql.hive.thriftServer.singleSession=false \
        --conf spark.hive.server2.thrift.port=10000 \
        --conf spark.hive.server2.thrift.bind.host=0.0.0.0 \
        spark-internal
    ;;

  *)
    # client / job mode: run whatever command was passed to the container
    exec "$@"
    ;;
esac
