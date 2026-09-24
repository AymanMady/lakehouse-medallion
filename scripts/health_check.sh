#!/usr/bin/env bash
# =============================================================================
#  Verifies that every running service of the stack actually answers.
#  Usage:  make health   (or)  ./scripts/health_check.sh
# =============================================================================
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f .env ]] && set -a && . ./.env && set +a

PASS=0; FAIL=0
ok()   { printf '  \033[32mv\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko()   { printf '  \033[31mx\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }
skip() { printf '  \033[90m-\033[0m %s (not running)\n' "$1"; }

running() { docker compose ps --services --status running 2>/dev/null | grep -qx "$1"; }

echo ""
echo "Lakehouse Medallion — health check"
echo "-----------------------------------"

# --- MinIO ---
if running minio; then
  curl -fsS "http://localhost:${MINIO_API_PORT:-19000}/minio/health/live" >/dev/null 2>&1 \
    && ok "MinIO S3 API        http://localhost:${MINIO_API_PORT:-19000}" \
    || ko "MinIO S3 API"
  curl -fsS -o /dev/null "http://localhost:${MINIO_CONSOLE_PORT:-19001}" \
    && ok "MinIO console       http://localhost:${MINIO_CONSOLE_PORT:-19001}" \
    || ko "MinIO console"
else skip "MinIO"; fi

# --- PostgreSQL ---
if running postgres; then
  docker compose exec -T postgres pg_isready -U "${POSTGRES_USER}" >/dev/null 2>&1 \
    && ok "PostgreSQL          localhost:${POSTGRES_EXTERNAL_PORT:-15432}" \
    || ko "PostgreSQL"
  DBS=$(docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB:-lakehouse}" \
        -tAc "SELECT string_agg(datname, ',' ORDER BY datname) FROM pg_database WHERE datistemplate = false;" 2>/dev/null)
  [[ -n "${DBS}" ]] && ok "  databases         ${DBS}" || ko "  databases"
else skip "PostgreSQL"; fi

# --- Kafka ---
if running kafka; then
  TOPICS=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
           --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ' ')
  [[ -n "${TOPICS}" ]] && ok "Kafka broker        topics: ${TOPICS}" || ko "Kafka broker"
else skip "Kafka"; fi

if running kafka-ui; then
  curl -fsS -o /dev/null "http://localhost:${KAFKA_UI_PORT:-18085}" \
    && ok "Kafka UI            http://localhost:${KAFKA_UI_PORT:-18085}" || ko "Kafka UI"
else skip "Kafka UI"; fi

# --- Spark ---
if running spark-master; then
  curl -fsS -o /dev/null "http://localhost:${SPARK_MASTER_UI_PORT:-18081}" \
    && ok "Spark master UI     http://localhost:${SPARK_MASTER_UI_PORT:-18081}" || ko "Spark master UI"
  WORKERS=$(curl -fsS "http://localhost:${SPARK_MASTER_UI_PORT:-18081}/json/" 2>/dev/null \
            | grep -o '"aliveworkers"[ :]*[0-9]*' | grep -o '[0-9]*$')
  [[ "${WORKERS:-0}" -ge 1 ]] && ok "  alive workers     ${WORKERS}" || ko "  alive workers (0)"
else skip "Spark"; fi

if running spark-thrift; then
  docker compose exec -T spark-thrift bash -c 'timeout 3 bash -c "</dev/tcp/localhost/10000"' 2>/dev/null \
    && ok "Spark Thrift Server localhost:${SPARK_THRIFT_PORT:-10000}" || ko "Spark Thrift Server"
else skip "Spark Thrift Server"; fi

# --- Airflow ---
if running airflow-webserver; then
  curl -fsS "http://localhost:${AIRFLOW_WEB_PORT:-18088}/health" >/dev/null 2>&1 \
    && ok "Airflow webserver   http://localhost:${AIRFLOW_WEB_PORT:-18088}" || ko "Airflow webserver"
else skip "Airflow"; fi

echo "-----------------------------------"
printf '  %s passed, %s failed\n\n' "${PASS}" "${FAIL}"
[[ "${FAIL}" -eq 0 ]]
