#!/bin/bash
# =============================================================================
#  Runs ONCE, at the very first boot of the PostgreSQL container.
#  Creates one database per consumer instead of running three Postgres
#  containers:
#     airflow   -> Airflow metadata (DAG runs, task instances, XComs...)
#     metastore -> Hive Metastore used by Spark & dbt (table catalog)
#     analytics -> optional serving layer for BI tools
# =============================================================================
set -euo pipefail

create_database() {
    local db="$1"
    echo "  -> creating database '${db}'"
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-SQL
        SELECT 'CREATE DATABASE ${db}'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db}')\gexec
        GRANT ALL PRIVILEGES ON DATABASE ${db} TO ${POSTGRES_USER};
SQL
}

if [[ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]]; then
    echo "[init] Requested databases: ${POSTGRES_MULTIPLE_DATABASES}"
    IFS=',' read -ra DBS <<< "${POSTGRES_MULTIPLE_DATABASES}"
    for db in "${DBS[@]}"; do
        create_database "$(echo "${db}" | xargs)"
    done
    echo "[init] Done."
fi
