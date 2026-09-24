#!/bin/bash
# =============================================================================
#  Loads the OFFICIAL Hive 2.3.0 metastore schema into the `metastore` database.
#  Runs once, at the very first boot of the PostgreSQL container.
#
#  WHY NOT LET SPARK CREATE IT?
#  ----------------------------
#  Spark can auto-create metastore tables (`datanucleus.schema.autoCreateAll`),
#  but that mechanism is:
#    1. LAZY  - it only creates the tables a given code path happens to touch,
#               so you hit "Required table missing: TBL_PRIVS" later on;
#    2. NOT CONCURRENCY-SAFE - two Spark apps starting together issue the same
#               DDL and deadlock PostgreSQL, hanging forever.
#
#  Loading the official DDL up-front is exactly what `schematool -initSchema`
#  does in a real Hive deployment. Schema version 2.3.0 matches the Hive
#  metastore client bundled with Spark 3.5.x.
# =============================================================================
set -euo pipefail

METASTORE_DB="${METASTORE_DB:-metastore}"
SRC_DIR=/hive-schema
WORK_DIR=/tmp/hive-schema

if [[ ! -d "${SRC_DIR}" ]]; then
    echo "[hive-metastore] ${SRC_DIR} not mounted, skipping"
    exit 0
fi

echo "[hive-metastore] initialising schema in database '${METASTORE_DB}'"

# The upstream script contains `\i hive-txn-schema-2.3.0.postgres.sql;` — psql
# treats the trailing ';' as part of the filename, so strip it. We also need a
# writable copy since the mount is read-only.
mkdir -p "${WORK_DIR}"
cp "${SRC_DIR}"/*.sql "${WORK_DIR}/"
sed -i 's|^\\i \(.*\)\.sql;$|\\i \1.sql|' "${WORK_DIR}/hive-schema-2.3.0.postgres.sql"

cd "${WORK_DIR}"
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${METASTORE_DB}" \
     -f hive-schema-2.3.0.postgres.sql > /tmp/hive-schema-load.log 2>&1

TABLES=$(psql -tA --username "${POSTGRES_USER}" --dbname "${METASTORE_DB}" \
         -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';")
VERSION=$(psql -tA --username "${POSTGRES_USER}" --dbname "${METASTORE_DB}" \
         -c 'SELECT "SCHEMA_VERSION" FROM "VERSION" LIMIT 1;')

echo "[hive-metastore] ${TABLES} tables created, schema version ${VERSION}"
rm -rf "${WORK_DIR}"
