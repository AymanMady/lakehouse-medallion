#!/bin/sh
# =============================================================================
#  Creates the lakehouse bucket and its medallion prefixes.
#  Runs once, right after MinIO reports healthy.
#
#  Reminder: object storage has NO real directories. "bronze/orders/x.parquet"
#  is a single flat object key; the "/" is only a naming convention that tools
#  display as a folder tree. We create a tiny .keep object so the prefixes show
#  up in the MinIO console from day one.
# =============================================================================
set -eu

ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"

echo "[minio-init] connecting to ${ENDPOINT}"
mc alias set lake "${ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null

echo "[minio-init] creating bucket '${BUCKET}'"
mc mb --ignore-existing "lake/${BUCKET}"

# Keep every version of a Delta table's files -> safety net while learning.
mc version enable "lake/${BUCKET}" >/dev/null 2>&1 || \
  echo "[minio-init] versioning not enabled (single-node MinIO), continuing"

for PREFIX in bronze silver gold quarantine warehouse checkpoints; do
    echo "[minio-init]   prefix ${PREFIX}/"
    echo "lakehouse-medallion" | mc pipe "lake/${BUCKET}/${PREFIX}/.keep" >/dev/null
done

echo "[minio-init] bucket layout:"
mc ls -r "lake/${BUCKET}"
echo "[minio-init] OK"
