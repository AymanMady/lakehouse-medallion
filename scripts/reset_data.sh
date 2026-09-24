#!/usr/bin/env bash
# =============================================================================
#  Wipe the DATA, keep the infrastructure.
#
#  `make clean` deletes the volumes and you have to rebuild everything.
#  This is the lighter tool you actually want while developing: empty the
#  topics, empty the lake, drop the catalog tables, and leave the containers
#  running.
#
#  Deliberately noisy and confirmation-gated: it is destructive.
# =============================================================================
set -euo pipefail

COMPOSE="docker compose"
BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"
TOPICS="customers products orders order_items payments shipments dead_letter"
LAYERS="bronze silver gold quarantine _checkpoints"

if [ "${1:-}" != "--yes" ]; then
    printf "  This deletes every Kafka message and everything in the lake. Continue? [y/N] "
    read -r answer
    [ "$answer" = "y" ] || { echo "  aborted"; exit 1; }
fi

echo "[reset] deleting the Kafka topics"
for topic in $TOPICS; do
    $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
done
# Deletion is asynchronous: recreating too early recreates the OLD topic.
sleep 5

echo "[reset] recreating the topics"
$COMPOSE run --rm --entrypoint /bin/sh kafka-init /scripts/create_topics.sh >/dev/null
echo "[reset]   topics recreated"

echo "[reset] emptying the lake (s3://$BUCKET)"
for layer in $LAYERS; do
    $COMPOSE run --rm --entrypoint sh minio-init -c \
        "mc alias set lake \"\$S3_ENDPOINT\" \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null && \
         mc rm -r --force \"lake/$BUCKET/$layer\" >/dev/null 2>&1 || true"
    echo "[reset]   $layer emptied"
done

echo "[reset] dropping the catalog tables"
$COMPOSE exec -T spark-master /opt/spark/bin/spark-sql --master "local[1]" -e "
DROP DATABASE IF EXISTS bronze CASCADE;
DROP DATABASE IF EXISTS silver CASCADE;
DROP DATABASE IF EXISTS gold CASCADE;
DROP DATABASE IF EXISTS quarantine CASCADE;
" >/dev/null 2>&1 || true

echo "[reset] re-registering the databases"
$COMPOSE exec -T spark-master /opt/spark/bin/spark-submit \
    --master "local[1]" /opt/lakehouse/scripts/init_metastore.py 2>&1 | grep -E "^\[metastore" || true

echo "[reset] done. Next: make generate && make produce && make bronze"
