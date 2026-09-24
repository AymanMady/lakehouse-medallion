#!/bin/sh
# =============================================================================
#  Creates the Kafka topics of the e-commerce domain.
#
#  One topic per event family. Partition counts reflect expected throughput:
#  `orders` and `order_items` are the hot paths, so they get more partitions.
#  The partition count is the MAXIMUM parallelism of a consumer group:
#  6 partitions -> at most 6 consumers reading in parallel.
#  Replication factor is 1 because we run a single broker locally.
# =============================================================================
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}"   # 7 days

echo "[kafka-init] waiting for broker ${BOOTSTRAP}"
until "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; do
    sleep 2
done
echo "[kafka-init] broker is up"

# topic:partitions
TOPICS="customers:3 products:3 orders:6 order_items:6 payments:3 shipments:3 dead_letter:1"

for ENTRY in ${TOPICS}; do
    NAME="${ENTRY%%:*}"
    PARTS="${ENTRY##*:}"
    echo "[kafka-init] creating topic '${NAME}' (${PARTS} partitions)"
    "${KAFKA_BIN}/kafka-topics.sh" \
        --bootstrap-server "${BOOTSTRAP}" \
        --create --if-not-exists \
        --topic "${NAME}" \
        --partitions "${PARTS}" \
        --replication-factor 1 \
        --config retention.ms="${RETENTION_MS}" \
        --config cleanup.policy=delete
done

echo "[kafka-init] topics now present:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
echo "[kafka-init] OK"
