"""
PHASE 3 - Publish the generated events into Kafka.

One topic per entity. The message KEY is the business key (customer_id,
order_id...), and that choice is not cosmetic:

    hash(key) % partitions  ->  the same key always lands in the same partition
    Kafka guarantees ordering WITHIN a partition, never across the topic

So keying on order_id means every event about one order is ordered, while
different orders are processed in parallel. Keying on nothing would spread the
load perfectly and give up ordering entirely.

    python3 -m ingestion.producers.kafka_producer --from-files
    python3 -m ingestion.producers.kafka_producer --entities orders --rate 200
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

from ingestion.generator.schemas import BUSINESS_KEYS, TOPICS


def build_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        # acks=all + idempotence: the broker deduplicates the producer's own
        # retries, so a lost acknowledgement does not create a duplicate.
        # It covers the retry, not a double call from our own code.
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.size": 65536,
        "queue.buffering.max.messages": 500_000,
    })


def read_events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def publish(producer: Producer, topic: str, events: list[dict], key_field: str,
            rate: float, stats: dict) -> None:
    def _on_delivery(err, _msg):
        if err is None:
            stats["delivered"] += 1
        else:
            stats["failed"] += 1
            if stats["failed"] <= 3:
                print(f"    delivery failed: {err}")

    interval = (1.0 / rate) if rate > 0 else 0.0
    next_at = time.perf_counter()

    for event in events:
        key = event.get("payload", {}).get(key_field)
        # A missing key is not an error here: it is one of the corruptions the
        # generator injects. Kafka then falls back to round-robin, and the
        # ordering guarantee for that entity is lost - which is exactly the
        # consequence worth seeing.
        key_bytes = str(key).encode() if key is not None else None
        while True:
            try:
                producer.produce(topic, key=key_bytes,
                                 value=json.dumps(event, ensure_ascii=False).encode(),
                                 on_delivery=_on_delivery)
                break
            except BufferError:
                # The local queue is full: we are producing faster than the
                # broker accepts. Serving the callbacks drains it.
                producer.poll(0.1)
        stats["produced"] += 1
        producer.poll(0)

        if interval:
            next_at += interval
            sleep_for = next_at - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 3 - publish the events to Kafka")
    p.add_argument("--bootstrap",
                   default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    p.add_argument("--source", type=Path, default=Path("/opt/lakehouse/data/raw"))
    p.add_argument("--entities", nargs="+", default=sorted(TOPICS),
                   choices=sorted(TOPICS))
    p.add_argument("--rate", type=float, default=0,
                   help="events per second (0 = as fast as possible)")
    p.add_argument("--limit", type=int, default=0,
                   help="cap the number of events per entity (0 = all)")
    args = p.parse_args(argv)

    missing = [e for e in args.entities if not (args.source / f"{e}.jsonl").exists()]
    if missing:
        print(f"  missing files for {missing} in {args.source}")
        print("  run: python3 -m ingestion.generator.generate")
        return 1

    producer = build_producer(args.bootstrap)
    stats = {"produced": 0, "delivered": 0, "failed": 0}
    started = time.perf_counter()

    print(f"  publishing to {args.bootstrap}")
    for entity in args.entities:
        events = read_events(args.source / f"{entity}.jsonl")
        if args.limit:
            events = events[:args.limit]
        print(f"    {entity:<14} {len(events):>8,} events -> topic {TOPICS[entity]}")
        publish(producer, TOPICS[entity], events, BUSINESS_KEYS[entity], args.rate, stats)

    # flush() blocks until every acknowledgement is back. Without it, a script
    # that exits right after produce() loses its last messages: produce() only
    # queues, it does not send.
    pending = producer.flush(120)
    elapsed = time.perf_counter() - started

    print(f"\n  produced={stats['produced']:,} delivered={stats['delivered']:,} "
          f"failed={stats['failed']} pending={pending}")
    print(f"  {elapsed:.1f}s ({stats['delivered'] / max(elapsed, 0.001):,.0f} ev/s)")
    return 1 if (stats["failed"] or pending) else 0


if __name__ == "__main__":
    sys.exit(main())
