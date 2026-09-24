"""
PHASE 3 - Read a topic back, to see what was actually written.

Not part of the pipeline: a diagnostic tool. Spark is the real consumer
(phase 4). This one answers "is the message shaped the way I think it is?",
which is the question you ask twenty times while building an ingestion layer.

    python3 -m ingestion.consumers.inspect_topic --topic orders --max 5
    python3 -m ingestion.consumers.inspect_topic --topic orders --count-only
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from collections import Counter

from confluent_kafka import Consumer, TopicPartition


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 3 - inspect a Kafka topic")
    p.add_argument("--bootstrap",
                   default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    p.add_argument("--topic", required=True)
    p.add_argument("--max", type=int, default=5, help="messages to display")
    p.add_argument("--count-only", action="store_true",
                   help="only report the offsets per partition")
    args = p.parse_args(argv)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        # A throwaway group: inspecting must not move the offsets of a real
        # consumer group.
        "group.id": f"inspect-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    try:
        meta = consumer.list_topics(args.topic, timeout=10)
        if args.topic not in meta.topics or meta.topics[args.topic].error:
            print(f"  topic '{args.topic}' not found")
            return 1
        partitions = sorted(meta.topics[args.topic].partitions)

        total = 0
        print(f"\n  topic {args.topic}")
        for part in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(args.topic, part), timeout=10)
            total += high - low
            print(f"    partition {part}: offsets {low} -> {high}  ({high - low:,} messages)")
        print(f"    TOTAL {total:,} messages")

        if args.count_only:
            return 0

        consumer.subscribe([args.topic])
        shown = 0
        entities: Counter[str] = Counter()
        while shown < args.max:
            msg = consumer.poll(10.0)
            if msg is None:
                break
            if msg.error():
                print(f"    error: {msg.error()}")
                break
            event = json.loads(msg.value())
            entities[event.get("entity", "?")] += 1
            print(f"\n    p{msg.partition()} offset={msg.offset()} "
                  f"key={msg.key().decode() if msg.key() else None}")
            print("    " + json.dumps(event, ensure_ascii=False, indent=2)[:600].replace(
                "\n", "\n    "))
            shown += 1
        return 0
    finally:
        consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())
