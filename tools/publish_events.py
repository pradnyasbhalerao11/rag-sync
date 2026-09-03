#!/usr/bin/env python3
"""
Publish the JSONL event stream to Kafka.

The only line here that really matters is `key=doc_id`. That is what pins every
event for a document to a single partition, which is what makes ordered,
per-document processing possible downstream. Get this wrong and nothing else in
the pipeline can be correct.
"""

import argparse
import json
import sys
import time

from confluent_kafka import Producer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/events.jsonl")
    ap.add_argument("--topic", default="doc.changes")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="max events/sec, 0 = as fast as possible")
    args = ap.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 20,
        "acks": "all",
        "enable.idempotence": True,
    })

    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))

    sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    start = time.time()

    with open(args.file, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            producer.produce(
                topic=args.topic,
                key=event["doc_id"].encode("utf-8"),
                value=line.encode("utf-8"),
                on_delivery=on_delivery,
            )
            sent += 1
            if sent % 1000 == 0:
                producer.poll(0)
            if interval:
                time.sleep(interval)

    producer.flush(30)
    elapsed = time.time() - start

    print(f"published={sent} failed={len(failures)} elapsed={elapsed:.2f}s "
          f"rate={sent / elapsed:.0f}/s")
    if failures:
        print("first failures:", failures[:5], file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
