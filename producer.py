"""Generates Sample JSON events (with PII fields) onto a Kafka topic.

  python producer.py --count 5000 --rate 200          # good events
  python producer.py --count 3 --bad-json             # unparseable -> DLQ
  python producer.py --count 3 --bad-amount           # rejected by Snowflake -> error table
"""
import argparse
import json
import random
import time
import uuid
from datetime import date, timedelta

from confluent_kafka import Producer

TOPIC = "demo-events"
TXN_TYPES = ["credit_inquiry", "identity_check", "fraud_score"]
FIRST = ["Ana", "Ben", "Chen", "Dee", "Eli"]
LAST = ["Garcia", "Smith", "Wu", "Okafor", "Novak"]


def make_event(bad_amount: bool = False) -> dict:
    txn_type = random.choice(TXN_TYPES)
    event = {
        "event_id": str(uuid.uuid4()),
        "application_pk": f"APP-{random.randint(1, 50_000):06d}",
        "application_date": str(date.today() - timedelta(days=random.randint(0, 30))),
        "txn_type": txn_type,
        "amount": "not_a_number" if bad_amount else round(random.uniform(10, 5000), 2),
        # PII -- must be stripped before Snowflake
        "full_name": f"{random.choice(FIRST)} {random.choice(LAST)}",
        "ssn": f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
        "dob": str(date(1960, 1, 1) + timedelta(days=random.randint(0, 15000))),
        # variable-depth nested detail by transaction type
        "detail": {
            "credit_inquiry": {"bureau": "BUREAU_A", "score": random.randint(300, 850)},
            "identity_check": {"checks": {"address": True, "phone": {"match": random.random() > 0.2}}},
            "fraud_score": {"model": "v3", "signals": [random.random() for _ in range(3)]},
        }[txn_type],
    }
    return event


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=100, help="events/sec")
    ap.add_argument("--bad-json", action="store_true")
    ap.add_argument("--bad-amount", action="store_true")
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": "localhost:29092", "linger.ms": 20})
    delay = 1.0 / args.rate if args.rate > 0 else 0

    for i in range(args.count):
        if args.bad_json:
            key, value = f"BAD-{i}", b'{"event_id": "broken", "amount": '
        else:
            event = make_event(bad_amount=args.bad_amount)
            key, value = event["application_pk"], json.dumps(event).encode()
        producer.produce(TOPIC, key=key, value=value)
        producer.poll(0)
        if delay:
            time.sleep(delay)
        if (i + 1) % 1000 == 0:
            print(f"produced {i + 1}")

    producer.flush(10)
    print(f"done: {args.count} events to {TOPIC}")


if __name__ == "__main__":
    main()
