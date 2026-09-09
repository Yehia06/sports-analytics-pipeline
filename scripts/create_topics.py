"""
create_topics.py
-----------------
Creates the Kafka topics this pipeline needs. Idempotent — safe to re-run.

Usage:
    python create_topics.py
    python create_topics.py --bootstrap-servers localhost:9092 --partitions 3
"""

import argparse
from kafka.admin import KafkaAdminClient, NewTopic
import kafka.errors as kerr

TOPICS = [
    "match-events",
    "shot-events",
    "pass-events",
    "pressure-events",
    "win-probability-updates",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--replication-factor", type=int, default=1)
    args = ap.parse_args()

    try:
        admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_servers, client_id="topic-setup")
    except Exception as e:
        raise SystemExit(
            f"Could not connect to Kafka at {args.bootstrap_servers}. Error: {e}\n"
            "Start the broker first (see README: docker compose up -d)."
        )

    new_topics = [
        NewTopic(name=t, num_partitions=args.partitions, replication_factor=args.replication_factor)
        for t in TOPICS
    ]

    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        print(f"Created topics: {TOPICS}")
    except getattr(kerr, "TopicAlreadyExistsError", Exception):
        for t in new_topics:
            try:
                admin.create_topics(new_topics=[t], validate_only=False)
                print(f"Created topic: {t.name}")
            except getattr(kerr, "TopicAlreadyExistsError", Exception):
                print(f"Topic already exists, skipping: {t.name}")

    existing = admin.list_topics()
    print("\nCurrent topics on broker:", sorted(existing))
    admin.close()


if __name__ == "__main__":
    main()