import argparse
import json
import time
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

CLEAN_DIR = Path(__file__).resolve().parent.parent / "data" / "clean"
GENERIC_TOPIC = "match-events"

TOPIC_FOR_EVENT_TYPE = {
    "Shot": "shot-events",
    "Pass": "pass-events",
    "Pressure": "pressure-events",
}

def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        linger_ms=5,
        acks="all",
    )

def load_match_events(match_id: int) -> pd.DataFrame:
    path = CLEAN_DIR / GENERIC_TOPIC / f"{match_id}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No cleaned data for match {match_id} at {path}")
    df = pd.read_parquet(path)
    df["match_id"] = match_id
    sort_cols = [c for c in ["period", "minute", "second", "index"] if c in df.columns]
    return df.sort_values(sort_cols).reset_index(drop=True)

def send_event(producer: KafkaProducer, row: dict):
    match_id = row["match_id"]
    etype = row.get("type.name") or row.get("type")
    topic = TOPIC_FOR_EVENT_TYPE.get(etype, None)

    producer.send(GENERIC_TOPIC, key=match_id, value=row)
    if topic:
        producer.send(topic, key=match_id, value=row)

def replay_match(producer: KafkaProducer, match_id: int, mode: str, speed: float):
    df = load_match_events(match_id)
    print(f"Replaying match {match_id}: {len(df)} events, mode={mode}, speed={speed}x")

    sent = 0
    t_start = time.time()
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        send_event(producer, row_dict)
        sent += 1
        if sent % 500 == 0:
            print(f"  ... sent {sent}/{len(df)} events")

    producer.flush()
    elapsed = time.time() - t_start
    print(f"Done: {sent} events sent for match {match_id} in {elapsed:.1f}s")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--match-id", type=int, default=3825907)
    ap.add_argument("--mode", choices=["bulk", "live"], default="bulk")
    ap.add_argument("--speed", type=float, default=1.0)
    args = ap.parse_args()

    producer = make_producer(args.bootstrap_servers)
    replay_match(producer, args.match_id, mode=args.mode, speed=args.speed)
    producer.close()
    print("Producer closed.")

if __name__ == "__main__":
    main()