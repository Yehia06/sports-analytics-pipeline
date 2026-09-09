import json
from kafka import KafkaConsumer

print("Connecting to Kafka consumer for topic 'match-events'...")

consumer = KafkaConsumer(
    "match-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  # read from the very beginning
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Reading first 5 messages from topic:\n")
for i, msg in enumerate(consumer):
    event = msg.value
    print(f"[{i+1}] Offset: {msg.offset} | Key: {msg.key.decode() if msg.key else 'None'}")
    print(f"    Type: {event.get('type')} | Player: {event.get('player_name')} | Minute: {event.get('minute')}:{event.get('second')}")
    if i >= 4:
        break

consumer.close()
print("\nConsumer test successful! Downstream read verified.")