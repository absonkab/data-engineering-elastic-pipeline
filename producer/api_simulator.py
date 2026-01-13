import json
import random
import time
import yaml
from datetime import datetime, timedelta
from kafka import KafkaProducer


def generate_event():
    event = {
        "transaction_id": random.randint(1, 10_000_000),
        "user_id": random.randint(1, 1_000),
        "amount": round(random.uniform(5, 500), 2),
        "currency": "EUR",
        "status": random.choice(["SUCCESS", "FAILED", None]),
    }

    # 20% des events sans timestamp
    if random.random() > 0.2:
        # 10% des events en retard (jusqu'à 5 min)
        delay = random.choice([0, 0, 0, random.randint(60, 300)])
        event["event_time"] = (
            datetime.utcnow() - timedelta(seconds=delay)
        ).isoformat()
    else:
        event["event_time"] = None

    return event


def main():
    with open("producer/config.yaml") as f:
        config = yaml.safe_load(f)

    producer = KafkaProducer(
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Kafka API simulator started")

    while True:
        event = generate_event()
        producer.send(config["kafka"]["topic"], event)
        print("Sent:", event)
        time.sleep(config["generator"]["interval_sec"])


if __name__ == "__main__":
    main()
