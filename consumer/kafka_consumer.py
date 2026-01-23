import json
import yaml
import time
from kafka import KafkaConsumer
from transform import transform_event
from postgres_writer import PostgresWriter


# ------------------------
# Config
# ------------------------
MAX_MESSAGES = 500
MAX_RUNTIME_SECONDS = 10


def load_config():
    with open("/opt/airflow/config.yaml") as f:
        return yaml.safe_load(f)


def main():
    config = load_config()
    pg_writer = PostgresWriter()

    consumer = KafkaConsumer(
        config["kafka"]["topic"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="data-engineering-consumer",
        consumer_timeout_ms=1000,
    )

    print("Kafka batch consumer started")
    
    consumed = 0
    start_time = time.time()
    
    while consumed < MAX_MESSAGES:
        records = consumer.poll(timeout_ms=1000)

        if not records:
            elapsed = time.time() - start_time
            if elapsed > MAX_RUNTIME_SECONDS:
                print("No more messages, timeout reached")
                break
            continue

        for _, messages in records.items():
            for message in messages:
                raw_event = message.value
                transformed_event = transform_event(raw_event)

                if transformed_event is None:
                    print("Invalid event skipped:", raw_event)
                    continue

                pg_writer.insert_transaction(transformed_event)
                print("Inserted event:", transformed_event)
                consumed += 1

                if consumed >= MAX_MESSAGES:
                    break

    consumer.close()
    print(f"Batch finished. Total messages consumed: {consumed}")


if __name__ == "__main__":
    main()
