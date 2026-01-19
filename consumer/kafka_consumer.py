import json
import yaml
from kafka import KafkaConsumer
from transform import transform_event


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def main():
    config = load_config()

    consumer = KafkaConsumer(
        config["kafka"]["topic"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="data-engineering-consumer",
    )

    print("Kafka consumer started")

    for message in consumer:
        raw_event = message.value
        transformed_event = transform_event(raw_event)

        if transformed_event is None:
            print("Invalid event skipped:", raw_event)
            continue

        print("Processed event:", transformed_event)


if __name__ == "__main__":
    main()
