import json
import os

from kafka import KafkaConsumer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "diemthi_realtime")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "diemthi_demo_consumer")
MAX_MESSAGES = int(os.getenv("KAFKA_MAX_MESSAGES", "0"))


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
    )
    print(f"Consumer connected: topic={TOPIC}, group={GROUP_ID}")
    received = 0
    try:
        for message in consumer:
            received += 1
            print(
                f"RECEIVED partition={message.partition} offset={message.offset} "
                f"key={message.key} data={json.dumps(message.value, ensure_ascii=False)}",
                flush=True,
            )
            if MAX_MESSAGES > 0 and received >= MAX_MESSAGES:
                break
    except KeyboardInterrupt:
        print("\nStopping consumer safely...")
    except (KafkaError, json.JSONDecodeError) as error:
        print(f"Kafka consumer error: {error}", flush=True)
        raise
    finally:
        consumer.close(autocommit=True)
        print("Consumer stopped.")


if __name__ == "__main__":
    main()

