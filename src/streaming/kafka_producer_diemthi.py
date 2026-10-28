import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "diemthi_realtime")
INTERVAL_SECONDS = float(os.getenv("KAFKA_INTERVAL_SECONDS", "60"))
MAX_RECORDS = int(os.getenv("KAFKA_MAX_RECORDS", "0"))
INVALID_EVERY = int(os.getenv("KAFKA_INVALID_EVERY", "0"))


def score_or_none(probability: float = 0.8):
    if random.random() > probability:
        return None
    return round(random.randint(0, 40) * 0.25, 2)


def build_record(sequence: int) -> dict:
    # Timestamp đến microsecond + sequence giúp SBD không trùng giữa các lần chạy demo.
    unique_number = int(datetime.now().strftime("%d%H%M%S%f")) + sequence
    record = {
        "SBD": f"02{unique_number % 1_000_000:06d}",
        "Nam": 2024,
        "Tinh": 2,
        "Toan": score_or_none(1.0),
        "NguVan": score_or_none(1.0),
        "VatLy": score_or_none(),
        "HoaHoc": score_or_none(),
        "SinhHoc": score_or_none(),
        "LichSu": score_or_none(),
        "DiaLy": score_or_none(),
        "NgoaiNgu": score_or_none(),
        "event_time": datetime.now().isoformat(timespec="seconds"),
        "source": "kafka_demo",
    }
    if INVALID_EVERY > 0 and sequence % INVALID_EVERY == 0:
        record["Toan"] = 11.0
    return record


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=5,
    )
    print(f"Producer connected: brokers={BOOTSTRAP_SERVERS}, topic={TOPIC}")
    sequence = 0
    try:
        while MAX_RECORDS == 0 or sequence < MAX_RECORDS:
            sequence += 1
            record = build_record(sequence)
            metadata = producer.send(TOPIC, key=record["SBD"], value=record).get(timeout=20)
            print(
                f"SENT record={sequence} partition={metadata.partition} "
                f"offset={metadata.offset} data={json.dumps(record, ensure_ascii=False)}",
                flush=True,
            )
            if MAX_RECORDS == 0 or sequence < MAX_RECORDS:
                time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping producer safely...")
    except KafkaError as error:
        print(f"Kafka producer error: {error}", flush=True)
        raise
    finally:
        producer.flush(timeout=20)
        producer.close(timeout=20)
        print("Producer stopped.")


if __name__ == "__main__":
    main()

