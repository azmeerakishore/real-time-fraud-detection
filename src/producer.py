
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "PS_20174392719_1491204439457_log.csv"
)

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "financial-transactions"

BATCH_SIZE = 100
DELAY_SECONDS = 1
MAX_RECORDS = 1000


def validate_record(record):
    """Check essential transaction fields before sending."""
    required_fields = {
        "step", "type", "amount", "nameOrig",
        "oldbalanceOrg", "newbalanceOrig", "nameDest",
        "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud",
    }

    if not required_fields.issubset(record):
        return False

    if record["amount"] < 0:
        return False

    if record["isFraud"] not in (0, 1):
        return False

    if record["type"] not in {
        "CASH_OUT", "PAYMENT", "CASH_IN", "TRANSFER", "DEBIT"
    }:
        return False

    return True


def main():
    if not DATA_PATH.exists():
        print(f"Dataset not found: {DATA_PATH}")
        return

    print("Loading PaySim dataset...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )

    sent_count = 0
    skipped_count = 0

    try:
        # Read the dataset in chunks to limit memory usage.
        for chunk in pd.read_csv(DATA_PATH, chunksize=BATCH_SIZE):
            for record in chunk.to_dict(orient="records"):
                if not validate_record(record):
                    skipped_count += 1
                    continue

                try:
                    producer.send(
                        KAFKA_TOPIC,
                        value=record,
                    ).get(timeout=10)
                    sent_count += 1

                except KafkaError as error:
                    print(f"Kafka send failed: {error}")
                    continue

                if sent_count % BATCH_SIZE == 0:
                    print(f"Sent {sent_count} transactions...")
                    time.sleep(DELAY_SECONDS)

                if sent_count >= MAX_RECORDS:
                    print("Reached the 1,000-record test limit.")
                    return

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")

    finally:
        producer.flush()
        producer.close()
        print(f"Total sent: {sent_count}")
        print(f"Total skipped: {skipped_count}")


if __name__ == "__main__":
    main()
