# consumer.py
import json
import os
import signal
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
).split(",")

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "high-volume-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "high-volume-consumers")
MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "1000"))
FETCH_MIN_BYTES = int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1048576"))
FETCH_MAX_WAIT_MS = int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "100"))
MAX_POLL_INTERVAL_MS = int(
    os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000")
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def process_batch(records):
    """
    Replace with bulk database writes, Spark ingestion, or another
    idempotent sink. Avoid one network/database call per record.
    """
    if not records:
        return

    # Example: parse records and process them in bulk.
    events = [record.value for record in records]

    # TODO: perform a durable, idempotent batch write here.
    logger.info(
        "Processed %d records from %d partitions",
        len(events),
        len({(r.topic, r.partition) for r in records}),
    )


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=f"consumer-{os.getpid()}",
        group_id=GROUP_ID,
        group_instance_id=os.getenv("KAFKA_GROUP_INSTANCE_ID"),
        enable_auto_commit=False,
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        isolation_level="read_committed",
        max_poll_records=MAX_POLL_RECORDS,
        fetch_min_bytes=FETCH_MIN_BYTES,
        fetch_max_wait_ms=FETCH_MAX_WAIT_MS,
        max_partition_fetch_bytes=4 * 1024 * 1024,
        request_timeout_ms=40000,
        session_timeout_ms=15000,
        heartbeat_interval_ms=5000,
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
        check_crcs=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
    )

    consumer.subscribe([TOPIC_NAME])

    try:
        while running:
            batches = consumer.poll(
                timeout_ms=1000,
                max_records=MAX_POLL_RECORDS,
            )

            for _, records in batches.items():
                if not records:
                    continue

                # Commit offsets only after the entire batch is durable.
                process_batch(records)

            if batches:
                consumer.commit()

    except Exception:
        logger.exception("Consumer failed")
        raise

    finally:
        consumer.close(autocommit=False)


if __name__ == "__main__":
    main()