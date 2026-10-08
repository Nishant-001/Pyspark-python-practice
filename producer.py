# producer.py
import json
import os
import signal
import time
import logging
from concurrent.futures import Future
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
).split(",")

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "high-volume-events")
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "65536"))
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "10"))
BUFFER_MEMORY = int(os.getenv("KAFKA_BUFFER_MEMORY", str(128 * 1024 * 1024)))
MAX_IN_FLIGHT = int(os.getenv("KAFKA_MAX_IN_FLIGHT", "5"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

running = True

def stop(*_):
    global running
    running = False

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def on_success(metadata):
    pass


def on_error(exc):
    logger.error("Kafka delivery failed: %s", exc)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="high-throughput-producer",
        acks="all",
        enable_idempotence=True,
        compression_type="lz4",
        linger_ms=LINGER_MS,
        batch_size=BATCH_SIZE,
        buffer_memory=BUFFER_MEMORY,
        max_in_flight_requests_per_connection=MAX_IN_FLIGHT,
        retries=2147483647,
        retry_backoff_ms=100,
        request_timeout_ms=30000,
        max_block_ms=60000,
        delivery_timeout_ms=120000,
        connections_max_idle_ms=540000,
        key_serializer=lambda key: (
            key.encode("utf-8") if isinstance(key, str) else key
        ),
        value_serializer=lambda value: json.dumps(
            value, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8"),
    )


def generate_events():
    """Replace with your database, API, file, or streaming source."""
    while running:
        yield {
            "event_id": os.urandom(16).hex(),
            "event_time": time.time_ns(),
            "event_type": "measurement",
            "value": 42,
        }


def main():
    producer = create_producer()
    pending = set()
    max_pending = 10000
    sent = 0

    try:
        for event in generate_events():
            if not running:
                break

            key = event["event_id"]

            while len(pending) >= max_pending:
                done = {f for f in pending if f.done()}
                for future in done:
                    pending.remove(future)
                    future.get(timeout=0)

                if len(pending) >= max_pending:
                    pending.pop().get(timeout=60)

            future: Future = producer.send(
                TOPIC_NAME,
                key=key,
                value=event,
            )
            future.add_callback(on_success)
            future.add_errback(on_error)
            pending.add(future)
            sent += 1

            if sent % 100000 == 0:
                logger.info("Submitted %d events", sent)

    except Exception:
        logger.exception("Producer failed")
        raise

    finally:
        producer.flush(timeout=120)
        producer.close(timeout=120)


if __name__ == "__main__":
    main()