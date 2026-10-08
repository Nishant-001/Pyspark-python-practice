# create_topic.py
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "high-volume-events")

PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "24"))
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))

TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "compression.type": "producer",
    "retention.ms": str(7 * 24 * 60 * 60 * 1000),
    "segment.bytes": str(1024 * 1024 * 1024),
    "min.insync.replicas": "2",
}

def create_topic():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        client_id="topic-admin",
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )

    try:
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            topic_configs=TOPIC_CONFIG,
        )

        admin.create_topics(
            new_topics=[topic],
            validate_only=False,
            timeout_ms=30000,
        )
        print(f"Created topic: {TOPIC_NAME}")

    except TopicAlreadyExistsError:
        print(f"Topic already exists: {TOPIC_NAME}")

    finally:
        admin.close()

if __name__ == "__main__":
    create_topic()