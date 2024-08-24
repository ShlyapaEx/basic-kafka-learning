import random

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from config import KAFKA_SERVERS, KAFKA_TOPICS_NAMES


def create_topics():
    """Подключиться к Kafka и создать топики в случае, если они ещё не были созданы"""

    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)

    already_existing_topics = admin_client.list_topics()

    new_topics = []
    for topic_name in KAFKA_TOPICS_NAMES:
        if topic_name not in already_existing_topics:
            new_topics.append(
                NewTopic(name=topic_name, num_partitions=5, replication_factor=2)
            )

    if new_topics:
        admin_client.create_topics(new_topics)

    admin_client.close()


def send_messages():
    """Подключиться к Kafka и положить в него 1000 сообщений"""

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda x: x.encode("utf-8"),
    )

    for i in range(1000):
        random_topic_name = random.choice(KAFKA_TOPICS_NAMES)
        producer.send(topic=random_topic_name, value=f"Сообщение №{i}")
    producer.flush()
    producer.close()


def main():
    create_topics()
    send_messages()


if __name__ == "__main__":
    main()
