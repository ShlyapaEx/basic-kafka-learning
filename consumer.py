from datetime import datetime

from kafka import KafkaConsumer

from config import KAFKA_SERVERS, KAFKA_TOPICS_NAMES


def consume():
    """Подключиться к Kafka и получить из него все сообщения"""

    consumer = KafkaConsumer(
        *KAFKA_TOPICS_NAMES,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=None,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    for msg in consumer:
        print(
            "Принято сообщение: ",
            f"Топик: {msg.topic}",
            f"Раздел: {msg.partition}",
            f"Оффсет: {msg.offset}",
            f"Ключ: {msg.key}",
            f"Значение: {msg.value}",
            f"Дата и время: {datetime.fromtimestamp(msg.timestamp / 1000)}",
            sep="\n ",
        )
        print("_" * 50)


def main():
    consume()


if __name__ == "__main__":
    main()
