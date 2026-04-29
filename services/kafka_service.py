import json
import time
from kafka import KafkaProducer
from app.config.settings import KAFKA_TOPIC, KAFKA_SERVER


def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all"
            )
            print(f"Connected to Kafka at {KAFKA_SERVER}")
            return producer
        except Exception as e:
            print(f"Kafka not ready, retrying... Error: {e}")
            time.sleep(5)


def send_messages_to_kafka(messages):
    producer = create_kafka_producer()

    for message in messages:
        producer.send(KAFKA_TOPIC, value=message)
        print(f"Produced prev data: {message.get('data_type')}")

    producer.flush()
    producer.close()