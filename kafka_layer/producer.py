import json
import time
from kafka import KafkaProducer
from services.batch_service import run_scrapper
from app.config.settings import KAFKA_TOPIC, KAFKA_SERVER

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka")
            return producer
        except Exception as e:
            print("Kafka not ready, retrying...", e)
            time.sleep(5)

def send_to_kafka(data, producer):
    producer.send(KAFKA_TOPIC, value=data)
    producer.flush()

def start_producer_loop():
    producer = create_producer()
    while True:
        data = run_scrapper()
        send_to_kafka(data, producer)
