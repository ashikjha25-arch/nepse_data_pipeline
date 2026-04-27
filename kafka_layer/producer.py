import json
import time
from kafka import KafkaProducer
from services.batch_service import run_scrapper
from app.config.settings import KAFKA_TOPIC, KAFKA_SERVER

def create_producer():
    """Initializes Kafka Producer with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks='all' # Ensure data reaches broker
            )
            print(f"Connected to Kafka at {KAFKA_SERVER}")
            return producer
        except Exception as e:
            print(f"Kafka Producer not ready, retrying... Error: {e}")
            time.sleep(5)

def start_producer_loop():
    """Continuously scrapes data and produces messages to Kafka."""
    producer = create_producer()
    print("Starting producer execution loop...")

    while True:
        try:
            messages = run_scrapper()
            for message in messages:
                producer.send(KAFKA_TOPIC, value=message)
                print(f"Produced: {message['data_type']}")
            producer.flush()
        except Exception as e:
            print(f"Producer loop error: {e}")
            time.sleep(10)


