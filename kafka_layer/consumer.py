import json
import time
from kafka import KafkaConsumer

KAFKA_TOPIC = "nepse-topic"
KAFKA_SERVER = "kafka:9092"

latest_data = {}

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print("Connected to Kafka consumer")
            return consumer
        except Exception as e:
            print("Kafka not ready for consumer, retrying...", e)
            time.sleep(5)

def start_consumer_loop():
    global latest_data
    consumer = create_consumer()

    for message in consumer:
        latest_data['data'] = message.value

def get_latest_data():
    return latest_data