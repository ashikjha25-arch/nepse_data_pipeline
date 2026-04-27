import json
import time
from kafka import KafkaConsumer
from app.config.settings import KAFKA_TOPIC, KAFKA_SERVER
from db.db_connection import get_db_connection
from db.repository import route_and_insert_data

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
    consumer = create_consumer()
    conn, cur = get_db_connection()

    for message in consumer:
        data = message.value
        print("Consumer Loop Running")

        try:
            print("Consumer TRY Running")
            data_type = data.get("data_type")
            payload = data.get("payload")
            fetched_at = data.get("fetched_at")

            route_and_insert_data(cur, data_type, payload, fetched_at)
            conn.commit()
            print(f"Inserted {data_type}")

        except Exception as e:
            conn.rollback()
            print(f"Exception occurred: {e}")