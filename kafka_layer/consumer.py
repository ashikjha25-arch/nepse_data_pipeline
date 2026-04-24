import json
import time
from kafka import KafkaConsumer
from app.config.settings import KAFKA_TOPIC, KAFKA_SERVER
from db.db_connection import get_db_connection

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
    latest_data = {}

    for message in consumer:
        latest_data['data'] = message.value
        try:
            cur.execute('INSERT COMMAND FROM REPOSITORY.PY')
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f'Exception occurred: {e}')