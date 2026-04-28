import json
import time
from kafka import KafkaConsumer
from app.config.settings import KAFKA_TOPIC, KAFKA_SERVER
from db.db_connection import get_db_connection
from db.repository import route_and_insert_data

def create_consumer():
    """Initializes Kafka Consumer with retry logic."""
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="nepse_consumer_group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print(f"Connected to Kafka at {KAFKA_SERVER}")
            return consumer
        except Exception as e:
            print(f"Kafka Consumer not ready, retrying... Error: {e}")
            time.sleep(5)

def start_consumer_loop():
    """Listens for Kafka messages and persists them to Postgres."""
    consumer = create_consumer()
    conn, cur = get_db_connection()

    print("Starting consumer persistence loop...")

    try:
        for message in consumer:
            data = message.value
            data_type = data.get("data_type")
            payload = data.get("payload")
            fetched_at = data.get("fetched_at") if data.get("fetched_at") else None

            if data_type == "error":
                print(f"Logged upstream error: {payload}")
                continue

            try:
                route_and_insert_data(cur, data_type, payload, fetched_at)
                conn.commit()
                print(f"Persisted: {data_type}")
            except Exception as e:
                conn.rollback()
                print(f"Database insertion failed for {data_type}: {e}")

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        cur.close()
        conn.close()
