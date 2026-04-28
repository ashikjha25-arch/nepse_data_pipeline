from pyspark.sql.functions import col, from_json
from spark import create_spark_session
from schema import kafka_message_schema

KAFKA_TOPIC = "nepse-topic"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/nepse_db"

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

def write_stock_price_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df.write.jdbc(
        url=POSTGRES_URL,
        table="nepse.stock_price_history",
        mode="append",
        properties=POSTGRES_PROPERTIES
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

    parsed_df = json_df.select(
        from_json(col("json_value"), kafka_message_schema).alias("data")
    ).select("data.*")

    stock_df = parsed_df \
        .filter(col("data_type") == "stock_price_history") \
        .select(
            col("payload.business_date").alias("issue_date"),
            col("payload.symbol").alias("symbol"),
            col("payload.open_price").alias("open_price"),
            col("payload.high_price").alias("high_price"),
            col("payload.low_price").alias("low_price"),
            col("payload.close_price").alias("close_price"),
            col("payload.volume").alias("volume"),
            col("payload.turnover").alias("turnover")
        )

    query = stock_df.writeStream \
        .foreachBatch(write_stock_price_to_postgres) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()