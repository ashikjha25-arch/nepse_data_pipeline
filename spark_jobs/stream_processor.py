from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    to_date,
    to_timestamp
)
from pyspark.sql.types import BooleanType

from spark import create_spark_session
from schema import stock_payload_schema, company_info_schema


KAFKA_TOPIC = "nepse-topic"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/nepse_db"

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def write_df_to_postgres(df, table_name):
    if df.count() == 0:
        return

    df.write.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        mode="append",
        properties=POSTGRES_PROPERTIES
    )


def process_batch(batch_df, batch_id):
    print(f"Processing batch_id: {batch_id}")

    if batch_df.count() == 0:
        print("Empty batch. Skipping...")
        return

    # -------------------------
    # 1. Stock Price History
    # -------------------------
    stock_df = batch_df \
        .filter(col("data_type") == "stock_price_history") \
        .select(
            explode(
                from_json(
                    col("payload"),
                    f"array<{stock_payload_schema.simpleString()}>"
                )
            ).alias("payload")
        ) \
        .select(
            to_date(col("payload.business_date")).alias("issue_date"),
            col("payload.symbol").alias("symbol"),
            col("payload.open_price").alias("open_price"),
            col("payload.high_price").alias("high_price"),
            col("payload.low_price").alias("low_price"),
            col("payload.close_price").alias("close_price"),
            col("payload.volume").alias("volume"),
            col("payload.turnover").alias("turnover")
        )

    write_df_to_postgres(stock_df, "nepse.stock_price_history")

    # -------------------------
    # 2. Market Status Log
    # -------------------------
    status_df = batch_df \
        .filter(col("data_type") == "market_open_status") \
        .select(
            to_date(to_timestamp(col("fetched_at"))).alias("checked_date"),
            col("payload").cast(BooleanType()).alias("is_open"),
            to_timestamp(col("fetched_at")).alias("checked_at")
        )

    write_df_to_postgres(status_df, "nepse.status_log")

    # -------------------------
    # 3. Company Info
    # -------------------------
    company_df = batch_df \
        .filter(col("data_type") == "company_info") \
        .select(
            from_json(col("payload"), company_info_schema).alias("payload")
        ) \
        .select(
            col("payload.symbol").alias("symbol"),
            col("payload.company_name").alias("company_name"),
            col("payload.sector").alias("sector")
        )

    write_df_to_postgres(company_df, "nepse.company_info")

    print(f"Finished processing batch_id: {batch_id}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

    parsed_df = json_df.select(
        from_json(
            col("json_value"),
            "data_type STRING, payload STRING, fetched_at STRING"
        ).alias("data")
    ).select("data.*")

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/nepse-main-stream") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()