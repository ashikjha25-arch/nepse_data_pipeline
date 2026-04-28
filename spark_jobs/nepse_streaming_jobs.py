import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField


KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
SPARK_TABLE = os.getenv("SPARK_TABLE")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

spark = (
    SparkSession.builder
    .appName("NEPSE Kafka to Postgres Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

json_stream = raw_stream.selectExpr("CAST(value AS STRING) as raw_value")


processed_stream = (
    json_stream
    .withColumn("payload", col("raw_value"))
    .withColumn("created_at", current_timestamp())
    .select("payload", "created_at")
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", SPARK_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id} written to Postgres")


query = (
    processed_stream.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/spark-checkpoints/nepse-stream")
    .start()
)

query.awaitTermination()