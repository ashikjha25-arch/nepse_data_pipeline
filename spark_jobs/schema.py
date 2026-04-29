from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)


stock_payload_schema = StructType([
    StructField("business_date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("turnover", DoubleType(), True)
])


company_info_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("sector", StringType(), True)
])