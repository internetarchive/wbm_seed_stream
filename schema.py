from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, FloatType, IntegerType, BooleanType
)

RAW_INPUT_SCHEMA = StructType([
    StructField("timestamp", LongType(), True),
    StructField("url", StringType(), True),
    StructField("data_source", StringType(), True),
])

PROCESSED_URL_SCHEMA = StructType([
    StructField("url", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("domain", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("confidence", FloatType(), True),
    StructField("meta", StringType(), True),
    StructField("domain_frequency", IntegerType(), True),
    StructField("domain_frequency_pct", FloatType(), True),
    StructField("is_spam", BooleanType(), True),
    StructField("received_at", TimestampType(), True),
    StructField("domain_reputation_score", FloatType(), True),
    StructField("data_source", StringType(), True),
])