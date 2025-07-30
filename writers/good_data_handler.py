import os
import shutil
import glob
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp

from testing.get_good_data import main as generate_good_data_main
from spark.config.spark_config import SparkConfig

def generate_good_data():
    good_data_folder = SparkConfig.GOOD_DATA_FOLDER
    if os.path.exists(good_data_folder):
        try:
            shutil.rmtree(good_data_folder)
        except Exception as e:
            print(f"WARNING: Failed to clear good data folder: {e}")

    try:
        os.makedirs(good_data_folder, exist_ok=True)
    except Exception as e:
        print(f"ERROR: Failed to create good data folder: {e}")
        raise

    original_argv = sys.argv
    try:
        sys.argv = [
            'get_good_data.py',
            '--wiki', str(SparkConfig.WIKI_DAYS),
            '--mediacloud', str(SparkConfig.MEDIACLOUD_DAYS)
        ]
        generate_good_data_main()
        print("Good data generation completed successfully")
    except Exception as e:
        print(f"ERROR: Good data generation failed during direct call: {e}")
        raise
    finally:
        sys.argv = original_argv

def load_good_data_urls(spark: SparkSession):
    good_data_pattern = os.path.join(SparkConfig.GOOD_DATA_FOLDER, "*.tsv")
    good_data_files = glob.glob(good_data_pattern)

    if not good_data_files:
        print("No good data files found")
        return None

    print(f"Found {len(good_data_files)} good data files")

    try:
        good_data_schema = StructType([
            StructField("url", StringType(), True),
            StructField("source_uri", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("user_text", StringType(), True),
            StructField("source_type", StringType(), True),
            StructField("metadata", StringType(), True)
        ])

        good_df = spark.read \
            .option("delimiter", "\t") \
            .option("header", "true") \
            .schema(good_data_schema) \
            .csv(good_data_files)

        good_df = good_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        good_df = good_df.select("url", "timestamp") \
            .filter(col("url").isNotNull() & (col("url") != ""))

        return good_df
    except Exception as e:
        print(f"ERROR: Failed to load good data into Spark: {e}")
        raise

def handle_good_data(spark: SparkSession):
    generate_good_data()
    good_df = load_good_data_urls(spark)
    return good_df
