import os
import sys
import glob
import shutil
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark.config.spark_config import SparkConfig

def clear_good_data_folder():
    good_data_folder = SparkConfig.GOOD_DATA_FOLDER
    if os.path.exists(good_data_folder):
        try:
            shutil.rmtree(good_data_folder)
            print(f"Cleared good data folder: {good_data_folder}")
        except Exception as e:
            print(f"WARNING: Failed to clear good data folder: {e}")

    try:
        os.makedirs(good_data_folder, exist_ok=True)
        print(f"Created good data folder: {good_data_folder}")
    except Exception as e:
        print(f"ERROR: Failed to create good data folder: {e}")
        raise

def generate_good_data():
    script_path = os.path.join(os.path.dirname(__file__), '..', 'testing', 'get_good_data.py')

    if not os.path.exists(script_path):
        print(f"ERROR: Good data script not found at {script_path}")
        raise FileNotFoundError(f"Good data script not found at {script_path}")

    try:
        cmd = [sys.executable, script_path, '--wiki', str(SparkConfig.WIKI_DAYS), '--mediacloud', str(SparkConfig.MEDIACLOUD_DAYS)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=True)
        print(f"Good data generation completed successfully")
        if result.stdout:
            print(f"STDOUT: {result.stdout}")
        if result.stderr:
            print(f"STDERR: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("ERROR: Good data generation timed out after 300 seconds")
        raise
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Good data generation failed with return code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise
    except Exception as e:
        print(f"ERROR: Unexpected error during good data generation: {e}")
        raise

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

        good_df = spark.read.option("delimiter", "\t").option("header", "true").schema(good_data_schema).csv(good_data_files)
        good_df = good_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        good_df = good_df.select("url", "timestamp").filter(col("url").isNotNull() & (col("url") != ""))

        return good_df
    except Exception as e:
        print(f"ERROR: Failed to load good data: {e}")
        raise

def handle_good_data(spark: SparkSession):
    clear_good_data_folder()
    generate_good_data()
    return load_good_data_urls(spark)
