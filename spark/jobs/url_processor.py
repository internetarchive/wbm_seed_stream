import json
import os
import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (BooleanType, FloatType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

from config.settings import settings
from spark.config.spark_config import get_spark_session
from spark.utils.url_analysis import process_batch_worker_optimized

PROCESSED_URL_SCHEMA = StructType([
    StructField("url", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("priority_score", FloatType(), True),
    StructField("reasons", StringType(), True),
    StructField("domain_frequency", IntegerType(), True),
    StructField("domain_frequency_pct", FloatType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("received_at", TimestampType(), True),
])

def process_url_batch(urls_list):
    if not urls_list:
        return []

    try:
        processed_results = process_batch_worker_optimized(urls_list)

        output_data = []
        for result in processed_results:
            try:
                reasons_json = json.dumps(result.reasons) if result.reasons else "{}"

                output_data.append({
                    "url": result.url,
                    "timestamp": result.timestamp,
                    "domain": result.domain,
                    "priority_score": result.priority_score,
                    "reasons": reasons_json,
                    "domain_frequency": result.domain_frequency,
                    "domain_frequency_pct": result.domain_frequency_pct,
                    "is_active": result.is_active,
                    "received_at": result.received_at
                })
            except Exception as e:
                print(f"Error processing individual result: {e}")
                continue

        return output_data

    except Exception as e:
        print(f"Error in process_url_batch: {e}")
        return []

def process_pandas_partition(iterator):
    for pandas_df_in in iterator:
        if len(pandas_df_in) == 0:
            continue

        rows_as_dicts = []
        for idx, row in pandas_df_in.iterrows():
            try:
                # Simple access since we know the format: timestamp\turl
                timestamp_val = row['timestamp']
                url_val = row['url']
                
                # Validate and clean the data
                if pd.isna(timestamp_val) or pd.isna(url_val):
                    continue
                
                timestamp_clean = str(timestamp_val).strip()
                url_clean = str(url_val).strip()
                
                # Skip if URL is empty or invalid
                if not url_clean or url_clean.lower() in ['nan', 'none', ''] or len(url_clean) < 5:
                    continue

                row_dict = {
                    "timestamp": timestamp_clean,
                    "url": url_clean
                }
                
                rows_as_dicts.append(row_dict)
                
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                continue

        if not rows_as_dicts:
            continue

        output_data = process_url_batch(rows_as_dicts)

        if not output_data:
            continue

        output_pandas_df = pd.DataFrame(output_data)

        ordered_columns = [field.name for field in PROCESSED_URL_SCHEMA]
        for col_name in ordered_columns:
            if col_name not in output_pandas_df.columns:
                if col_name == "received_at":
                    output_pandas_df[col_name] = pd.Timestamp.now()
                else:
                    output_pandas_df[col_name] = None

        output_pandas_df = output_pandas_df[ordered_columns]

        # Handle received_at timestamp conversion with flexible parsing
        if "received_at" in output_pandas_df.columns:
            try:
                # Use format='mixed' to handle different timestamp formats flexibly
                output_pandas_df["received_at"] = pd.to_datetime(output_pandas_df["received_at"], format='mixed')
            except Exception as e:
                print(f"DEBUG: Timestamp parsing failed with mixed format, trying ISO8601: {e}")
                try:
                    output_pandas_df["received_at"] = pd.to_datetime(output_pandas_df["received_at"], format='ISO8601')
                except Exception as e2:
                    print(f"DEBUG: ISO8601 parsing also failed, using infer_datetime_format: {e2}")
                    output_pandas_df["received_at"] = pd.to_datetime(output_pandas_df["received_at"], infer_datetime_format=True, errors='coerce')

        yield output_pandas_df
        
def process_tsv_file(spark: SparkSession, input_path: str, output_path: str):
    try:
        print(f"DEBUG: Starting processing of {input_path}")
        print(f"DEBUG: Output path: {output_path}")
        
        # Read the TSV file with proper delimiter
        df_raw = spark.read.option("delimiter", "\t").csv(input_path, header=False)
        
        # Rename columns to match expected format
        df_raw = df_raw.withColumnRenamed("_c0", "timestamp").withColumnRenamed("_c1", "url")
        
        print(f"DEBUG: Raw DataFrame created with {df_raw.count()} rows")
        print(f"DEBUG: DataFrame columns: {df_raw.columns}")
        
        # Show sample data for debugging
        print("DEBUG: Sample data:")
        df_raw.show(5, truncate=False)

        # Clean the data - filter out null/empty values
        df_cleaned = df_raw.filter(
            col("url").isNotNull() &
            (col("url") != "") &
            col("timestamp").isNotNull() &
            (col("timestamp") != "")
        )

        total_rows = df_cleaned.count()
        print(f"DEBUG: After cleaning: {total_rows} rows")

        if total_rows == 0:
            print("DEBUG: No rows after cleaning, exiting")
            return

        # Repartition for better performance if needed
        # Set a fixed number of partitions to leverage allocated cores
        num_partitions = 32 
        df_cleaned = df_cleaned.repartition(num_partitions)
        print(f"DEBUG: Repartitioned to {num_partitions} partitions")

        print("DEBUG: Starting mapInPandas processing")
        processed_df = df_cleaned.mapInPandas(process_pandas_partition, schema=PROCESSED_URL_SCHEMA)

        # For counting, we can use a different approach to avoid caching
        print("DEBUG: Processing and writing data without caching")

        # Show sample processed data first (this will trigger some computation)
        print("DEBUG: Sample processed data:")
        processed_df.show(5, truncate=False)

        print(f"DEBUG: Writing Parquet to {output_path}")
        processed_df.write.mode("overwrite").parquet(output_path)
        print("DEBUG: Parquet write completed")

        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # csv_path = output_path.replace('.parquet', '.csv')
        # print(f"DEBUG: Writing CSV to {csv_path}")
        # processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
        # print("DEBUG: CSV write completed")

        try:
            print("DEBUG: Writing to database")
            processed_df.write \
                .format("jdbc") \
                .option("url", settings.JDBC_URL) \
                .option("dbtable", settings.URLS_TABLE) \
                .option("user", settings.POSTGRES_USER) \
                .option("password", settings.POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .option("numPartitions", "8") \
                .mode("ignore") \
                .save()
            print("DEBUG: Database write completed")
        except Exception as e:
            print(f"DEBUG: Database write failed: {e}")
            raise
            
        print("DEBUG: Processing completed successfully")

    except Exception as e:
        print(f"DEBUG: Error in process_tsv_file: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit url_processor.py <input_path> <output_path>")
        sys.exit(1)

    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    print(f"DEBUG: Input file: {input_file_path}")
    print(f"DEBUG: Output file: {output_file_path}")

    if not os.path.exists(input_file_path):
        print(f"Error: Input file does not exist: {input_file_path}")
        sys.exit(1)

    spark_session = get_spark_session("URLBatchProcessor")
    try:
        process_tsv_file(spark_session, input_file_path, output_file_path)
    finally:
        spark_session.stop()