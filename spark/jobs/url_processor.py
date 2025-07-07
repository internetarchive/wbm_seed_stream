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
from services.url_scoring_service import process_batch_worker_optimized

PROCESSED_URL_SCHEMA = StructType([
    StructField("url", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("domain", StringType(), True),
    StructField("classification_score", FloatType(), True),
    StructField("confidence", FloatType(), True),
    StructField("reasons", StringType(), True),
    StructField("domain_frequency", IntegerType(), True),
    StructField("domain_frequency_pct", FloatType(), True),
    StructField("is_spam", BooleanType(), True),
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
                    "classification_score": result.classification_score,
                    "confidence": result.confidence,
                    "reasons": reasons_json,
                    "domain_frequency": result.domain_frequency,
                    "domain_frequency_pct": result.domain_frequency_pct,
                    "is_spam": result.is_spam,
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
                timestamp_val = row['timestamp']
                url_val = row['url']
                
                if pd.isna(timestamp_val) or pd.isna(url_val):
                    continue
                
                timestamp_clean = str(timestamp_val).strip()
                url_clean = str(url_val).strip()
                
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

        if "received_at" in output_pandas_df.columns:
            try:
                output_pandas_df["received_at"] = pd.to_datetime(output_pandas_df["received_at"], errors='coerce')
            except Exception as e:
                print(f"DEBUG: received_at timestamp conversion failed: {e}")
                output_pandas_df["received_at"] = pd.Timestamp.now()
        
        if "timestamp" in output_pandas_df.columns:
            try:
                if not pd.api.types.is_datetime64_any_dtype(output_pandas_df["timestamp"]):
                    numeric_timestamps = pd.to_numeric(output_pandas_df["timestamp"], errors='coerce')
                    valid_mask = ~pd.isna(numeric_timestamps)
                    if valid_mask.any():
                        try:
                            output_pandas_df.loc[valid_mask, "timestamp"] = pd.to_datetime(
                                numeric_timestamps[valid_mask], unit='ms', errors='coerce'
                            )
                        except:
                            try:
                                output_pandas_df.loc[valid_mask, "timestamp"] = pd.to_datetime(
                                    numeric_timestamps[valid_mask], unit='s', errors='coerce'
                                )
                            except:
                                output_pandas_df["timestamp"] = pd.Timestamp.now()
                    else:
                        output_pandas_df["timestamp"] = pd.Timestamp.now()
            except Exception as e:
                print(f"DEBUG: timestamp conversion failed: {e}")
                output_pandas_df["timestamp"] = pd.Timestamp.now()

        output_pandas_df = output_pandas_df.dropna(subset=['url', 'domain'])
        
        if len(output_pandas_df) == 0:
            continue

        yield output_pandas_df

def set_job_description(spark, description, group_id="url_processing"):
    try:
        spark.sparkContext.setJobGroup(group_id, description)
        spark.sparkContext.setJobDescription(description)
    except Exception as e:
        print(f"DEBUG: Failed to set job description: {e}")
        
def process_tsv_file(spark: SparkSession, input_path: str, output_path: str):
    try:
        print(f"DEBUG: Starting processing of {input_path}")
        print(f"DEBUG: Output path: {output_path}")
        input_filename = os.path.basename(input_path)
        set_job_description(spark, f"Reading TSV file: {input_filename}")
        df_raw = spark.read.option("delimiter", "\t").csv(input_path, header=False)
        if len(df_raw.columns) >= 2:
            df_raw = df_raw.withColumnRenamed("_c0", "timestamp").withColumnRenamed("_c1", "url")
        else:
            print("ERROR: TSV file doesn't have expected number of columns")
            return
        set_job_description(spark, f"Analyzing data structure and quality: {input_filename}")
        print(f"DEBUG: Raw DataFrame created with {df_raw.count()} rows")
        print(f"DEBUG: DataFrame columns: {df_raw.columns}")
        print("DEBUG: Sample data:")
        df_raw.show(5, truncate=False)
        set_job_description(spark, f"Cleaning and filtering data: {input_filename}")
        df_cleaned = df_raw.filter(
            col("url").isNotNull() &
            (col("url") != "") &
            col("timestamp").isNotNull() &
            (col("timestamp") != "")
        )
        total_rows = df_cleaned.count()
        print(f"DEBUG: After cleaning: {total_rows} rows")
        if total_rows == 0:
            print("DEBUG: No rows after cleaning, creating empty output")
            set_job_description(spark, f"Creating empty output for: {input_filename}")
            empty_df = spark.createDataFrame([], PROCESSED_URL_SCHEMA)
            empty_df.write.mode("overwrite").parquet(output_path)
            return
        set_job_description(spark, f"Optimizing data partitioning: {input_filename}")
        num_partitions = min(32, max(1, total_rows // 1000))
        df_cleaned = df_cleaned.repartition(num_partitions)
        print(f"DEBUG: Repartitioned to {num_partitions} partitions")
        set_job_description(spark, f"Processing URLs with ML scoring: {input_filename}")
        print("DEBUG: Starting mapInPandas processing")
        processed_df = df_cleaned.mapInPandas(process_pandas_partition, schema=PROCESSED_URL_SCHEMA)
        set_job_description(spark, f"Validating processed data: {input_filename}")
        print("DEBUG: Sample processed data:")
        processed_df.show(5, truncate=False)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        set_job_description(spark, f"Writing processed data to Parquet: {input_filename}")
        print(f"DEBUG: Writing Parquet to {output_path}")
        processed_df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
        print("DEBUG: Parquet write completed")
        try:
            set_job_description(spark, f"Writing analyzed URLs to database: {input_filename}")
            print("DEBUG: Writing to database")
            df_to_write = processed_df.select(
                col("url"),
                col("timestamp"),
                col("classification_score"),
                col("confidence"),
                col("reasons")
            )
            df_to_write.write \
                .format("jdbc") \
                .option("url", settings.JDBC_URL) \
                .option("dbtable", "analyzed_urls") \
                .option("user", settings.POSTGRES_USER) \
                .option("password", settings.POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .option("numPartitions", "8") \
                .mode("append") \
                .save()
            print("DEBUG: Database write completed")
        except Exception as e:
            print(f"DEBUG: Database write failed (continuing anyway): {e}")
        spark.sparkContext.setJobGroup("", "")
        spark.sparkContext.setJobDescription("")
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
    input_filename = os.path.basename(input_file_path)
    spark_session.sparkContext.setJobGroup("url_processing", f"URL Processing Pipeline: {input_filename}")
    spark_session.sparkContext.setJobDescription(f"Initializing URL processing for: {input_filename}")
    try:
        process_tsv_file(spark_session, input_file_path, output_file_path)
    finally:
        spark_session.sparkContext.setJobGroup("", "")
        spark_session.sparkContext.setJobDescription("")
        spark_session.stop()
