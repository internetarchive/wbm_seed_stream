import os
import sys
import json
from collections import defaultdict
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '''..''')))

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (BooleanType, FloatType, IntegerType, StringType,
                               StructField, StructType, TimestampType, LongType)
from pyspark import StorageLevel
from spark_logic.score_urls import score_urls_batch
from spark_logic.rank_domains import DomainRanker
from writers.database_writer import write_to_database
from writers.parquet_writer import write_to_parquet
from writers.summary_writer import write_summary
from writers.good_data_handler import handle_good_data
from writers.domain_processor import create_domain_ranker, aggregate_domain_stats_vectorized, collect_domain_updates_from_temp_files, update_domain_reputations, cleanup_temp_files
from spark.config.spark_config import (
    SparkConfig,
    get_spark_session
)

from schema import PROCESSED_URL_SCHEMA
from testing.profiler_integration import (
    get_active_profiler,
    profile_spark_stage,
    log_domain_stats_update,
    log_spark_dataframe_operation,
    log_file_operation,
    log_database_operation,
    log_memory_checkpoint
)

RAW_INPUT_SCHEMA = StructType([
    StructField("timestamp", LongType(), True),
    StructField("url", StringType(), True),
    StructField("data_source", StringType(), True)
])

def execute_with_job_name(spark, job_name, action_func):
    print(f"\n=== EXECUTING: {job_name} ===")
    start_time = time.time()
    spark.sparkContext.setJobDescription(job_name)
    result = action_func()
    duration = time.time() - start_time
    print(f"=== COMPLETED: {job_name} in {duration:.2f}s ===\n")
    return result

def process_pandas_partition_optimized(iterator, broadcast_domain_reputations):
    try:
        all_domain_reputations = broadcast_domain_reputations.value
    except Exception as e:
        print(f"ERROR: Failed to get broadcast domain reputations: {e}")
        raise

    partition_domain_updates = defaultdict(lambda: {
        'reputation_score': 0.0,
        'total_urls': 0,
        'malicious_urls': 0,
        'benign_urls': 0
    })

    for pandas_df in iterator:
        if pandas_df is None or len(pandas_df) == 0:
            continue

        try:
            memory_usage = pandas_df.memory_usage(deep=True).sum()
            if memory_usage > 10_000_000:
                chunk_size = max(50, len(pandas_df) // 50)
                for i in range(0, len(pandas_df), chunk_size):
                    chunk = pandas_df.iloc[i:i+chunk_size].copy()
                    try:
                        yield from process_single_chunk(chunk, all_domain_reputations, partition_domain_updates)
                    except Exception as e:
                        print(f"ERROR: Failed to process chunk: {e}")
                        continue
            else:
                yield from process_single_chunk(pandas_df, all_domain_reputations, partition_domain_updates)

        except Exception as e:
            print(f"ERROR: Failed to process pandas partition: {e}")
            continue

    save_domain_updates(partition_domain_updates)

def process_single_chunk(pandas_df, all_domain_reputations, partition_domain_updates):
    try:
        is_good_data = pandas_df['data_source'] == SparkConfig.GOOD_DATA_LABEL
        good_mask = is_good_data & pandas_df['url'].notna() & (pandas_df['url'].str.len() > 0)
        input_mask = (~is_good_data &
                     pandas_df['url'].notna() &
                     pandas_df['timestamp'].notna() &
                     (pandas_df['url'].str.len() > 5) &
                     pandas_df['url'].str.startswith(('http://', 'https://')))

        combined_mask = good_mask | input_mask
        clean_df = pandas_df[combined_mask].copy()

        if len(clean_df) == 0:
            return

        clean_df = clean_df[clean_df['url'].str.len() <= 1000]

        if len(clean_df) == 0:
            return

        results_df = score_urls_batch(clean_df, all_domain_reputations)

        if results_df.empty:
            return

        results_df = validate_and_clean_results(results_df)

        if results_df.empty:
            return

        batch_updates = aggregate_domain_stats_vectorized(results_df)
        for domain, stats in batch_updates.items():
            partition_domain_updates[domain]['reputation_score'] += stats['reputation_score'] * stats['total_urls']
            partition_domain_updates[domain]['total_urls'] += stats['total_urls']
            partition_domain_updates[domain]['malicious_urls'] += stats['malicious_urls']
            partition_domain_updates[domain]['benign_urls'] += stats['benign_urls']

        yield results_df

    except Exception as e:
        print(f"ERROR: Failed to process chunk: {e}")

def validate_and_clean_results(results_df):
    try:
        current_time = pd.Timestamp.now()
        for field in PROCESSED_URL_SCHEMA:
            if field.name not in results_df.columns:
                if field.name == "received_at":
                    results_df[field.name] = current_time
                elif field.name == "data_source":
                    results_df[field.name] = "input_data"
                else:
                    results_df[field.name] = None

        output_df = results_df[[field.name for field in PROCESSED_URL_SCHEMA]]

        for col in output_df.columns:
            if output_df[col].dtype == 'object':
                output_df[col] = output_df[col].astype(str)
                output_df[col] = output_df[col].str.slice(0, 1000)
                output_df[col] = output_df[col].replace('nan', None)

        if not pd.api.types.is_datetime64_any_dtype(output_df["timestamp"]):
            output_df["timestamp"] = pd.to_datetime(output_df["timestamp"], errors='coerce', utc=True)

        if not pd.api.types.is_datetime64_any_dtype(output_df["received_at"]):
            output_df["received_at"] = pd.to_datetime(output_df["received_at"], errors='coerce', utc=True)

        output_df = output_df.dropna(subset=['url', 'domain'])

        return output_df

    except Exception as e:
        print(f"ERROR: Failed to validate results: {e}")
        return pd.DataFrame()

def save_domain_updates(partition_domain_updates):
    for domain, stats in partition_domain_updates.items():
        if stats['total_urls'] > 0:
            stats['reputation_score'] = stats['reputation_score'] / stats['total_urls']

    if partition_domain_updates:
        import tempfile
        import os

        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, f"domain_updates_{os.getpid()}_{hash(str(partition_domain_updates))}.json")

        try:
            with open(temp_file, 'w') as f:
                json.dump(dict(partition_domain_updates), f)
        except Exception as e:
            print(f"Failed to write domain updates: {e}")

def process_tsv_file(spark: SparkSession, input_path: str, output_path: str):
    if not os.path.exists(input_path):
        print(f"ERROR: Input file does not exist: {input_path}")
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    profiler = get_active_profiler()

    try:
        good_df = None
        if SparkConfig.USE_GOOD_DATA:
            with profile_spark_stage(profiler, "good_data_generation"):
                print("Generating and loading good data...")
                try:
                    good_df = handle_good_data(spark)
                    if good_df:
                        good_df = good_df.withColumn("data_source", lit(SparkConfig.GOOD_DATA_LABEL))
                        
                        execute_with_job_name(spark, "STEP_1A_GOOD_DATA_COUNT", 
                                            lambda: good_df.count())
                        
                        good_df = good_df.repartition(spark.sparkContext.defaultParallelism // 2)
                        print(f"Successfully loaded and repartitioned good data.")
                        
                        if profiler:
                            log_spark_dataframe_operation(profiler, "good_data_load", good_df.count(), spark.sparkContext.defaultParallelism)
                except Exception as e:
                    print(f"ERROR: Failed to create or union good data DataFrame: {e}")
                    raise

        with profile_spark_stage(profiler, "tsv_data_loading"):
            print(f"Loading TSV data from: {input_path}")
            try:
                if profiler and os.path.exists(input_path):
                    file_size = os.path.getsize(input_path)
                    log_file_operation(profiler, "read", input_path, file_size)

                df_raw = (spark.read
                         .option("delimiter", "\t")
                         .option("multiline", "false")
                         .option("escape", "")
                         .option("quote", "")
                         .schema(StructType([
                             StructField("timestamp", LongType(), True),
                             StructField("url", StringType(), True)
                         ]))
                         .csv(input_path, header=False))

                df_raw = df_raw.repartition(spark.sparkContext.defaultParallelism)

                raw_count = execute_with_job_name(spark, "STEP_1B_RAW_DATA_COUNT", 
                                                lambda: df_raw.count())
                
                print(f"Loaded {raw_count} raw records from TSV")

                if profiler:
                    log_spark_dataframe_operation(profiler, "raw_data_load", raw_count, spark.sparkContext.defaultParallelism * 2)

            except Exception as e:
                print(f"ERROR: Failed to read TSV file: {e}")
                raise

        try:
            df_raw = df_raw.withColumn("data_source", lit("input_data"))
        except Exception as e:
            print(f"ERROR: Failed to transform raw DataFrame: {e}")
            raise

        if good_df:
            try:
                df_raw = df_raw.unionByName(good_df)
                
                union_count = execute_with_job_name(spark, "STEP_2_UNION_COUNT", 
                                                  lambda: df_raw.count())
                
                print(f"After union with good data: {union_count} total records")
                
                if profiler:
                    log_spark_dataframe_operation(profiler, "union_with_good_data", union_count)
            except Exception as e:
                print(f"ERROR: Failed to union good data DataFrame: {e}")
                raise

        with profile_spark_stage(profiler, "data_filtering"):
            try:
                print("Applying minimal filtering...")
                df_cleaned = df_raw.filter(col("url").isNotNull() & (col("url") != ""))
                df_cleaned = df_cleaned.repartition(spark.sparkContext.defaultParallelism // 2)

                print("Counting filtered records...")
                initial_count = execute_with_job_name(spark, "STEP_3A_FILTERED_COUNT", 
                                                    lambda: df_cleaned.count())
                
                print("Removing duplicate URLs...")
                df_cleaned = df_cleaned.dropDuplicates(["url", "data_source"])
                
                final_count = execute_with_job_name(spark, "STEP_3B_DEDUPED_COUNT", 
                                                  lambda: df_cleaned.count())
                
                duplicates_removed = initial_count - final_count
                print(f"Removed {duplicates_removed} duplicate URLs ({initial_count} -> {final_count})")

                if profiler:
                    log_spark_dataframe_operation(profiler, "filtered_data", final_count, spark.sparkContext.defaultParallelism)
                    log_memory_checkpoint(profiler, "after_filtering")

            except Exception as e:
                print(f"ERROR: Failed to filter DataFrame: {e}")
                raise

        domain_ranker = create_domain_ranker()

        if SparkConfig.READ_REPUTATION:
            with profile_spark_stage(profiler, "domain_reputation_loading"):
                print("Loading domain reputation data...")
                try:
                    all_domain_reputations = domain_ranker.get_all_domain_reputations()
                    broadcast_domain_reputations = spark.sparkContext.broadcast(all_domain_reputations)

                    if profiler:
                        log_database_operation(profiler, "load_domain_reputations", "domain_reputation", len(all_domain_reputations))
                        log_memory_checkpoint(profiler, "after_domain_load")

                except Exception as e:
                    print(f"ERROR: Failed to load domain reputation data: {e}")
                    raise
        else:
            print("Skipping domain reputation loading (READ_REPUTATION=False)")
            broadcast_domain_reputations = spark.sparkContext.broadcast({})

        with profile_spark_stage(profiler, "url_scoring_processing"):
            print("Processing URLs with scoring...")
            try:
                processed_df = df_cleaned.mapInPandas(
                    lambda iterator: process_pandas_partition_optimized(iterator, broadcast_domain_reputations),
                    schema=PROCESSED_URL_SCHEMA
                )
                
                processed_df = processed_df.coalesce(12)
                processed_df.persist(StorageLevel.MEMORY_AND_DISK)
                
                processed_count = execute_with_job_name(spark, "STEP_4_URL_SCORING_AND_COUNT", 
                                                      lambda: processed_df.count())
                
                print(f"Processed and scored {processed_count} URLs")
                
                if profiler:
                    log_spark_dataframe_operation(profiler, "url_scoring", processed_count, 12)
                    log_memory_checkpoint(profiler, "after_url_scoring")

            except Exception as e:
                print(f"ERROR: Failed to process URLs: {e}")
                raise

        try:
            if SparkConfig.WRITE_PARQUET:
                with profile_spark_stage(profiler, "parquet_output"):
                    try:
                        execute_with_job_name(spark, "STEP_5A_WRITE_PARQUET", 
                                            lambda: write_to_parquet(processed_df, output_path))
                    except Exception as e:
                        print(f"ERROR: Failed to write parquet file: {e}")
                        raise
            else:
                print("Skipping parquet output (WRITE_PARQUET=False)")

            if SparkConfig.WRITE_DB:
                with profile_spark_stage(profiler, "database_output"):
                    try:
                        from writers.database_writer import write_to_database
                        execute_with_job_name(spark, "STEP_5B_WRITE_DATABASE", 
                                            lambda: write_to_database(processed_df))
                        print("Database write completed successfully")
                    except Exception as e:
                        print(f"ERROR: Failed to write to database: {e}")
                        raise
            else:
                print("Skipping database output (WRITE_DB=False)")

            if SparkConfig.WRITE_SUMMARY:
                with profile_spark_stage(profiler, "summary_generation"):
                    try:
                        execute_with_job_name(spark, "STEP_5C_WRITE_SUMMARY", 
                                            lambda: write_summary(processed_df, output_path))
                        print("Summary generation completed")
                    except Exception as e:
                        print(f"WARNING: Summary generation failed: {e}")
            else:
                print("Skipping summary generation (WRITE_SUMMARY=False)")

        finally:
            processed_df.unpersist()

        print("Processing completed successfully")
        return True

    except Exception as e:
        print(f"FATAL ERROR in process_tsv_file: {e}")
        import traceback
        traceback.print_exc()
        raise

def write_partition_to_database(partition, batch_size):
    try:
        from writers.database_writer import insert_batch
        
        batch = []
        for row in partition:
            batch.append(row)
            if len(batch) >= batch_size:
                insert_batch(batch)  
                batch = []
        
        if batch:
            insert_batch(batch)
                
    except Exception as e:
        print(f"ERROR: Failed to write partition to database: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python process_urls.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    print(f"Starting URL processing: {input_path} -> {output_path}")

    spark_session = None
    try:
        spark_session = get_spark_session()
    except Exception as e:
        print(f"FATAL ERROR: Failed to get Spark session: {e}")
        sys.exit(1)

    try:
        process_tsv_file(spark_session, input_path, output_path)
    except Exception as e:
        print(f"FATAL ERROR: Processing failed: {e}")
        sys.exit(1)
    finally:
        if spark_session:
            try:
                spark_session.stop()
            except Exception as e:
                print(f"WARNING: Failed to stop Spark session: {e}")

    print("Script completed successfully")