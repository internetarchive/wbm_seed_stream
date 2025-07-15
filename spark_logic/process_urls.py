import os
import sys
import glob
import shutil
import subprocess
import json
from collections import defaultdict
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '''..''')))

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (BooleanType, FloatType, IntegerType, StringType,
                               StructField, StructType, TimestampType, LongType)
from spark_logic.score_urls import score_urls_batch
from spark_logic.rank_domains import DomainRanker
from spark_logic.create_summary import create_summary
from spark.config.spark_config import (
    SparkConfig,
    get_spark_session
)
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

summary_stats = {
    'total_urls': 0,
    'spam_urls': 0,
    'benign_urls': 0,
    'domains_processed': set(),
    'score_sum': 0.0,
    'confidence_sum': 0.0
}

def create_domain_ranker():
    try:
        return DomainRanker()
    except Exception as e:
        print(f"FATAL ERROR: Failed to initialize DomainRanker: {e}")
        raise

def clear_good_data_folder():
    if os.path.exists(SparkConfig.GOOD_DATA_FOLDER):
        try:
            shutil.rmtree(SparkConfig.GOOD_DATA_FOLDER)
        except Exception as e:
            print(f"ERROR: Failed to remove existing good data folder: {e}")
            raise

    try:
        os.makedirs(SparkConfig.GOOD_DATA_FOLDER, exist_ok=True)
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
    if not os.path.exists(SparkConfig.GOOD_DATA_FOLDER):
        print(f"WARNING: Good data folder does not exist: {SparkConfig.GOOD_DATA_FOLDER}")
        return None

    tsv_files = glob.glob(os.path.join(SparkConfig.GOOD_DATA_FOLDER, "*.tsv"))
    if not tsv_files:
        print(f"WARNING: No TSV files found in {SparkConfig.GOOD_DATA_FOLDER}")
        return None

    try:
        good_df = spark.read.csv(tsv_files, sep='\t', schema=StructType([
            StructField("timestamp", LongType(), True),
            StructField("url", StringType(), True)
        ]), header=False)
        return good_df
    except Exception as e:
        print(f"ERROR: Failed to read good data files with Spark: {e}")
        return None

def aggregate_domain_stats_vectorized(results_df: pd.DataFrame) -> dict:
    if results_df.empty:
        return {}

    grouped = results_df.groupby('domain').agg({
        'score': 'mean',
        'is_spam': ['sum', 'count'],
        'data_source': 'first'
    }).reset_index()

    grouped.columns = ['domain', 'avg_score', 'malicious_count', 'total_count', 'data_source']

    domain_aggregates = {}
    for _, row in grouped.iterrows():
        domain = row['domain']
        avg_score = row['avg_score']
        malicious_count = int(row['malicious_count'])
        total_count = int(row['total_count'])
        data_source = row['data_source']

        if data_source == SparkConfig.GOOD_DATA_LABEL:
            reputation_score = max(0.5, avg_score)
        else:
            reputation_score = avg_score

        domain_aggregates[domain] = {
            'reputation_score': reputation_score,
            'total_urls': total_count,
            'malicious_urls': malicious_count,
            'benign_urls': total_count - malicious_count
        }

    return domain_aggregates

def update_summary_stats(results_df: pd.DataFrame):
    global summary_stats
    if results_df.empty:
        return
    
    summary_stats['total_urls'] += len(results_df)
    summary_stats['spam_urls'] += results_df['is_spam'].sum()
    summary_stats['benign_urls'] += (~results_df['is_spam']).sum()
    summary_stats['domains_processed'].update(results_df['domain'].unique())
    summary_stats['score_sum'] += results_df['score'].sum()
    summary_stats['confidence_sum'] += results_df['confidence'].sum()

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

    processed_dfs = []

    for pandas_df in iterator:
        if len(pandas_df) == 0:
            continue

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
                continue

            results_df = score_urls_batch(clean_df, all_domain_reputations)

            if results_df.empty:
                continue

            update_summary_stats(results_df)
            processed_dfs.append(results_df)

            batch_updates = aggregate_domain_stats_vectorized(results_df)
            for domain, stats in batch_updates.items():
                partition_domain_updates[domain]['reputation_score'] += stats['reputation_score'] * stats['total_urls']
                partition_domain_updates[domain]['total_urls'] += stats['total_urls']
                partition_domain_updates[domain]['malicious_urls'] += stats['malicious_urls']
                partition_domain_updates[domain]['benign_urls'] += stats['benign_urls']

        except Exception as e:
            print(f"ERROR: Failed to process pandas partition: {e}")
            import traceback
            traceback.print_exc()
            continue

    if processed_dfs:
        final_df = pd.concat(processed_dfs, ignore_index=True)

        current_time = pd.Timestamp.now()
        for field in PROCESSED_URL_SCHEMA:
            if field.name not in final_df.columns:
                if field.name == "received_at":
                    final_df[field.name] = current_time
                elif field.name == "data_source":
                    final_df[field.name] = "input_data"
                else:
                    final_df[field.name] = None

        output_df = final_df[[field.name for field in PROCESSED_URL_SCHEMA]]

        if not pd.api.types.is_datetime64_any_dtype(output_df["timestamp"]):
            output_df["timestamp"] = pd.to_datetime(output_df["timestamp"], errors='coerce', utc=True)

        if not pd.api.types.is_datetime64_any_dtype(output_df["received_at"]):
            output_df["received_at"] = pd.to_datetime(output_df["received_at"], errors='coerce', utc=True)

        output_df = output_df.dropna(subset=['url', 'domain'])

        if not output_df.empty:
            yield output_df

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
            print(f"Domain updates written to: {temp_file}")
        except Exception as e:
            print(f"Failed to write domain updates: {e}")

def collect_domain_updates_from_temp_files():
    import tempfile
    import glob
    import os
    
    temp_dir = tempfile.gettempdir()
    update_files = glob.glob(os.path.join(temp_dir, "domain_updates_*.json"))
    
    all_updates = defaultdict(lambda: {
        'reputation_score': 0.0,
        'total_urls': 0,
        'malicious_urls': 0,
        'benign_urls': 0
    })
    
    for file_path in update_files:
        try:
            with open(file_path, 'r') as f:
                updates = json.load(f)
            
            for domain, stats in updates.items():
                all_updates[domain]['reputation_score'] += stats['reputation_score'] * stats['total_urls']
                all_updates[domain]['total_urls'] += stats['total_urls']
                all_updates[domain]['malicious_urls'] += stats['malicious_urls']
                all_updates[domain]['benign_urls'] += stats['benign_urls']
            
            os.remove(file_path)
            
        except Exception as e:
            print(f"Failed to process update file {file_path}: {e}")
    
    for domain, stats in all_updates.items():
        if stats['total_urls'] > 0:
            stats['reputation_score'] = stats['reputation_score'] / stats['total_urls']
    
    return dict(all_updates)

def generate_summary_from_stats(output_path: str, job_output_base_dir: str):
    global summary_stats
    
    try:
        summary_data = {
            'total_urls_processed': summary_stats['total_urls'],
            'spam_urls': summary_stats['spam_urls'],
            'benign_urls': summary_stats['benign_urls'],
            'unique_domains': len(summary_stats['domains_processed']),
            'average_score': summary_stats['score_sum'] / summary_stats['total_urls'] if summary_stats['total_urls'] > 0 else 0.0,
            'average_confidence': summary_stats['confidence_sum'] / summary_stats['total_urls'] if summary_stats['total_urls'] > 0 else 0.0,
            'spam_percentage': (summary_stats['spam_urls'] / summary_stats['total_urls'] * 100) if summary_stats['total_urls'] > 0 else 0.0
        }
        
        summary_file_path = os.path.join(job_output_base_dir, "job_summary.json")
        os.makedirs(os.path.dirname(summary_file_path), exist_ok=True)
        
        with open(summary_file_path, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        print(f"Job summary saved to: {summary_file_path}")
        print(f"Summary: {summary_data}")
        
    except Exception as e:
        print(f"ERROR: Failed to generate summary from stats: {e}")
        raise

def process_tsv_file(spark: SparkSession, input_path: str, output_path: str):
    global summary_stats
    summary_stats = {
        'total_urls': 0,
        'spam_urls': 0,
        'benign_urls': 0,
        'domains_processed': set(),
        'score_sum': 0.0,
        'confidence_sum': 0.0
    }
    
    if not os.path.exists(input_path):
        print(f"ERROR: Input file does not exist: {input_path}")
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    profiler = get_active_profiler()

    try:
        good_df = None
        if SparkConfig.USE_GOOD_DATA:
            with profile_spark_stage(profiler, "good_data_generation"):
                print("Generating and loading good data...")
                spark.sparkContext.setJobDescription("Loading good data")
                try:
                    clear_good_data_folder()
                    generate_good_data()
                    good_df = load_good_data_urls(spark)
                    if good_df:
                        good_df = good_df.withColumn("data_source", lit(SparkConfig.GOOD_DATA_LABEL))
                        good_df = good_df.repartition(spark.sparkContext.defaultParallelism)
                        print(f"Successfully loaded and repartitioned good data.")
                        if profiler:
                            log_spark_dataframe_operation(profiler, "good_data_load", good_df.count(), spark.sparkContext.defaultParallelism)
                except Exception as e:
                    print(f"ERROR: Failed to create or union good data DataFrame: {e}")
                    raise

        with profile_spark_stage(profiler, "tsv_data_loading"):
            print(f"Loading TSV data from: {input_path}")
            spark.sparkContext.setJobDescription("Loading TSV data with settings")
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

                df_raw = df_raw.repartition(spark.sparkContext.defaultParallelism * 2)

                if profiler:
                    log_spark_dataframe_operation(profiler, "raw_data_load", df_raw.count(), spark.sparkContext.defaultParallelism * 2)

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
                if profiler:
                    log_spark_dataframe_operation(profiler, "union_with_good_data", df_raw.count())
            except Exception as e:
                print(f"ERROR: Failed to union good data DataFrame: {e}")
                raise

        with profile_spark_stage(profiler, "data_filtering"):
            try:
                print("Applying minimal filtering...")
                spark.sparkContext.setJobDescription("Minimal URL filtering")
                df_cleaned = df_raw.filter(col("url").isNotNull() & (col("url") != ""))
                df_cleaned = df_cleaned.repartition(spark.sparkContext.defaultParallelism)

                if profiler:
                    log_spark_dataframe_operation(profiler, "filtered_data", df_cleaned.count(), spark.sparkContext.defaultParallelism)
                    log_memory_checkpoint(profiler, "after_filtering")

            except Exception as e:
                print(f"ERROR: Failed to filter DataFrame: {e}")
                raise

        with profile_spark_stage(profiler, "domain_reputation_loading"):
            print("Loading domain reputation data...")
            spark.sparkContext.setJobDescription("Loading domain reputation data")
            try:
                domain_ranker = create_domain_ranker()
                all_domain_reputations = domain_ranker.get_all_domain_reputations()
                broadcast_domain_reputations = spark.sparkContext.broadcast(all_domain_reputations)

                if profiler:
                    log_database_operation(profiler, "load_domain_reputations", "domain_reputation", len(all_domain_reputations))
                    log_memory_checkpoint(profiler, "after_domain_load")

            except Exception as e:
                print(f"ERROR: Failed to load domain reputation data: {e}")
                raise

        with profile_spark_stage(profiler, "url_scoring_processing"):
            print("Processing URLs with scoring...")
            spark.sparkContext.setJobDescription("URL scoring")
            try:
                processed_df = df_cleaned.mapInPandas(
                    lambda iterator: process_pandas_partition_optimized(iterator, broadcast_domain_reputations),
                    schema=PROCESSED_URL_SCHEMA
                )

                processed_df = processed_df.repartition(spark.sparkContext.defaultParallelism)

                if profiler:
                    log_spark_dataframe_operation(profiler, "url_scoring", processed_df.count(), spark.sparkContext.defaultParallelism)
                    log_memory_checkpoint(profiler, "after_url_scoring")

            except Exception as e:
                print(f"ERROR: Failed to process URLs: {e}")
                raise

        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
        except Exception as e:
            print(f"ERROR: Failed to create output directory: {e}")
            raise

        with profile_spark_stage(profiler, "parquet_output"):
            print(f"Writing processed URLs to: {output_path}")
            spark.sparkContext.setJobDescription("Writing processed URLs to parquet")
            try:
                (processed_df
                 .write
                 .mode("overwrite")
                 .option("compression", "snappy")
                 .option("maxRecordsPerFile", 500000)
                 .parquet(output_path))

                if profiler:
                    total_size = 0
                    if os.path.exists(output_path):
                        for root, dirs, files in os.walk(output_path):
                            for file in files:
                                total_size += os.path.getsize(os.path.join(root, file))
                    log_file_operation(profiler, "write", output_path, total_size)

            except Exception as e:
                print(f"ERROR: Failed to write parquet file: {e}")
                raise

        with profile_spark_stage(profiler, "domain_reputation_updates"):
            print("Collecting and updating domain reputations...")
            
            accumulated_updates = collect_domain_updates_from_temp_files()
            
            if accumulated_updates:
                print(f"Updating {len(accumulated_updates)} domains...")
                domain_ranker.update_domain_reputation_batch(accumulated_updates)
                print("Domain reputation updates completed successfully")

                if profiler:
                    log_domain_stats_update(profiler, len(accumulated_updates), "batch_update")
                    log_database_operation(profiler, "update_domain_reputations", "domain_reputation", len(accumulated_updates))
            else:
                print("No domain updates to apply")

        try:
            if 'broadcast_domain_reputations' in locals():
                broadcast_domain_reputations.unpersist(blocking=False)
        except Exception as e:
            print(f"WARNING: Failed to unpersist broadcast variables: {e}")

        print("Processing completed successfully")
        
        return True

    except Exception as e:
        print(f"FATAL ERROR in process_tsv_file: {e}")
        import traceback
        traceback.print_exc()
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
        print("Generating job summary...")
        try:
            job_output_base_dir = os.path.dirname(os.path.dirname(output_path))
            generate_summary_from_stats(output_path, job_output_base_dir)
            print("Job summary generation completed")
        except Exception as e:
            print(f"WARNING: Job summary generation failed: {e}")

    except Exception as e:
        print(f"FATAL ERROR: Processing failed: {e}")
        sys.exit(1)
    finally:
        if spark_session:
            try:
                print("Stopping Spark session...")
                spark_session.stop()
                print("Spark session stopped")
            except Exception as e:
                print(f"WARNING: Failed to stop Spark session: {e}")

    print("Script completed successfully")