import os
import sys
import json
from collections import defaultdict
import time
from functools import partial

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_unixtime, current_timestamp
from pyspark.sql.types import (StringType, StructField, StructType, LongType)
from pyspark import StorageLevel
from spark_logic.score import score_urls
from writers.parquet_writer import write_to_parquet
from writers.summary_writer import write_summary
from writers.good_data_handler import handle_good_data
from writers.domain_processor import create_domain_ranker, aggregate_domain_stats_vectorized
from spark.config.spark_config import SparkConfig, get_spark_session
from schema import PROCESSED_URL_SCHEMA
from models.code.lightgbm import URLArchivalClassifier
from testing.profiler_integration import (
    get_active_profiler,
    profile_spark_stage,
    log_spark_dataframe_operation,
    log_file_operation,
    log_memory_checkpoint
)


def execute_with_job_name(spark, job_name, action_func):
    print(f"\n=== EXECUTING: {job_name} ===")
    start_time = time.time()
    spark.sparkContext.setJobDescription(job_name)
    result = action_func()
    duration = time.time() - start_time
    print(f"=== COMPLETED: {job_name} in {duration:.2f}s ===\n")
    return result


def process_pandas_partition_optimized(iterator, model_path: str):
    domain_ranker = create_domain_ranker()
    all_domain_reputations = domain_ranker.get_all_domain_reputations() if SparkConfig.READ_REPUTATION else {}

    partition_domain_updates = defaultdict(lambda: {
        'reputation_score': 0.0, 'total_urls': 0, 'malicious_urls': 0, 'benign_urls': 0
    })

    for pandas_df in iterator:
        if pandas_df is None or pandas_df.empty:
            continue

        try:
            results_df = score_urls(pandas_df, all_domain_reputations, model_path)
            results_df = validate_and_clean_results(results_df)

            if results_df.empty:
                raise ValueError("Scoring and validation returned empty dataframe")

            batch_updates = aggregate_domain_stats_vectorized(results_df)
            for domain, stats in batch_updates.items():
                partition_domain_updates[domain]['reputation_score'] += stats['reputation_score'] * stats[
                    'total_urls']
                partition_domain_updates[domain]['total_urls'] += stats['total_urls']
                partition_domain_updates[domain]['malicious_urls'] += stats['malicious_urls']
                partition_domain_updates[domain]['benign_urls'] += stats['benign_urls']

            yield results_df

        except Exception as e:
            print(f"ERROR: Failed to process partition for model {model_path}, creating fallback: {e}")
            try:
                fallback_df = pd.DataFrame(index=pandas_df.index)
                current_time = pd.Timestamp.now()

                fallback_df['url'] = pandas_df.get('url', '').fillna('invalid_url').astype(str)
                fallback_df['domain'] = 'unknown.invalid'
                fallback_df['path'] = ''
                fallback_df['query'] = ''
                fallback_df['timestamp'] = pd.to_datetime(pandas_df.get('timestamp'), errors='coerce').fillna(
                    current_time)
                fallback_df['score'] = -2.0
                fallback_df['is_spam'] = True
                fallback_df['domain_reputation_score'] = 0.0
                fallback_df['risk_score'] = 2.0
                fallback_df['trust_score'] = 0.0
                fallback_df['composite_score'] = -2.0
                fallback_df['entropy_score'] = 0.0
                fallback_df['readability_score'] = 0.0
                fallback_df['confidence'] = 0.1
                fallback_df['domain_frequency'] = 1
                fallback_df['domain_frequency_pct'] = 1.0 / len(pandas_df) if len(pandas_df) > 0 else 1.0
                fallback_df['url_hash'] = fallback_df['url'].apply(lambda x: str(hash(x))[:16])
                fallback_df['time_delta'] = 0.0
                fallback_df['received_at'] = current_time
                fallback_df['meta'] = None

                for field in PROCESSED_URL_SCHEMA:
                    if field.name not in fallback_df.columns:
                        if field.name == "data_source":
                            fallback_df[field.name] = "input_data"
                        else:
                            fallback_df[field.name] = None

                ordered_cols = [field.name for field in PROCESSED_URL_SCHEMA if field.name in fallback_df.columns]
                yield fallback_df[ordered_cols]

            except Exception as fallback_error:
                print(f"ERROR: Fallback creation failed: {fallback_error}")
                yield pd.DataFrame(columns=[field.name for field in PROCESSED_URL_SCHEMA])

    save_domain_updates(partition_domain_updates)


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

        for col_name in output_df.columns:
            if output_df[col_name].dtype == 'object':
                output_df[col_name] = output_df[col_name].astype(str)
                output_df[col_name] = output_df[col_name].str.slice(0, 1000)
                output_df[col_name] = output_df[col_name].replace('nan', None)

        if not pd.api.types.is_datetime64_any_dtype(output_df["timestamp"]):
            output_df["timestamp"] = pd.to_datetime(output_df["timestamp"], errors='coerce', utc=True)

        if not pd.api.types.is_datetime64_any_dtype(output_df["received_at"]):
            output_df["received_at"] = pd.to_datetime(output_df["received_at"], errors='coerce', utc=True)

        output_df['url'] = output_df['url'].fillna('invalid_url')
        output_df['domain'] = output_df['domain'].fillna('unknown.invalid')

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


def create_efficient_good_data(spark):
    good_df = handle_good_data(spark)
    if not good_df:
        return None

    if 'user_text' in good_df.columns:
        good_df = good_df.drop('user_text')

    good_df = good_df.withColumn("timestamp", current_timestamp())
    good_df = good_df.withColumn("data_source", lit("good_data"))

    good_df = good_df.dropDuplicates(["url"])

    return good_df


def process_tsv_file(spark: SparkSession, input_path: str, output_path: str):
    if not os.path.exists(input_path):
        print(f"ERROR: Input file does not exist: {input_path}")
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    profiler = get_active_profiler()

    try:
        with profile_spark_stage(profiler, "tsv_data_loading"):
            print(f"Loading TSV data from: {input_path}")
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

            df_raw = df_raw.withColumn("timestamp", from_unixtime(col("timestamp")))
            df_raw = df_raw.withColumn("data_source", lit("input_data"))
            raw_count = df_raw.count()
            print(f"Loaded {raw_count} records from TSV")

        if SparkConfig.USE_GOOD_DATA:
            print("Creating filtered good data...")
            good_df = create_efficient_good_data(spark)
            if good_df:
                good_df_for_union = good_df.select("url", "timestamp", "data_source")
                df_raw = df_raw.unionByName(good_df_for_union).dropDuplicates(["url"])
                union_count = df_raw.count()
                print(f"After union with good data: {union_count} total records")

        df_raw.persist(StorageLevel.MEMORY_AND_DISK)

        base_training_df = df_raw

        for model_path in SparkConfig.USE_PATHS:
            model_name = os.path.splitext(os.path.basename(model_path))[0]
            model_output_path = os.path.join(output_path, model_name)
            os.makedirs(model_output_path, exist_ok=True)

            print("\n" + "="*80)
            print(f"PROCESSING WITH MODEL: {model_name}")
            print(f"Output Path: {model_output_path}")
            print("="*80 + "\n")

            with profile_spark_stage(profiler, f"url_scoring_processing_{model_name}"):
                print(f"Processing URLs with scoring model: {model_name}...")

                process_partition_with_model = partial(process_pandas_partition_optimized, model_path=model_path)

                processed_df = df_raw.mapInPandas(
                    process_partition_with_model,
                    schema=PROCESSED_URL_SCHEMA
                )

                processed_df.persist(StorageLevel.MEMORY_AND_DISK)
                processed_count = execute_with_job_name(spark, f"STEP_4_URL_SCORING_{model_name}",
                                                        lambda: processed_df.count())
                print(f"Processed and scored {processed_count} URLs with {model_name}")

            try:
                if SparkConfig.WRITE_PARQUET:
                    execute_with_job_name(spark, f"STEP_5A_WRITE_PARQUET_{model_name}",
                                          lambda: write_to_parquet(processed_df, model_output_path))

                if SparkConfig.WRITE_SUMMARY:
                    execute_with_job_name(spark, f"STEP_5C_WRITE_SUMMARY_{model_name}",
                                          lambda: write_summary(processed_df, model_output_path))

                if SparkConfig.WRITE_DB:
                    print(f"Warning: Writing to DB for model {model_name}. If multiple models are used, this may overwrite data.")
                    from writers.database_writer import write_to_database
                    execute_with_job_name(spark, f"STEP_5B_WRITE_DATABASE_{model_name}",
                                          lambda: write_to_database(processed_df))

            finally:
                processed_df.unpersist()

        df_raw.unpersist()

        if SparkConfig.TRAIN_MODEL:
            print(f"\n=== EXECUTING: In-Memory Model Training ===")

            classical_model_path = next((p for p in SparkConfig.USE_PATHS if "score_urls" in p), None)
            if not classical_model_path:
                 print("WARNING: Could not find classical model ('score_urls.py') in USE_PATHS. Skipping model training.")
            else:
                print(f"Generating training labels using '{classical_model_path}'...")

                process_partition_for_training = partial(process_pandas_partition_optimized, model_path=classical_model_path)
                training_label_df = base_training_df.mapInPandas(process_partition_for_training, schema=PROCESSED_URL_SCHEMA)
                training_label_df.persist(StorageLevel.MEMORY_AND_DISK)

                try:
                    print("Collecting data for training... This may take a moment.")
                    training_pd = training_label_df.select("url", "is_spam", "data_source").toPandas()
                    print(f"Collected {len(training_pd)} records into memory for training.")

                    classifier = URLArchivalClassifier()
                    classifier.train(training_pd)

                except Exception as e:
                    print(f"FATAL ERROR during in-memory model training step: {e}")
                    import traceback
                    traceback.print_exc()
                finally:
                    training_label_df.unpersist()

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
    except Exception as e:
        print(f"FATAL ERROR: Processing failed: {e}")
        sys.exit(1)
    finally:
        if spark_session:
            try:
                spark_session.stop()
            except Exception as e:
                print(f"WARNING: Failed to stop Spark session: {e}")
