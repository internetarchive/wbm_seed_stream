import os
import sys
import json
from collections import defaultdict
import time
import traceback

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_unixtime, current_timestamp, coalesce, broadcast, avg as spark_avg, count as spark_count, sum as spark_sum
from pyspark.sql.types import (StringType, StructField, StructType, LongType)
from pyspark import StorageLevel
from spark_logic.score_urls import score_urls_batch
from spark_logic.use_features import enhance_scores_with_features
from spark_logic.rank_domains import DomainRanker
from writers.parquet_writer import write_to_parquet
from writers.summary_writer import write_summary
from spark_logic.create_summary import create_comparison_summary
from writers.good_data_handler import handle_good_data
from writers.domain_processor import create_domain_ranker, aggregate_domain_stats_vectorized, collect_domain_updates_from_temp_files, update_domain_reputations, cleanup_temp_files
from spark.config.spark_config import SparkConfig, get_spark_session
from schema import PROCESSED_URL_SCHEMA
from models.code.lightgbm import URLArchivalRegressor
from testing.profiler_integration import (
    get_active_profiler,
    profile_spark_stage,
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MODEL_PATH = os.path.join(PROJECT_ROOT, 'models', 'trained', 'url_archival_regressor_model.joblib')

def execute_with_job_name(spark, job_name, action_func):
    print(f"\n=== EXECUTING: {job_name} ===")
    start_time = time.time()
    spark.sparkContext.setJobDescription(job_name)
    result = action_func()
    duration = time.time() - start_time
    print(f"=== COMPLETED: {job_name} in {duration:.2f}s ===\n")
    return result

def process_pandas_partition_classical(iterator):
    domain_ranker = create_domain_ranker()
    all_domain_reputations = domain_ranker.get_all_domain_reputations() if SparkConfig.READ_REPUTATION else {}
    for pandas_df in iterator:
        if pandas_df is None or pandas_df.empty:
            continue
        try:
            results_df = score_urls_batch(pandas_df, all_domain_reputations)
            yield validate_and_clean_results(results_df)
        except Exception as e:
            print(f"ERROR: Failed to process partition with classical method, creating fallback: {e}")
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

def process_pandas_partition_lightgbm(iterator):
    regressor = URLArchivalRegressor(model_path=os.path.basename(MODEL_PATH))
    for pandas_df in iterator:
        if pandas_df is None or pandas_df.empty:
            continue
        try:
            results_df = regressor.predict(pandas_df)
            yield validate_and_clean_results(results_df)
        except Exception as e:
            print(f"ERROR: Failed to process partition with lightgbm, creating fallback. Exception: {e}")
            traceback.print_exc()
            try:
                fallback_df = pd.DataFrame(index=pandas_df.index)
                current_time = pd.Timestamp.now()
                fallback_df['url'] = pandas_df.get('url', '').fillna('invalid_url').astype(str)
                fallback_df['domain'] = 'unknown.invalid'
                fallback_df['path'] = ''
                fallback_df['query'] = ''
                fallback_df['timestamp'] = pd.to_datetime(pandas_df.get('timestamp'), errors='coerce').fillna(
                    current_time)
                fallback_df['score'] = -3.0
                fallback_df['is_spam'] = True
                fallback_df['domain_reputation_score'] = 0.0
                fallback_df['risk_score'] = 3.0
                fallback_df['trust_score'] = 0.0
                fallback_df['composite_score'] = -3.0
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
                print(f"ERROR: Fallback creation failed for lightgbm: {fallback_error}")
                yield pd.DataFrame(columns=[field.name for field in PROCESSED_URL_SCHEMA])

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
        for method in SparkConfig.USE_METHODS:
            model_name = method.replace(".py", "")
            model_base_output_path = os.path.join(output_path, model_name)
            print("\n" + "="*80)
            print(f"PROCESSING WITH METHOD: {model_name}")
            print(f"Output Path: {model_base_output_path}")
            print("="*80 + "\n")
            with profile_spark_stage(profiler, f"url_scoring_processing_{model_name}"):
                print(f"Processing URLs with scoring method: {model_name}...")
                if method == "classical":
                    process_partition_func = process_pandas_partition_classical
                elif method == "lightgbm.py":
                    process_partition_func = process_pandas_partition_lightgbm
                else:
                    print(f"WARNING: Unknown method '{method}' specified. Skipping.")
                    continue
                # PASS 1: Provisional Scoring
                provisional_df = df_raw.mapInPandas(
                    process_partition_func,
                    schema=PROCESSED_URL_SCHEMA
                )
                provisional_df.persist(StorageLevel.MEMORY_AND_DISK)

                # AGGREGATE batch-wide stats
                print("Aggregating batch-wide domain statistics...")
                batch_domain_stats_df = provisional_df.groupBy("domain").agg(
                    spark_avg("score").alias("batch_reputation_score"),
                    spark_count("*").alias("batch_count"),
                    spark_sum(col("is_spam").cast("integer")).alias("batch_malicious_urls")
                )

                # PASS 2: Rescore with batch-aware reputations
                print("Rescoring URLs with batch-aware domain reputations...")

                # The original score contribution from domain reputation was (domain_reputation_score * 0.2)
                # We subtract the old contribution and add the new one.
                processed_df = provisional_df.join(broadcast(batch_domain_stats_df), "domain", "left") \
                    .withColumn(
                        "final_score",
                        col("score")
                        - (col("domain_reputation_score") * 0.2)
                        + (coalesce(col("batch_reputation_score"), lit(0.0)) * 0.2)
                    ) \
                    .withColumn("score", col("final_score")) \
                    .withColumn("domain_reputation_score", coalesce(col("batch_reputation_score"), lit(0.0))) \
                    .drop("batch_reputation_score", "batch_count", "batch_malicious_urls", "final_score")

                processed_df.persist(StorageLevel.MEMORY_AND_DISK)
                provisional_df.unpersist()

                if SparkConfig.USE_FEATURES and method == "classical":
                    print("Enhancing scores with deep feature analysis...")
                    enhanced_df = enhance_scores_with_features(processed_df)
                    enhanced_df.persist(StorageLevel.MEMORY_AND_DISK)

                    if not enhanced_df.rdd.isEmpty():
                        actual_enhanced_count = enhanced_df.count()
                        print(f"Received {actual_enhanced_count} enhanced results.")

                        # Update domain reputations based on score changes from the feature analysis
                        if SparkConfig.WRITE_REPUTATION:
                            print("Aggregating feature analysis results for domain reputation updates...")
                            domain_updates_df = enhanced_df.groupBy("domain").agg(
                                {"score_delta": "avg", "url": "count"}
                            ).withColumnRenamed("avg(score_delta)", "avg_score_delta") \
                             .withColumnRenamed("count(url)", "enhanced_url_count")

                            domain_updates_pd = domain_updates_df.toPandas()

                            if not domain_updates_pd.empty:
                                print(f"Updating reputations for {len(domain_updates_pd)} domains based on feature analysis.")
                                updates_for_db = {}
                                for _, row in domain_updates_pd.iterrows():
                                    if row['domain']: # Ensure domain is not null
                                        updates_for_db[row['domain']] = {
                                            'reputation_score': row['avg_score_delta'],
                                            'total_urls': int(row['enhanced_url_count']),
                                            'malicious_urls': 0, # Not directly modifying these counts
                                            'benign_urls': 0
                                        }
                                if updates_for_db:
                                    try:
                                        ranker = DomainRanker()
                                        ranker.update_domain_reputation_batch(updates_for_db)
                                        print("Domain reputations updated successfully from feature analysis.")
                                    except Exception as e:
                                        print(f"WARNING: Failed to update domain reputations from feature analysis: {e}")

                        # Join enhanced scores back to the main dataframe
                        print("Joining enhanced scores back to main dataset...")
                        enhanced_updates = enhanced_df.select(
                            col("url").alias("enhanced_url"),
                            col("enhanced_score"),
                            col("enhanced_confidence")
                        )

                        processed_df = processed_df.join(
                            enhanced_updates,
                            processed_df.url == enhanced_updates.enhanced_url,
                            "left_outer"
                        ).withColumn(
                            "score",
                            coalesce(col("enhanced_score"), col("score"))
                        ).withColumn(
                            "confidence",
                            coalesce(col("enhanced_confidence"), col("confidence"))
                        ).drop("enhanced_url", "enhanced_score", "enhanced_confidence")

                        enhanced_df.unpersist()
                    else:
                        print("No URLs were enhanced by the feature analysis process.")

            try:
                if SparkConfig.WRITE_PARQUET:
                    parquet_output_path = os.path.join(model_base_output_path, "parquet")
                    os.makedirs(parquet_output_path, exist_ok=True)
                    execute_with_job_name(spark, f"STEP_5A_WRITE_PARQUET_{model_name}",
                                          lambda: write_to_parquet(processed_df, parquet_output_path))
                if SparkConfig.WRITE_SUMMARY:
                    summary_output_path = os.path.join(model_base_output_path, "summary")
                    os.makedirs(summary_output_path, exist_ok=True)
                    execute_with_job_name(spark, f"STEP_5C_WRITE_SUMMARY_{model_name}",
                                          lambda: write_summary(processed_df, summary_output_path))
                if SparkConfig.WRITE_DB:
                    print(f"Warning: Writing to DB for model {model_name}. If multiple models are used, this may overwrite data.")
                    from writers.database_writer import write_to_database
                    execute_with_job_name(spark, f"STEP_5B_WRITE_DATABASE_{model_name}",
                                          lambda: write_to_database(processed_df))

                # Update domain reputations using the batch-aware stats
                if SparkConfig.WRITE_REPUTATION and method == "classical":
                    try:
                        print("\n" + "="*80)
                        print("UPDATING DOMAIN REPUTATION DATABASE")
                        print("="*80 + "\n")

                        # The domain stats were already computed. Now collect them to the driver.
                        print("Collecting domain updates...")
                        batch_domain_stats_pd = batch_domain_stats_df.toPandas()

                        accumulated_updates = {}
                        for _, row in batch_domain_stats_pd.iterrows():
                            domain = row['domain']
                            if domain and pd.notna(domain):
                                accumulated_updates[domain] = {
                                    'reputation_score': row['batch_reputation_score'],
                                    'total_urls': row['batch_count'],
                                    'malicious_urls': row['batch_malicious_urls'],
                                    'benign_urls': row['batch_count'] - row['batch_malicious_urls']
                                }

                        if accumulated_updates:
                            print(f"Found updates for {len(accumulated_updates)} domains.")
                            update_domain_reputations(accumulated_updates)
                            print("Domain reputation update process completed.")
                        else:
                            print("No domain updates to apply for this batch.")

                    except Exception as e:
                        print(f"WARNING: Failed to update domain reputations: {e}")
                        import traceback
                        traceback.print_exc()
            finally:
                processed_df.unpersist()
        df_raw.unpersist()
        methods_used = SparkConfig.USE_METHODS
        if "classical" in methods_used and "lightgbm.py" in methods_used and not SparkConfig.TRAIN_MODEL and SparkConfig.WRITE_PARQUET:
            print("\n" + "="*80)
            print("GENERATING MODEL COMPARISON REPORT")
            print("="*80 + "\n")
            def compare_models():
                try:
                    classical_path = os.path.join(output_path, "classical", "parquet")
                    lightgbm_path = os.path.join(output_path, "lightgbm", "parquet")
                    df_classical = spark.read.parquet(classical_path)
                    df_lightgbm = spark.read.parquet(lightgbm_path)
                    df_classical_renamed = df_classical.select(
                        col("url"),
                        col("score").alias("score_classical"),
                        col("is_spam").alias("is_spam_classical")
                    )
                    df_lightgbm_renamed = df_lightgbm.select(
                        col("url"),
                        col("score").alias("score_lightgbm"),
                        col("is_spam").alias("is_spam_lightgbm")
                    )
                    comparison_df = df_classical_renamed.join(df_lightgbm_renamed, "url", "inner")
                    create_comparison_summary(comparison_df, output_path)
                except Exception as e:
                    print(f"ERROR: Could not generate model comparison report: {e}")
                    import traceback
                    traceback.print_exc()
            execute_with_job_name(spark, "STEP_6_MODEL_COMPARISON", compare_models)

        if SparkConfig.TRAIN_MODEL and "lightgbm.py" in SparkConfig.USE_METHODS:
            print(f"\n=== EXECUTING: In-Memory Model Training for LightGBM ===")
            print("Generating training labels using 'classical' method...")
            training_label_df = base_training_df.mapInPandas(process_pandas_partition_classical, schema=PROCESSED_URL_SCHEMA)
            training_label_df.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                print("Collecting data for training... This may take a moment.")
                training_pd = training_label_df.select("url", "score", "data_source").toPandas()
                print(f"Collected {len(training_pd)} records into memory for training.")
                regressor = URLArchivalRegressor(model_path=MODEL_PATH)
                regressor.train(training_pd)
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
        if "lightgbm.py" in SparkConfig.USE_METHODS or SparkConfig.TRAIN_MODEL:
            if os.path.exists(MODEL_PATH):
                spark_session.sparkContext.addFile(MODEL_PATH)
                print(f"Added model file to Spark context: {MODEL_PATH}")
            else:
                if not SparkConfig.TRAIN_MODEL:
                    print(f"FATAL ERROR: LightGBM model not found at {MODEL_PATH} and TRAIN_MODEL is False.", file=sys.stderr)
                    spark_session.stop()
                    sys.exit(1)
                else:
                    print(f"WARNING: Model file not found at {MODEL_PATH}, but proceeding because TRAIN_MODEL is True.")
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
