import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json, hash, spark_partition_id
from pyspark.sql.types import StringType
from models import generate_batch_id
from testing.profiler_integration import get_active_profiler, log_database_operation

def write_to_database_batch_optimized(df_batch: DataFrame, batch_num: int, total_batches: int):
    batch_id = generate_batch_id()

    df_to_write = df_batch.select(
        col("url"),
        col("data_source").alias("source"),
        col("received_at"),
        col("received_at").alias("processed_at"),
        lit("processed").alias("status"),
        col("score"),
        lit(batch_id).alias("analysis_batch_id"),
        to_json(struct(
            col("confidence"),
            col("domain"),
            col("domain_frequency"),
            col("domain_frequency_pct"),
            col("is_spam"),
            col("domain_reputation_score")
        )).alias("meta")
    ).coalesce(16)

    row_count = df_batch.count()
    print(f"Processing batch {batch_num}/{total_batches} - Rows: {row_count}")

    if row_count == 0:
        print(f"Batch {batch_num} is empty, skipping")
        return 0.0

    start_time = time.time()

    try:
        df_to_write.write.format("jdbc") \
            .option("url", get_jdbc_url(batch_num)) \
            .option("dbtable", "urls") \
            .option("user", os.getenv('DB_USER', 'postgres')) \
            .option("password", os.getenv('DB_PASSWORD', 'password')) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", 100000) \
            .option("numPartitions", 16) \
            .option("isolationLevel", "NONE") \
            .option("createTableColumnTypes", "meta JSONB") \
            .option("stringtype", "unspecified") \
            .option("rewriteBatchedStatements", "true") \
            .option("prepareThreshold", "0") \
            .option("tcpKeepAlive", "true") \
            .option("socketTimeout", "0") \
            .option("connectTimeout", "60") \
            .option("loginTimeout", "60") \
            .option("ApplicationName", f"spark-batch-{batch_num}") \
            .option("fetchsize", "0") \
            .option("queryTimeout", "0") \
            .mode("append") \
            .save()

        elapsed = time.time() - start_time
        print(f"Batch {batch_num} completed in {elapsed:.2f} seconds - {row_count} rows")
        return elapsed

    except Exception as e:
        error_msg = str(e).lower()
        if "duplicate key" in error_msg or "unique constraint" in error_msg:
            print(f"Batch {batch_num} failed with duplicate key error. Using upsert method")
            return write_partition_upsert(df_to_write, batch_num, total_batches)
        else:
            print(f"Batch {batch_num} failed with error: {e}")
            print(f"Error type: {type(e)}")
            raise e

def get_jdbc_url(batch_num=1):
    return f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5434')}/{os.getenv('DB_NAME', 'sentinel')}?reWriteBatchedInserts=true&prepareThreshold=0&ApplicationName=spark-bulk-insert-{batch_num}&tcpKeepAlive=true&socketTimeout=0&defaultRowFetchSize=0"

def write_partition_upsert(df_to_write: DataFrame, batch_num: int, total_batches: int):
    start_time = time.time()

    def upsert_partition(iterator):
        import psycopg2
        import psycopg2.extras
        import json

        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5434'),
                database=os.getenv('DB_NAME', 'sentinel'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'password'),
                connect_timeout=60
            )
            conn.set_session(autocommit=False)

            with conn.cursor() as cur:
                batch_data = []
                total_processed = 0

                for row in iterator:
                    batch_data.append((
                        row['url'],
                        row['source'],
                        row['received_at'],
                        row['processed_at'],
                        row['status'],
                        row['score'],
                        row['analysis_batch_id'],
                        row['meta']
                    ))

                    if len(batch_data) >= 2000:
                        try:
                            psycopg2.extras.execute_values(
                                cur,
                                """INSERT INTO urls (url, source, received_at, processed_at, status, score, analysis_batch_id, meta)
                                   VALUES %s ON CONFLICT (url, source) DO NOTHING""",
                                batch_data,
                                template=None,
                                page_size=2000
                            )
                            conn.commit()
                            total_processed += len(batch_data)
                            batch_data = []
                        except Exception as batch_error:
                            print(f"Batch upsert error: {batch_error}")
                            conn.rollback()
                            batch_data = []

                if batch_data:
                    try:
                        psycopg2.extras.execute_values(
                            cur,
                            """INSERT INTO urls (url, source, received_at, processed_at, status, score, analysis_batch_id, meta)
                               VALUES %s ON CONFLICT (url, source) DO NOTHING""",
                            batch_data,
                            template=None,
                            page_size=len(batch_data)
                        )
                        conn.commit()
                        total_processed += len(batch_data)
                    except Exception as batch_error:
                        print(f"Final batch upsert error: {batch_error}")
                        conn.rollback()

            conn.close()
            print(f"Partition processed {total_processed} rows")
            return [total_processed]

        except Exception as conn_error:
            print(f"Connection error in upsert_partition: {conn_error}")
            return [0]

    result = df_to_write.rdd.mapPartitions(upsert_partition).collect()
    total_upserted = sum(result)

    elapsed = time.time() - start_time
    print(f"Batch {batch_num} upsert completed in {elapsed:.2f} seconds - {total_upserted} rows processed")
    return elapsed

def write_to_database_parallel(processed_df: DataFrame):
    profiler = get_active_profiler()

    total_rows = processed_df.count()
    print(f"Total rows to process: {total_rows}")

    if total_rows == 0:
        print("No rows to process")
        return

    num_partitions = 24
    processed_df = processed_df.repartition(num_partitions, hash(col("url")))

    total_start_time = time.time()

    partitions_per_batch = 4
    num_batches = (num_partitions + partitions_per_batch - 1) // partitions_per_batch

    print(f"Processing {num_partitions} partitions in {num_batches} batches of {partitions_per_batch} partitions each")

    batches = []
    for i in range(num_batches):
        start_partition = i * partitions_per_batch
        end_partition = min((i + 1) * partitions_per_batch, num_partitions)
        partition_ids = list(range(start_partition, end_partition))
        batches.append(partition_ids)

    batch_results = []
    
    def process_single_batch(partition_ids, batch_num):
        batch_start_time = time.time()
        
        batch_df = processed_df.filter(
            spark_partition_id().isin(partition_ids)
        )
        
        batch_rows = batch_df.count()
        if batch_rows == 0:
            print(f"Batch {batch_num} is empty")
            return 0.0, 0

        elapsed = write_to_database_batch_optimized(batch_df, batch_num, num_batches)
        
        import gc
        gc.collect()
        
        return elapsed, batch_rows

    total_batch_time = 0
    total_processed_rows = 0

    for i, partition_ids in enumerate(batches):
        batch_num = i + 1
        print(f"Starting batch {batch_num}/{num_batches}")
        
        try:
            batch_time, batch_rows = process_single_batch(partition_ids, batch_num)
            total_batch_time += batch_time
            total_processed_rows += batch_rows
            
            print(f"Completed batch {batch_num}/{num_batches} - {batch_rows} rows in {batch_time:.2f}s")
            
            if batch_num < num_batches:
                time.sleep(2)
                
        except Exception as e:
            print(f"Batch {batch_num} failed with exception: {e}")
            import traceback
            traceback.print_exc()

    total_elapsed = time.time() - total_start_time
    avg_batch_time = total_batch_time / num_batches if num_batches > 0 else 0

    print(f"All batches completed!")
    print(f"Total rows processed: {total_processed_rows}/{total_rows}")
    print(f"Total time: {total_elapsed:.2f} seconds")
    print(f"Average batch time: {avg_batch_time:.2f} seconds")
    if total_elapsed > 0:
        print(f"Throughput: {total_processed_rows / total_elapsed:.0f} rows/second")

    if profiler:
        log_database_operation(profiler, "insert_urls", "urls", total_processed_rows)

def write_to_database(processed_df: DataFrame):
    return write_to_database_parallel(processed_df)