import os
import time
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json

def get_jdbc_url():
    return (f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:"
            f"{os.getenv('DB_PORT', '5434')}/"
            f"{os.getenv('DB_NAME', 'sentinel')}?"
            f"reWriteBatchedInserts=true&"
            f"prepareThreshold=0&"
            f"ApplicationName=spark-bulk-writer&"
            f"tcpKeepAlive=true")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5434'),
            dbname=os.getenv('DB_NAME', 'sentinel'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            connect_timeout=60
        )
        return conn
    except psycopg2.OperationalError as e:
        raise Exception(f"FATAL: Could not connect to PostgreSQL on a worker node: {e}")

def _upsert_partition(iterator):
    rows = list(iterator)
    if not rows:
        return

    conn = get_db_connection()
    if not conn:
        return

    insert_sql = """
        INSERT INTO urls (url, source, received_at, processed_at, status, score, meta)
        VALUES %s
        ON CONFLICT (url, source) DO NOTHING
    """

    page_size = 5000
    try:
        with conn.cursor() as cursor:
            tuples = [
                (
                    row.url, row.source, row.received_at, row.processed_at,
                    row.status, row.score, row.meta
                ) for row in rows
            ]
            execute_values(cursor, insert_sql, tuples, page_size=page_size)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"ERROR: Database upsert for partition failed: {e}")
    finally:
        if conn:
            conn.close()

def write_to_database(processed_df: DataFrame):
    processed_df.persist()
    total_rows = processed_df.count()

    if total_rows == 0:
        print("No rows to write, skipping.")
        processed_df.unpersist()
        return

    print(f"Starting database write for {total_rows} rows.")

    df_to_write = processed_df.select(
        col("url"),
        col("data_source").alias("source"),
        col("received_at"),
        col("received_at").alias("processed_at"),
        lit("processed").alias("status"),
        col("score"),
        to_json(struct(
            col("confidence"),
            col("domain"),
            col("domain_frequency"),
            col("domain_frequency_pct"),
            col("is_spam"),
            col("domain_reputation_score")
        )).alias("meta")
    )

    num_write_partitions = 96
    final_df = df_to_write.repartition(num_write_partitions, "url")
    final_df.persist()

    start_time = time.time()
    try:
        print(f"Attempting high-performance JDBC batch insert with {num_write_partitions} parallel connections.")
        final_df.write.format("jdbc") \
            .option("url", get_jdbc_url()) \
            .option("dbtable", "urls") \
            .option("user", os.getenv('DB_USER', 'postgres')) \
            .option("password", os.getenv('DB_PASSWORD', 'password')) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", 20000) \
            .option("rewriteBatchedStatements", "true") \
            .option("stringtype", "unspecified") \
            .mode("append") \
            .save()

        elapsed = time.time() - start_time
        if elapsed > 0:
            throughput = total_rows / elapsed
            print(f"Successfully completed database write for {total_rows} rows.")
            print(f"Total time: {elapsed:.2f} seconds. Throughput: {throughput:,.0f} rows/second.")

    except Exception as e:
        error_msg = str(e).lower()
        if "duplicate key" in error_msg or "unique constraint" in error_msg:
            print("Warning: JDBC batch insert failed due to unique constraint violation. Falling back to upsert.")
            print("This will be significantly slower.")
            fallback_start_time = time.time()
            try:
                final_df.foreachPartition(_upsert_partition)
                fallback_elapsed = time.time() - fallback_start_time
                print(f"Fallback upsert completed in {fallback_elapsed:.2f} seconds.")
            except Exception as fallback_e:
                print(f"FATAL: Fallback database write also failed: {fallback_e}")
                raise fallback_e
        else:
            print(f"FATAL: Database write failed. Check Spark executor logs. Root exception: {e}")
            raise e
    finally:
        processed_df.unpersist()
        final_df.unpersist()