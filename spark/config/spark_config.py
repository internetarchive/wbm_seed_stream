from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="URLProcessingApp", master="local[*]"):
    postgresql_jdbc_driver_path = "/Users/akshithchowdary/jars/postgresql-42.7.7.jar"

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g")
    
    if os.path.exists(postgresql_jdbc_driver_path):
        builder = builder.config("spark.jars", postgresql_jdbc_driver_path)
    else:
        print(f"WARNING: PostgreSQL JDBC driver not found at {postgresql_jdbc_driver_path}. Please ensure it's available.")

    spark = builder.getOrCreate()
    return spark
