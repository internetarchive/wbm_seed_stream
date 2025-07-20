import os
import sys
from pyspark.sql import DataFrame

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from testing.profiler_integration import get_active_profiler, log_file_operation

def write_to_parquet(processed_df: DataFrame, output_path: str):
    profiler = get_active_profiler()

    print(f"Writing processed URLs to: {output_path}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

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
