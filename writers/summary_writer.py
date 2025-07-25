import os
import sys
from pyspark.sql import DataFrame

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_logic.create_summary import create_summary

def write_summary(processed_df: DataFrame, output_path: str):
    print("Generating comprehensive summary report...")

    job_output_dir = os.path.dirname(output_path)
    summary_path = create_summary(processed_df, output_path, job_output_dir)

    if summary_path:
        print(f"Summary report generated successfully: {summary_path}")
    else:
        print("Summary report generation returned None")

    return summary_path
