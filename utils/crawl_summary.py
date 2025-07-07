import os
import sys
import time
from datetime import datetime
from collections import Counter
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, asc
from spark.config.spark_config import get_spark_session

def generate_crawl_summary(parquet_path, output_dir):
    print(f"DEBUG: Starting summary generation for {parquet_path}")
    start_time = time.time()
    
    spark = SparkSession.builder \
        .appName("CrawlSummaryGenerator") \
        .config("spark.driver.memory", "32g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.executor.cores", "8") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "20MB") \
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false") \
        .config("spark.sql.columnar.cache.enabled", "false") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "1000") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()
    
    try:
        df = spark.read.parquet(parquet_path)
        
        total_urls = df.count()
        print(f"DEBUG: Loaded {total_urls} total records")
        
        unique_urls = df.select("url").distinct().count()
        
        domain_freq = df.groupBy("domain").agg(count("*").alias("frequency")) \
                       .orderBy(desc("frequency")) \
                       .limit(10) \
                       .collect()
        
        best_urls = df.filter(col("priority_score").isNotNull()) \
                     .orderBy(desc("priority_score")) \
                     .select("url", "priority_score", "domain") \
                     .limit(10) \
                     .collect()
        
        worst_urls = df.filter(col("priority_score").isNotNull()) \
                      .orderBy(asc("priority_score")) \
                      .select("url", "priority_score", "domain") \
                      .limit(10) \
                      .collect()
        
        avg_priority = df.select("priority_score").filter(col("priority_score").isNotNull()).agg({"priority_score": "avg"}).collect()[0][0]
        active_count = df.filter(col("is_active") == True).count()
        
        processing_time = time.time() - start_time
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        summary_path = os.path.join(output_dir, "crawl_summary.txt")
        
        with open(summary_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CRAWL JOB SUMMARY REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {timestamp}\n")
            f.write(f"Data Source: {parquet_path}\n")
            f.write(f"Processing Time: {processing_time:.2f} seconds\n")
            f.write("\n")
            
            f.write("OVERALL STATISTICS\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total URLs Processed: {total_urls:,}\n")
            f.write(f"Unique URLs: {unique_urls:,}\n")
            f.write(f"Active URLs: {active_count:,}\n")
            f.write(f"Average Priority Score: {avg_priority:.4f}\n")
            f.write("\n")
            
            f.write("TOP 10 MOST FREQUENT DOMAINS\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(domain_freq, 1):
                domain = row['domain'] if row['domain'] else 'Unknown'
                frequency = row['frequency']
                percentage = (frequency / total_urls) * 100
                f.write(f"{i:2d}. {domain:<30} {frequency:>8,} ({percentage:>6.2f}%)\n")
            f.write("\n")
            
            f.write("TOP 10 BEST URLs (HIGHEST PRIORITY SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(best_urls, 1):
                url = row['url']
                score = row['priority_score']
                domain = row['domain'] if row['domain'] else 'Unknown'
                f.write(f"{i:2d}. Score: {score:.4f} | Domain: {domain}\n")
                f.write(f"    URL: {url}\n")
                f.write("\n")
            
            f.write("BOTTOM 10 WORST URLs (LOWEST PRIORITY SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(worst_urls, 1):
                url = row['url']
                score = row['priority_score']
                domain = row['domain'] if row['domain'] else 'Unknown'
                f.write(f"{i:2d}. Score: {score:.4f} | Domain: {domain}\n")
                f.write(f"    URL: {url}\n")
                f.write("\n")
            
            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")
        
        print(f"DEBUG: Summary report generated: {summary_path}")
        print(f"DEBUG: Total processing time: {processing_time:.2f} seconds")
        
        return summary_path
        
    except Exception as e:
        print(f"ERROR: Failed to generate summary: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

def generate_summary_for_latest_job(base_output_dir):
    if not os.path.exists(base_output_dir):
        print(f"ERROR: Base output directory does not exist: {base_output_dir}")
        return
    
    job_dirs = []
    for item in os.listdir(base_output_dir):
        item_path = os.path.join(base_output_dir, item)
        if os.path.isdir(item_path) and item.replace('_', '').replace('-', '').isdigit():
            job_dirs.append(item_path)
    
    if not job_dirs:
        print(f"ERROR: No job directories found in {base_output_dir}")
        return
    
    latest_job_dir = max(job_dirs, key=os.path.getmtime)
    parquet_path = os.path.join(latest_job_dir, "output", "parquet")
    
    if not os.path.exists(parquet_path):
        print(f"ERROR: Parquet directory not found: {parquet_path}")
        return
    
    print(f"DEBUG: Processing latest job: {latest_job_dir}")
    return generate_crawl_summary(parquet_path, latest_job_dir)

def add_summary_to_filewatcher():
    def _generate_job_summary(self, job_output_base_dir):
        try:
            parquet_path = os.path.join(job_output_base_dir, "output", "parquet")
            if os.path.exists(parquet_path):
                print(f"DEBUG: Generating summary for {parquet_path}")
                summary_path = generate_crawl_summary(parquet_path, job_output_base_dir)
                print(f"DEBUG: Summary generated: {summary_path}")
            else:
                print(f"DEBUG: No parquet data found at {parquet_path}")
        except Exception as e:
            print(f"ERROR: Failed to generate summary: {e}")
    
    return _generate_job_summary

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python crawl_summary.py <parquet_path> [output_dir]")
        print("  python crawl_summary.py --latest [base_output_dir]")
        sys.exit(1)
    
    if sys.argv[1] == "--latest":
        base_output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/output"
        generate_summary_for_latest_job(base_output_dir)
    else:
        parquet_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(parquet_path)
        generate_crawl_summary(parquet_path, output_dir)