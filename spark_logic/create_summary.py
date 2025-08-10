import os
import sys
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc, asc, avg as spark_avg, stddev, percentile_approx, sum as spark_sum

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from spark.config.spark_config import SparkConfig

def create_summary(processed_df: DataFrame, parquet_path: str, output_dir: str):
    start_time = time.time()
    model_name = os.path.basename(os.path.normpath(output_dir))

    try:
        processed_df.persist()

        total_urls = processed_df.count()
        if total_urls == 0:
            print(f"No data found for model '{model_name}', creating empty summary")
            processed_df.unpersist()
            return None

        print(f"Generating comprehensive summary report for model '{model_name}'...")

        unique_urls = processed_df.select("url").distinct().count()
        spam_count_row = processed_df.select(spark_sum(col("is_spam").cast("int"))).first()
        spam_count = spam_count_row[0] if spam_count_row else 0
        non_spam_count = total_urls - spam_count

        overall_stats_df = processed_df.agg(
            spark_avg("score").alias("avg_score"),
            spark_avg("confidence").alias("avg_confidence"),
            stddev("score").alias("score_stddev"),
            percentile_approx("score", 0.5, 10000).alias("median_score"),
            percentile_approx("score", 0.75, 10000).alias("p75_score"),
            percentile_approx("score", 0.95, 10000).alias("p95_score")
        ).first()

        avg_score = overall_stats_df["avg_score"]
        avg_confidence = overall_stats_df["avg_confidence"]
        score_stddev = overall_stats_df["score_stddev"]
        median_score = overall_stats_df["median_score"]
        p75_score = overall_stats_df["p75_score"]
        p95_score = overall_stats_df["p95_score"]

        domain_freq_data = processed_df.groupBy("domain").count().orderBy(desc("count")).limit(10).collect()
        domain_freq_data = [(row['domain'], row['count']) for row in domain_freq_data]

        best_urls_data = processed_df.filter(col("score").isNotNull()).orderBy(desc("score")).limit(10).select("url", "score", "confidence", "domain", "data_source").collect()
        best_urls_data = [(row['url'], row['score'], row['confidence'], row['domain'], row['data_source']) for row in best_urls_data]

        worst_urls_data = processed_df.filter(col("score").isNotNull()).orderBy(asc("score")).limit(10).select("url", "score", "confidence", "domain", "data_source").collect()
        worst_urls_data = [(row['url'], row['score'], row['confidence'], row['domain'], row['data_source']) for row in worst_urls_data]

        data_source_stats = None
        good_data_stats = None
        input_data_stats = None
        good_data_top_domains = None

        if SparkConfig.USE_GOOD_DATA:
            data_source_stats_list = processed_df.groupBy("data_source").agg(
                count("*").alias("count"),
                spark_avg("score").alias("avg_score"),
                spark_avg("confidence").alias("avg_confidence"),
                spark_sum(col("is_spam").cast("int")).alias("spam_count"),
                stddev("score").alias("score_stddev"),
                percentile_approx("score", 0.5, 10000).alias("median_score"),
                percentile_approx("score", 0.75, 10000).alias("p75_score"),
                percentile_approx("score", 0.95, 10000).alias("p95_score")
            ).collect()

            data_source_stats = {row["data_source"]: row for row in data_source_stats_list}
            good_data_top_domains_df = processed_df.filter(col("data_source") == "good_data").groupBy("domain").count().orderBy(desc("count")).limit(5).collect()
            good_data_top_domains = [(row['domain'], row['count']) for row in good_data_top_domains_df]
            good_data_stats = data_source_stats.get("good_data")
            input_data_stats = data_source_stats.get("input_data")

        processing_time = time.time() - start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        summary_path = os.path.join(output_dir, "job_summary.txt")

        with open(summary_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("URL PROCESSING SUMMARY REPORT\n")
            f.write(f"MODEL: {model_name.upper()}\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {timestamp}\n")
            f.write(f"Data Source (Parquet): {parquet_path}\n")
            f.write(f"Processing Time: {processing_time:.2f} seconds\n")
            f.write("\n")
            f.write("OVERALL STATISTICS\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total URLs Processed: {total_urls:,}\n")
            f.write(f"Unique URLs: {unique_urls:,}\n")
            f.write(f"Non-Spam URLs: {non_spam_count:,}\n")
            f.write(f"Spam URLs: {spam_count:,}\n")
            f.write(f"Average Score: {avg_score:.4f}\n")
            f.write(f"Average Confidence: {avg_confidence:.4f}\n")

            if score_stddev:
                f.write(f"Score StdDev: {score_stddev:.4f}\n")
                f.write(f"Median Score: {median_score:.4f}\n")
                f.write(f"75th Percentile Score: {p75_score:.4f}\n")
                f.write(f"95th Percentile Score: {p95_score:.4f}\n")

            f.write("\n")
            f.write("TOP 10 MOST FREQUENT DOMAINS\n")
            f.write("-" * 40 + "\n")
            for i, (domain, frequency) in enumerate(domain_freq_data, 1):
                domain = domain if domain else 'Unknown'
                percentage = (frequency / total_urls) * 100
                f.write(f"{i:2d}. {domain:<30} {frequency:>8,} ({percentage:>6.2f}%)\n")
            f.write("\n")

            f.write("TOP 10 BEST URLs (HIGHEST SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, (url, score, confidence, domain, data_source) in enumerate(best_urls_data, 1):
                f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f}")
                if SparkConfig.USE_GOOD_DATA and data_source:
                    source_marker = "[GOOD DATA]" if data_source == "good_data" else "[INPUT DATA]"
                    f.write(f" | {source_marker}\n")
                else:
                    f.write("\n")
                f.write(f"     Domain: {domain if domain else 'Unknown'}\n")
                f.write(f"     URL: {url}\n\n")

            f.write("BOTTOM 10 WORST URLs (LOWEST SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, (url, score, confidence, domain, data_source) in enumerate(worst_urls_data, 1):
                f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f}")
                if SparkConfig.USE_GOOD_DATA and data_source:
                    source_marker = "[GOOD DATA]" if data_source == "good_data" else "[INPUT DATA]"
                    f.write(f" | {source_marker}\n")
                else:
                    f.write("\n")
                f.write(f"     Domain: {domain if domain else 'Unknown'}\n")
                f.write(f"     URL: {url}\n\n")

            if SparkConfig.USE_GOOD_DATA and good_data_stats:
                good_data_count = good_data_stats["count"]
                good_data_spam_count = good_data_stats["spam_count"]
                good_data_non_spam_count = good_data_count - good_data_spam_count
                gd_median = good_data_stats["median_score"]
                gd_p75 = good_data_stats["p75_score"]
                gd_p95 = good_data_stats["p95_score"]

                f.write("GOOD DATA BENCHMARK ANALYSIS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Good Data Count: {good_data_count:,}\n")
                if input_data_stats and input_data_stats["count"]:
                    input_data_count = input_data_stats["count"]
                    f.write(f"Input Data Count: {input_data_count:,}\n")
                f.write(f"Good Data Percentage: {(good_data_count/total_urls)*100:.2f}%\n")
                f.write("\n")

                f.write("GOOD DATA STATISTICS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Good Data Avg Score: {good_data_stats['avg_score']:.4f}\n")
                f.write(f"Good Data Median Score: {gd_median:.4f}\n")
                f.write(f"Good Data Score StdDev: {good_data_stats['score_stddev']:.4f}\n")
                f.write(f"Good Data 75th Percentile: {gd
