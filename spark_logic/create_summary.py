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

    try:
        processed_df.cache()
        processed_df.count()

        total_urls = processed_df.count()
        if total_urls == 0:
            print("No data found, creating empty summary")
            return None

        unique_urls = processed_df.select("url").distinct().count()
        spam_count = processed_df.filter(col("is_spam") == True).count()
        non_spam_count = total_urls - spam_count
        avg_score = processed_df.agg(spark_avg("score")).collect()[0][0]
        avg_confidence = processed_df.agg(spark_avg("confidence")).collect()[0][0]

        domain_freq_data = processed_df.groupBy("domain").count().orderBy(desc("count")).limit(10).collect()
        best_urls_data = processed_df.filter(col("score").isNotNull()).orderBy(desc("score")).limit(10).select("url", "score", "confidence", "domain", "data_source").collect()
        worst_urls_data = processed_df.filter(col("score").isNotNull()).orderBy(asc("score")).limit(10).select("url", "score", "confidence", "domain", "data_source").collect()

        all_stats = None
        good_data_stats = None
        input_data_stats = None
        good_data_top_domains = None

        if SparkConfig.USE_GOOD_DATA:
            all_stats = processed_df.agg(
                stddev("score").alias("score_stddev"),
                percentile_approx("score", 0.5, 10000).alias("median_score"),
                percentile_approx("score", 0.75, 10000).alias("p75_score"),
                percentile_approx("score", 0.95, 10000).alias("p95_score")
            ).collect()[0]

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

            data_source_stats = {}
            for row in data_source_stats_list:
                data_source_stats[row["data_source"]] = row

            good_data_top_domains = processed_df.filter(col("data_source") == "good_data").groupBy("domain").count().orderBy(desc("count")).limit(5).collect()
            good_data_stats = data_source_stats.get("good_data")
            input_data_stats = data_source_stats.get("input_data")

        processing_time = time.time() - start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        summary_path = os.path.join(output_dir, "job_summary.txt")

        with open(summary_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("URL PROCESSING SUMMARY REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {timestamp}\n")
            f.write(f"Data Source: {parquet_path}\n")
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

            if SparkConfig.USE_GOOD_DATA and all_stats:
                f.write(f"Score StdDev: {all_stats['score_stddev']:.4f}\n")
                f.write(f"Median Score: {all_stats['median_score']:.4f}\n")
                f.write(f"75th Percentile Score: {all_stats['p75_score']:.4f}\n")
                f.write(f"95th Percentile Score: {all_stats['p95_score']:.4f}\n")

            f.write("\n")
            f.write("TOP 10 MOST FREQUENT DOMAINS\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(domain_freq_data, 1):
                domain = row['domain'] if row['domain'] else 'Unknown'
                frequency = row['count']
                percentage = (frequency / total_urls) * 100
                f.write(f"{i:2d}. {domain:<30} {frequency:>8,} ({percentage:>6.2f}%)\n")
            f.write("\n")

            f.write("TOP 10 BEST URLs (HIGHEST SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(best_urls_data, 1):
                f.write(f"{i:2d}. Score: {row['score']:.4f} | Confidence: {row['confidence']:.4f}")
                if SparkConfig.USE_GOOD_DATA and 'data_source' in row:
                    source_marker = "[GOOD DATA]" if row['data_source'] == "good_data" else "[INPUT DATA]"
                    f.write(f" | {source_marker}\n")
                else:
                    f.write("\n")
                f.write(f"     Domain: {row['domain'] if row['domain'] else 'Unknown'}\n")
                f.write(f"     URL: {row['url']}\n\n")

            f.write("BOTTOM 10 WORST URLs (LOWEST SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(worst_urls_data, 1):
                f.write(f"{i:2d}. Score: {row['score']:.4f} | Confidence: {row['confidence']:.4f}")
                if SparkConfig.USE_GOOD_DATA and 'data_source' in row:
                    source_marker = "[GOOD DATA]" if row['data_source'] == "good_data" else "[INPUT DATA]"
                    f.write(f" | {source_marker}\n")
                else:
                    f.write("\n")
                f.write(f"     Domain: {row['domain'] if row['domain'] else 'Unknown'}\n")
                f.write(f"     URL: {row['url']}\n\n")

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
                f.write(f"Good Data 75th Percentile: {gd_p75:.4f}\n")
                f.write(f"Good Data 95th Percentile: {gd_p95:.4f}\n")
                f.write(f"Good Data Spam Rate: {(good_data_spam_count/good_data_count)*100:.2f}%\n")
                f.write(f"Good Data Non-Spam Rate: {(good_data_non_spam_count/good_data_count)*100:.2f}%\n")
                f.write("\n")

                if input_data_stats and input_data_stats.get('avg_score') is not None:
                    score_lift = good_data_stats['avg_score'] - input_data_stats['avg_score']

                    f.write("SCORING SYSTEM PERFORMANCE\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Good Data Avg Score: {good_data_stats['avg_score']:.4f}\n")
                    f.write(f"Input Data Avg Score: {input_data_stats['avg_score']:.4f}\n")
                    f.write(f"Score Lift (Good - Input): {score_lift:.4f}\n")
                    if input_data_stats['avg_score'] > 0:
                        f.write(f"Score Lift Percentage: {(score_lift/input_data_stats['avg_score'])*100:.2f}%\n")

                    good_data_in_top_10 = sum(1 for url in best_urls_data if url.get('data_source') == 'good_data')
                    good_data_in_bottom_10 = sum(1 for url in worst_urls_data if url.get('data_source') == 'good_data')

                    f.write(f"Good Data in Top 10: {good_data_in_top_10}/10\n")
                    f.write(f"Good Data in Bottom 10: {good_data_in_bottom_10}/10\n")

                    performance_grade = "EXCELLENT" if score_lift > 0.5 else \
                                      "GOOD" if score_lift > 0.2 else \
                                      "FAIR" if score_lift > 0.0 else \
                                      "POOR"
                    f.write(f"SYSTEM PERFORMANCE GRADE: {performance_grade}\n")
                    f.write("\n")

                if good_data_top_domains:
                    f.write("TOP GOOD DATA DOMAINS\n")
                    f.write("-" * 40 + "\n")
                    for i, row in enumerate(good_data_top_domains, 1):
                        domain = row['domain'] if row['domain'] else 'Unknown'
                        frequency = row['count']
                        f.write(f"{i}. {domain:<30} {frequency:>8,}\n")
                    f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        processed_df.unpersist()
        print(f"Summary report generated: {summary_path}")
        return summary_path

    except Exception as e:
        import traceback
        print(f"ERROR: Failed to generate summary: {e}")
        traceback.print_exc()
        raise
