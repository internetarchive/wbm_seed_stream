import os
import sys
import time
from datetime import datetime
from pyspark.sql import DataFrame 
from pyspark.sql.functions import col, count, desc, asc, avg as spark_avg, stddev, percentile_approx

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from spark.config.spark_config import SparkConfig

def create_summary(processed_df: DataFrame, parquet_path: str, output_dir: str):
    start_time = time.time()
    
    try:
        df = processed_df.persist()
        total_urls = df.count()
        
        if total_urls == 0:
            print("No data found, creating empty summary")
            return None

        unique_urls = df.select("url").distinct().count()
        
        domain_freq_data = df.groupBy("domain").agg(count("*").alias("frequency")) \
                           .orderBy(desc("frequency")) \
                           .limit(10) \
                           .collect()

        best_urls_cols = ["url", "score", "domain", "confidence"]
        worst_urls_cols = ["url", "score", "domain", "confidence"]
        if SparkConfig.USE_GOOD_DATA:
            best_urls_cols.append("data_source")
            worst_urls_cols.append("data_source")

        best_urls_data = df.filter(col("score").isNotNull()) \
                         .orderBy(desc("score")) \
                         .select(*best_urls_cols) \
                         .limit(10) \
                         .collect()
        
        worst_urls_data = df.filter(col("score").isNotNull()) \
                          .orderBy(asc("score")) \
                          .select(*worst_urls_cols) \
                          .limit(10) \
                          .collect()
        
        score_stats = df.select("score").filter(col("score").isNotNull()).agg({"score": "avg"}).collect()[0]
        confidence_stats = df.select("confidence").filter(col("confidence").isNotNull()).agg({"confidence": "avg"}).collect()[0]
        
        avg_score = score_stats[0]
        avg_confidence = confidence_stats[0]
        
        spam_count = df.filter(col("is_spam") == True).count()
        non_spam_count = df.filter(col("is_spam") == False).count()

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
            f.write("\n")
            f.write("TOP 10 MOST FREQUENT DOMAINS\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(domain_freq_data, 1):
                domain = row['domain'] if row['domain'] else 'Unknown'
                frequency = row['frequency']
                percentage = (frequency / total_urls) * 100
                f.write(f"{i:2d}. {domain:<30} {frequency:>8,} ({percentage:>6.2f}%)\n")
            f.write("\n")
            f.write("TOP 10 BEST URLs (HIGHEST SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(best_urls_data, 1):
                url = row['url']
                score = row['score']
                confidence = row['confidence']
                domain = row['domain'] if row['domain'] else 'Unknown'
                if SparkConfig.USE_GOOD_DATA:
                    data_source = row['data_source']
                    source_marker = "[GOOD DATA]" if data_source == "good_data" else "[INPUT DATA]"
                    f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f} | {source_marker}\n")
                else:
                    f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f}\n")
                f.write(f"    Domain: {domain}\n")
                f.write(f"    URL: {url}\n")
                f.write("\n")
            f.write("BOTTOM 10 WORST URLs (LOWEST SCORE)\n")
            f.write("-" * 40 + "\n")
            for i, row in enumerate(worst_urls_data, 1):
                url = row['url']
                score = row['score']
                confidence = row['confidence']
                domain = row['domain'] if row['domain'] else 'Unknown'
                if SparkConfig.USE_GOOD_DATA:
                    data_source = row['data_source']
                    source_marker = "[GOOD DATA]" if data_source == "good_data" else "[INPUT DATA]"
                    f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f} | {source_marker}\n")
                else:
                    f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f}\n")
                f.write(f"    Domain: {domain}\n")
                f.write(f"    URL: {url}\n")
                f.write("\n")

            if SparkConfig.USE_GOOD_DATA:
                good_data_df = df.filter(col("data_source") == "good_data")
                input_data_df = df.filter(col("data_source") == "input_data")

                good_data_count = good_data_df.count()
                input_data_count = input_data_df.count()

                if good_data_count > 0:
                    f.write("GOOD DATA BENCHMARK ANALYSIS\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Good Data Count: {good_data_count:,}\n")
                    f.write(f"Input Data Count: {input_data_count:,}\n")
                    f.write(f"Good Data Percentage: {(good_data_count/total_urls)*100:.2f}%\n")
                    f.write("\n")

                    good_data_stats = good_data_df.select("score", "confidence", "is_spam").filter(col("score").isNotNull()).agg(
                        spark_avg("score").alias("avg_score"),
                        spark_avg("confidence").alias("avg_confidence"),
                        stddev("score").alias("score_stddev"),
                        percentile_approx("score", 0.5).alias("median_score"),
                        percentile_approx("score", 0.75).alias("p75_score"),
                        percentile_approx("score", 0.95).alias("p95_score")
                    ).collect()[0]

                    good_data_spam_count = good_data_df.filter(col("is_spam") == True).count()
                    good_data_non_spam_count = good_data_df.filter(col("is_spam") == False).count()

                    f.write("GOOD DATA STATISTICS\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Good Data Avg Score: {good_data_stats['avg_score']:.4f}\n")
                    f.write(f"Good Data Median Score: {good_data_stats['median_score']:.4f}\n")
                    f.write(f"Good Data Score StdDev: {good_data_stats['score_stddev']:.4f}\n")
                    f.write(f"Good Data 75th Percentile: {good_data_stats['p75_score']:.4f}\n")
                    f.write(f"Good Data 95th Percentile: {good_data_stats['p95_score']:.4f}\n")
                    f.write(f"Good Data Spam Rate: {(good_data_spam_count/good_data_count)*100:.2f}%\n")
                    f.write(f"Good Data Non-Spam Rate: {(good_data_non_spam_count/good_data_count)*100:.2f}%\n")
                    f.write("\n")

                    if input_data_count > 0:
                        input_data_stats = input_data_df.select("score", "confidence").filter(col("score").isNotNull()).agg(
                            spark_avg("score").alias("avg_score"),
                            spark_avg("confidence").alias("avg_confidence"),
                            stddev("score").alias("score_stddev"),
                            percentile_approx("score", 0.5).alias("median_score")
                        ).collect()[0]

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

                        all_urls_ranked = df.filter(col("score").isNotNull()).orderBy(desc("score")).select("data_source").collect()
                        total_ranked = len(all_urls_ranked)

                        good_data_positions = []
                        for i, row in enumerate(all_urls_ranked):
                            if row['data_source'] == 'good_data':
                                good_data_positions.append(i + 1)

                        if good_data_positions:
                            avg_position = sum(good_data_positions) / len(good_data_positions)
                            median_position = sorted(good_data_positions)[len(good_data_positions) // 2]
                            top_10_percent_count = sum(1 for pos in good_data_positions if pos <= total_ranked * 0.1)
                            top_25_percent_count = sum(1 for pos in good_data_positions if pos <= total_ranked * 0.25)

                            f.write(f"Good Data Avg Position: {avg_position:.1f} (out of {total_ranked})\n")
                            f.write(f"Good Data Median Position: {median_position} (out of {total_ranked})\n")
                            f.write(f"Good Data in Top 10%: {top_10_percent_count}/{good_data_count} ({(top_10_percent_count/good_data_count)*100:.1f}%)\n")
                            f.write(f"Good Data in Top 25%: {top_25_percent_count}/{good_data_count} ({(top_25_percent_count/good_data_count)*100:.1f}%)\n")

                        performance_grade = "EXCELLENT" if score_lift > 0.5 else \
                                           "GOOD" if score_lift > 0.2 else \
                                           "FAIR" if score_lift > 0.0 else \
                                           "POOR"
                        f.write(f"SYSTEM PERFORMANCE GRADE: {performance_grade}\n")
                        f.write("\n")

                    good_data_top_domains = good_data_df.groupBy("domain").agg(count("*").alias("frequency")) \
                                                      .orderBy(desc("frequency")) \
                                                      .limit(5) \
                                                      .collect()

                    f.write("TOP GOOD DATA DOMAINS\n")
                    f.write("-" * 40 + "\n")
                    for i, row in enumerate(good_data_top_domains, 1):
                        domain = row['domain'] if row['domain'] else 'Unknown'
                        frequency = row['frequency']
                        f.write(f"{i}. {domain:<30} {frequency:>8,}\n")
                    f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Summary report generated: {summary_path}")
        return summary_path

    except Exception as e:
        print(f"ERROR: Failed to generate summary: {e}")
        raise
    finally:
        if 'df' in locals():
            df.unpersist()