import os
import sys
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, desc, asc, avg as spark_avg, stddev, 
    percentile_approx, sum as spark_sum, corr, when, abs as spark_abs
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from spark.config.spark_config import SparkConfig

def create_summary(processed_df: DataFrame, parquet_path: str, output_dir: str):
    start_time = time.time()
    is_multi_model = 'model_name' in processed_df.columns

    if is_multi_model:
        models = [row.model_name for row in processed_df.select('model_name').distinct().collect()]
    else:
        models = [os.path.basename(os.path.normpath(output_dir))]

    summary_path = os.path.join(output_dir, "job_summary.txt")

    try:
        processed_df.persist()
        with open(summary_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("URL PROCESSING SUMMARY REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Data Source (Parquet): {parquet_path}\n")

            for model_name in models:
                f.write("\n" + "=" * 80 + "\n")
                f.write(f"MODEL: {model_name.upper()}\n")
                f.write("=" * 80 + "\n")

                model_df = processed_df.filter(col('model_name') == model_name) if is_multi_model else processed_df
                model_df.persist()

                total_urls = model_df.count()
                if total_urls == 0:
                    f.write(f"No data found for model '{model_name}'.\n")
                    model_df.unpersist()
                    continue

                f.write(f"Processing Time: {(time.time() - start_time):.2f} seconds\n")
                f.write("\n")
                f.write("OVERALL STATISTICS\n")
                f.write("-" * 40 + "\n")

                unique_urls = model_df.select("url").distinct().count()
                spam_count_row = model_df.select(spark_sum(col("is_spam").cast("int"))).first()
                spam_count = spam_count_row[0] if spam_count_row else 0
                non_spam_count = total_urls - spam_count

                overall_stats_df = model_df.agg(
                    spark_avg("score").alias("avg_score"),
                    spark_avg("confidence").alias("avg_confidence"),
                    stddev("score").alias("score_stddev"),
                    percentile_approx("score", 0.5, 10000).alias("median_score"),
                    percentile_approx("score", 0.75, 10000).alias("p75_score"),
                    percentile_approx("score", 0.95, 10000).alias("p95_score")
                ).first()

                f.write(f"Total URLs Processed: {total_urls:,}\n")
                f.write(f"Unique URLs: {unique_urls:,}\n")
                f.write(f"Non-Spam URLs: {non_spam_count:,}\n")
                f.write(f"Spam URLs: {spam_count:,}\n")
                f.write(f"Average Score: {overall_stats_df['avg_score']:.4f}\n")
                f.write(f"Average Confidence: {overall_stats_df['avg_confidence']:.4f}\n")
                if overall_stats_df['score_stddev']:
                    f.write(f"Score StdDev: {overall_stats_df['score_stddev']:.4f}\n")
                    f.write(f"Median Score: {overall_stats_df['median_score']:.4f}\n")
                    f.write(f"75th Percentile Score: {overall_stats_df['p75_score']:.4f}\n")
                    f.write(f"95th Percentile Score: {overall_stats_df['p95_score']:.4f}\n")

                domain_freq_data = model_df.groupBy("domain").count().orderBy(desc("count")).limit(10).collect()
                best_urls_data = model_df.filter(col("score").isNotNull()).orderBy(desc("score")).limit(10).select("url", "score", "confidence", "domain", "data_source").collect()
                worst_urls_data = model_df.filter(col("score").isNotNull()).orderBy(asc("score")).limit(10).select("url", "score", "confidence", "domain", "data_source").collect()

                f.write("\nTOP 10 MOST FREQUENT DOMAINS\n")
                f.write("-" * 40 + "\n")
                for i, (domain, frequency) in enumerate([(row['domain'], row['count']) for row in domain_freq_data], 1):
                    domain = domain if domain else 'Unknown'
                    percentage = (frequency / total_urls) * 100
                    f.write(f"{i:2d}. {domain:<30} {frequency:>8,} ({percentage:>6.2f}%)\n")

                f.write("\nTOP 10 BEST URLs (HIGHEST SCORE)\n")
                f.write("-" * 40 + "\n")
                for i, (url, score, confidence, domain, data_source) in enumerate([(row['url'], row['score'], row['confidence'], row['domain'], row['data_source']) for row in best_urls_data], 1):
                    f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f}")
                    if SparkConfig.USE_GOOD_DATA and data_source:
                        f.write(f" | {'[GOOD DATA]' if data_source == 'good_data' else '[INPUT DATA]'}\n")
                    else:
                        f.write("\n")
                    f.write(f"     Domain: {domain if domain else 'Unknown'}\n")
                    f.write(f"     URL: {url}\n\n")

                f.write("BOTTOM 10 WORST URLs (LOWEST SCORE)\n")
                f.write("-" * 40 + "\n")
                for i, (url, score, confidence, domain, data_source) in enumerate([(row['url'], row['score'], row['confidence'], row['domain'], row['data_source']) for row in worst_urls_data], 1):
                    f.write(f"{i:2d}. Score: {score:.4f} | Confidence: {confidence:.4f}")
                    if SparkConfig.USE_GOOD_DATA and data_source:
                        f.write(f" | {'[GOOD DATA]' if data_source == 'good_data' else '[INPUT DATA]'}\n")
                    else:
                        f.write("\n")
                    f.write(f"     Domain: {domain if domain else 'Unknown'}\n")
                    f.write(f"     URL: {url}\n\n")

                if SparkConfig.USE_GOOD_DATA:
                    data_source_stats_list = model_df.groupBy("data_source").agg(
                        count("*").alias("count"),
                        spark_avg("score").alias("avg_score"),
                        spark_sum(col("is_spam").cast("int")).alias("spam_count")
                    ).collect()
                    data_source_stats = {row["data_source"]: row for row in data_source_stats_list}
                    good_data_stats = data_source_stats.get("good_data")
                    input_data_stats = data_source_stats.get("input_data")

                    if good_data_stats:
                        f.write("GOOD DATA BENCHMARK ANALYSIS\n")
                        f.write("-" * 40 + "\n")
                        good_data_count = good_data_stats['count']
                        f.write(f"Good Data Count: {good_data_count:,}\n")
                        if total_urls > 0:
                            f.write(f"Good Data Percentage: {(good_data_count/total_urls)*100:.2f}%\n\n")

                        good_df = model_df.filter(col("data_source") == "good_data")
                        good_data_spam_count = good_data_stats['spam_count']
                        good_data_non_spam_count = good_data_count - good_data_spam_count

                        good_data_percentiles = good_df.agg(
                            percentile_approx("score", 0.75, 10000).alias("p75"),
                            percentile_approx("score", 0.95, 10000).alias("p95")
                        ).first()

                        if good_data_percentiles and good_data_percentiles.p75 is not None:
                            gd_p75 = good_data_percentiles.p75
                            f.write(f"Good Data 75th Percentile: {gd_p75:.4f}\n")
                        if good_data_percentiles and good_data_percentiles.p95 is not None:
                            gd_p95 = good_data_percentiles.p95
                            f.write(f"Good Data 95th Percentile: {gd_p95:.4f}\n")

                        if good_data_count > 0:
                            f.write(f"Good Data Spam Rate: {(good_data_spam_count/good_data_count)*100:.2f}%\n")
                            f.write(f"Good Data Non-Spam Rate: {(good_data_non_spam_count/good_data_count)*100:.2f}%\n")
                        f.write("\n")

                        if input_data_stats and input_data_stats['avg_score'] is not None and good_data_stats['avg_score'] is not None:
                            score_lift = good_data_stats['avg_score'] - input_data_stats['avg_score']

                            f.write("SCORING SYSTEM PERFORMANCE\n")
                            f.write("-" * 40 + "\n")
                            f.write(f"Good Data Avg Score: {good_data_stats['avg_score']:.4f}\n")
                            f.write(f"Input Data Avg Score: {input_data_stats['avg_score']:.4f}\n")
                            f.write(f"Score Lift (Good - Input): {score_lift:.4f}\n")
                            if input_data_stats['avg_score'] > 0:
                                f.write(f"Score Lift Percentage: {(score_lift/input_data_stats['avg_score'])*100:.2f}%\n")

                            good_data_in_top_10 = sum(1 for row in best_urls_data if row['data_source'] == "good_data")
                            good_data_in_bottom_10 = sum(1 for row in worst_urls_data if row['data_source'] == "good_data")

                            f.write(f"Good Data in Top 10: {good_data_in_top_10}/10\n")
                            f.write(f"Good Data in Bottom 10: {good_data_in_bottom_10}/10\n")

                            performance_grade = "EXCELLENT" if score_lift > 0.5 else \
                                              "GOOD" if score_lift > 0.2 else \
                                              "FAIR" if score_lift > 0.0 else \
                                              "POOR"
                            f.write(f"SYSTEM PERFORMANCE GRADE: {performance_grade}\n")
                            f.write("\n")

                        good_data_top_domains = good_df.groupBy("domain").count().orderBy(desc("count")).limit(10).collect()
                        if good_data_top_domains:
                            f.write("TOP GOOD DATA DOMAINS\n")
                            f.write("-" * 40 + "\n")
                            for i, (domain, frequency) in enumerate([(row['domain'], row['count']) for row in good_data_top_domains], 1):
                                domain = domain if domain else 'Unknown'
                                f.write(f"{i}. {domain:<30} {frequency:>8,}\n")
                            f.write("\n")

                model_df.unpersist()

            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")
        
        print(f"Summary report generated: {summary_path}")
        return summary_path

    except Exception as e:
        import traceback
        print(f"ERROR: Failed to generate summary: {e}")
        traceback.print_exc()
        raise
    finally:
        if 'processed_df' in locals() and processed_df.is_cached:
            processed_df.unpersist()


def create_comparison_summary(comparison_df: DataFrame, output_dir: str):
    start_time = time.time()
    summary_path = os.path.join(output_dir, "comparison_summary.txt")

    try:
        comparison_df.persist()
        total_common_urls = comparison_df.count()

        if total_common_urls == 0:
            print("No common URLs found between models to compare.")
            return

        with open(summary_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("MODEL COMPARISON REPORT (CLASSICAL vs. LIGHTGBM)\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total URLs Compared (in common): {total_common_urls:,}\n")
            f.write(f"Comparison Generation Time: {(time.time() - start_time):.2f} seconds\n")
            f.write("\n")

            f.write("SCORE CORRELATION\n")
            f.write("-" * 40 + "\n")
            score_corr_row = comparison_df.agg(corr("score_classical", "score_lightgbm").alias("correlation")).first()
            score_correlation = score_corr_row['correlation'] if score_corr_row else 0.0
            f.write(f"Pearson Correlation between scores: {score_correlation:.4f}\n\n")

            f.write("SPAM CLASSIFICATION AGREEMENT\n")
            f.write("-" * 40 + "\n")
            agreement_counts = comparison_df.withColumn(
                "agreement_type",
                when(col("is_spam_classical") == col("is_spam_lightgbm"), "Agree")
                .otherwise("Disagree")
            ).groupBy("agreement_type").count().collect()
            
            agreement_stats = {row['agreement_type']: row['count'] for row in agreement_counts}
            agree_count = agreement_stats.get('Agree', 0)
            disagree_count = agreement_stats.get('Disagree', 0)

            f.write(f"URLs with same classification: {agree_count:,} ({(agree_count/total_common_urls)*100:.2f}%)\n")
            f.write(f"URLs with different classification: {disagree_count:,} ({(disagree_count/total_common_urls)*100:.2f}%)\n\n")

            f.write("CLASSIFICATION BREAKDOWN\n")
            f.write("-" * 40 + "\n")
            
            cm_stats = comparison_df.groupBy("is_spam_classical", "is_spam_lightgbm").count().collect()
            cm_map = {(row['is_spam_classical'], row['is_spam_lightgbm']): row['count'] for row in cm_stats}

            agree_ham = cm_map.get((False, False), 0)
            agree_spam = cm_map.get((True, True), 0)
            classical_spam_lgbm_ham = cm_map.get((True, False), 0)
            classical_ham_lgbm_spam = cm_map.get((False, True), 0)

            f.write(f"  - Classified as HAM by both:      {agree_ham:,}\n")
            f.write(f"  - Classified as SPAM by both:     {agree_spam:,}\n")
            f.write(f"  - Classical SPAM -> LightGBM HAM: {classical_spam_lgbm_ham:,}\n")
            f.write(f"  - Classical HAM -> LightGBM SPAM: {classical_ham_lgbm_spam:,}\n\n")

            f.write("SCORE DIFFERENCE ANALYSIS\n")
            f.write("-" * 40 + "\n")
            
            diff_df = comparison_df.withColumn("score_diff", spark_abs(col("score_classical") - col("score_lightgbm")))
            
            top_diff_urls = diff_df.orderBy(desc("score_diff")).limit(10).select("url", "score_classical", "score_lightgbm", "score_diff").collect()
            
            f.write("TOP 10 URLs WITH LARGEST SCORE DIFFERENCE\n")
            for i, row in enumerate(top_diff_urls, 1):
                f.write(f"{i:2d}. Diff: {row['score_diff']:.4f}\n")
                f.write(f"     URL: {row['url']}\n")
                f.write(f"     Scores (Classical | LightGBM): {row['score_classical']:.4f} | {row['score_lightgbm']:.4f}\n\n")

            f.write("=" * 80 + "\n")
            f.write("END OF COMPARISON REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Comparison summary report generated: {summary_path}")

    except Exception as e:
        import traceback
        print(f"ERROR: Failed to generate comparison summary: {e}")
        traceback.print_exc()
        raise
    finally:
        if 'comparison_df' in locals() and comparison_df.is_cached:
            comparison_df.unpersist()