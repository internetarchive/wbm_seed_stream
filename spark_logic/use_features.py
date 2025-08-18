import os
import sys
import pandas as pd
import time
from typing import Iterator

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rand, row_number, floor
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from features.use_content import analyze_content
from features.use_outlinks import analyze_outlinks
from features.use_requests import analyze_requests
from features.use_cdx import analyze_cdx

ENHANCED_SCHEMA = StructType([
    StructField("url", StringType(), nullable=False),
    StructField("domain", StringType(), nullable=True),
    StructField("original_score", DoubleType(), nullable=True),
    StructField("enhanced_score", DoubleType(), nullable=True),
    StructField("score_delta", DoubleType(), nullable=True),
    StructField("original_confidence", DoubleType(), nullable=True),
    StructField("enhanced_confidence", DoubleType(), nullable=True),
    StructField("content_quality", FloatType(), nullable=True),
    StructField("content_trust", FloatType(), nullable=True),
    StructField("content_spam", FloatType(), nullable=True),
    StructField("outlinks_quality_ratio", FloatType(), nullable=True),
    StructField("outlinks_spam_ratio", FloatType(), nullable=True),
    StructField("status_code", IntegerType(), nullable=True),
    StructField("response_time", FloatType(), nullable=True),
    StructField("redirect_count", IntegerType(), nullable=True),
    StructField("days_since_archive", IntegerType(), nullable=True),
    StructField("archive_count", IntegerType(), nullable=True),
    StructField("etag", StringType(), nullable=True),
    StructField("last_modified", StringType(), nullable=True),
])

def enhance_url_features_batch(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for url_data in iterator:
        batch_size = len(url_data)
        print(f"Worker starting feature analysis for a batch of {batch_size} URLs...")
        batch_start_time = time.time()

        results = []
        processed_count = 0

        for _, row in url_data.iterrows():
            url = row['url']
            original_score = row.get('score', 0.0)
            confidence = row.get('confidence', 0.5)
            domain = row.get('domain', '')

            try:
                request_features = analyze_requests(url)
                cdx_features = analyze_cdx(url)

                content_features = {'has_content': False, 'quality_score': 0, 'trust_signals': 0, 'spam_indicators': 1}
                outlinks_features = {'total_outlinks': 0, 'quality_ratio': 0, 'spam_ratio': 0}

                if request_features.get('accessible') and request_features.get('status_code') == 200:
                    content_features = analyze_content(url)
                    outlinks_features = analyze_outlinks(url)

                base_score = original_score if original_score is not None else 0.0
                enhanced_score = base_score

                if content_features.get('has_content'):
                    enhanced_score += content_features.get('quality_score', 0) * 0.3
                    enhanced_score += content_features.get('trust_signals', 0) * 0.2
                    enhanced_score -= content_features.get('spam_indicators', 1) * 0.4

                if outlinks_features.get('total_outlinks', 0) > 0:
                    enhanced_score += outlinks_features.get('quality_ratio', 0) * 0.15
                    enhanced_score -= outlinks_features.get('spam_ratio', 0) * 0.25

                if request_features.get('accessible'):
                    if request_features.get('status_code') == 200:
                        enhanced_score += 0.1
                    elif request_features.get('status_code', 0) >= 400:
                        enhanced_score -= 0.3
                    if request_features.get('redirect_count', 0) > 3:
                        enhanced_score -= 0.1
                    if request_features.get('response_time', 0) > 5.0:
                        enhanced_score -= 0.05
                else:
                    enhanced_score -= 0.5

                if cdx_features.get('last_archived'):
                    days_since_archive = cdx_features.get('days_since_last_archive', -1)
                    if days_since_archive != -1 and days_since_archive < 30:
                        enhanced_score += 0.1
                    elif days_since_archive > 365:
                        enhanced_score -= 0.1
                    enhanced_score += min(cdx_features.get('archive_count', 0) / 100.0, 0.2)
                else:
                    enhanced_score -= 0.2

                enhanced_confidence = confidence if confidence is not None else 0.5
                feature_count = sum([
                    1 if content_features.get('has_content') else 0,
                    1 if outlinks_features.get('total_outlinks', 0) > 0 else 0,
                    1 if request_features.get('accessible') else 0,
                    1 if cdx_features.get('last_archived') else 0
                ])
                enhanced_confidence = min(enhanced_confidence + (feature_count * 0.1), 1.0)
                score_delta = enhanced_score - base_score

                results.append({
                    'url': url,
                    'domain': domain,
                    'original_score': base_score,
                    'enhanced_score': enhanced_score,
                    'score_delta': score_delta,
                    'original_confidence': confidence,
                    'enhanced_confidence': enhanced_confidence,
                    'content_quality': content_features.get('quality_score'),
                    'content_trust': content_features.get('trust_signals'),
                    'content_spam': content_features.get('spam_indicators'),
                    'outlinks_quality_ratio': outlinks_features.get('quality_ratio'),
                    'outlinks_spam_ratio': outlinks_features.get('spam_ratio'),
                    'status_code': request_features.get('status_code'),
                    'response_time': request_features.get('response_time'),
                    'redirect_count': request_features.get('redirect_count'),
                    'days_since_archive': cdx_features.get('days_since_last_archive'),
                    'archive_count': cdx_features.get('archive_count'),
                    'etag': request_features.get('etag'),
                    'last_modified': request_features.get('last_modified'),
                })

            except Exception as e:
                print(f"ERROR processing URL '{url}': {e}")
                results.append({
                    'url': url,
                    'domain': domain,
                    'original_score': original_score,
                    'enhanced_score': original_score,
                    'score_delta': 0.0,
                    'original_confidence': confidence,
                    'enhanced_confidence': confidence,
                })

            processed_count += 1
            if processed_count % 100 == 0:
                print(f"  ... worker processed {processed_count}/{batch_size} URLs in this batch.")

        batch_duration = time.time() - batch_start_time
        print(f"Worker finished batch of {batch_size} URLs in {batch_duration:.2f} seconds.")

        if results:
            yield pd.DataFrame(results)


def enhance_scores_with_features(processed_df: DataFrame) -> DataFrame:
    MAX_SAMPLE_SIZE = 6500
    SKEW_FACTOR = 100

    df_for_sampling = processed_df.select("url", "domain", "score", "confidence")

    salted_window = Window.partitionBy("domain", (floor(rand() * SKEW_FACTOR))).orderBy(rand())
    intermediate_sample = df_for_sampling.withColumn("row_num", row_number().over(salted_window)) \
                                      .filter(col("row_num") == 1) \
                                      .drop("row_num")

    domain_window = Window.partitionBy("domain").orderBy(rand())
    sampled_df = intermediate_sample.withColumn("row_num", row_number().over(domain_window)) \
                                    .filter(col("row_num") == 1) \
                                    .drop("row_num")

    print(f"Sampling up to {MAX_SAMPLE_SIZE} URLs (one per domain) for deep feature analysis.")
    sampled_df = sampled_df.limit(MAX_SAMPLE_SIZE)

    urls_to_enhance = sampled_df.persist()

    if not urls_to_enhance.take(1):
        print("No URLs were sampled for enhancement.")
        urls_to_enhance.unpersist()
        return processed_df.sparkSession.createDataFrame([], ENHANCED_SCHEMA)

    print(f"Applying enhanced feature analysis to sampled URLs...")

    enhanced_features_df = urls_to_enhance.mapInPandas(
        enhance_url_features_batch,
        schema=ENHANCED_SCHEMA
    )

    urls_to_enhance.unpersist()

    return enhanced_features_df
