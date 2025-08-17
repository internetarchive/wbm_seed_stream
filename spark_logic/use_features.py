import os
import sys
import pandas as pd
from multiprocessing import Pool, cpu_count
import time
from typing import Dict, List, Tuple

# currently random determination and calling from after process_urls after score_urls is done.
# would it be better to use places where confidence score is the lowest?
#   - issue is confidence score is calculated in sort of a deterministic sense.

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.connect_to_db import get_connection
from features.use_content import analyze_content
from features.use_outlinks import analyze_outlinks
from features.use_requests import analyze_requests
from features.use_cdx import analyze_cdx

def get_sample_urls(sample_percentage: float = 0.01) -> List[Tuple]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT url, score, (meta->>'confidence')::float as confidence, 
                       meta->>'domain' as domain, source as data_source
                FROM urls 
                WHERE url IS NOT NULL 
                ORDER BY RANDOM()
                LIMIT (SELECT CEIL(COUNT(*) * %s) FROM urls)
            """, (sample_percentage,))
            return cur.fetchall()
    finally:
        conn.close()

def enhance_url_features(url_data: Tuple) -> Dict:
    url, original_score, confidence, domain, data_source = url_data
    
    try:
        content_features = analyze_content(url)
        outlinks_features = analyze_outlinks(url)
        request_features = analyze_requests(url)
        cdx_features = analyze_cdx(url)
        
        base_score = original_score if original_score else 0.0
        enhanced_score = base_score
        
        if content_features['has_content']:
            enhanced_score += content_features['quality_score'] * 0.3
            enhanced_score += content_features['trust_signals'] * 0.2
            enhanced_score -= content_features['spam_indicators'] * 0.4
        
        if outlinks_features['total_outlinks'] > 0:
            enhanced_score += outlinks_features['quality_ratio'] * 0.15
            enhanced_score -= outlinks_features['spam_ratio'] * 0.25
        
        if request_features['accessible']:
            if request_features['status_code'] == 200:
                enhanced_score += 0.1
            elif request_features['status_code'] >= 400:
                enhanced_score -= 0.3
            
            if request_features['redirect_count'] > 3:
                enhanced_score -= 0.1
            
            if request_features['response_time'] > 5.0:
                enhanced_score -= 0.05
        else:
            enhanced_score -= 0.5
        
        if cdx_features['last_archived']:
            days_since_archive = cdx_features['days_since_last_archive']
            if days_since_archive < 30:
                enhanced_score += 0.1
            elif days_since_archive > 365:
                enhanced_score -= 0.1
            
            enhanced_score += min(cdx_features['archive_count'] / 100, 0.2)
        else:
            enhanced_score -= 0.2
        
        enhanced_confidence = confidence if confidence else 0.5
        
        feature_count = sum([
            1 if content_features['has_content'] else 0,
            1 if outlinks_features['total_outlinks'] > 0 else 0,
            1 if request_features['accessible'] else 0,
            1 if cdx_features['last_archived'] else 0
        ])
        
        enhanced_confidence = min(enhanced_confidence + (feature_count * 0.1), 1.0)
        
        score_delta = enhanced_score - base_score
        
        return {
            'url': url,
            'original_score': base_score,
            'enhanced_score': max(0.0, min(1.0, enhanced_score)),
            'score_delta': score_delta,
            'enhanced_confidence': enhanced_confidence,
            'domain': domain,
            'data_source': data_source,
            'content_quality': content_features.get('quality_score', 0),
            'content_trust': content_features.get('trust_signals', 0),
            'content_spam': content_features.get('spam_indicators', 1),
            'outlinks_quality_ratio': outlinks_features.get('quality_ratio', 0),
            'outlinks_spam_ratio': outlinks_features.get('spam_ratio', 0),
            'status_code': request_features.get('status_code', 0),
            'response_time': request_features.get('response_time', -1),
            'redirect_count': request_features.get('redirect_count', 0),
            'days_since_archive': cdx_features.get('days_since_last_archive', -1),
            'archive_count': cdx_features.get('archive_count', 0),
            'processing_time': time.time()
        }
        
    except Exception as e:
        return {
            'url': url,
            'original_score': original_score if original_score else 0.0,
            'enhanced_score': original_score if original_score else 0.0,
            'score_delta': 0.0,
            'enhanced_confidence': confidence if confidence else 0.1,
            'domain': domain,
            'data_source': data_source,
            'error': str(e),
            'processing_time': time.time()
        }

def evaluate_enhancement_performance(results_df: pd.DataFrame) -> Dict:
    total_urls = len(results_df)
    successful_enhancements = len(results_df[results_df['score_delta'] != 0])
    
    avg_score_improvement = results_df['score_delta'].mean()
    positive_improvements = len(results_df[results_df['score_delta'] > 0])
    negative_improvements = len(results_df[results_df['score_delta'] < 0])
    
    good_data_mask = results_df['data_source'] == 'good_data'
    input_data_mask = results_df['data_source'] == 'input_data'
    
    good_data_avg_delta = results_df[good_data_mask]['score_delta'].mean() if good_data_mask.any() else 0
    input_data_avg_delta = results_df[input_data_mask]['score_delta'].mean() if input_data_mask.any() else 0
    
    high_confidence_mask = results_df['enhanced_confidence'] > 0.8
    high_conf_avg_delta = results_df[high_confidence_mask]['score_delta'].mean() if high_confidence_mask.any() else 0
    
    return {
        'total_urls_processed': total_urls,
        'successful_enhancements': successful_enhancements,
        'enhancement_rate': successful_enhancements / total_urls if total_urls > 0 else 0,
        'avg_score_improvement': avg_score_improvement,
        'positive_improvements': positive_improvements,
        'negative_improvements': negative_improvements,
        'good_data_avg_delta': good_data_avg_delta,
        'input_data_avg_delta': input_data_avg_delta,
        'high_confidence_avg_delta': high_conf_avg_delta,
        'score_delta_std': results_df['score_delta'].std(),
        'avg_enhanced_confidence': results_df['enhanced_confidence'].mean()
    }

def save_enhanced_results(results_df: pd.DataFrame, output_path: str):
    results_df.to_csv(output_path, index=False)
    print(f"Enhanced results saved to: {output_path}")

def update_database_scores(results_df: pd.DataFrame):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for _, row in results_df.iterrows():
                if row['score_delta'] != 0:
                    cur.execute("""
                        UPDATE urls 
                        SET score = %s,
                            meta = jsonb_set(
                                jsonb_set(meta, '{confidence}', %s::text::jsonb),
                                '{enhanced}', 'true'::jsonb
                            )
                        WHERE url = %s
                    """, (
                        float(row['enhanced_score']),
                        str(float(row['enhanced_confidence'])),
                        row['url']
                    ))
        conn.commit()
        print(f"Updated {len(results_df[results_df['score_delta'] != 0])} URL scores in database")
    except Exception as e:
        conn.rollback()
        print(f"Failed to update database: {e}")
    finally:
        conn.close()

def main():
    start_time = time.time()
    
    print("Fetching sample URLs...")
    sample_urls = get_sample_urls(0.01)
    print(f"Selected {len(sample_urls)} URLs for enhancement")
    
    if not sample_urls:
        print("No URLs found for enhancement")
        return
    
    print("Processing URLs with enhanced features...")
    
    num_workers = min(cpu_count(), 8)
    with Pool(num_workers) as pool:
        results = pool.map(enhance_url_features, sample_urls)
    
    results_df = pd.DataFrame(results)
    
    print("Evaluating enhancement performance...")
    performance_metrics = evaluate_enhancement_performance(results_df)
    
    print("\n" + "="*60)
    print("ENHANCEMENT PERFORMANCE REPORT")
    print("="*60)
    for metric, value in performance_metrics.items():
        if isinstance(value, float):
            print(f"{metric}: {value:.4f}")
        else:
            print(f"{metric}: {value}")
    
    output_path = f"enhanced_results_{int(time.time())}.csv"
    save_enhanced_results(results_df, output_path)
    
    if input("Update database with enhanced scores? (y/n): ").lower() == 'y':
        update_database_scores(results_df)
    
    total_time = time.time() - start_time
    print(f"\nTotal processing time: {total_time:.2f} seconds")
    print(f"Average time per URL: {total_time/len(sample_urls):.2f} seconds")

if __name__ == "__main__":
    main()