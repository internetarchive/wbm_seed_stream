#003
import pandas as pd
import re
import urllib.parse
from collections import Counter, defaultdict
import logging
from tqdm import tqdm
import math
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import networkx as nx

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_entropy(text):
    if not text:
        return 0
    char_counts = Counter(text)
    length = len(text)
    entropy = 0
    for count in char_counts.values():
        p = count / length
        entropy -= p * math.log2(p)
    return entropy

def get_tld_type(domain):
    if not domain or '.' not in domain:
        return 'none'
    tld = domain.split('.')[-1].lower()
    generic_tlds = {'com', 'org', 'net', 'info', 'biz', 'name'}
    country_tlds = {'uk', 'de', 'fr', 'jp', 'au', 'ca', 'br', 'cn', 'in', 'ru'}
    new_tlds = {'app', 'blog', 'cloud', 'dev', 'tech', 'ai', 'io', 'co'}
    suspicious_tlds = {'tk', 'ml', 'ga', 'cf', 'click', 'download', 'win', 'bid'}
    if tld in generic_tlds:
        return 'generic'
    elif tld in country_tlds:
        return 'country'
    elif tld in new_tlds:
        return 'new'
    elif tld in suspicious_tlds:
        return 'suspicious'
    else:
        return 'other'

def get_file_extension_score(extension):
    if extension == 'none':
        return 0
    common_web = {'html', 'htm', 'php', 'asp', 'jsp', 'css', 'js'}
    media = {'jpg', 'jpeg', 'png', 'gif', 'mp4', 'mp3', 'pdf'}
    suspicious = {'exe', 'bat', 'scr', 'com', 'pif', 'vbs'}
    if extension in common_web:
        return 1
    elif extension in media:
        return 2
    elif extension in suspicious:
        return 4
    else:
        return 3

def count_common_words(url):
    common_words = {
        'about', 'contact', 'home', 'news', 'blog', 'article', 'post', 'page',
        'search', 'login', 'register', 'profile', 'user', 'admin', 'help',
        'support', 'privacy', 'terms', 'policy', 'shop', 'store', 'product',
        'service', 'company', 'team', 'career', 'job', 'work', 'business'
    }
    url_lower = url.lower()
    return sum(1 for word in common_words if word in url_lower)

def detect_gibberish(url):
    clean_url = re.sub(r'https?://', '', url.lower())
    clean_url = re.sub(r'[/\-_\.]', ' ', clean_url)
    words = clean_url.split()
    gibberish_score = 0
    for word in words:
        if len(word) > 3:
            vowels = sum(1 for c in word if c in 'aeiou')
            if len(word) > 6 and vowels / len(word) < 0.2:
                gibberish_score += 1
            if re.search(r'(.)\1{3,}', word):
                gibberish_score += 1
            if re.search(r'[bcdfghjklmnpqrstvwxyz]{4,}', word):
                gibberish_score += 1
    return gibberish_score

def get_file_extension(url):
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.lower()
    if '.' in path:
        return path.split('.')[-1]
    return 'none'

def is_media_file(url):
    media_extensions = {
        'jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'svg', 'ico',
        'mp3', 'wav', 'ogg', 'flac', 'mp4', 'avi', 'mov', 'wmv', 'flv', 'webm'
    }
    ext = get_file_extension(url)
    return 1 if ext in media_extensions else 0

def is_document(url):
    doc_extensions = {'pdf', 'doc', 'docx', 'txt', 'rtf', 'xls', 'xlsx', 'ppt', 'pptx'}
    ext = get_file_extension(url)
    return 1 if ext in doc_extensions else 0

def extract_features(url):
    features = {}
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]

        features['url_length'] = len(url)
        features['domain_length'] = len(domain)
        features['path_length'] = len(parsed.path)
        features['query_length'] = len(parsed.query) if parsed.query else 0
        features['fragment_length'] = len(parsed.fragment) if parsed.fragment else 0

        features['has_https'] = 1 if url.startswith('https://') else 0
        features['has_www'] = 1 if 'www.' in domain else 0
        features['subdomain_count'] = len(domain.split('.')) - 2 if '.' in domain else 0
        features['path_depth'] = len([p for p in parsed.path.split('/') if p])
        features['param_count'] = len(parsed.query.split('&')) if parsed.query else 0

        features['digit_ratio'] = sum(c.isdigit() for c in url) / len(url)
        features['alpha_ratio'] = sum(c.isalpha() for c in url) / len(url)
        features['special_char_ratio'] = sum(not c.isalnum() and c not in ':/.-_' for c in url) / len(url)
        features['dash_count'] = url.count('-')
        features['underscore_count'] = url.count('_')
        features['dot_count'] = url.count('.')
        features['slash_count'] = url.count('/')

        features['url_entropy'] = calculate_entropy(url)
        features['domain_entropy'] = calculate_entropy(domain)
        features['path_entropy'] = calculate_entropy(parsed.path)

        features['has_long_numbers'] = 1 if re.search(r'\d{8,}', url) else 0
        features['has_hex_patterns'] = 1 if re.search(r'[a-fA-F0-9]{16,}', url) else 0
        features['has_base64_patterns'] = 1 if re.search(r'[A-Za-z0-9+/]{20,}={0,2}', url) else 0
        features['has_uuid_patterns'] = 1 if re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', url.lower()) else 0

        tld_type = get_tld_type(domain)
        features['tld_is_generic'] = 1 if tld_type == 'generic' else 0
        features['tld_is_country'] = 1 if tld_type == 'country' else 0
        features['tld_is_new'] = 1 if tld_type == 'new' else 0
        features['tld_is_suspicious'] = 1 if tld_type == 'suspicious' else 0
        
        features['has_common_words'] = count_common_words(url)
        features['has_gibberish'] = detect_gibberish(url)

        file_ext = get_file_extension(url)
        features['file_ext_score'] = get_file_extension_score(file_ext)
        features['is_media_file'] = is_media_file(url)
        features['is_document'] = is_document(url)

    except Exception as e:
        logger.debug(f"Feature extraction failed for {url}: {e}")
        features = {
            'url_length': 0, 'domain_length': 0, 'path_length': 0, 'query_length': 0,
            'fragment_length': 0, 'has_https': 0, 'has_www': 0, 'subdomain_count': 0,
            'path_depth': 0, 'param_count': 0, 'digit_ratio': 0, 'alpha_ratio': 0,
            'special_char_ratio': 0, 'dash_count': 0, 'underscore_count': 0,
            'dot_count': 0, 'slash_count': 0, 'url_entropy': 0, 'domain_entropy': 0,
            'path_entropy': 0, 'has_long_numbers': 0, 'has_hex_patterns': 0,
            'has_base64_patterns': 0, 'has_uuid_patterns': 0, 'tld_is_generic': 0,
            'tld_is_country': 0, 'tld_is_new': 0, 'tld_is_suspicious': 0,
            'has_common_words': 0, 'has_gibberish': 0, 'file_ext_score': 0,
            'is_media_file': 0, 'is_document': 0
        }
    return features

def extract_domain(url):
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None

def learn_domain_patterns(urls_df):
    logger.info("Learning domain patterns...")
    domain_stats = defaultdict(lambda: {
        'urls': [],
        'features': [],
        'timestamps': []
    })
    
    for _, row in urls_df.iterrows():
        domain = extract_domain(row['url'])
        if domain:
            features = extract_features(row['url'])
            domain_stats[domain]['urls'].append(row['url'])
            domain_stats[domain]['features'].append(features)
            domain_stats[domain]['timestamps'].append(row['timestamp'])

    quality_indicators = defaultdict(float)
    spam_indicators = defaultdict(float)
    
    for domain, stats in domain_stats.items():
        if len(stats['urls']) < 5:
            continue

        features_df = pd.DataFrame(stats['features'])

        url_diversity = len(set(stats['urls'])) / len(stats['urls'])
        path_diversity = len(set(urllib.parse.urlparse(url).path for url in stats['urls'])) / len(stats['urls'])

        avg_entropy = features_df['url_entropy'].mean()
        avg_length = features_df['url_length'].mean()
        param_usage = features_df['param_count'].mean()

        quality_score = 0
        if url_diversity > 0.8:
            quality_score += 2
        if path_diversity > 0.7:
            quality_score += 2
        if features_df['has_https'].mean() > 0.8:
            quality_score += 1
        if avg_entropy < 4.0:
            quality_score += 1
        if features_df['has_common_words'].mean() > 1:
            quality_score += 1

        spam_score = 0
        if url_diversity < 0.3:
            spam_score += 3
        if avg_length > 200:
            spam_score += 2
        if features_df['has_gibberish'].mean() > 0.5:
            spam_score += 3
        if features_df['has_long_numbers'].mean() > 0.3:
            spam_score += 2
        if param_usage > 10:
            spam_score += 2

        quality_indicators[domain] = quality_score
        spam_indicators[domain] = spam_score
    
    return quality_indicators, spam_indicators

def calculate_domain_similarity(domain1, domain2):
    tld1 = get_tld_type(domain1)
    tld2 = get_tld_type(domain2)
    tld_sim = 1.0 if tld1 == tld2 else 0.0
    set1 = set(domain1.split('.'))
    set2 = set(domain2.split('.'))
    jaccard = len(set1 & set2) / len(set1 | set2) if set1 | set2 else 0
    return (tld_sim + jaccard) / 2

def build_domain_graph(urls_df):
    logger.info("Building domain relationship graph...")
    domain_graph = nx.Graph()
    domain_counts = Counter()
    
    for _, row in urls_df.iterrows():
        domain = extract_domain(row['url'])
        if domain:
            domain_counts[domain] += 1
            domain_graph.add_node(domain)

    domains = list(domain_counts.keys())
    for i, domain1 in enumerate(domains):
        for domain2 in domains[i+1:]:
            similarity = calculate_domain_similarity(domain1, domain2)
            if similarity > 0.7:
                domain_graph.add_edge(domain1, domain2, weight=similarity)
    
    return domain_graph

def detect_anomalies(urls_df):
    logger.info("Training anomaly detection model...")
    scaler = StandardScaler()
    anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
    
    features_list = []
    for _, row in urls_df.iterrows():
        features = extract_features(row['url'])
        features_list.append([float(v) for v in features.values()])

    features_array = np.array(features_list)
    features_array = np.nan_to_num(features_array, nan=0.0, posinf=0.0, neginf=0.0)
    features_scaled = scaler.fit_transform(features_array)
    anomaly_scores = anomaly_detector.fit_predict(features_scaled)
    return anomaly_scores

def calculate_priority_score(url, domain_counts, total_urls, quality_indicators, spam_indicators, domain_graph, anomaly_score=0):
    domain = extract_domain(url)
    features = extract_features(url)

    score = 0.0
    reasons = {}

    if features['url_length'] > 200:
        score += 3
        reasons['length'] = 'very_long_url'
    elif features['url_length'] > 100:
        score += 1
        reasons['length'] = 'long_url'

    if features['url_entropy'] > 5.5:
        score += 2
        reasons['entropy'] = 'high_entropy'
    elif features['url_entropy'] < 3.0:
        score -= 1
        reasons['entropy'] = 'structured'

    if features['has_https']:
        score -= 1
        reasons['security'] = 'https'

    if features['has_common_words'] > 2:
        score -= 1
        reasons['content'] = 'common_words'

    if features['has_gibberish'] > 0:
        score += features['has_gibberish']
        reasons['content'] = 'gibberish_detected'

    if features['is_media_file']:
        score += 2
        reasons['file_type'] = 'media_file'

    if features['param_count'] > 10:
        score += 2
        reasons['complexity'] = 'many_parameters'

    if anomaly_score == -1:
        score += 5
        reasons['anomaly'] = 'ml_detected_anomaly'

    if domain:
        if domain in quality_indicators:
            score -= quality_indicators[domain] * 0.5
            if quality_indicators[domain] > 3:
                reasons['domain_quality'] = 'high_quality_domain'

        if domain in spam_indicators:
            score += spam_indicators[domain] * 0.8
            if spam_indicators[domain] > 3:
                reasons['domain_spam'] = 'spam_indicators'

        if domain in domain_counts:
            freq_pct = (domain_counts[domain] / total_urls) * 100
            if freq_pct > 10:
                score += 8
                reasons['frequency'] = f'very_high_frequency({freq_pct:.1f}%)'
            elif freq_pct > 5:
                score += 4
                reasons['frequency'] = f'high_frequency({freq_pct:.1f}%)'
            elif freq_pct > 1:
                score += 1
                reasons['frequency'] = f'elevated_frequency({freq_pct:.1f}%)'

        if domain_graph.has_node(domain):
            neighbors = list(domain_graph.neighbors(domain))
            if len(neighbors) > 10:
                score += 2
                reasons['network'] = 'highly_connected_domain'

    return max(0, score), reasons

def analyze_file(filepath, sample_size=None):
    logger.info(f"Loading file: {filepath}")
    df = pd.read_csv(filepath, sep='\t', header=None, names=['timestamp', 'url'])
    logger.info(f"Loaded {len(df)} URLs")

    if sample_size and len(df) > sample_size:
        df = df.sample(n=sample_size, random_state=42)
        logger.info(f"Sampled {len(df)} URLs for analysis")

    domain_counts = Counter()
    for _, row in df.iterrows():
        domain = extract_domain(row['url'])
        if domain:
            domain_counts[domain] += 1

    total_urls = len(df)

    quality_indicators, spam_indicators = learn_domain_patterns(df)
    domain_graph = build_domain_graph(df)
    anomaly_scores = detect_anomalies(df)

    logger.info("Calculating priority scores...")
    results = []

    for idx, (_, row) in enumerate(tqdm(df.iterrows(), total=len(df), desc="Analyzing URLs")):
        url = row['url']
        timestamp = row['timestamp']
        domain = extract_domain(url)

        anomaly_score = anomaly_scores[idx] if idx < len(anomaly_scores) else 0
        score, reasons = calculate_priority_score(url, domain_counts, total_urls, quality_indicators, spam_indicators, domain_graph, anomaly_score)

        results.append({
            'url': url,
            'timestamp': timestamp,
            'domain': domain,
            'priority_score': score,
            'reasons': str(reasons) if reasons else '',
            'anomaly_score': anomaly_score,
            'domain_frequency': domain_counts.get(domain, 0) if domain else 0,
            'domain_frequency_pct': (domain_counts.get(domain, 0) / total_urls * 100) if domain else 0
        })

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('priority_score', ascending=False)

    return results_df, domain_counts, quality_indicators, spam_indicators, domain_graph

def generate_summary(results_df, domain_counts, quality_indicators, spam_indicators, domain_graph):
    total_urls = len(results_df)
    score_stats = results_df['priority_score'].describe()

    excellent = len(results_df[results_df['priority_score'] < 0])
    good = len(results_df[(results_df['priority_score'] >= 0) & (results_df['priority_score'] < 3)])
    medium = len(results_df[(results_df['priority_score'] >= 3) & (results_df['priority_score'] < 8)])
    poor = len(results_df[results_df['priority_score'] >= 8])

    anomalies = len(results_df[results_df['anomaly_score'] == -1])

    quality_domains = len([d for d, s in quality_indicators.items() if s > 3])
    spam_domains = len([d for d, s in spam_indicators.items() if s > 3])

    return {
        'total_urls': total_urls,
        'unique_domains': len(domain_counts),
        'score_stats': score_stats,
        'quality_distribution': {
            'excellent': excellent,
            'good': good,
            'medium': medium,
            'poor': poor
        },
        'ml_insights': {
            'anomalies_detected': anomalies,
            'quality_domains_learned': quality_domains,
            'spam_domains_learned': spam_domains,
            'domain_relationships': domain_graph.number_of_edges()
        }
    }

def main():
    INPUT_FILE = "data/indexnow/indexnow-log-bing-20250618-005959.tsv"
    OUTPUT_FILE = "intelligent_urls.csv"
    SAMPLE_SIZE = 10000

    try:
        results_df, domain_counts, quality_indicators, spam_indicators, domain_graph = analyze_file(INPUT_FILE, sample_size=SAMPLE_SIZE)
        summary = generate_summary(results_df, domain_counts, quality_indicators, spam_indicators, domain_graph)

        results_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Results saved to {OUTPUT_FILE}")

        print("\n" + "="*60)
        print("INTELLIGENT ML-BASED ANALYSIS SUMMARY")
        print("="*60)
        print(f"Total URLs analyzed: {summary['total_urls']:,}")
        print(f"Unique domains: {summary['unique_domains']:,}")

        print("\nML Insights:")
        print(f"  Anomalies detected: {summary['ml_insights']['anomalies_detected']:,}")
        print(f"  Quality domains learned: {summary['ml_insights']['quality_domains_learned']:,}")
        print(f"  Spam domains learned: {summary['ml_insights']['spam_domains_learned']:,}")
        print(f"  Domain relationships: {summary['ml_insights']['domain_relationships']:,}")

        print("\nQuality Distribution:")
        for category, count in summary['quality_distribution'].items():
            pct = (count / summary['total_urls']) * 100
            print(f"  {category.title()}: {count:,} ({pct:.1f}%)")

        print("\nTop 5 Highest Priority URLs:")
        for _, row in results_df.head(5).iterrows():
            print(f"  Score {row['priority_score']:.1f}: {row['url'][:80]}...")

        print("\nTop 5 Lowest Priority URLs:")
        for _, row in results_df.tail(5).iterrows():
            print(f"  Score {row['priority_score']:.1f}: {row['url'][:80]}...")

    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    main()