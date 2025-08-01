import os
import sys
import re
from typing import Dict, FrozenSet
from functools import lru_cache
import numpy as np
import pandas as pd
from collections import Counter
import hashlib
import math

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from schema import PROCESSED_URL_SCHEMA

CURRENT_TIMESTAMP = pd.Timestamp.now()
MAX_URL_LENGTH = 2048

@lru_cache(maxsize=1)
def load_list_from_file(filepath: str) -> FrozenSet[str]:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Required file not found: {filepath}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = {line.strip().lower() for line in f if line.strip()}
        if not lines:
            raise ValueError(f"File is empty or contains no valid entries: {filepath}")
        return frozenset(lines)
    except (IOError, OSError) as e:
        raise IOError(f"Failed to read file {filepath}: {e}")

NSFW_KEYWORDS = load_list_from_file("data/storage/lists/nsfw_keywords.txt")
SPAM_KEYWORDS = load_list_from_file("data/storage/lists/spam_keywords.txt")
NSFW_DOMAINS = load_list_from_file("data/storage/lists/nsfw_domains.txt")
QUALITY_DOMAINS = load_list_from_file("data/storage/lists/quality_domains.txt")
NEWS_DOMAINS = load_list_from_file("data/storage/lists/news_domains.txt")
UGC_DOMAINS = frozenset({'github.com', 'gitlab.com', 'gist.github.com', 'sourceforge.net', 'bitbucket.org'})

def build_regex_pattern(keywords: FrozenSet[str]) -> re.Pattern | None:
    if not keywords:
        return None
    return re.compile('|'.join(re.escape(k) for k in sorted(list(keywords), key=len, reverse=True)), re.IGNORECASE)

NSFW_PATTERN = build_regex_pattern(NSFW_KEYWORDS)
SPAM_PATTERN = build_regex_pattern(SPAM_KEYWORDS)

URL_PARSE_REGEX = re.compile(r'^(?:[a-z]+:)?//(?P<domain>[^/?#:]+)(?P<path>[^?#]*)(?:\?(?P<query>[^#]*))?.*$')
IP_ADDRESS_PATTERN = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
SHORTENER_PATTERN = re.compile(r'\b(bit\.ly|tinyurl|goo\.gl|t\.co|short\.link|ow\.ly)\b', re.IGNORECASE)

CONTENT_PATTERNS = {
    'contains_download': re.compile(r'\b(download|exe|zip|rar|torrent|crack|keygen|apk)\b', re.IGNORECASE),
    'contains_phishing': re.compile(r'\b(login|signin|account|verify|suspended|update|confirm|secure|password)\b', re.IGNORECASE),
    'contains_financial': re.compile(r'\b(bank|paypal|amazon|ebay|visa|mastercard|credit|card|payment)\b', re.IGNORECASE),
    'contains_urgency': re.compile(r'\b(urgent|immediate|expire|limited|act now|hurry|last chance)\b', re.IGNORECASE),
    'is_media_or_api': re.compile(r'\b(api|json|xml|v[1-9]/|/media/)\b', re.IGNORECASE),
    'path_contains_sensitive': re.compile(r'/(login|admin|credentials|wp-admin|config|backup)\b', re.IGNORECASE),
}

SUSPICIOUS_TLDS = frozenset({'.tk', '.ml', '.ga', '.cf', '.gq', '.ru', '.cn', '.cc', '.info', '.biz', '.xyz', '.top'})
TRUSTWORTHY_TLDS = frozenset({'.gov', '.edu', '.mil'})

@lru_cache(maxsize=1024)
def get_shannon_entropy(text: str) -> float:
    if not text or len(text) <= 1:
        return 0.0
    counts = Counter(text)
    length = len(text)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())

def calculate_entropy_vectorized(series: pd.Series) -> np.ndarray:
    unique_values = series.unique()
    entropy_map = {val: get_shannon_entropy(val) for val in unique_values}
    return series.map(entropy_map).values.astype(np.float32)

def parse_urls_vectorized(urls: pd.Series) -> pd.DataFrame:
    urls_str = urls.astype(str)
    no_scheme_mask = ~urls_str.str.contains('://', na=False, regex=False)
    if no_scheme_mask.any():
        urls_str = urls_str.copy()
        urls_str.loc[no_scheme_mask] = '//' + urls_str.loc[no_scheme_mask]

    parts = urls_str.str.extract(URL_PARSE_REGEX)
    parts.rename(columns={'domain': 'domain', 'path': 'path', 'query': 'query'}, inplace=True)
    
    parts['domain'] = parts['domain'].str.lower().fillna('unknown.invalid')
    parts['path'] = parts['path'].fillna('')
    parts['query'] = parts['query'].fillna('')
    return parts

def extract_features_vectorized(pdf: pd.DataFrame) -> Dict[str, np.ndarray]:
    features = {}
    urls, domains, paths, queries, timestamps = pdf['url'], pdf['domain'], pdf['path'], pdf['query'], pdf['timestamp']
    path_query = paths + '?' + queries
    
    url_lengths = urls.str.len().fillna(0)
    features['very_short_url'] = (url_lengths <= 15).values
    features['very_long_url'] = (url_lengths > 1024).values
    features['no_scheme'] = (~urls.str.contains('://', na=False, regex=False)).values
    features['long_url'] = (url_lengths > 200).values
    features['path_keyword_stuffing'] = (paths.str.count('/') > 10) & (paths.str.count('-') > 10)

    tlds = domains.str.rsplit('.', n=1, expand=True)[1].fillna('')
    features['suspicious_tld'] = tlds.isin(SUSPICIOUS_TLDS).values
    features['trustworthy_tld'] = tlds.isin(TRUSTWORTHY_TLDS).values

    features['has_ip_address'] = domains.str.match(IP_ADDRESS_PATTERN.pattern, na=False).values
    features['excessive_subdomains'] = (domains.str.count(r'\.') > 4).values
    features['is_url_shortener'] = domains.str.contains(SHORTENER_PATTERN.pattern, regex=True, na=False).values

    features['suspicious_chars_path'] = (paths.str.count(r'[^\w\s/.-]') > 3).values
    features['excessive_encoding'] = (urls.str.count('%') > 5).values

    for name, pattern in CONTENT_PATTERNS.items():
        features[name] = path_query.str.contains(pattern.pattern, regex=True, na=False).values

    features['excessive_params'] = (queries.str.count('&') > 9).values
    features['long_query'] = (queries.str.len() > 150).values

    domain_lengths = domains.str.len().fillna(0)
    features['short_domain'] = (domain_lengths < 6).values
    features['very_long_domain'] = (domain_lengths > 25).values

    digit_counts = domains.str.count(r'\d').fillna(0)
    features['domain_is_numeric_heavy'] = ((digit_counts / domain_lengths.replace(0, 1)) > 0.4).values
    features['domain_hyphen_heavy'] = (domains.str.count('-') > 2).values

    features['domain_entropy'] = calculate_entropy_vectorized(domains)
    vowel_counts = domains.str.count(r'[aeiou]').fillna(0)
    consonant_counts = domains.str.count(r'[bcdfghjklmnpqrstvwxyz]').fillna(0)
    features['vowel_consonant_ratio'] = (vowel_counts / consonant_counts.replace(0, 1)).astype(np.float32).values

    features['is_malformed_url'] = (domains == 'unknown.invalid').values
    features['has_invalid_timestamp'] = timestamps.isna().values

    valid_timestamps = timestamps.fillna(CURRENT_TIMESTAMP)
    hours = valid_timestamps.dt.hour
    features['is_night_hours'] = ((hours < 6) | (hours > 22)).values
    features['is_business_hours'] = ((hours >= 9) & (hours <= 17)).values
    features['is_weekend'] = (valid_timestamps.dt.weekday >= 5).values

    age_days = (CURRENT_TIMESTAMP - valid_timestamps).dt.days.fillna(9999)
    features['is_very_recent'] = (age_days < 1).values
    features['is_recent'] = ((age_days >= 1) & (age_days < 7)).values
    features['is_old'] = (age_days > 365).values

    path_segment_count = paths.str.count('/')
    features['has_deep_path'] = (path_segment_count > 6).values
    features['is_root_page'] = (paths.isin(['/', '']))
    features['is_canonical_page'] = (path_segment_count <= 3) & (~features['is_root_page'])
    
    is_github = domains == 'github.com'
    features['is_github_repo_root'] = is_github & (path_segment_count == 2) & ~paths.str.contains('/blob/|/issues/|/pull/|/actions/')
    features['is_github_non_code'] = is_github & paths.str.contains('/issues/|/pull/|/discussions/')

    if NSFW_PATTERN:
        features['is_nsfw_content'] = path_query.str.contains(NSFW_PATTERN.pattern, regex=True, na=False).values
    if SPAM_PATTERN:
        features['is_spam_content'] = path_query.str.contains(SPAM_PATTERN.pattern, regex=True, na=False).values

    features['is_nsfw_domain'] = domains.isin(NSFW_DOMAINS).values
    features['is_quality_domain'] = domains.isin(QUALITY_DOMAINS).values
    features['is_news_domain'] = domains.isin(NEWS_DOMAINS).values
    features['is_ugc_domain'] = domains.isin(UGC_DOMAINS).values

    return features

def calculate_anomaly_features(pdf: pd.DataFrame) -> Dict[str, np.ndarray]:
    n_urls = len(pdf)
    if n_urls == 0: return {}
    features = {}
    url_lengths = pdf['url'].str.len().fillna(0).values
    len_mean, len_std = url_lengths.mean(), url_lengths.std() if url_lengths.std() > 0 else 1
    features['length_anomaly'] = np.abs(url_lengths - len_mean) > 2 * len_std

    param_counts = pdf['query'].str.count('&').fillna(0).values
    param_mean, param_std = param_counts.mean(), param_counts.std() if param_counts.std() > 0 else 1
    features['param_anomaly'] = np.abs(param_counts - param_mean) > 2 * param_std

    return features

def calculate_scores(features: Dict[str, np.ndarray], pdf: pd.DataFrame, all_domain_reputations: Dict[str, float], anomaly_features: Dict[str, np.ndarray]) -> tuple:
    n_urls = len(pdf)
    priority_score = np.zeros(n_urls, dtype=np.float32)

    risk_weights = {
        'has_ip_address': -1.5, 'suspicious_tld': -0.6, 'is_url_shortener': -0.4, 'contains_phishing': -2.0,
        'contains_download': -0.6, 'is_malformed_url': -2.0, 'domain_is_numeric_heavy': -0.4,
        'is_nsfw_content': -1.0, 'is_spam_content': -0.8, 'is_nsfw_domain': -1.5, 'very_long_url': -0.5,
        'path_keyword_stuffing': -0.8, 'is_media_or_api': -0.5, 'very_short_url': -0.7, 'no_scheme': -0.3,
        'has_invalid_timestamp': -0.5, 'domain_hyphen_heavy': -0.2, 'excessive_params': -0.3,
        'suspicious_chars_path': -0.4, 'excessive_encoding': -0.3, 'has_deep_path': -0.2
    }
    
    priority_weights = {
        'trustworthy_tld': 0.8, 'is_quality_domain': 0.15, 'is_news_domain': 0.7,
        'is_root_page': 0.5, 'is_canonical_page': 0.3, 'is_github_repo_root': 0.6, 'is_github_non_code': 0.4,
        'is_business_hours': 0.05,
    }

    score = priority_score
    for feature, weight in risk_weights.items():
        if feature in features:
            score += features[feature].astype(np.float32) * weight
    for feature, weight in priority_weights.items():
        if feature in features:
            score += features[feature].astype(np.float32) * weight

    domain_entropy = features.get('domain_entropy', np.zeros(n_urls))
    score -= np.clip((domain_entropy - 3.8) * 0.25, 0, None)
    
    readability = np.clip(features.get('vowel_consonant_ratio', np.ones(n_urls)) * 0.1, -0.1, 0.1)
    score += readability

    domain_rep_scores = pdf['domain'].map(all_domain_reputations).fillna(0.0).astype(np.float32).values
    score += domain_rep_scores * 0.2

    if 'length_anomaly' in anomaly_features:
        score -= anomaly_features['length_anomaly'].astype(np.float32) * 0.2
    if 'param_anomaly' in anomaly_features:
        score -= anomaly_features['param_anomaly'].astype(np.float32) * 0.2
        
    spam_conditions = (
        (score < -1.2) | features.get('contains_phishing', False) | features.get('is_nsfw_domain', False)
    )
    is_spam = np.array(spam_conditions, dtype=bool)

    risk_score_val = sum(features[f].astype(np.float32) * abs(w) for f, w in risk_weights.items() if f in features)
    trust_score_val = sum(features[f].astype(np.float32) * w for f, w in priority_weights.items() if f in features)

    return score, is_spam, risk_score_val, trust_score_val

def score_urls_batch(pdf: pd.DataFrame, all_domain_reputations: Dict[str, float]) -> pd.DataFrame:
    if pdf.empty:
        return pd.DataFrame()

    pdf_copy = pdf.copy()
    pdf_copy['timestamp'] = pd.to_datetime(pdf_copy['timestamp'], errors='coerce')
    pdf_copy['url'] = pdf_copy['url'].astype(str).str.replace(r'""\s*target=.*$', '', regex=True).str.strip('"')

    parsed_parts = parse_urls_vectorized(pdf_copy['url'])
    pdf_copy = pd.concat([pdf_copy, parsed_parts], axis=1)

    features = extract_features_vectorized(pdf_copy)
    anomaly_features = calculate_anomaly_features(pdf_copy)
    final_score, is_spam, risk_score, trust_score = calculate_scores(features, pdf_copy, all_domain_reputations, anomaly_features)
    
    n_urls = len(pdf_copy)
    result_pdf = pd.DataFrame(index=pdf_copy.index)
    result_pdf['url'] = pdf_copy['url'].str.slice(0, MAX_URL_LENGTH)
    result_pdf['score'] = final_score
    result_pdf['is_spam'] = is_spam
    result_pdf['risk_score'] = risk_score
    result_pdf['trust_score'] = trust_score
    result_pdf['composite_score'] = trust_score - risk_score
    result_pdf['domain_reputation_score'] = pdf_copy['domain'].map(all_domain_reputations).fillna(0.0).astype(np.float32).values
    result_pdf['entropy_score'] = features.get('domain_entropy', np.zeros(n_urls))
    result_pdf['readability_score'] = features.get('vowel_consonant_ratio', np.zeros(n_urls))

    strong_signals = (
        features['trustworthy_tld'] + features['contains_phishing'] + features['has_ip_address']
    ).astype(np.float32)
    confidence = np.clip(0.6 + (strong_signals * 0.3) - (features['is_malformed_url'] * 0.5), 0.1, 1.0)
    result_pdf['confidence'] = confidence

    domain_counts = pdf_copy['domain'].map(pdf_copy['domain'].value_counts()).fillna(1)
    result_pdf['domain_frequency'] = domain_counts.astype(int)
    
    url_values = pdf_copy['url'].astype(str).values
    result_pdf['url_hash'] = [hashlib.sha256(url.encode('utf-8', 'ignore')).hexdigest()[:16] for url in url_values]

    result_pdf['received_at'] = CURRENT_TIMESTAMP
    for col in ['domain', 'path', 'query', 'timestamp']:
        result_pdf[col] = pdf_copy[col]
    
    if 'data_source' in pdf_copy.columns:
        result_pdf['data_source'] = pdf_copy['data_source']
    
    if 'meta' in pdf_copy.columns:
        result_pdf['meta'] = pdf_copy['meta']
    else:
        result_pdf['meta'] = None

    if PROCESSED_URL_SCHEMA:
        schema_cols = [field.name for field in PROCESSED_URL_SCHEMA]
        for col in schema_cols:
             if col not in result_pdf.columns:
                 result_pdf[col] = 0.0
        return result_pdf[[col for col in schema_cols if col in result_pdf.columns]]
    else:
        return result_pdf