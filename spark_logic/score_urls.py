import os
import sys
import re
from typing import Dict
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
def load_list_from_file(filepath: str) -> frozenset:
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

NSFW_PATTERN = re.compile('|'.join(re.escape(k) for k in sorted(NSFW_KEYWORDS, key=len, reverse=True)), re.IGNORECASE) if NSFW_KEYWORDS else None
SPAM_PATTERN = re.compile('|'.join(re.escape(k) for k in sorted(SPAM_KEYWORDS, key=len, reverse=True)), re.IGNORECASE) if SPAM_KEYWORDS else None
SHORTENER_PATTERN = re.compile(r'\b(bit\.ly|tinyurl|goo\.gl|t\.co|short\.link|ow\.ly)\b', re.IGNORECASE)
IP_ADDRESS_PATTERN = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
URL_PARSE_REGEX = re.compile(r'^(?:[a-z]+:)?//([^/?#:]+)([^?#]*)(?:\?([^#]*))?.*$')

CONTENT_PATTERNS = {
    'contains_download': re.compile(r'\b(download|exe|zip|rar|torrent|crack|keygen)\b', re.IGNORECASE),
    'contains_phishing': re.compile(r'\b(login|signin|account|verify|suspended|update|confirm|secure)\b', re.IGNORECASE),
    'contains_financial': re.compile(r'\b(bank|paypal|amazon|ebay|visa|mastercard|credit|card|payment)\b', re.IGNORECASE),
    'contains_urgency': re.compile(r'\b(urgent|immediate|expire|limited|act now|hurry|last chance)\b', re.IGNORECASE),
    'multiple_redirects': re.compile(r'(redirect|redir|goto|link|ref)', re.IGNORECASE),
}

SUSPICIOUS_TLDS = frozenset({'.tk', '.ml', '.ga', '.cf', '.gq', '.ru', '.cn', '.cc', '.info', '.biz'})
TRUSTWORTHY_TLDS = frozenset({'.com', '.org', '.net', '.edu', '.gov', '.mil'})

NSFW_DOMAINS_SET = NSFW_DOMAINS
QUALITY_DOMAINS_SET = QUALITY_DOMAINS
NEWS_DOMAINS_SET = NEWS_DOMAINS

@lru_cache(maxsize=128)
def calculate_entropy(text: str) -> float:
    if not text:
        return 0.0
    counts = Counter(text)
    length = len(text)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())

def calculate_entropy_vectorized(domains: pd.Series) -> np.ndarray:
    unique_domains = domains.unique()
    entropy_map = {domain: calculate_entropy(domain) for domain in unique_domains}
    return domains.map(entropy_map).values.astype(np.float32)

def parse_urls_vectorized(urls: pd.Series) -> pd.DataFrame:
    urls_str = urls.astype(str)
    no_scheme_mask = ~urls_str.str.contains('://', na=False, regex=False)
    if no_scheme_mask.any():
        urls_str = urls_str.copy()
        urls_str.loc[no_scheme_mask] = '//' + urls_str.loc[no_scheme_mask]

    parts = urls_str.str.extract(URL_PARSE_REGEX)
    parts.columns = ['domain', 'path', 'query']

    domain_empty_mask = parts['domain'].isna() | (parts['domain'] == '')
    parts.loc[domain_empty_mask, 'domain'] = 'unknown.invalid'

    parts['domain'] = parts['domain'].str.lower().fillna('unknown.invalid')
    parts['path'] = parts['path'].str.lower().fillna('')
    parts['query'] = parts['query'].str.lower().fillna('')
    return parts

def extract_features_vectorized(pdf: pd.DataFrame) -> Dict[str, np.ndarray]:
    features = {}
    n_urls = len(pdf)

    urls = pdf['url']
    domains = pdf['domain']
    paths = pdf['path']
    queries = pdf['query']
    timestamps = pdf['timestamp']
    path_queries = paths + ' ' + queries

    url_lengths = urls.str.len()
    features['very_short_url'] = (url_lengths <= 5).values
    features['very_long_url'] = (url_lengths > 1000).values
    features['no_scheme'] = (~urls.str.contains('://', na=False, regex=False)).values
    features['long_url'] = (url_lengths > 200).values

    tlds = domains.str.rsplit('.', n=1, expand=True)[1].fillna('')
    features['suspicious_tld'] = tlds.isin(SUSPICIOUS_TLDS).values
    features['trustworthy_tld'] = tlds.isin(TRUSTWORTHY_TLDS).values

    features['has_ip_address'] = domains.str.match(IP_ADDRESS_PATTERN.pattern, na=False).values
    features['excessive_subdomains'] = (domains.str.count(r'\.') > 4).values
    features['url_shortener'] = domains.str.contains(SHORTENER_PATTERN.pattern, regex=True, na=False).values

    features['suspicious_chars'] = (paths.str.count(r'[^\w\s/.-]') > 3).values
    features['encoded_chars'] = (urls.str.count('%') > 2).values

    for name, pattern in CONTENT_PATTERNS.items():
        features[name] = path_queries.str.contains(pattern.pattern, regex=True, na=False).values

    features['suspicious_params'] = (queries.str.count('&') > 9).values

    domain_lengths = domains.str.len()
    features['short_domain'] = (domain_lengths < 5).values
    features['very_long_domain'] = (domain_lengths > 25).values

    digit_counts = domains.str.count(r'\d')
    features['numeric_heavy'] = ((digit_counts / domain_lengths.replace(0, 1)) > 0.3).values
    features['hyphen_heavy'] = (domains.str.count('-') > 2).values

    features['entropy_score'] = calculate_entropy_vectorized(domains)

    lower_domains = domains.str.lower()
    vowel_counts = lower_domains.str.count(r'[aeiou]')
    consonant_counts = lower_domains.str.count(r'[bcdfghjklmnpqrstvwxyz]')
    features['vowel_consonant_ratio'] = (vowel_counts / consonant_counts.replace(0, 1)).astype(np.float32).values

    features['malformed_url'] = (domains == 'unknown.invalid').values
    features['invalid_timestamp'] = timestamps.isna().values

    valid_timestamps = timestamps.fillna(CURRENT_TIMESTAMP)
    hours = valid_timestamps.dt.hour
    features['night_hours'] = ((hours < 6) | (hours > 22)).values
    features['business_hours'] = ((hours >= 9) & (hours <= 17)).values
    features['weekend'] = (valid_timestamps.dt.weekday >= 5).values

    age_days = (CURRENT_TIMESTAMP - valid_timestamps).dt.days
    features['very_recent'] = (age_days < 1).values
    features['recent'] = ((age_days >= 1) & (age_days < 7)).values
    features['old'] = (age_days > 365).values

    features['deep_path'] = (paths.str.count('/') > 5).values
    features['long_params'] = (queries.str.len() > 50).values

    if NSFW_PATTERN:
        features['nsfw_content'] = path_queries.str.contains(NSFW_PATTERN.pattern, regex=True, na=False).values
    else:
        features['nsfw_content'] = np.zeros(n_urls, dtype=bool)

    if SPAM_PATTERN:
        features['spam_content'] = path_queries.str.contains(SPAM_PATTERN.pattern, regex=True, na=False).values
    else:
        features['spam_content'] = np.zeros(n_urls, dtype=bool)

    features['nsfw_domain'] = domains.isin(NSFW_DOMAINS_SET).values
    features['quality_domain'] = domains.isin(QUALITY_DOMAINS_SET).values
    features['news_domain'] = domains.isin(NEWS_DOMAINS_SET).values

    return features

def calculate_advanced_scores(all_features: Dict[str, np.ndarray], n_urls: int) -> Dict[str, np.ndarray]:
    advanced_scores = {}

    risk_weights = {
        'has_ip_address': 0.8, 'suspicious_tld': 0.6, 'url_shortener': 0.4, 'contains_phishing': 0.9,
        'contains_download': 0.5, 'encoded_chars': 0.3, 'excessive_subdomains': 0.4, 'suspicious_chars': 0.3,
        'long_url': 0.2, 'numeric_heavy': 0.3, 'hyphen_heavy': 0.2, 'night_hours': 0.1, 'very_recent': 0.1,
        'malformed_url': 1.0, 'invalid_timestamp': 0.5, 'very_short_url': 0.8, 'very_long_url': 0.4, 'no_scheme': 0.6
    }
    trust_weights = {'trustworthy_tld': 0.5, 'quality_domain': 0.7, 'news_domain': 0.4, 'business_hours': 0.1}

    risk_score = sum(all_features[feature].astype(np.float32) * weight for feature, weight in risk_weights.items() if feature in all_features)
    trust_score = sum(all_features[feature].astype(np.float32) * weight for feature, weight in trust_weights.items() if feature in all_features)

    advanced_scores['risk_score'] = risk_score
    advanced_scores['trust_score'] = 1.0 + trust_score
    advanced_scores['composite_score'] = advanced_scores['trust_score'] - risk_score

    entropy_scores = all_features.get('entropy_score', np.zeros(n_urls))
    entropy_std = entropy_scores.std()
    advanced_scores['normalized_entropy'] = (entropy_scores - entropy_scores.mean()) / (entropy_std if entropy_std > 0 else 1.0)

    vowel_ratios = all_features.get('vowel_consonant_ratio', np.ones(n_urls))
    advanced_scores['readability_score'] = np.clip(vowel_ratios * 2, 0, 1)

    return advanced_scores

def calculate_scores(features: Dict[str, np.ndarray], pdf: pd.DataFrame, all_domain_reputations: Dict[str, float], advanced_scores: Dict) -> tuple:
    n_urls = len(pdf)

    score_weights = {
        'deep_path': -0.1, 'long_params': -0.1, 'nsfw_content': -0.5, 'spam_content': -0.4, 'nsfw_domain': -1.0, 'quality_domain': 0.5,
        'news_domain': 0.2, 'suspicious_tld': -0.3, 'trustworthy_tld': 0.2, 'has_ip_address': -0.6, 'url_shortener': -0.3,
        'contains_phishing': -0.8, 'contains_download': -0.4, 'contains_financial': -0.3, 'contains_urgency': -0.2,
        'encoded_chars': -0.2, 'long_url': -0.1, 'short_domain': -0.1, 'numeric_heavy': -0.2, 'hyphen_heavy': -0.1,
        'night_hours': -0.05, 'business_hours': 0.05, 'malformed_url': -1.5, 'invalid_timestamp': -0.8,
        'very_short_url': -0.7, 'very_long_url': -0.3, 'no_scheme': -0.4
    }

    scores = sum(features[feature].astype(np.float32) * weight for feature, weight in score_weights.items() if feature in features)

    if all_domain_reputations:
        domain_rep_scores = pdf['domain'].map(all_domain_reputations).fillna(0.0).astype(np.float32).values
    else:
        domain_rep_scores = np.zeros(n_urls, dtype=np.float32)

    scores += domain_rep_scores * 0.5
    scores += advanced_scores['composite_score'] * 0.3
    scores += advanced_scores['readability_score'] * 0.1
    scores -= np.abs(advanced_scores['normalized_entropy']) * 0.1

    spam_features = ['nsfw_content', 'spam_content', 'nsfw_domain', 'contains_phishing', 'malformed_url', 'very_short_url']
    is_spam = np.zeros(n_urls, dtype=bool)
    for feature in spam_features:
        if feature in features:
            is_spam |= features[feature]

    is_spam |= (scores < -1.5)

    return scores, is_spam, domain_rep_scores

def calculate_anomaly_and_cluster_features(pdf: pd.DataFrame) -> Dict[str, np.ndarray]:
    n_urls = len(pdf)
    if n_urls == 0:
        return {}

    features = {}

    url_lengths = pdf['url'].str.len().fillna(0).values
    len_mean, len_std = url_lengths.mean(), url_lengths.std()
    features['length_anomaly'] = np.abs(url_lengths - len_mean) > 2 * (len_std if len_std > 0 else 1)

    param_counts = pdf['query'].str.count('&').fillna(0).values
    param_mean, param_std = param_counts.mean(), param_counts.std()
    features['param_anomaly'] = np.abs(param_counts - param_mean) > 2 * (param_std if param_std > 0 else 1)

    subdomain_counts = pdf['domain'].str.count(r'\.').fillna(0).values
    sub_mean, sub_std = subdomain_counts.mean(), subdomain_counts.std()
    features['subdomain_anomaly'] = np.abs(subdomain_counts - sub_mean) > 2 * (sub_std if sub_std > 0 else 1)

    domain_counts = pdf['domain'].map(pdf['domain'].value_counts()).fillna(1)
    features['domain_freq_pct'] = (domain_counts / n_urls).values

    return features

def score_urls_batch(pdf: pd.DataFrame, all_domain_reputations: Dict[str, float]) -> pd.DataFrame:
    if pdf.empty:
        return pd.DataFrame(columns=[field.name for field in PROCESSED_URL_SCHEMA])

    if 'url' not in pdf.columns or 'timestamp' not in pdf.columns:
        raise ValueError("Input DataFrame must contain 'url' and 'timestamp' columns.")

    pdf = pdf.copy()
    pdf['timestamp'] = pd.to_datetime(pdf['timestamp'], errors='coerce')
    pdf['url'] = pdf['url'].astype(str).str.replace(r'""\s*target=.*$', '', regex=True).str.strip('"')

    parsed_parts = parse_urls_vectorized(pdf['url'])
    pdf = pd.concat([pdf, parsed_parts], axis=1)

    n_urls = len(pdf)
    features = extract_features_vectorized(pdf)
    advanced_scores = calculate_advanced_scores(features, n_urls)
    scores, is_spam, domain_rep_scores = calculate_scores(features, pdf, all_domain_reputations, advanced_scores)
    anomaly_features = calculate_anomaly_and_cluster_features(pdf)

    final_pdf = pd.DataFrame(index=pdf.index)
    final_pdf['score'] = scores
    final_pdf['is_spam'] = is_spam
    final_pdf['domain_reputation_score'] = domain_rep_scores
    final_pdf['risk_score'] = advanced_scores['risk_score']
    final_pdf['trust_score'] = advanced_scores['trust_score']
    final_pdf['composite_score'] = advanced_scores['composite_score']
    final_pdf['entropy_score'] = features.get('entropy_score', np.zeros(n_urls))
    final_pdf['readability_score'] = advanced_scores['readability_score']

    confidence_adjustments = (
        anomaly_features.get('length_anomaly', False) * -0.1 +
        anomaly_features.get('param_anomaly', False) * -0.1 +
        anomaly_features.get('subdomain_anomaly', False) * -0.1 +
        (anomaly_features.get('domain_freq_pct', 0) > 0.1) * 0.1 +
        (final_pdf['trust_score'] > 1.5) * 0.2 +
        features.get('malformed_url', False) * -0.3 +
        features.get('invalid_timestamp', False) * -0.2 +
        features.get('very_short_url', False) * -0.2 +
        features.get('very_long_url', False) * -0.1 +
        features.get('no_scheme', False) * -0.15
    )
    final_pdf['confidence'] = np.clip(1.0 + confidence_adjustments, 0.1, 1.0)

    domain_value_counts = pdf['domain'].value_counts()
    final_pdf['domain_frequency'] = pdf['domain'].map(domain_value_counts).fillna(0).astype(int)
    final_pdf['domain_frequency_pct'] = (final_pdf['domain_frequency'] / n_urls).astype(np.float32)

    url_values = pdf['url'].astype(str).values
    final_pdf['url_hash'] = [hashlib.sha256(url.encode()).hexdigest()[:16] for url in url_values]

    valid_timestamps = pdf['timestamp'].fillna(CURRENT_TIMESTAMP)
    final_pdf['time_delta'] = (valid_timestamps - valid_timestamps.shift()).dt.total_seconds().fillna(0).values
    final_pdf['received_at'] = CURRENT_TIMESTAMP
    final_pdf['meta'] = None

    if 'data_source' in pdf.columns:
        final_pdf['data_source'] = pdf['data_source']

    final_pdf['url'] = pdf['url'].astype(str).str.slice(0, MAX_URL_LENGTH)
    for col in ['domain', 'path', 'query', 'timestamp']:
        final_pdf[col] = pdf[col]
    final_pdf['timestamp'] = final_pdf['timestamp'].fillna(CURRENT_TIMESTAMP)

    ordered_cols = [field.name for field in PROCESSED_URL_SCHEMA if field.name in final_pdf.columns]
    return final_pdf[ordered_cols]