import os
import sys
import re
from typing import Dict
from functools import lru_cache
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from schema import PROCESSED_URL_SCHEMA

CURRENT_TIMESTAMP = pd.Timestamp.now()

def extract_url_features(pdf: pd.DataFrame) -> Dict[str, np.ndarray]:
    n_urls = len(pdf)
    path_queries = pdf['path'] + pdf['query']
    domains_array = np.asarray(pdf['domain'].values)

    features = {}

    features['deep_path'] = (pdf['path'].str.count('/') > 5).values
    features['long_params'] = (pdf['query'].str.len() > 50).values
    features['nsfw_content'] = path_queries.str.contains(NSFW_PATTERN, regex=True, na=False).values if NSFW_PATTERN else np.zeros(n_urls, dtype=bool)
    features['spam_content'] = path_queries.str.contains(SPAM_PATTERN, regex=True, na=False).values if SPAM_PATTERN else np.zeros(n_urls, dtype=bool)
    features['nsfw_domain'] = np.isin(domains_array, list(NSFW_DOMAINS))
    features['quality_domain'] = np.isin(domains_array, list(QUALITY_DOMAINS))
    features['news_domain'] = np.isin(domains_array, list(NEWS_DOMAINS))

    return features

def calculate_scores(features: Dict[str, np.ndarray], pdf: pd.DataFrame, all_domain_reputations: Dict[str, float]) -> tuple:
    n_urls = len(pdf)
    scores = np.zeros(n_urls, dtype=np.float32)
    is_spam = np.zeros(n_urls, dtype=bool)

    scores[features['deep_path']] -= 0.1
    scores[features['long_params']] -= 0.1

    scores[features['nsfw_content']] -= 0.5
    is_spam[features['nsfw_content']] = True

    new_spam_mask = features['spam_content'] & ~is_spam
    scores[new_spam_mask] -= 0.4
    is_spam[features['spam_content']] = True

    scores[features['nsfw_domain']] -= 1.0
    is_spam[features['nsfw_domain']] = True

    scores[features['quality_domain']] += 0.5
    scores[features['news_domain']] += 0.2

    domain_rep_scores = pdf['domain'].map(all_domain_reputations).fillna(0.0).values.astype(np.float32)
    scores += domain_rep_scores * 0.5

    return scores, is_spam, domain_rep_scores

@lru_cache(maxsize=1)
def load_list_from_file(filepath: str) -> frozenset:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Required file not found: {filepath}")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = [line.strip().lower() for line in f if line.strip()]
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

NSFW_PATTERN = None
SPAM_PATTERN = None

if NSFW_KEYWORDS:
    sorted_nsfw = sorted(NSFW_KEYWORDS, key=len, reverse=True)
    try:
        NSFW_PATTERN = re.compile('|'.join(re.escape(keyword) for keyword in sorted_nsfw), re.IGNORECASE)
    except re.error as e:
        raise ValueError(f"Failed to compile NSFW regex pattern: {e}")

if SPAM_KEYWORDS:
    sorted_spam = sorted(SPAM_KEYWORDS, key=len, reverse=True)
    try:
        SPAM_PATTERN = re.compile('|'.join(re.escape(keyword) for keyword in sorted_spam), re.IGNORECASE)
    except re.error as e:
        raise ValueError(f"Failed to compile SPAM regex pattern: {e}")

def extract_domains_vectorized(urls: pd.Series) -> pd.Series:
    domains = np.empty(len(urls), dtype=object)
    url_array = urls.values
    url_str_array = np.asarray(url_array, dtype=str)
    protocol_mask = np.char.find(url_str_array, '://') == -1
    url_array = np.where(protocol_mask, '//' + url_str_array, url_str_array)

    for i in range(len(url_array)):
        url = url_array[i]
        if not url or url == 'nan':
            domains[i] = None
            continue

        try:
            if url.startswith('//'):
                netloc_start = 2
            else:
                proto_end = url.find('://')
                if proto_end == -1:
                    domains[i] = None
                    continue
                netloc_start = proto_end + 3

            netloc_end = len(url)
            for char in ['/', '?', '#']:
                pos = url.find(char, netloc_start)
                if pos != -1:
                    netloc_end = min(netloc_end, pos)

            netloc = url[netloc_start:netloc_end]

            if ':' in netloc:
                netloc = netloc.split(':')[0]

            domains[i] = netloc.lower() if netloc else None

        except Exception:
            domains[i] = None

    return pd.Series(domains, index=urls.index)

def extract_path_query_vectorized(urls: pd.Series) -> tuple:
    paths = np.empty(len(urls), dtype=object)
    queries = np.empty(len(urls), dtype=object)

    url_array = urls.values.astype(str)

    for i in range(len(url_array)):
        url = url_array[i]
        if not url or url == 'nan':
            paths[i] = ''
            queries[i] = ''
            continue

        try:
            if url.startswith('//'):
                netloc_start = 2
            else:
                proto_end = url.find('://')
                if proto_end == -1:
                    paths[i] = ''
                    queries[i] = ''
                    continue
                netloc_start = proto_end + 3

            path_start = url.find('/', netloc_start)
            if path_start == -1:
                paths[i] = ''
                queries[i] = ''
                continue

            query_start = url.find('?', path_start)
            if query_start == -1:
                paths[i] = url[path_start:].lower()
                queries[i] = ''
            else:
                paths[i] = url[path_start:query_start].lower()
                fragment_start = url.find('#', query_start)
                if fragment_start == -1:
                    queries[i] = url[query_start+1:].lower()
                else:
                    queries[i] = url[query_start+1:fragment_start].lower()
        except Exception:
            paths[i] = ''
            queries[i] = ''

    return pd.Series(paths, index=urls.index), pd.Series(queries, index=urls.index)

def score_urls_batch(pdf: pd.DataFrame, all_domain_reputations: Dict[str, float]) -> pd.DataFrame:
    if pdf.empty:
        return pd.DataFrame(columns=[field.name for field in PROCESSED_URL_SCHEMA])

    if 'url' not in pdf.columns or 'timestamp' not in pdf.columns:
        raise ValueError("Input DataFrame must contain 'url' and 'timestamp' columns.")

    if not pd.api.types.is_datetime64_any_dtype(pdf['timestamp']):
        pdf['timestamp'] = pd.to_datetime(pdf['timestamp'], errors='coerce')

    pdf = pdf.dropna(subset=['url', 'timestamp'])
    if pdf.empty:
        return pd.DataFrame(columns=[field.name for field in PROCESSED_URL_SCHEMA])

    pdf['domain'] = extract_domains_vectorized(pdf['url'])
    pdf = pdf.dropna(subset=['domain'])

    if pdf.empty:
        return pd.DataFrame(columns=[field.name for field in PROCESSED_URL_SCHEMA])

    pdf['path'], pdf['query'] = extract_path_query_vectorized(pdf['url'])

    features = extract_url_features(pdf)
    scores, is_spam, domain_rep_scores = calculate_scores(features, pdf, all_domain_reputations)

    domain_counts = pdf['domain'].value_counts()
    domain_frequencies = pdf['domain'].map(domain_counts).values
    domain_freq_pcts = domain_frequencies.astype(float) / len(pdf)

    pdf['score'] = scores
    pdf['confidence'] = 1.0
    pdf['is_spam'] = is_spam
    pdf['domain_reputation_score'] = domain_rep_scores
    pdf['domain_frequency'] = domain_frequencies
    pdf['domain_frequency_pct'] = domain_freq_pcts
    pdf['received_at'] = CURRENT_TIMESTAMP
    pdf['meta'] = None

    return pdf[[field.name for field in PROCESSED_URL_SCHEMA]]
