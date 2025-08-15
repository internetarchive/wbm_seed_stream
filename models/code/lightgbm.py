import os
import sys
import re
from typing import FrozenSet, List, Dict
from functools import lru_cache
import numpy as np
import pandas as pd
from collections import Counter
import math
import joblib
import lightgbm as lgb
from pyspark import SparkFiles

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from spark.config.spark_config import SparkConfig
from schema import PROCESSED_URL_SCHEMA

MODEL_SAVE_PATH = os.path.join(PROJECT_ROOT, 'models', 'trained', 'url_archival_model.joblib')
LISTS_DIR = os.path.join(PROJECT_ROOT, 'data', 'storage', 'lists')
CURRENT_TIMESTAMP = pd.Timestamp.now()
MAX_URL_LENGTH = 2048

@lru_cache(maxsize=1)
def load_list_from_file(filepath: str) -> FrozenSet[str]:
    if not os.path.exists(filepath):
        print(f"Warning: List file not found, returning empty set: {filepath}")
        return frozenset()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = {line.strip().lower() for line in f if line.strip()}
        return frozenset(lines)
    except (IOError, OSError) as e:
        raise IOError(f"Failed to read file {filepath}: {e}")

if SparkConfig.USE_LISTS:
    NSFW_KEYWORDS = load_list_from_file(os.path.join(LISTS_DIR, "nsfw_keywords.txt"))
    SPAM_KEYWORDS = load_list_from_file(os.path.join(LISTS_DIR, "spam_keywords.txt"))
    NSFW_DOMAINS = load_list_from_file(os.path.join(LISTS_DIR, "nsfw_domains.txt"))
    QUALITY_DOMAINS = load_list_from_file(os.path.join(LISTS_DIR, "quality_domains.txt"))
    NEWS_DOMAINS = load_list_from_file(os.path.join(LISTS_DIR, "news_domains.txt"))
else:
    NSFW_KEYWORDS, SPAM_KEYWORDS, NSFW_DOMAINS, QUALITY_DOMAINS, NEWS_DOMAINS = (frozenset(),) * 5

def build_regex_pattern(keywords: FrozenSet[str]) -> re.Pattern | None:
    if not keywords: return None
    return re.compile('|'.join(re.escape(k) for k in sorted(list(keywords), key=len, reverse=True)), re.IGNORECASE)

NSFW_PATTERN = build_regex_pattern(NSFW_KEYWORDS)
SPAM_PATTERN = build_regex_pattern(SPAM_KEYWORDS)

URL_PARSE_REGEX = re.compile(r'^(?:[a-z]+:)?//(?P<domain>[^/?#:]+)(?P<path>[^?#]*)(?:\?(?P<query>[^#]*))?.*$')
IP_ADDRESS_PATTERN = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
CONTENT_PATTERNS = {
    'contains_download': re.compile(r'\b(download|exe|zip|rar|torrent|crack|keygen|apk)\b', re.IGNORECASE),
    'contains_phishing': re.compile(r'\b(login|signin|account|verify|suspended|update|confirm|secure|password)\b', re.IGNORECASE),
    'is_media_or_api': re.compile(r'\b(api|json|xml|v[1-9]/|/media/)\b', re.IGNORECASE),
}
SUSPICIOUS_TLDS = frozenset({'.tk', '.ml', '.ga', '.cf', '.gq', '.ru', '.cn', '.cc', '.info', '.biz', '.xyz', '.top'})
TRUSTWORTHY_TLDS = frozenset({'.gov', '.edu', '.mil'})

@lru_cache(maxsize=2048)
def get_shannon_entropy(text: str) -> float:
    if not text or len(text) <= 1: return 0.0
    counts = Counter(text)
    length = len(text)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())

def calculate_entropy_vectorized(series: pd.Series) -> np.ndarray:
    return series.apply(get_shannon_entropy).values.astype(np.float32)

def parse_urls_vectorized(urls: pd.Series) -> pd.DataFrame:
    urls_str = urls.astype(str).str.strip()
    no_scheme_mask = ~urls_str.str.contains('://', na=False, regex=False)
    if no_scheme_mask.any():
        urls_str.loc[no_scheme_mask] = '//' + urls_str.loc[no_scheme_mask]
    
    parts = urls_str.str.extract(URL_PARSE_REGEX)
    parts.rename(columns={'domain': 'domain', 'path': 'path', 'query': 'query'}, inplace=True)
    
    parts['domain'] = parts['domain'].str.lower().fillna('unknown.invalid')
    parts['path'] = parts['path'].fillna('')
    parts['query'] = parts['query'].fillna('')
    return parts

def extract_features_vectorized(pdf: pd.DataFrame) -> pd.DataFrame:
    features = pd.DataFrame(index=pdf.index)
    
    urls = pdf['url'].fillna('')
    domains = pdf['domain'].fillna('')
    paths = pdf['path'].fillna('')
    queries = pdf['query'].fillna('')
    path_query = paths + '?' + queries

    features['len_url'] = urls.str.len()
    features['len_path'] = paths.str.len()
    features['len_query'] = queries.str.len()
    features['len_domain'] = domains.str.len()

    tlds = domains.str.rsplit('.', n=1, expand=True)[1].fillna('')
    features['is_suspicious_tld'] = tlds.isin(SUSPICIOUS_TLDS).astype(int)
    features['is_trustworthy_tld'] = tlds.isin(TRUSTWORTHY_TLDS).astype(int)

    features['has_ip_address'] = domains.str.match(IP_ADDRESS_PATTERN).astype(int)
    features['num_subdomains'] = domains.str.count(r'\.').astype(int)
    features['num_path_dirs'] = paths.str.count('/').astype(int)
    
    non_empty_queries = queries != ''
    features['num_params'] = (queries.str.count('&').fillna(0) + non_empty_queries).astype(int)
    
    features['num_digits_domain'] = domains.str.count(r'\d').astype(int)
    features['num_hyphens_domain'] = domains.str.count('-').astype(int)

    for name, pattern in CONTENT_PATTERNS.items():
        features[name] = path_query.str.contains(pattern, regex=True).astype(int)

    features['domain_entropy'] = calculate_entropy_vectorized(domains)
    features['path_entropy'] = calculate_entropy_vectorized(paths)

    vowel_counts = domains.str.count(r'[aeiou]').fillna(0)
    consonant_counts = domains.str.count(r'[bcdfghjklmnpqrstvwxyz]').fillna(0)
    features['vowel_consonant_ratio'] = (vowel_counts / np.maximum(consonant_counts, 1)).astype(np.float32)

    if NSFW_PATTERN:
        features['is_nsfw_content'] = path_query.str.contains(NSFW_PATTERN, regex=True).astype(int)
    else:
        features['is_nsfw_content'] = 0
        
    if SPAM_PATTERN:
        features['is_spam_content'] = path_query.str.contains(SPAM_PATTERN, regex=True).astype(int)
    else:
        features['is_spam_content'] = 0

    features['is_nsfw_domain'] = domains.isin(NSFW_DOMAINS).astype(int)
    features['is_quality_domain'] = domains.isin(QUALITY_DOMAINS).astype(int)
    features['is_news_domain'] = domains.isin(NEWS_DOMAINS).astype(int)

    return features

class URLArchivalClassifier:
    def __init__(self, model_path: str = MODEL_SAVE_PATH):
        self.model_path = model_path
        self.model: lgb.LGBMClassifier | None = None
        self.feature_names: List[str] | None = None
        self.label_map = {0: 'spam', 1: 'ham', 2: 'good'}
        self.num_classes = len(self.label_map)

    def _prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        parsed_df = parse_urls_vectorized(df['url'])
        full_df = pd.concat([df.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)
        features_df = extract_features_vectorized(full_df)
        return features_df

    def train(self, training_df: pd.DataFrame):
        print("Starting 3-class model training with in-memory DataFrame...")
        if training_df.empty:
            print("Error: DataFrame is empty.")
            return

        training_df['label'] = 1
        training_df.loc[training_df['is_spam'] == 1, 'label'] = 0
        training_df.loc[training_df['data_source'] == 'good_data', 'label'] = 2

        label_counts = training_df['label'].value_counts()
        print(f"Dataset counts: Spam={label_counts.get(0, 0)}, Ham={label_counts.get(1, 0)}, Good={label_counts.get(2, 0)}")

        if label_counts.get(2, 0) == 0:
            print("=" * 60)
            print("WARNING: No 'good_data' found. Model may not effectively identify high-priority content.")
            print("=" * 60)

        if label_counts.get(0, 0) < 100 or label_counts.get(1, 0) < 100:
            print(f"Error: Not enough spam/ham data for training. (Threshold: 100 each)")
            return

        training_df = training_df.drop_duplicates(subset=['url']).reset_index(drop=True)
        print(f"Final training set size after deduplication: {len(training_df)}")

        print("Extracting features for the training set...")
        X = self._prepare_features(training_df)
        y = training_df['label']
        self.feature_names = X.columns.tolist()

        print("Training LightGBM multi-class classifier...")
        lgbm = lgb.LGBMClassifier(
            objective='multiclass',
            num_class=self.num_classes,
            metric='multi_logloss',
            n_estimators=1000,
            learning_rate=0.05,
            num_leaves=31,
            n_jobs=12,
            random_state=42,
            boosting_type='goss',
            force_col_wise=True,
            class_weight='balanced'
        )

        lgbm.fit(X, y, eval_set=[(X, y)], callbacks=[lgb.early_stopping(50, verbose=True)])
        self.model = lgbm
        self._save_model()

    def _calculate_scores(self, probs: np.ndarray, features_df: pd.DataFrame) -> Dict:
        p_spam, p_ham, p_good = probs[:, 0], probs[:, 1], probs[:, 2]
        return {
            'score': p_ham + p_good,
            'is_spam': p_spam > 0.5,
            'risk_score': p_spam,
            'trust_score': p_good,
            'composite_score': p_good - p_spam,
            'confidence': np.max(probs, axis=1),
            'entropy_score': features_df['domain_entropy'].values,
            'readability_score': features_df['vowel_consonant_ratio'].values
        }

    def predict(self, df_to_score: pd.DataFrame) -> pd.DataFrame:
        if df_to_score.empty:
            return pd.DataFrame()

        self._load_model()
        if self.model.n_classes_ != self.num_classes:
             raise ValueError(f"Model trained on {self.model.n_classes_} classes, but requires {self.num_classes}.")

        print(f"Scoring {len(df_to_score)} URLs with 3-class model...")
        parsed_df = parse_urls_vectorized(df_to_score['url'])
        full_df = pd.concat([df_to_score.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)

        features_df = extract_features_vectorized(full_df)
        X_predict = features_df[self.feature_names]

        probabilities = self.model.predict_proba(X_predict)
        calculated_scores = self._calculate_scores(probabilities, features_df)

        result_df = full_df.copy()
        for col, values in calculated_scores.items():
            result_df[col] = values

        result_df['domain_reputation_score'] = 0.0
        result_df['url'] = result_df['url'].str.slice(0, MAX_URL_LENGTH)
        result_df['received_at'] = CURRENT_TIMESTAMP
        result_df['domain_frequency'] = result_df['domain'].map(result_df['domain'].value_counts()).fillna(1).astype(int)
        
        total_domains = len(result_df)
        if total_domains > 0:
            result_df['domain_frequency_pct'] = (result_df['domain_frequency'] / total_domains).astype(np.float32)
        else:
            result_df['domain_frequency_pct'] = 0.0

        schema_cols = [field.name for field in PROCESSED_URL_SCHEMA]
        for col in schema_cols:
             if col not in result_df.columns:
                 result_df[col] = None
        
        return result_df[[col for col in schema_cols if col in result_df.columns]]

    def _save_model(self):
        if self.model and self.feature_names:
            print(f"Saving model to {self.model_path}...")
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            joblib.dump({'model': self.model, 'features': self.feature_names}, self.model_path)
            print("Model saved successfully.")
        else:
            print("Error: Model not trained. Cannot save.")

    def _load_model(self):
        if self.model is None:
            model_file_path = self.model_path
            if not os.path.exists(model_file_path):
                spark_model_path = SparkFiles.get(os.path.basename(self.model_path))
                if os.path.exists(spark_model_path):
                    model_file_path = spark_model_path
                else:
                    raise FileNotFoundError(f"Model file not found at '{self.model_path}' or in SparkFiles. Please ensure it is distributed to worker nodes.")
            
            print(f"Loading pre-trained model from {model_file_path}...")
            data = joblib.load(model_file_path)
            self.model = data['model']
            self.feature_names = data['features']