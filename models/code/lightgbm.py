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

MODEL_SAVE_PATH = os.path.join(PROJECT_ROOT, 'models', 'trained', 'url_archival_regressor_model.joblib')
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
    'contains_financial': re.compile(r'\b(bank|paypal|amazon|ebay|visa|mastercard|credit|card|payment)\b', re.IGNORECASE),
    'contains_urgency': re.compile(r'\b(urgent|immediate|expire|limited|act now|hurry|last chance)\b', re.IGNORECASE),
    'is_media_or_api': re.compile(r'\b(api|json|xml|v[1-9]/|/media/)\b', re.IGNORECASE),
    'path_contains_sensitive': re.compile(r'/(login|admin|credentials|wp-admin|config|backup)\b', re.IGNORECASE),
}
SUSPICIOUS_TLDS = frozenset({'.tk', '.ml', '.ga', '.cf', '.gq', '.ru', '.cn', '.cc', '.info', '.biz', '.xyz', '.top'})
TRUSTWORTHY_TLDS = frozenset({'.gov', '.edu', '.mil'})

@lru_cache(maxsize=4096)
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
    features['is_very_short_url'] = (features['len_url'] <= 15).astype(int)
    features['is_very_long_url'] = (features['len_url'] > 1024).astype(int)
    tlds = domains.str.rsplit('.', n=1, expand=True)[1].fillna('')
    features['is_suspicious_tld'] = tlds.isin(SUSPICIOUS_TLDS).astype(int)
    features['is_trustworthy_tld'] = tlds.isin(TRUSTWORTHY_TLDS).astype(int)
    features['has_ip_address'] = domains.str.match(IP_ADDRESS_PATTERN).astype(int)
    features['num_subdomains'] = domains.str.count(r'\.')
    features['has_excessive_subdomains'] = (features['num_subdomains'] > 4).astype(int)
    features['num_path_dirs'] = paths.str.count('/')
    features['is_root_path'] = (paths.isin(['/', ''])).astype(int)
    non_empty_queries = queries != ''
    features['num_params'] = (queries.str.count('&').fillna(0) + non_empty_queries).astype(int)
    features['has_excessive_params'] = (features['num_params'] > 9).astype(int)
    features['num_digits_domain'] = domains.str.count(r'\d')
    features['digit_ratio_domain'] = (features['num_digits_domain'] / np.maximum(features['len_domain'], 1)).astype(np.float32)
    features['domain_is_numeric_heavy'] = (features['digit_ratio_domain'] > 0.4).astype(int)
    features['num_hyphens_domain'] = domains.str.count('-')
    features['domain_hyphen_heavy'] = (features['num_hyphens_domain'] > 2).astype(int)
    features['has_suspicious_chars_path'] = (paths.str.count(r'[^\w\s/.-]') > 3).astype(int)
    features['has_excessive_encoding'] = (urls.str.count('%') > 5).astype(int)
    features['path_keyword_stuffing'] = (features['num_path_dirs'] > 10) & (paths.str.count('-') > 10)

    for name, pattern in CONTENT_PATTERNS.items():
        features[name] = path_query.str.contains(pattern, regex=True, na=False).astype(int)

    features['domain_entropy'] = calculate_entropy_vectorized(domains)
    features['path_entropy'] = calculate_entropy_vectorized(paths)
    vowel_counts = domains.str.count(r'[aeiou]')
    consonant_counts = domains.str.count(r'[bcdfghjklmnpqrstvwxyz]')
    features['vowel_consonant_ratio'] = (vowel_counts / np.maximum(consonant_counts, 1)).astype(np.float32)
    features['is_ugc_path'] = paths.str.contains(r'/(user|profile|c|channel|u)/', regex=True, na=False).astype(int)
    features['path_has_article_slug'] = paths.str.contains(r'/[a-z0-9-]+-\d{4,}/?', na=False, regex=True).astype(int)
    features['path_has_date'] = paths.str.contains(r'/20\d{2}/\d{1,2}/', na=False, regex=True).astype(int)
    features['path_is_descriptive'] = ((paths.str.count('-') >= 2) & (features['len_path'] > 15)).astype(int)
    is_readable_domain = (features['domain_entropy'] < 3.3) & (~features['domain_is_numeric_heavy'].astype(bool)) & (~features['domain_hyphen_heavy'].astype(bool))
    is_article_path = features['path_has_article_slug'].astype(bool) | features['path_has_date'].astype(bool) | features['path_is_descriptive'].astype(bool)
    features['is_quality_article'] = (is_readable_domain & is_article_path).astype(int)

    if NSFW_PATTERN:
        features['is_nsfw_content'] = path_query.str.contains(NSFW_PATTERN, regex=True, na=False).astype(int)
    else:
        features['is_nsfw_content'] = 0
    if SPAM_PATTERN:
        features['is_spam_content'] = path_query.str.contains(SPAM_PATTERN, regex=True, na=False).astype(int)
    else:
        features['is_spam_content'] = 0

    features['is_nsfw_domain'] = domains.isin(NSFW_DOMAINS).astype(int)
    features['is_quality_domain'] = domains.isin(QUALITY_DOMAINS).astype(int)
    features['is_news_domain'] = domains.isin(NEWS_DOMAINS).astype(int)
    features.fillna(0, inplace=True)
    return features

class URLArchivalRegressor:
    def __init__(self, model_path: str = MODEL_SAVE_PATH):
        self.model_path = model_path
        self.model: lgb.LGBMRegressor | None = None
        self.feature_names: List[str] | None = None

    def _prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        parsed_df = parse_urls_vectorized(df['url'])
        full_df = pd.concat([df.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)
        features_df = extract_features_vectorized(full_df)
        return features_df

    def train(self, training_df: pd.DataFrame):
        print("Starting regression model training with in-memory DataFrame...")
        if training_df.empty or 'score' not in training_df.columns:
            print("Error: DataFrame is empty or missing 'score' column for training.")
            return

        training_df = training_df.drop_duplicates(subset=['url']).reset_index(drop=True)
        print(f"Final training set size after deduplication: {len(training_df)}")

        print("Extracting features for the training set...")
        X = self._prepare_features(training_df)
        y = training_df['score'].astype(np.float32)
        good_data_mask = training_df['data_source'] == 'good_data'
        num_good_data = good_data_mask.sum()
        if num_good_data > 0:
            print(f"Found {num_good_data} 'good_data' samples. Boosting their target scores to 2.0.")
            y.loc[good_data_mask] = 2.0
        y.clip(lower=-5.0, upper=2.0, inplace=True)

        self.feature_names = X.columns.tolist()

        print("Training LightGBM Regressor to predict URL scores...")
        lgbm = lgb.LGBMRegressor(
            objective='regression_l1',
            metric='mae',
            n_estimators=2000,
            learning_rate=0.02,
            num_leaves=61,
            max_depth=8,
            n_jobs=-1,
            random_state=42,
            boosting_type='gbdt',
            colsample_bytree=0.7,
            subsample=0.7,
            reg_alpha=0.1,
            reg_lambda=0.1,
        )

        lgbm.fit(X, y, eval_set=[(X, y)], callbacks=[lgb.early_stopping(100, verbose=True)])
        self.model = lgbm
        self._save_model()

    def _calculate_scores(self, predicted_scores: np.ndarray, features_df: pd.DataFrame) -> Dict:
        risk_score = np.maximum(0, -predicted_scores) / 5.0 
        trust_score = np.maximum(0, predicted_scores) / 2.0
        confidence = 1 / (1 + np.exp(-predicted_scores))

        return {
            'score': predicted_scores,
            'is_spam': predicted_scores < -1.5,
            'risk_score': np.clip(risk_score, 0, 1),
            'trust_score': np.clip(trust_score, 0, 1),
            'composite_score': trust_score - risk_score,
            'confidence': confidence,
            'entropy_score': features_df['domain_entropy'].values,
            'readability_score': features_df['vowel_consonant_ratio'].values
        }

    def predict(self, df_to_score: pd.DataFrame) -> pd.DataFrame:
        if df_to_score.empty:
            return pd.DataFrame()

        self._load_model()
        print(f"Scoring {len(df_to_score)} URLs with regression model...")
        parsed_df = parse_urls_vectorized(df_to_score['url'])
        full_df = pd.concat([df_to_score.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)
        features_df = extract_features_vectorized(full_df)
        missing_cols = set(self.feature_names) - set(features_df.columns)
        if missing_cols:
            raise ValueError(f"The following features are missing from the prediction data: {missing_cols}")
        X_predict = features_df[self.feature_names]
        predicted_scores = self.model.predict(X_predict)
        calculated_scores = self._calculate_scores(predicted_scores, features_df)
        result_df = full_df.copy()
        for col, values in calculated_scores.items():
            result_df[col] = values

        result_df['domain_reputation_score'] = 0.0
        result_df['url'] = result_df['url'].str.slice(0, MAX_URL_LENGTH)
        result_df['received_at'] = CURRENT_TIMESTAMP
        result_df['domain_frequency'] = result_df['domain'].map(result_df['domain'].value_counts()).fillna(1).astype(int)
        
        total_urls = len(result_df)
        if total_urls > 0:
            result_df['domain_frequency_pct'] = (result_df['domain_frequency'] / total_urls).astype(np.float32)
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