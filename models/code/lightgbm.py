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

if os.getcwd() not in sys.path:
    sys.path.append(os.getcwd())

from spark.config.spark_config import SparkConfig
from schema import PROCESSED_URL_SCHEMA

MODEL_SAVE_PATH = os.path.join('models', 'trained', 'url_archival_model.joblib')
LISTS_DIR = os.path.join('data', 'storage', 'lists')
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


@lru_cache(maxsize=1024)
def get_shannon_entropy(text: str) -> float:
    if not text or len(text) <= 1: return 0.0
    counts = Counter(text)
    length = len(text)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())


def calculate_entropy_vectorized(series: pd.Series) -> np.ndarray:
    unique_values = series.unique()
    entropy_map = {val: get_shannon_entropy(val) for val in unique_values}
    return series.map(entropy_map).values.astype(np.float32)


def parse_urls_vectorized(urls: pd.Series) -> pd.DataFrame:
    urls_str = urls.astype(str).str.strip()
    no_scheme_mask = ~urls_str.str.contains('://', na=False, regex=False)
    if no_scheme_mask.any():
        urls_str_copy = urls_str.copy()
        urls_str_copy.loc[no_scheme_mask] = '//' + urls_str_copy.loc[no_scheme_mask]
        urls_str = urls_str_copy
    parts = urls_str.str.extract(URL_PARSE_REGEX)
    parts.rename(columns={'domain': 'domain', 'path': 'path', 'query': 'query'}, inplace=True)
    parts['domain'] = parts['domain'].str.lower().fillna('unknown.invalid')
    parts['path'] = parts['path'].fillna('')
    parts['query'] = parts['query'].fillna('')
    return parts


def extract_features_vectorized(pdf: pd.DataFrame) -> pd.DataFrame:
    features = {}
    pdf['domain'] = pdf.get('domain', pd.Series(index=pdf.index, dtype=str))
    pdf['path'] = pdf.get('path', pd.Series(index=pdf.index, dtype=str))
    pdf['query'] = pdf.get('query', pd.Series(index=pdf.index, dtype=str))

    urls, domains, paths, queries = pdf['url'], pdf['domain'], pdf['path'], pdf['query']
    path_query = paths.fillna('') + '?' + queries.fillna('')

    features['len_url'] = urls.str.len().fillna(0)
    features['len_path'] = paths.str.len().fillna(0)
    features['len_query'] = queries.str.len().fillna(0)
    features['len_domain'] = domains.str.len().fillna(0)

    tlds = domains.str.rsplit('.', n=1, expand=True)[1].fillna('')
    features['is_suspicious_tld'] = tlds.isin(SUSPICIOUS_TLDS)
    features['is_trustworthy_tld'] = tlds.isin(TRUSTWORTHY_TLDS)

    features['has_ip_address'] = domains.str.match(IP_ADDRESS_PATTERN.pattern, na=False)
    features['num_subdomains'] = domains.str.count(r'\.').fillna(0)
    features['num_path_dirs'] = paths.str.count('/').fillna(0)
    features['num_params'] = queries.str.count('&').fillna(0) + (queries.fillna('') != '')
    features['num_digits_domain'] = domains.str.count(r'\d').fillna(0)
    features['num_hyphens_domain'] = domains.str.count('-').fillna(0)

    for name, pattern in CONTENT_PATTERNS.items():
        features[name] = path_query.str.contains(pattern.pattern, regex=True, na=False)

    features['domain_entropy'] = calculate_entropy_vectorized(domains)
    features['path_entropy'] = calculate_entropy_vectorized(paths)

    vowel_counts = domains.str.count(r'[aeiou]').fillna(0)
    consonant_counts = domains.str.count(r'[bcdfghjklmnpqrstvwxyz]').fillna(0)
    features['vowel_consonant_ratio'] = (vowel_counts / consonant_counts.replace(0, 1)).astype(np.float32)

    if NSFW_PATTERN:
        features['is_nsfw_content'] = path_query.str.contains(NSFW_PATTERN.pattern, regex=True, na=False)
    if SPAM_PATTERN:
        features['is_spam_content'] = path_query.str.contains(SPAM_PATTERN.pattern, regex=True, na=False)

    features['is_nsfw_domain'] = domains.isin(NSFW_DOMAINS)
    features['is_quality_domain'] = domains.isin(QUALITY_DOMAINS)
    features['is_news_domain'] = domains.isin(NEWS_DOMAINS)

    features_df = pd.DataFrame(features)
    for col in features_df.columns:
        if features_df[col].dtype == bool:
            features_df[col] = features_df[col].astype(int)

    return features_df

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
        num_spam = label_counts.get(0, 0)
        num_ham = label_counts.get(1, 0)
        num_good = label_counts.get(2, 0)

        print(f"Dataset counts: Spam={num_spam}, Ham={num_ham}, Good={num_good}")

        if num_good == 0:
            print("=" * 60)
            print("WARNING: No 'good_data' found for training. The resulting model")
            print("will not be able to effectively identify high-priority content.")
            print("=" * 60)

        if num_spam < 100 or num_ham < 100:
            print(f"Error: Not enough spam/ham data. Spam={num_spam}, Ham={num_ham}. (Threshold: 100 each)")
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
        scores = {}
        p_spam = probs[:, 0]
        p_ham = probs[:, 1]
        p_good = probs[:, 2]

        scores['score'] = p_ham + p_good
        scores['is_spam'] = (p_spam > 0.5)
        scores['risk_score'] = p_spam
        scores['trust_score'] = p_good
        scores['composite_score'] = scores['trust_score'] - scores['risk_score']
        scores['confidence'] = np.max(probs, axis=1)

        scores['entropy_score'] = features_df['domain_entropy'].values
        scores['readability_score'] = features_df['vowel_consonant_ratio'].values

        return scores

    def predict(self, df_to_score: pd.DataFrame) -> pd.DataFrame:
        if df_to_score.empty:
            return pd.DataFrame()

        self._load_model()
        if self.model.n_classes_ != self.num_classes:
             raise ValueError(f"Model was trained on {self.model.n_classes_} classes, but this version requires {self.num_classes}.")

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

        result_df['url'] = result_df['url'].str.slice(0, MAX_URL_LENGTH)
        result_df['received_at'] = CURRENT_TIMESTAMP
        result_df['domain_frequency'] = result_df['domain'].map(result_df['domain'].value_counts()).fillna(1).astype(int)

        for field in PROCESSED_URL_SCHEMA:
            if field.name not in result_df.columns:
                result_df[field.name] = None

        return result_df[[field.name for field in PROCESSED_URL_SCHEMA if field.name in result_df.columns]]

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
            if not os.path.exists(self.model_path):
                raise FileNotFoundError(f"Model file not found at {self.model_path}. Please run train() first.")
            print(f"Loading pre-trained model from {self.model_path}...")
            data = joblib.load(self.model_path)
            self.model = data['model']
            self.feature_names = data['features']
