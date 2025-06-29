import gzip
import logging
import os
import pickle
import re
import shutil
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple, Optional

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"


def setup_output_directory():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory set to: {OUTPUT_DIR.absolute()}")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from imblearn.over_sampling import SMOTE

from sklearn.ensemble import (GradientBoostingClassifier,
                              RandomForestClassifier, VotingClassifier)
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, classification_report,
                             confusion_matrix, precision_recall_curve,
                             precision_recall_fscore_support, roc_auc_score, roc_curve, average_precision_score)
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.tree import DecisionTreeClassifier

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GraphWebSpamDatasetLoader:

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.url_data = None
        self.host_graph: Dict[int, List[Tuple[int, int]]] = {} # To store graph data: host_id -> [(dest_host_id, nlinks)]
        self.host_id_to_hostname: Dict[int, str] = {} # To map host_id to hostname
        self.hostname_to_host_id: Dict[str, int] = {} # To map hostname to host_id

    def load_labels(self, dataset: str = "set1") -> pd.DataFrame:
        logger.info(f"Loading labels from {dataset}...")

        if dataset == "set1":
            labels_file = self.data_dir / "001_model_data" / "webspam-uk2007-set1-1.0" / "WEBSPAM-UK2007-SET1-labels.txt"
            hostnames_file = self.data_dir / "001_model_data" / "webspam-uk2007-set1-1.0" / "WEBSPAM-UK2007-hostnames.txt"
        else:
            labels_file = self.data_dir / "001_model_data" / "webspam-uk2007-set2-1.0" / "WEBSPAM-UK2007-SET2-labels.txt"
            hostnames_file = self.data_dir / "001_model_data" / "webspam-uk2007-set2-1.0" / "WEBSPAM-UK2007-hostnames.txt"

        labels_data = []
        with open(labels_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split(' ', 3)
                if len(parts) >= 2:
                    host_id = int(parts[0])
                    label = parts[1]
                    spamicity = parts[2] if len(parts) > 2 else '-'
                    assessments = parts[3] if len(parts) > 3 else ''

                    if label in ['spam', 'nonspam']:
                        labels_data.append({
                            'host_id': host_id,
                            'label': label,
                            'spamicity': spamicity,
                            'assessments': assessments
                        })

        labels_df = pd.DataFrame(labels_data)

        hostname_mapping = {}
        with open(hostnames_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        host_id = int(parts[0])
                        hostname = parts[1]
                        hostname_mapping[host_id] = hostname

        labels_df['hostname'] = labels_df['host_id'].map(hostname_mapping)

        logger.info(f"Loaded {len(labels_df)} labeled hosts ({dataset})")
        logger.info(
            f"Label distribution: {labels_df['label'].value_counts().to_dict()}")

        return labels_df

    def load_host_graph_data(self):
        logger.info("Loading host graph data...")
        hostnames_file = self.data_dir / "compressed" / "uk-2007-05.hostnames.txt.gz"
        hostgraph_file = self.data_dir / "compressed" / "uk-2007-05.hostgraph_weighted.graph-txt.gz"

        if not hostnames_file.exists() or not hostgraph_file.exists():
            logger.error("Host graph files not found. Please ensure 'uk-2007-05.hostnames.txt.gz' and 'uk-2007-05.hostgraph_weighted.graph-txt.gz' are in the 'data/compressed' directory.")
            return

        # Load host ID to hostname mapping
        with gzip.open(hostnames_file, 'rt', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        host_id = int(parts[0])
                        hostname = parts[1]
                        self.host_id_to_hostname[host_id] = hostname
                        self.hostname_to_host_id[hostname] = host_id
        logger.info(f"Loaded {len(self.host_id_to_hostname)} hostnames.")

        # Load host graph
        with gzip.open(hostgraph_file, 'rt', encoding='utf-8', errors='ignore') as f:
            num_hosts = int(f.readline().strip()) # First line is number of hosts
            for i, line in enumerate(f):
                line = line.strip()
                if line:
                    outlinks = []
                    for link_str in line.split(' '):
                        if ':' in link_str:
                            dest_id, nlinks = link_str.split(':')
                            outlinks.append((int(dest_id), int(nlinks)))
                    self.host_graph[i] = outlinks # host_id maps to its outlinks
        logger.info(f"Loaded graph for {len(self.host_graph)} hosts.")


class GraphSuperiorFeatureExtractor: # Renamed for clarity and potential inheritance

    def __init__(self, host_graph: Dict[int, List[Tuple[int, int]]], host_id_to_hostname: Dict[int, str], hostname_to_host_id: Dict[str, int]): # Pass graph data
        self.spam_tlds = {
            '.tk', '.ml', '.ga', '.cf', '.info', '.biz', '.cc', '.su', '.ws',
            '.click', '.download', '.work', '.party', '.review', '.stream',
            '.science', '.date', '.racing', '.accountant', '.faith', '.cricket',
            '.win', '.bid', '.loan', '.men', '.webcam', '.top', '.xyz', '.online',
            '.site', '.website', '.space', '.tech', '.store', '.fun', '.pw'
        }

        self.spam_keywords = {

            'viagra': 5, 'casino': 5, 'poker': 5, 'gambling': 5, 'porn': 5,
            'adult': 5, 'sex': 5, 'xxx': 5, 'pills': 5, 'pharmacy': 5,
            'loan': 4, 'credit': 4, 'money': 4, 'cash': 4, 'debt': 4,
            'mortgage': 4, 'refinance': 4, 'insurance': 4, 'investment': 4,

            'free': 3, 'cheap': 3, 'discount': 3, 'sale': 3, 'offer': 3,
            'deal': 3, 'promo': 3, 'bonus': 3, 'gift': 3, 'prize': 3,
            'winner': 3, 'win': 3, 'lottery': 3, 'sweepstakes': 3,

            'click': 2, 'now': 2, 'urgent': 2, 'limited': 2, 'exclusive': 2,
            'special': 2, 'amazing': 2, 'guaranteed': 2, 'instant': 2,
            'earn': 2, 'income': 2, 'business': 2, 'opportunity': 2,
            'work': 1, 'home': 1, 'online': 1, 'internet': 1
        }

        self.legitimate_domains = {
            'google.com', 'facebook.com', 'twitter.com', 'youtube.com', 'wikipedia.org',
            'amazon.com', 'ebay.com', 'paypal.com', 'apple.com', 'microsoft.com',
            'linkedin.com', 'instagram.com', 'reddit.com', 'stackoverflow.com',
            'github.com', 'bbc.co.uk', 'cnn.com', 'nytimes.com', 'washingtonpost.com'
        }

        self.suspicious_hosting_patterns = {
            'free', 'hosting', 'host', 'server', 'dns', 'redirect', 'shortener',
            'tiny', 'short', 'bit', 'goo', 'ow', 'is', 'to'
        }

        self.common_words = {
            'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had',
            'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his',
            'how', 'its', 'may', 'new', 'now', 'old', 'see', 'two', 'way', 'who',
            'boy', 'did', 'man', 'run', 'eat', 'far', 'sea', 'eye', 'red', 'top',
            'news', 'blog', 'about', 'contact', 'home', 'page', 'site', 'web'
        }

        self.suspicious_patterns = {
            'random_chars': re.compile(r'[a-z]{2,}[0-9]{3,}[a-z]{2,}'),
            'excessive_hyphens': re.compile(r'-{2,}'),
            'excessive_numbers': re.compile(r'[0-9]{4,}'),
            'mixed_case_random': re.compile(r'[a-z][A-Z][a-z][A-Z]'),
            'keyboard_walk': re.compile(r'(qwerty|asdf|zxcv|1234|abcd)'),
            'repeated_chars': re.compile(r'(.)\1{3,}'),
            'ip_address': re.compile(r'^(\d{1,3}\.){3}\d{1,3}$'),
            'suspicious_suffix': re.compile(r'(porn|sex|xxx|casino|poker|loan|pill)'),
        }

        self.host_graph = host_graph
        self.host_id_to_hostname = host_id_to_hostname
        self.hostname_to_host_id = hostname_to_host_id
        # Pre-calculate in-degrees for efficiency
        self.in_degrees = self._calculate_in_degrees()


    def _calculate_in_degrees(self) -> Dict[int, int]:
        in_degrees = Counter()
        for source_id, outlinks in self.host_graph.items():
            for dest_id, _ in outlinks:
                in_degrees[dest_id] += 1
        return in_degrees

    def extract_comprehensive_features(self, hostname: str, host_id: Optional[int] = None) -> Dict:
        """Extract comprehensive features from hostname and graph data"""
        if not hostname:
            return self._get_empty_features()

        try:
            hostname = hostname.lower().strip()

            features = self._extract_basic_features(hostname)

            features.update(self._extract_linguistic_features(hostname))

            features.update(self._extract_structural_features(hostname))

            features.update(self._extract_reputation_features(hostname))

            features.update(self._extract_pattern_features(hostname))

            features.update(self._extract_statistical_features(hostname))

            # Add graph-based features
            graph_features = self._extract_graph_features(host_id if host_id is not None else self.hostname_to_host_id.get(hostname))
            features.update(graph_features)

            return features

        except Exception as e:
            logger.warning(
                f"Error extracting features from hostname '{hostname}': {e}")
            return self._get_empty_features()

    def _extract_basic_features(self, hostname: str) -> Dict:
        """Extract basic hostname features"""
        parts = hostname.split('.')

        return {
            'hostname_length': len(hostname),
            'subdomain_count': len(parts) - 2 if len(parts) > 2 else 0,
            'domain_count': len(parts),
            'digit_count': len(re.findall(r'\d', hostname)),
            'digit_ratio': len(re.findall(r'\d', hostname)) / len(hostname) if hostname else 0,
            'letter_count': len(re.findall(r'[a-zA-Z]', hostname)),
            'letter_ratio': len(re.findall(r'[a-zA-Z]', hostname)) / len(hostname) if hostname else 0,
            'hyphen_count': hostname.count('-'),
            'underscore_count': hostname.count('_'),
            'dot_count': hostname.count('.'),
            'has_www': int(hostname.startswith('www.')),
            'has_port': int(':' in hostname),
        }

    def _extract_linguistic_features(self, hostname: str) -> Dict:
        """Extract linguistic and semantic features"""

        spam_score = 0
        spam_word_count = 0
        for word, weight in self.spam_keywords.items():
            if word in hostname:
                spam_score += weight
                spam_word_count += 1

        common_word_count = sum(
            1 for word in self.common_words if word in hostname)

        readability_score = self._calculate_readability(hostname)

        return {
            'spam_score_weighted': spam_score,
            'spam_word_count': spam_word_count,
            'common_word_count': common_word_count,
            'readability_score': readability_score,
            'vowel_count': len(re.findall(r'[aeiou]', hostname)),
            'consonant_count': len(re.findall(r'[bcdfghjklmnpqrstvwxyz]', hostname)),
            'vowel_consonant_ratio': self._safe_divide(
                len(re.findall(r'[aeiou]', hostname)),
                len(re.findall(r'[bcdfghjklmnpqrstvwxyz]', hostname))
            ),
        }

    def _extract_structural_features(self, hostname: str) -> Dict:
        """Extract structural features"""
        parts = hostname.split('.')

        tld = '.' + parts[-1] if len(parts) > 1 else ''

        domain_part = parts[-2] if len(parts) >= 2 else hostname

        subdomains = parts[:-2] if len(parts) > 2 else []

        return {
            'tld': tld,
            'is_suspicious_tld': int(tld in self.spam_tlds),
            'domain_part_length': len(domain_part),
            'max_subdomain_length': max([len(s) for s in subdomains], default=0),
            'min_subdomain_length': min([len(s) for s in subdomains], default=0),
            'avg_subdomain_length': np.mean([len(s) for s in subdomains]) if subdomains else 0,
            'subdomain_digit_ratio': np.mean([
                len(re.findall(r'\d', s)) / len(s) if s else 0 for s in subdomains
            ]) if subdomains else 0,
            'has_deep_subdomain': int(len(subdomains) > 3),
            'longest_part_length': max([len(p) for p in parts], default=0),
        }

    def _extract_reputation_features(self, hostname: str) -> Dict:
        """Extract reputation-based features"""

        is_legitimate = int(
            any(domain in hostname for domain in self.legitimate_domains))

        hosting_suspicion = sum(1 for pattern in self.suspicious_hosting_patterns
                                if pattern in hostname)

        return {
            'is_legitimate_domain': is_legitimate,
            'hosting_suspicion_score': hosting_suspicion,
            'has_suspicious_hosting': int(hosting_suspicion > 0),
        }

    def _extract_pattern_features(self, hostname: str) -> Dict:
        """Extract pattern-based features"""
        features = {}

        for pattern_name, pattern in self.suspicious_patterns.items():
            features[f'has_{pattern_name}'] = int(
                bool(pattern.search(hostname)))

        features['consecutive_digits'] = len(
            max(re.findall(r'\d+', hostname), key=len, default=''))
        features['consecutive_letters'] = len(
            max(re.findall(r'[a-zA-Z]+', hostname), key=len, default=''))
        features['char_repetition_max'] = max([len(list(g)) for k, g in
                                               __import__('itertools').groupby(hostname)], default=0)

        return features

    def _extract_statistical_features(self, hostname: str) -> Dict:
        """Extract statistical features"""
        if not hostname:
            return {'entropy': 0, 'char_diversity': 0, 'uniqueness_ratio': 0}

        entropy = self._calculate_entropy(hostname)

        unique_chars = len(set(hostname))
        char_diversity = unique_chars / len(hostname) if hostname else 0

        uniqueness_ratio = len(set(hostname)) / \
            len(hostname) if hostname else 0

        char_counts = Counter(hostname)
        char_frequencies = np.array(list(char_counts.values()))

        return {
            'entropy': entropy,
            'char_diversity': char_diversity,
            'uniqueness_ratio': uniqueness_ratio,
            'char_freq_std': np.std(char_frequencies),
            'char_freq_mean': np.mean(char_frequencies),
            'most_common_char_freq': max(char_counts.values()) if char_counts else 0,
        }

    def _extract_graph_features(self, host_id: int | None) -> Dict:
        """Extract graph-based features for a given host_id"""
        if host_id is None or host_id not in self.host_graph:
            return {
                'in_degree': 0,
                'out_degree': 0,
                'total_out_links_weight': 0,
                'avg_out_link_weight': 0,
                'has_inbound_links': 0,
                'has_outbound_links': 0,
            }

        outlinks = self.host_graph.get(host_id, [])
        in_degree = self.in_degrees.get(host_id, 0)
        out_degree = len(outlinks)
        total_out_links_weight = sum(nlinks for _, nlinks in outlinks)
        avg_out_link_weight = self._safe_divide(total_out_links_weight, out_degree)

        return {
            'in_degree': in_degree,
            'out_degree': out_degree,
            'total_out_links_weight': total_out_links_weight,
            'avg_out_link_weight': avg_out_link_weight,
            'has_inbound_links': int(in_degree > 0),
            'has_outbound_links': int(out_degree > 0),
        }


    def _calculate_entropy(self, text: str) -> float:
        """Calculate Shannon entropy"""
        if not text:
            return 0.0

        char_counts = Counter(text)
        length = len(text)
        entropy = 0.0

        for count in char_counts.values():
            p = count / length
            entropy -= p * np.log2(p)

        return entropy

    def _calculate_readability(self, hostname: str) -> float:
        """Calculate hostname readability score based on common patterns"""
        if not hostname:
            return 0.0

        parts = hostname.split('.')
        if len(parts) > 1:
            domain_part = parts[-2]
        else:
            domain_part = hostname

        bigrams = [domain_part[i:i+2] for i in range(len(domain_part)-1)]
        common_bigrams = {'th', 'he', 'in', 'er',
                          'an', 're', 'nd', 'on', 'en', 'at'}

        if not bigrams:
            return 0.0

        common_count = sum(1 for bg in bigrams if bg in common_bigrams)
        return common_count / len(bigrams)

    def _safe_divide(self, numerator: float, denominator: float) -> float:
        """Safe division to avoid division by zero"""
        return numerator / denominator if denominator != 0 else 0.0

    def _get_empty_features(self) -> Dict:
        """Return empty feature dictionary including graph features"""
        return {
            'hostname_length': 0, 'subdomain_count': 0, 'domain_count': 0,
            'digit_count': 0, 'digit_ratio': 0, 'letter_count': 0, 'letter_ratio': 0,
            'hyphen_count': 0, 'underscore_count': 0, 'dot_count': 0,
            'has_www': 0, 'has_port': 0, 'tld': 'unknown',
            'is_suspicious_tld': 0, 'spam_score_weighted': 0, 'spam_word_count': 0,
            'common_word_count': 0, 'readability_score': 0, 'vowel_count': 0,
            'consonant_count': 0, 'vowel_consonant_ratio': 0, 'domain_part_length': 0,
            'max_subdomain_length': 0, 'min_subdomain_length': 0, 'avg_subdomain_length': 0,
            'subdomain_digit_ratio': 0, 'has_deep_subdomain': 0, 'longest_part_length': 0,
            'is_legitimate_domain': 0, 'hosting_suspicion_score': 0, 'has_suspicious_hosting': 0,
            'has_random_chars': 0, 'has_excessive_hyphens': 0, 'has_excessive_numbers': 0,
            'has_mixed_case_random': 0, 'has_keyboard_walk': 0, 'has_repeated_chars': 0,
            'has_ip_address': 0, 'has_suspicious_suffix': 0, 'consecutive_digits': 0,
            'consecutive_letters': 0, 'char_repetition_max': 0, 'entropy': 0,
            'char_diversity': 0, 'uniqueness_ratio': 0, 'char_freq_std': 0,
            'char_freq_mean': 0, 'most_common_char_freq': 0,
            'in_degree': 0, 'out_degree': 0, 'total_out_links_weight': 0,
            'avg_out_link_weight': 0, 'has_inbound_links': 0, 'has_outbound_links': 0
        }


class GraphWebSpamClassifier:
    def __init__(self, random_state=42):
        self.random_state = random_state

        self.classifiers = {
            'decision_tree': DecisionTreeClassifier(
                max_depth=25,
                min_samples_split=3,
                min_samples_leaf=2,
                class_weight='balanced',
                random_state=random_state
            ),
            'random_forest': RandomForestClassifier(
                n_estimators=300,
                max_depth=25,
                min_samples_split=3,
                min_samples_leaf=2,
                class_weight='balanced',
                random_state=random_state,
                n_jobs=-1
            ),
            'gradient_boosting': GradientBoostingClassifier(
                n_estimators=200,
                max_depth=10,
                learning_rate=0.1,
                random_state=random_state
            ),
            'logistic_regression': LogisticRegression(
                class_weight='balanced',
                random_state=random_state,
                max_iter=1000
            )
        }

        self.ensemble_classifier = VotingClassifier([
            ('rf', self.classifiers['random_forest']),
            ('gb', self.classifiers['gradient_boosting']),
            ('lr', self.classifiers['logistic_regression'])
        ], voting='soft')

        self.feature_extractor: GraphSuperiorFeatureExtractor | None = None
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        self.feature_selector = SelectKBest(score_func=f_classif, k='all')

        self.feature_names = []
        self.tld_categories_fitted = None
        self.tld_dummy_columns = []

    def prepare_advanced_features(self, labels_df: pd.DataFrame, loader: GraphWebSpamDatasetLoader, fit_encoders: bool = True) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare advanced features with better preprocessing, including graph features"""
        logger.info("Extracting advanced features from hostnames and graph data...")

        if self.feature_extractor is None:
            self.feature_extractor = GraphSuperiorFeatureExtractor(loader.host_graph, loader.host_id_to_hostname, loader.hostname_to_host_id)


        features_list = []
        total_hosts = len(labels_df)

        for i, (_, row) in enumerate(labels_df.iterrows()):
            if i % 1000 == 0:
                logger.info(f"Processing {i}/{total_hosts} hosts...")

            hostname = row.get('hostname', '')
            host_id = row.get('host_id') # Pass host_id to extractor for direct lookup
            features = self.feature_extractor.extract_comprehensive_features(
                hostname, host_id)
            features['host_id'] = row['host_id']
            features_list.append(features)

        features_df = pd.DataFrame(features_list)

        if fit_encoders:
            tld_counts = features_df['tld'].value_counts()

            frequent_tlds = set(tld_counts[tld_counts >= 10].index)
            self.tld_categories_fitted = frequent_tlds

        if self.tld_categories_fitted is not None:
            features_df['tld_category'] = features_df['tld'].apply(
                lambda x: x if x in self.tld_categories_fitted else 'other'
            )
        else:
            features_df['tld_category'] = 'other'

        tld_dummies = pd.get_dummies(features_df['tld_category'], prefix='tld')

        if fit_encoders:
            self.tld_dummy_columns = tld_dummies.columns.tolist()
        else:

            for col in self.tld_dummy_columns:
                if col not in tld_dummies.columns:
                    tld_dummies[col] = 0
            tld_dummies = tld_dummies[self.tld_dummy_columns]

        features_df = pd.concat([features_df, tld_dummies], axis=1)

        combined_df = labels_df.merge(features_df, on='host_id', how='inner')

        feature_cols = [col for col in combined_df.columns
                        if col not in ['host_id', 'label', 'hostname', 'spamicity', 'assessments', 'tld', 'tld_category']]

        X = combined_df[feature_cols].fillna(0)
        y = combined_df['label']

        if fit_encoders:
            y_encoded = self.label_encoder.fit_transform(y)
            self.feature_names = feature_cols
        else:
            y_encoded = []
            for label in y:
                if label in self.label_encoder.classes_:
                    y_encoded.append(self.label_encoder.transform([label])[0])
                else:
                    logger.warning(f"Unseen label in test data: {label}")
                    y_encoded.append(-1)
            y_encoded = np.array(y_encoded)

            valid_mask = y_encoded != -1
            X = X[valid_mask]
            y_encoded = y_encoded[valid_mask]

        logger.info(
            f"Prepared {len(X)} samples with {len(feature_cols)} features")

        return X, y_encoded

    def train_and_evaluate_cross_dataset(self, train_df: pd.DataFrame, test_df: pd.DataFrame, loader: GraphWebSpamDatasetLoader):
        """Train on Set1 and evaluate on Set2 with advanced techniques"""
        logger.info("Starting advanced cross-dataset evaluation with graph features...")

        logger.info("Preparing training features...")
        X_train, y_train = self.prepare_advanced_features(
            train_df, loader, fit_encoders=True)

        logger.info("Preparing test features...")
        X_test, y_test = self.prepare_advanced_features(
            test_df, loader, fit_encoders=False)

        for col in self.feature_names:
            if col not in X_test.columns:
                X_test[col] = 0
        X_test = X_test[self.feature_names]

        logger.info("Applying SMOTE for class balancing...")
        smote = SMOTE(random_state=self.random_state)
        X_train_balanced, y_train_balanced = smote.fit_resample(
            X_train, y_train)

        logger.info(
            f"Training set after SMOTE: {len(X_train_balanced)} samples")
        logger.info(
            f"Class distribution after SMOTE: {Counter(y_train_balanced)}")

        logger.info("Scaling features...")
        X_train_scaled = self.scaler.fit_transform(X_train_balanced)
        X_test_scaled = self.scaler.transform(X_test)

        logger.info("Selecting best features...")
        X_train_selected = self.feature_selector.fit_transform(
            X_train_scaled, y_train_balanced)
        X_test_selected = self.feature_selector.transform(X_test_scaled)

        selected_features = [self.feature_names[i]
                             for i in self.feature_selector.get_support(indices=True)]
        logger.info(f"Selected {len(selected_features)} best features")

        results = {}

        for name, classifier in self.classifiers.items():
            logger.info(f"Training {name}...")
            classifier.fit(X_train_selected, y_train_balanced)

            y_pred = classifier.predict(X_test_selected)
            y_pred_proba = classifier.predict_proba(X_test_selected)[:, 1] if hasattr(
                classifier, 'predict_proba') else None

            accuracy = accuracy_score(y_test, y_pred)
            precision, recall, f1, _ = precision_recall_fscore_support(
                y_test, y_pred, average='binary', pos_label=1)

            results[name] = {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1': f1,
                'predictions': y_pred,
                'probabilities': y_pred_proba
            }

            if y_pred_proba is not None:
                auc_score = roc_auc_score(y_test, y_pred_proba)
                results[name]['auc'] = auc_score

            logger.info(f"{name} - Accuracy: {accuracy:.4f}, F1: {f1:.4f}")

        logger.info("Training ensemble classifier...")
        self.ensemble_classifier.fit(X_train_selected, y_train_balanced)

        ensemble_pred = self.ensemble_classifier.predict(X_test_selected)
        ensemble_proba = self.ensemble_classifier.predict_proba(X_test_selected)[
            :, 1]

        ensemble_accuracy = accuracy_score(y_test, ensemble_pred)
        ensemble_precision, ensemble_recall, ensemble_f1, _ = precision_recall_fscore_support(
            y_test, ensemble_pred, average='binary', pos_label=1
        )
        ensemble_auc = roc_auc_score(y_test, ensemble_proba)

        results['ensemble'] = {
            'accuracy': ensemble_accuracy,
            'precision': ensemble_precision,
            'recall': ensemble_recall,
            'f1': ensemble_f1,
            'auc': ensemble_auc,
            'predictions': ensemble_pred,
            'probabilities': ensemble_proba
        }

        logger.info(
            f"Ensemble - Accuracy: {ensemble_accuracy:.4f}, F1: {ensemble_f1:.4f}, AUC: {ensemble_auc:.4f}")

        self._print_detailed_results(results, y_test)

        self._generate_visualizations(
            results, y_test, selected_features, X_train_selected, y_train_balanced)

        self._save_model_and_results(results, selected_features)

        return results

    def _print_detailed_results(self, results: Dict, y_test: np.ndarray):
        """Print detailed evaluation results"""
        logger.info("\n" + "="*60)
        logger.info("DETAILED CLASSIFICATION RESULTS")
        logger.info("="*60)

        test_distribution = Counter(y_test)
        total_test = len(y_test)
        logger.info("\nTest Set Distribution:")
        logger.info(
            f"Non-spam (0): {test_distribution[0]} ({test_distribution[0]/total_test*100:.1f}%)")
        logger.info(
            f"Spam (1): {test_distribution[1]} ({test_distribution[1]/total_test*100:.1f}%)")

        logger.info(
            f"\n{'Classifier':<20} {'Accuracy':<10} {'Precision':<10} {'Recall':<10} {'F1-Score':<10} {'AUC':<10}")
        logger.info("-" * 70)

        for name, metrics in results.items():
            auc_str = f"{metrics.get('auc', 0):.4f}" if 'auc' in metrics else "N/A"
            logger.info(f"{name:<20} {metrics['accuracy']:<10.4f} {metrics['precision']:<10.4f} "
                        f"{metrics['recall']:<10.4f} {metrics['f1']:<10.4f} {auc_str:<10}")

        best_f1 = max(results.items(), key=lambda x: x[1]['f1'])
        best_auc = max(results.items(), key=lambda x: x[1].get('auc', 0))

        logger.info(f"\nBest F1-Score: {best_f1[0]} ({best_f1[1]['f1']:.4f})")
        logger.info(
            f"Best AUC: {best_auc[0]} ({best_auc[1].get('auc', 0):.4f})")

        logger.info(f"\nConfusion Matrix - {best_f1[0]} (Best F1):")
        cm = confusion_matrix(y_test, best_f1[1]['predictions'])
        logger.info(f"True Negatives: {cm[0,0]}, False Positives: {cm[0,1]}")
        logger.info(f"False Negatives: {cm[1,0]}, True Positives: {cm[1,1]}")

        if 'ensemble' in results:
            logger.info(f"\nDetailed Classification Report - Ensemble:")
            class_names = self.label_encoder.classes_
            report = classification_report(y_test, results['ensemble']['predictions'],
                                           target_names=class_names, digits=4)
            logger.info(report)

    def _generate_visualizations(self, results: Dict, y_test: np.ndarray,
                                 selected_features: List[str], X_train: np.ndarray, y_train: np.ndarray):
        """Generate comprehensive visualizations"""
        logger.info("Generating visualizations...")

        # 1. Confusion Matrices
        self._plot_confusion_matrices(results, y_test)

        # 2. ROC Curves
        self._plot_roc_curves(results, y_test)

        # 3. Precision-Recall Curves
        self._plot_precision_recall_curves(results, y_test)

        # 4. Feature Analysis
        self._plot_feature_analysis(selected_features, X_train, y_train)

        # 5. Class Distribution
        self._plot_class_distribution(y_train, y_test)

        logger.info(f"Visualizations saved to {OUTPUT_DIR}")

    def _plot_confusion_matrices(self, results: Dict, y_test: np.ndarray):
        """Plot confusion matrices for each classifier"""
        logger.info("Plotting confusion matrices...")

        fig, axes = plt.subplots(2, 3, figsize=(18, 12))

        # Ensure we don't try to plot more than 6 classifiers on a 2x3 grid
        items_to_plot = list(results.items())[:6]

        for i, (name, metrics) in enumerate(items_to_plot):
            ax = axes[i // 3, i % 3]
            cm = confusion_matrix(y_test, metrics['predictions'])
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax)
            ax.set_title(f'Confusion Matrix - {name}')
            ax.set_xlabel('Predicted')
            ax.set_ylabel('Actual')

        # Hide any unused subplots if fewer than 6 classifiers
        for i in range(len(items_to_plot), 6):
            fig.delaxes(axes.flatten()[i])

        plt.tight_layout()
        output_file = OUTPUT_DIR / 'confusion_matrices.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Confusion matrices saved to {output_file}")

    def _plot_roc_curves(self, results: Dict, y_test: np.ndarray):
        """Plot ROC curves for each classifier"""
        logger.info("Plotting ROC curves...")

        fig, ax = plt.subplots(figsize=(12, 8))

        for name, metrics in results.items():
            if 'probabilities' in metrics and metrics['probabilities'] is not None:
                fpr, tpr, _ = roc_curve(y_test, metrics['probabilities'])
                auc_score = metrics.get('auc', 0)
                ax.plot(fpr, tpr, label=f'{name} (AUC = {auc_score:.3f})')

        ax.plot([0, 1], [0, 1], 'k--', label='Random')
        ax.set_xlabel('False Positive Rate')
        ax.set_ylabel('True Positive Rate')
        ax.set_title('ROC Curves')
        ax.legend()
        ax.grid(True, alpha=0.3)

        output_file = OUTPUT_DIR / 'roc_curves.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"ROC curves saved to {output_file}")

    def _plot_precision_recall_curves(self, results: Dict, y_test: np.ndarray):
        """Plot precision-recall curves for each classifier"""
        logger.info("Plotting precision-recall curves...")

        fig, ax = plt.subplots(figsize=(12, 8))

        for name, metrics in results.items():
            if 'probabilities' in metrics and metrics['probabilities'] is not None:
                precision, recall, _ = precision_recall_curve(
                    y_test, metrics['probabilities'])
                ap_score = average_precision_score(
                    y_test, metrics['probabilities'])
                ax.plot(recall, precision,
                        label=f'{name} (AP = {ap_score:.3f})')

        ax.set_xlabel('Recall')
        ax.set_ylabel('Precision')
        ax.set_title('Precision-Recall Curves')
        ax.legend()
        ax.grid(True, alpha=0.3)

        output_file = OUTPUT_DIR / 'precision_recall_curves.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Precision-recall curves saved to {output_file}")

    def _plot_feature_analysis(self, selected_features: List[str], X_train: np.ndarray, y_train: np.ndarray):
        """Create detailed feature analysis plots"""
        logger.info("Generating feature analysis plots...")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        feature_df = pd.DataFrame(X_train, columns=selected_features)
        feature_df['label'] = y_train

        ax1 = axes[0, 0]
        feature_vars = feature_df.iloc[:, :-
                                       1].var().sort_values(ascending=False)[:10]
        feature_vars.plot(kind='bar', ax=ax1)
        ax1.set_title('Top 10 Features by Variance')
        ax1.set_xlabel('Features')
        ax1.set_ylabel('Variance')
        ax1.tick_params(axis='x', rotation=45)

        ax2 = axes[0, 1]
        top_features = feature_vars.index[:8]
        corr_matrix = feature_df[top_features].corr()
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, ax=ax2)
        ax2.set_title('Feature Correlation Heatmap (Top 8 Features)')

        ax3 = axes[1, 0]

        # Key features for distribution comparison (updated with potential graph features)
        key_features = ['spam_score_weighted', 'hostname_length', 'entropy', 'digit_ratio',
                        'in_degree', 'out_degree', 'total_out_links_weight']
        available_features = [
            f for f in key_features if f in selected_features]

        if available_features:
            feature_name = available_features[0]
            spam_values = feature_df[feature_df['label'] == 1][feature_name]
            nonspam_values = feature_df[feature_df['label'] == 0][feature_name]

            ax3.hist(nonspam_values, alpha=0.7,
                     label='Non-spam', bins=30, density=True)
            ax3.hist(spam_values, alpha=0.7,
                     label='Spam', bins=30, density=True)
            ax3.set_xlabel(feature_name)
            ax3.set_ylabel('Density')
            ax3.set_title(f'Distribution Comparison: {feature_name}')
            ax3.legend()
        else:
            ax3.set_title('No key features available for distribution plot')
            ax3.text(0.5, 0.5, 'No relevant features selected for this plot.',
                     horizontalalignment='center', verticalalignment='center',
                     transform=ax3.transAxes)


        ax4 = axes[1, 1]
        f_scores = self.feature_selector.scores_[
            self.feature_selector.get_support()]
        feature_scores = pd.Series(
            f_scores, index=selected_features).sort_values(ascending=False)[:10]
        feature_scores.plot(kind='bar', ax=ax4)
        ax4.set_title('Top 10 Feature Selection Scores (F-statistic)')
        ax4.set_xlabel('Features')
        ax4.set_ylabel('F-score')
        ax4.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        output_file = OUTPUT_DIR / 'feature_analysis.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Feature analysis saved to {output_file}")

    def _plot_class_distribution(self, y_train: np.ndarray, y_test: np.ndarray):
        """Plot class distribution comparison"""
        logger.info("Plotting class distribution comparison...")

        fig, ax = plt.subplots(figsize=(8, 6))

        train_dist = Counter(y_train)
        test_dist = Counter(y_test)

        labels = ['Non-spam', 'Spam']
        train_counts = [train_dist[0], train_dist[1]]
        test_counts = [test_dist[0], test_dist[1]]

        x = np.arange(len(labels))
        width = 0.35

        ax.bar(x - width/2, train_counts, width, label='Training Set')
        ax.bar(x + width/2, test_counts, width, label='Test Set')

        ax.set_xlabel('Class')
        ax.set_ylabel('Count')
        ax.set_title('Class Distribution Comparison')
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()
        ax.grid(True, alpha=0.3)

        output_file = OUTPUT_DIR / 'class_distribution.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Class distribution comparison saved to {output_file}")

    def _save_model_and_results(self, results: Dict, selected_features: List[str]):
        """Save trained models and results"""
        logger.info("Saving models and results...")

        models_dir = OUTPUT_DIR / 'saved_models'
        models_dir.mkdir(exist_ok=True)

        with open(models_dir / 'ensemble_classifier.pkl', 'wb') as f:
            pickle.dump(self.ensemble_classifier, f)

        with open(models_dir / 'scaler.pkl', 'wb') as f:
            pickle.dump(self.scaler, f)

        with open(models_dir / 'feature_selector.pkl', 'wb') as f:
            pickle.dump(self.feature_selector, f)

        with open(models_dir / 'label_encoder.pkl', 'wb') as f:
            pickle.dump(self.label_encoder, f)

        if self.feature_extractor:
            with open(models_dir / 'feature_extractor.pkl', 'wb') as f:
                pickle.dump(self.feature_extractor, f)

        metadata = {
            'selected_features': selected_features,
            'feature_names': self.feature_names,
            'tld_categories_fitted': self.tld_categories_fitted,
            'tld_dummy_columns': self.tld_dummy_columns
        }

        with open(models_dir / 'metadata.pkl', 'wb') as f:
            pickle.dump(metadata, f)

        results_summary = {}
        for name, metrics in results.items():
            results_summary[name] = {
                'accuracy': metrics['accuracy'],
                'precision': metrics['precision'],
                'recall': metrics['recall'],
                'f1': metrics['f1'],
                'auc': metrics.get('auc', None)
            }

        with open(models_dir / 'results_summary.pkl', 'wb') as f:
            pickle.dump(results_summary, f)

        with open(OUTPUT_DIR / 'evaluation_results.txt', 'w') as f:
            f.write("Graph-Enhanced Web Spam Classifier - Evaluation Results\n")
            f.write("=" * 60 + "\n\n")

            f.write("Selected Features:\n")
            for i, feature in enumerate(selected_features, 1):
                f.write(f"{i:2d}. {feature}\n")

            f.write(f"\nTotal Features Used: {len(selected_features)}\n\n")

            f.write("Classification Results:\n")
            f.write(
                f"{'Classifier':<20} {'Accuracy':<10} {'Precision':<10} {'Recall':<10} {'F1-Score':<10} {'AUC':<10}\n")
            f.write("-" * 70 + "\n")

            for name, metrics in results.items():
                auc_str = f"{metrics.get('auc', 0):.4f}" if 'auc' in metrics else "N/A"
                f.write(f"{name:<20} {metrics['accuracy']:<10.4f} {metrics['precision']:<10.4f} "
                        f"{metrics['recall']:<10.4f} {metrics['f1']:<10.4f} {auc_str:<10}\n")

        logger.info("Models and results saved successfully!")

    def predict_hostname(self, hostname: str) -> Dict:
        """Predict if a single hostname is spam or not"""
        if self.feature_extractor is None:
            logger.error("Feature extractor not initialized. Train the model first.")
            return {'error': 'Model not trained'}

        host_id = self.feature_extractor.hostname_to_host_id.get(hostname)
        features = self.feature_extractor.extract_comprehensive_features(
            hostname, host_id)

        features_df = pd.DataFrame([features])

        if self.tld_categories_fitted is not None:
            features_df['tld_category'] = features_df['tld'].apply(
                lambda x: x if x in self.tld_categories_fitted else 'other'
            )
        else:
            features_df['tld_category'] = 'other'

        tld_dummies = pd.get_dummies(features_df['tld_category'], prefix='tld')

        for col in self.tld_dummy_columns:
            if col not in tld_dummies.columns:
                tld_dummies[col] = 0
        tld_dummies = tld_dummies[self.tld_dummy_columns]

        features_df = pd.concat([features_df, tld_dummies], axis=1)

        X = features_df[self.feature_names].fillna(0)

        X_scaled = self.scaler.transform(X)
        X_selected = self.feature_selector.transform(X_scaled)

        prediction = self.ensemble_classifier.predict(X_selected)[0]
        probability = self.ensemble_classifier.predict_proba(X_selected)[0]

        label = self.label_encoder.inverse_transform([prediction])[0]

        return {
            'hostname': hostname,
            'prediction': label,
            'spam_probability': probability[1],
            'nonspam_probability': probability[0],
            'confidence': f"{max(probability) * 100:.1f}%"
        }


def main():
    """Main execution function"""
    logger.info("Starting Graph-Enhanced Web Spam Classification System")
    setup_output_directory()

    loader = GraphWebSpamDatasetLoader(data_dir="data")
    classifier = GraphWebSpamClassifier(random_state=42)

    try:
        logger.info("Loading host graph data...")
        loader.load_host_graph_data()

        logger.info("Loading Set1 (training data)...")
        set1_labels = loader.load_labels("set1")

        logger.info("Loading Set2 (test data)...")
        set2_labels = loader.load_labels("set2")

        if set1_labels.empty or set2_labels.empty:
            logger.error("Failed to load dataset labels")
            return

        logger.info("Starting cross-dataset evaluation...")
        results = classifier.train_and_evaluate_cross_dataset(
            set1_labels, set2_labels, loader)

        logger.info("\nDemonstrating single hostname predictions:")
        test_hostnames = [
            'google.com',
            'casino-poker-gambling.tk',
            'free-viagra-pills123.info',
            'legitimate-business.co.uk',
            'xyz123abc456.ml',
            'youtube.com',
            'best-online-casino.ru',
            'health-supplements-usa.biz'
        ]

        for hostname in test_hostnames:
            try:
                prediction = classifier.predict_hostname(hostname)
                logger.info(f"Hostname: {prediction['hostname']}")
                logger.info(
                    f"Prediction: {prediction['prediction']} (Confidence: {prediction['confidence']})")
                logger.info(
                    f"Spam Probability: {prediction['spam_probability']:.4f}")
                logger.info("-" * 40)
            except Exception as e:
                logger.error(f"Error predicting {hostname}: {e}")

        logger.info("Graph-Enhanced Web Spam Classification completed successfully!")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise


if __name__ == "__main__":
    main()
