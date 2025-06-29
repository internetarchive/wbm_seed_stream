import logging
import os
import re
import shutil
from pathlib import Path
from typing import Dict, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, classification_report,
                             confusion_matrix, precision_recall_fscore_support)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.tree import DecisionTreeClassifier, export_text

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

def setup_output_directory():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebSpamDatasetLoader:
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
    
    def load_labels(self, dataset: str = "set1") -> pd.DataFrame:
        logger.info(f"Loading labels from {dataset}...")
        
        if dataset == "set1":
            labels_file = self.data_dir / "webspam-uk2007-set1-1.0" / "WEBSPAM-UK2007-SET1-labels.txt"
            hostnames_file = self.data_dir / "webspam-uk2007-set1-1.0" / "WEBSPAM-UK2007-hostnames.txt"
        else:
            labels_file = self.data_dir / "webspam-uk2007-set2-1.0" / "WEBSPAM-UK2007-SET2-labels.txt"
            hostnames_file = self.data_dir / "webspam-uk2007-set2-1.0" / "WEBSPAM-UK2007-hostnames.txt"
        
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
        logger.info(f"Label distribution: {labels_df['label'].value_counts().to_dict()}")
        
        return labels_df

class EnhancedFeatureExtractor:
    
    def __init__(self):
        self.spam_tlds = {
            '.tk', '.ml', '.ga', '.cf', '.info', '.biz', '.cc', '.su', '.ws', 
            '.click', '.download', '.work', '.party', '.review', '.stream',
            '.science', '.date', '.racing', '.accountant', '.faith', '.cricket',
            '.win', '.bid', '.loan', '.men', '.webcam'
        }
        
        self.spam_words = {
            'cheap', 'free', 'promo', 'discount', 'offer', 'deal', 'save',
            'buy', 'click', 'now', 'money', 'cash', 'win', 'winner', 'prize',
            'adult', 'sex', 'porn', 'casino', 'gambling', 'loan', 'credit',
            'pharmacy', 'viagra', 'pills', 'weight', 'loss', 'diet', 'earn',
            'income', 'work', 'home', 'business', 'opportunity', 'guaranteed',
            'million', 'billion', 'rich', 'wealthy', 'investment', 'profit',
            'forex', 'crypto', 'bitcoin', 'trading', 'bonus', 'reward',
            'congratulations', 'selected', 'chosen', 'lucky', 'instant',
            'urgent', 'limited', 'exclusive', 'special', 'amazing'
        }
        
        self.legitimate_tlds = {
            '.com', '.org', '.net', '.edu', '.gov', '.mil', '.uk', '.de', 
            '.fr', '.jp', '.au', '.ca', '.us', '.it', '.es', '.nl', '.br'
        }
        
        self.suspicious_chars = re.compile(r'[0-9]{4,}|[!@#$%^&*]{2,}')
        self.random_pattern = re.compile(r'[a-z]{2,}[0-9]{3,}[a-z]{2,}')
        
    def extract_hostname_features(self, hostname: str) -> Dict:
        if not hostname:
            return self._get_empty_features()
            
        try:
            hostname = hostname.lower()
            
            features = {
                'hostname_length': len(hostname),
                'subdomain_count': hostname.count('.'),
                'digit_count': len(re.findall(r'\d', hostname)),
                'digit_ratio': len(re.findall(r'\d', hostname)) / len(hostname) if hostname else 0,
                'hyphen_count': hostname.count('-'),
                'underscore_count': hostname.count('_'),
                'has_suspicious_chars': int(bool(self.suspicious_chars.search(hostname))),
                'consecutive_digits': len(max(re.findall(r'\d+', hostname), key=len, default='')),
                'has_www': int(hostname.startswith('www.')),
                'is_ip_like': int(self._looks_like_ip(hostname)),
                'has_port': int(':' in hostname),
            }
            
            tld = '.' + hostname.split('.')[-1] if '.' in hostname else hostname
            features['tld'] = tld
            features['is_suspicious_tld'] = int(tld in self.spam_tlds)
            features['is_legitimate_tld'] = int(tld in self.legitimate_tlds)
            
            features['spam_words_count'] = sum(1 for word in self.spam_words if word in hostname)
            features['hostname_entropy'] = self._calculate_entropy(hostname)
            features['vowel_consonant_ratio'] = self._vowel_consonant_ratio(hostname)
            
            features.update(self._extract_advanced_patterns(hostname))
            
            return features
            
        except Exception as e:
            logger.warning(f"Error extracting features from hostname '{hostname}': {e}")
            return self._get_empty_features()
    
    def _extract_advanced_patterns(self, hostname: str) -> Dict:
        features = {}
        
        parts = hostname.split('.')
        if len(parts) >= 2:
            domain_part = parts[-2]
            
            features['domain_part_length'] = len(domain_part)
            features['avg_subdomain_length'] = np.mean([len(p) for p in parts[:-2]]) if len(parts) > 2 else 0
            
            features['has_random_pattern'] = int(bool(self.random_pattern.search(domain_part)))
            features['consonant_clusters'] = len(re.findall(r'[bcdfghjklmnpqrstvwxyz]{3,}', domain_part))
            features['vowel_clusters'] = len(re.findall(r'[aeiou]{3,}', domain_part))
            
            features['char_repetition'] = max([len(list(g)) for k, g in 
                                             __import__('itertools').groupby(domain_part)], default=0)
            
            features['likely_words'] = self._count_likely_words(domain_part)
            features['word_boundary_score'] = self._calculate_word_boundary_score(domain_part)
        else:
            features.update({
                'domain_part_length': 0, 'avg_subdomain_length': 0,
                'has_random_pattern': 0, 'consonant_clusters': 0, 'vowel_clusters': 0,
                'char_repetition': 0, 'likely_words': 0, 'word_boundary_score': 0
            })
        
        if len(parts) > 2:
            subdomains = parts[:-2]
            features['max_subdomain_length'] = max([len(s) for s in subdomains], default=0)
            features['subdomain_digit_ratio'] = np.mean([
                len(re.findall(r'\d', s)) / len(s) if s else 0 for s in subdomains
            ])
            features['subdomain_spam_words'] = sum(1 for s in subdomains 
                                                 for word in self.spam_words if word in s)
        else:
            features.update({
                'max_subdomain_length': 0, 'subdomain_digit_ratio': 0, 'subdomain_spam_words': 0
            })
        
        return features
    
    def _count_likely_words(self, text: str) -> int:
        common_patterns = [
            r'the', r'and', r'for', r'are', r'but', r'not', r'you', r'all',
            r'can', r'had', r'her', r'was', r'one', r'our', r'out', r'day',
            r'get', r'has', r'him', r'his', r'how', r'its', r'may', r'new',
            r'now', r'old', r'see', r'two', r'way', r'who', r'boy', r'did',
            r'man', r'new', r'old', r'see', r'two', r'way', r'who', r'oil',
            r'sit', r'set', r'run', r'eat', r'far', r'sea', r'eye', r'red',
            r'top', r'arm', r'far', r'off', r'bad', r'big', r'box', r'cut'
        ]
        
        count = 0
        for pattern in common_patterns:
            if re.search(pattern, text):
                count += 1
        
        return count
    
    def _calculate_word_boundary_score(self, text: str) -> float:
        if len(text) < 3:
            return 0.0
        
        bigrams = [text[i:i+2] for i in range(len(text)-1)]
        trigrams = [text[i:i+3] for i in range(len(text)-2)]
        
        common_bigrams = {'th', 'he', 'in', 'er', 'an', 're', 'ed', 'nd', 'ou', 'ea'}
        common_trigrams = {'the', 'and', 'ing', 'her', 'hat', 'his', 'tha', 'ere', 'for', 'ent'}
        
        bigram_score = sum(1 for bg in bigrams if bg in common_bigrams) / len(bigrams) if bigrams else 0
        trigram_score = sum(1 for tg in trigrams if tg in common_trigrams) / len(trigrams) if trigrams else 0
        
        return (bigram_score + trigram_score) / 2
    
    def _get_empty_features(self) -> Dict:
        return {
            'hostname_length': 0, 'subdomain_count': 0, 'digit_count': 0, 'digit_ratio': 0,
            'hyphen_count': 0, 'underscore_count': 0, 'has_suspicious_chars': 0,
            'consecutive_digits': 0, 'tld': 'unknown', 'is_suspicious_tld': 0,
            'is_legitimate_tld': 0, 'spam_words_count': 0, 'hostname_entropy': 0,
            'vowel_consonant_ratio': 0, 'has_www': 0, 'is_ip_like': 0, 'has_port': 0,
            'domain_part_length': 0, 'avg_subdomain_length': 0, 'has_random_pattern': 0,
            'consonant_clusters': 0, 'vowel_clusters': 0, 'char_repetition': 0,
            'likely_words': 0, 'word_boundary_score': 0, 'max_subdomain_length': 0,
            'subdomain_digit_ratio': 0, 'subdomain_spam_words': 0
        }
    
    def _looks_like_ip(self, hostname: str) -> bool:
        ip_pattern = re.compile(r'^\d+\.\d+\.\d+\.\d+')
        return bool(ip_pattern.match(hostname))
    
    def _calculate_entropy(self, text: str) -> float:
        if not text:
            return 0.0
        
        char_counts = {}
        for char in text.lower():
            char_counts[char] = char_counts.get(char, 0) + 1
        
        entropy = 0.0
        text_len = len(text)
        for count in char_counts.values():
            p = count / text_len
            entropy -= p * np.log2(p)
        
        return entropy
    
    def _vowel_consonant_ratio(self, text: str) -> float:
        if not text:
            return 0.0
            
        vowels = sum(1 for c in text.lower() if c in 'aeiou')
        consonants = sum(1 for c in text.lower() if c.isalpha() and c not in 'aeiou')
        
        if consonants == 0:
            return vowels
        return vowels / consonants

class WebSpamDecisionTree:
    
    def __init__(self, max_depth=20, min_samples_split=5, random_state=42):
        self.dt_classifier = DecisionTreeClassifier(
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            random_state=random_state,
            class_weight='balanced'
        )
        self.rf_classifier = RandomForestClassifier(
            n_estimators=200,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            random_state=random_state,
            class_weight='balanced',
            n_jobs=-1
        )
        self.feature_names = []
        self.label_encoder = LabelEncoder()
        self.feature_extractor = EnhancedFeatureExtractor()
        self.tld_categories_fitted = None
        
    def prepare_features(self, labels_df: pd.DataFrame, fit_encoders: bool = True) -> Tuple[pd.DataFrame, pd.Series]:
        logger.info("Extracting features from hostnames...")
        
        features_list = []
        total_hosts = len(labels_df)
        
        for i, (_, row) in enumerate(labels_df.iterrows()):
            if i % 1000 == 0:
                logger.info(f"Processing {i}/{total_hosts} hosts...")
                
            hostname = row.get('hostname', '')
            features = self.feature_extractor.extract_hostname_features(hostname)
            features['host_id'] = row['host_id']
            features_list.append(features)
        
        features_df = pd.DataFrame(features_list)
        
        if fit_encoders:
            tld_counts = features_df['tld'].value_counts()
            frequent_tlds = set(tld_counts[tld_counts >= 5].index)
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
        
        logger.info(f"Prepared {len(X)} samples with {len(feature_cols)} features")
        
        return X, y_encoded
    
    def train_on_set1_test_on_set2(self, train_df, test_df):
        logger.info("Training on Set1, testing on Set2")
        
        logger.info("Preparing training data (Set1)...")
        X_train, y_train = self.prepare_features(train_df, fit_encoders=True)
        
        logger.info("Preparing test data (Set2)...")
        X_test, y_test = self.prepare_features(test_df, fit_encoders=False)
        
        for col in self.feature_names:
            if col not in X_test.columns:
                X_test[col] = 0
        X_test = X_test[self.feature_names]
        
        logger.info(f"Training set: {len(X_train)} samples")
        logger.info(f"Test set: {len(X_test)} samples")
        
        logger.info("Training Decision Tree on Set1...")
        self.dt_classifier.fit(X_train, y_train)
        dt_pred = self.dt_classifier.predict(X_test)
        dt_accuracy = accuracy_score(y_test, dt_pred)
        
        logger.info("Training Random Forest on Set1...")
        self.rf_classifier.fit(X_train, y_train)
        rf_pred = self.rf_classifier.predict(X_test)
        rf_accuracy = accuracy_score(y_test, rf_pred)
        
        logger.info(f"Decision Tree Accuracy on Set2: {dt_accuracy:.4f}")
        logger.info(f"Random Forest Accuracy on Set2: {rf_accuracy:.4f}")
        
        print("\nDECISION TREE RESULTS (Set1 → Set2)")
        print(classification_report(y_test, dt_pred, 
                                  target_names=self.label_encoder.classes_))
        
        print("\nRANDOM FOREST RESULTS (Set1 → Set2)")
        print(classification_report(y_test, rf_pred, 
                                  target_names=self.label_encoder.classes_))
        
        dt_precision, dt_recall, dt_f1, _ = precision_recall_fscore_support(y_test, dt_pred, average=None)
        rf_precision, rf_recall, rf_f1, _ = precision_recall_fscore_support(y_test, rf_pred, average=None)
        
        spam_idx = list(self.label_encoder.classes_).index('spam')
        print("\nSPAM DETECTION PERFORMANCE (Set1 → Set2):")
        print(f"Decision Tree - Spam Precision: {dt_precision[spam_idx]:.3f}, Recall: {dt_recall[spam_idx]:.3f}, F1: {dt_f1[spam_idx]:.3f}")
        print(f"Random Forest - Spam Precision: {rf_precision[spam_idx]:.3f}, Recall: {rf_recall[spam_idx]:.3f}, F1: {rf_f1[spam_idx]:.3f}")
        
        self._plot_confusion_matrices(y_test, dt_pred, rf_pred)
        
        return X_train, X_test, y_train, y_test, dt_pred, rf_pred
    
    def train_with_cross_validation(self, labels_df):
        logger.info("Training with cross-validation on Set1")
        
        X, y = self.prepare_features(labels_df, fit_encoders=True)
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        logger.info(f"Training set: {len(X_train)} samples")
        logger.info(f"Test set: {len(X_test)} samples")
        
        logger.info("Training Decision Tree...")
        self.dt_classifier.fit(X_train, y_train)
        dt_pred = self.dt_classifier.predict(X_test)
        dt_accuracy = accuracy_score(y_test, dt_pred)
        
        logger.info("Training Random Forest...")
        self.rf_classifier.fit(X_train, y_train)
        rf_pred = self.rf_classifier.predict(X_test)
        rf_accuracy = accuracy_score(y_test, rf_pred)
        
        logger.info(f"Decision Tree Accuracy: {dt_accuracy:.4f}")
        logger.info(f"Random Forest Accuracy: {rf_accuracy:.4f}")
        
        print("\nDECISION TREE RESULTS (Cross-validation)")
        print(classification_report(y_test, dt_pred, 
                                  target_names=self.label_encoder.classes_))
        
        print("\nRANDOM FOREST RESULTS (Cross-validation)")
        print(classification_report(y_test, rf_pred, 
                                  target_names=self.label_encoder.classes_))
        
        return X_train, X_test, y_train, y_test, dt_pred, rf_pred
    
    def _plot_confusion_matrices(self, y_test, dt_pred, rf_pred):
        OUTPUT_DIR.mkdir(exist_ok=True)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        dt_cm = confusion_matrix(y_test, dt_pred)
        sns.heatmap(dt_cm, annot=True, fmt='d', cmap='Blues',
                   xticklabels=self.label_encoder.classes_,
                   yticklabels=self.label_encoder.classes_, ax=ax1)
        ax1.set_title('Decision Tree Confusion Matrix')
        ax1.set_ylabel('True Label')
        ax1.set_xlabel('Predicted Label')
        
        rf_cm = confusion_matrix(y_test, rf_pred)
        sns.heatmap(rf_cm, annot=True, fmt='d', cmap='Greens',
                   xticklabels=self.label_encoder.classes_,
                   yticklabels=self.label_encoder.classes_, ax=ax2)
        ax2.set_title('Random Forest Confusion Matrix')
        ax2.set_ylabel('True Label')
        ax2.set_xlabel('Predicted Label')
        
        plt.tight_layout()
        output_file = OUTPUT_DIR / 'confusion_matrices.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Confusion matrices saved to {output_file}")
    
    def analyze_feature_importance(self):
        if not self.feature_names:
            logger.error("No features available. Train the model first.")
            return None, None
            
        dt_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.dt_classifier.feature_importances_
        }).sort_values('importance', ascending=False)
        
        rf_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.rf_classifier.feature_importances_
        }).sort_values('importance', ascending=False)
        
        plt.ioff()
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        top_dt = dt_importance.head(15)
        ax1.barh(range(len(top_dt)), top_dt['importance'])
        ax1.set_yticks(range(len(top_dt)))
        ax1.set_yticklabels(top_dt['feature'])
        ax1.set_title('Decision Tree - Top 15 Feature Importance')
        ax1.set_xlabel('Importance')
        ax1.invert_yaxis()
        
        top_rf = rf_importance.head(15)
        ax2.barh(range(len(top_rf)), top_rf['importance'])
        ax2.set_yticks(range(len(top_rf)))
        ax2.set_yticklabels(top_rf['feature'])
        ax2.set_title('Random Forest - Top 15 Feature Importance')
        ax2.set_xlabel('Importance')
        ax2.invert_yaxis()
        
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / 'feature_importance.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Feature importance plots saved to", OUTPUT_DIR / 'feature_importance.png')
        
        return dt_importance, rf_importance
    
    def get_decision_rules(self) -> str:
        tree_rules = export_text(self.dt_classifier, feature_names=self.feature_names)
        rules_path = OUTPUT_DIR / 'decision_rules.txt'
        with open(rules_path, 'w') as f:
            f.write("WEBSPAM DETECTION DECISION RULES\n")
            f.write("=" * 50 + "\n\n")
            f.write(tree_rules)
        logger.info(f"Decision rules saved to {rules_path}")
        return tree_rules

def main():
    setup_output_directory()
    logger.info("Starting WEBSPAM-UK2007 Enhanced Decision Tree Analysis")
    
    try:
        loader = WebSpamDatasetLoader()
        classifier = WebSpamDecisionTree()
        
        logger.info("Loading datasets...")
        set1_df = loader.load_labels("set1")
        set2_df = loader.load_labels("set2")
        
        logger.info("Starting cross-dataset evaluation...")
        X_train, X_test, y_train, y_test, dt_pred, rf_pred = classifier.train_on_set1_test_on_set2(set1_df, set2_df)
        
        logger.info("Analyzing feature importance...")
        dt_imp, rf_imp = classifier.analyze_feature_importance()
        
        if dt_imp is not None:
            print("\nTOP 10 MOST IMPORTANT FEATURES (Decision Tree)")
            print(dt_imp.head(10).to_string(index=False))
            
            print("\nTOP 10 MOST IMPORTANT FEATURES (Random Forest)")
            print(rf_imp.head(10).to_string(index=False)) #pyright: ignore[reportOptionalMemberAccess]
        logger.info("Extracting decision rules...")
        rules = classifier.get_decision_rules()
        
        with open('decision_rules.txt', 'w') as f:
            f.write("WEBSPAM DETECTION DECISION RULES\n")
            f.write("=" * 50 + "\n\n")
            f.write(rules)
        
        logger.info("Analysis complete! Check the generated files:")
        logger.info("- confusion_matrices.png")
        logger.info("- feature_importance.png") 
        logger.info("- decision_rules.txt")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    main()

"""Notes:

Additional Features for consideration:

Ultra-Low Complexity (Microseconds)

DNS-based features: TLD reputation scores, domain age estimates
URL pattern analysis: Path depth, query parameter count, special characters
Domain reputation: Pre-computed blacklists, known hosting providers
Registrar information: Cheap domain registrars often host more spam

Low Complexity (Milliseconds)

WHOIS data: Registration date, registrar, privacy protection status
DNS record analysis: Missing MX records, suspicious NS records
Subdomain enumeration: Number of active subdomains
Certificate information: SSL cert validity, issuer reputation

Medium Complexity (Seconds)

Network-level features: IP geolocation, ASN reputation, hosting provider
Historical data: Archive.org previous captures, domain history
Social signals: Social media mentions, Wikipedia links (pre-computed indexes)

Next Possible Steps?

Fix the class imbalance - SMOTE, cost-sensitive learning, or ensemble methods
Add DNS/WHOIS features - sorta cheap and highly predictive
Use domain reputation databases - pre-computed scores are fast lookups
Implement threshold tuning - optimize for specific precision/recall requirements
"""