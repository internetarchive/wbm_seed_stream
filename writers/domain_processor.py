import os
import sys
import json
import tempfile
import glob
from collections import defaultdict
from typing import Dict, Any
import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_logic.rank_domains import DomainRanker
from testing.profiler_integration import get_active_profiler, log_database_operation, log_domain_stats_update

def create_domain_ranker():
    try:
        return DomainRanker()
    except Exception as e:
        print(f"FATAL ERROR: Failed to initialize DomainRanker: {e}")
        raise

def aggregate_domain_stats_vectorized(pdf: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if pdf.empty:
        return {}

    domain_stats = defaultdict(lambda: {
        'reputation_score': 0.0,
        'total_urls': 0,
        'malicious_urls': 0,
        'benign_urls': 0
    })

    for _, row in pdf.iterrows():
        domain = row['domain']
        if domain and pd.notna(domain):
            domain_stats[domain]['reputation_score'] += row['score']
            domain_stats[domain]['total_urls'] += 1
            if row['is_spam']:
                domain_stats[domain]['malicious_urls'] += 1
            else:
                domain_stats[domain]['benign_urls'] += 1

    for domain, stats in domain_stats.items():
        if stats['total_urls'] > 0:
            stats['reputation_score'] = stats['reputation_score'] / stats['total_urls']

    return dict(domain_stats)

def collect_domain_updates_from_temp_files():
    temp_dir = tempfile.gettempdir()
    update_files = glob.glob(os.path.join(temp_dir, "domain_updates_*.json"))

    all_updates = defaultdict(lambda: {
        'reputation_score': 0.0,
        'total_urls': 0,
        'malicious_urls': 0,
        'benign_urls': 0
    })

    for file_path in update_files:
        try:
            with open(file_path, 'r') as f:
                updates = json.load(f)

            for domain, stats in updates.items():
                all_updates[domain]['reputation_score'] += stats['reputation_score'] * stats['total_urls']
                all_updates[domain]['total_urls'] += stats['total_urls']
                all_updates[domain]['malicious_urls'] += stats['malicious_urls']
                all_updates[domain]['benign_urls'] += stats['benign_urls']

            os.remove(file_path)

        except Exception as e:
            print(f"Failed to process update file {file_path}: {e}")

    for domain, stats in all_updates.items():
        if stats['total_urls'] > 0:
            stats['reputation_score'] = stats['reputation_score'] / stats['total_urls']

    return dict(all_updates)

def update_domain_reputations(accumulated_updates: Dict[str, Dict[str, Any]]):
    if not accumulated_updates:
        print("No domain updates to apply")
        return

    profiler = get_active_profiler()
    domain_ranker = create_domain_ranker()

    print(f"Updating {len(accumulated_updates)} domains...")
    domain_ranker.update_domain_reputation_batch(accumulated_updates)
    print("Domain reputation updates completed successfully")

    if profiler:
        log_domain_stats_update(profiler, len(accumulated_updates), "batch_update")
        log_database_operation(profiler, "update_domain_reputations", "domain_reputation", len(accumulated_updates))

def cleanup_temp_files():
    try:
        temp_dir = tempfile.gettempdir()
        update_files = glob.glob(os.path.join(temp_dir, "domain_updates_*.json"))
        for temp_file in update_files:
            try:
                os.remove(temp_file)
            except:
                pass
    except Exception as e:
        print(f"WARNING: Failed to clean up temp files: {e}")
