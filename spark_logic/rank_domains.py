import threading
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from psycopg2.extras import execute_values

from utils.connect_to_db import get_connection, return_connection
from testing.profiler_integration import get_active_profiler, log_database_operation

@dataclass
class DomainRank:
    domain: str
    reputation_score: float
    total_urls_seen: int
    malicious_urls_count: int
    benign_urls_count: int
    last_updated: str
    rank: int = 0
    category: str = "unknown"

class DomainRanker:
    CACHE_TTL_SECONDS = 300

    def __init__(self):
        self._cache_lock = threading.Lock()
        self._reputation_cache: Dict[str, float] = {}
        self._last_cache_update: Optional[float] = None

    def _is_cache_valid(self) -> bool:
        if self._last_cache_update is None:
            return False
        return (time.time() - self._last_cache_update) < self.CACHE_TTL_SECONDS

    def _invalidate_cache(self):
        with self._cache_lock:
            self._last_cache_update = None
            self._reputation_cache.clear()

    def get_domain_reputation(self, domain: str) -> Optional[float]:
        with self._cache_lock:
            if self._is_cache_valid() and domain in self._reputation_cache:
                return self._reputation_cache[domain]

        reputations = self.get_all_domain_reputations()
        return reputations.get(domain)

    def get_all_domain_reputations(self) -> Dict[str, float]:
        with self._cache_lock:
            if self._is_cache_valid():
                return self._reputation_cache.copy()

            profiler = get_active_profiler()
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT domain, reputation_score FROM domain_reputation")
                    results = cur.fetchall()
                    self._reputation_cache = dict(results)
                    self._last_cache_update = time.time()

                    if profiler:
                        log_database_operation(profiler, "load_all_reputations", "domain_reputation", len(results))

                    return self._reputation_cache.copy()
            finally:
                return_connection(conn)

    def get_domain_stats(self, domain: str) -> Optional[DomainRank]:
        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT domain, reputation_score, total_urls_seen,
                           malicious_urls_count, benign_urls_count, updated_at
                    FROM domain_reputation
                    WHERE domain = %s
                """, (domain,))

                result = cur.fetchone()
                if result:
                    return DomainRank(
                        domain=result[0],
                        reputation_score=result[1],
                        total_urls_seen=result[2],
                        malicious_urls_count=result[3],
                        benign_urls_count=result[4],
                        last_updated=result[5].isoformat() if result[5] else "",
                        category=self._categorize_domain(result[1], result[2], result[3], result[4])
                    )
                return None
        finally:
            return_connection(conn)

    def get_top_domains(self, limit: int = 100, min_urls: int = 10) -> List[DomainRank]:
        profiler = get_active_profiler()
        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT domain, reputation_score, total_urls_seen,
                           malicious_urls_count, benign_urls_count, updated_at
                    FROM domain_reputation
                    WHERE total_urls_seen >= %s
                    ORDER BY reputation_score DESC, total_urls_seen DESC
                    LIMIT %s
                """, (min_urls, limit))

                rows = cur.fetchall()
                results = []
                for i, row in enumerate(rows, 1):
                    results.append(DomainRank(
                        domain=row[0],
                        reputation_score=row[1],
                        total_urls_seen=row[2],
                        malicious_urls_count=row[3],
                        benign_urls_count=row[4],
                        last_updated=row[5].isoformat() if row[5] else "",
                        rank=i,
                        category=self._categorize_domain(row[1], row[2], row[3], row[4])
                    ))

                if profiler:
                    log_database_operation(profiler, "get_top_domains", "domain_reputation", len(rows))

                return results
        finally:
            return_connection(conn)

    def get_worst_domains(self, limit: int = 100, min_urls: int = 10) -> List[DomainRank]:
        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT domain, reputation_score, total_urls_seen,
                           malicious_urls_count, benign_urls_count, updated_at
                    FROM domain_reputation
                    WHERE total_urls_seen >= %s
                    ORDER BY reputation_score ASC, malicious_urls_count DESC
                    LIMIT %s
                """, (min_urls, limit))

                results = []
                for i, row in enumerate(cur.fetchall(), 1):
                    results.append(DomainRank(
                        domain=row[0],
                        reputation_score=row[1],
                        total_urls_seen=row[2],
                        malicious_urls_count=row[3],
                        benign_urls_count=row[4],
                        last_updated=row[5].isoformat() if row[5] else "",
                        rank=i,
                        category=self._categorize_domain(row[1], row[2], row[3], row[4])
                    ))
                return results
        finally:
            return_connection(conn)

    def get_domains_by_category(self, category: str, limit: int = 100) -> List[DomainRank]:
        all_domains = self.get_all_domain_stats()
        filtered = [d for d in all_domains if d.category == category]
        return sorted(filtered, key=lambda x: x.reputation_score, reverse=True)[:limit]

    def get_all_domain_stats(self) -> List[DomainRank]:
        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT domain, reputation_score, total_urls_seen,
                           malicious_urls_count, benign_urls_count, updated_at
                    FROM domain_reputation
                    ORDER BY reputation_score DESC
                """)

                results = []
                for i, row in enumerate(cur.fetchall(), 1):
                    results.append(DomainRank(
                        domain=row[0],
                        reputation_score=row[1],
                        total_urls_seen=row[2],
                        malicious_urls_count=row[3],
                        benign_urls_count=row[4],
                        last_updated=row[5].isoformat() if row[5] else "",
                        rank=i,
                        category=self._categorize_domain(row[1], row[2], row[3], row[4])
                    ))
                return results
        finally:
            return_connection(conn)

    def update_domain_reputation_batch(self, domain_aggregates: Dict[str, Dict]):
        if not domain_aggregates:
            return

        profiler = get_active_profiler()
        if profiler:
            profiler.log_process_event("domain_batch_update_start", f"Starting batch update of {len(domain_aggregates)} domains")

        conn = get_connection()
        try:
            update_data = [
                (
                    domain,
                    data.get('reputation_score', 0.0),
                    data.get('total_urls', 0),
                    data.get('malicious_urls', 0),
                    data.get('benign_urls', 0)
                ) for domain, data in domain_aggregates.items()
            ]

            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO domain_reputation (domain, reputation_score, total_urls_seen, malicious_urls_count, benign_urls_count)
                    VALUES %s
                    ON CONFLICT (domain) DO UPDATE SET
                        reputation_score = (domain_reputation.reputation_score * domain_reputation.total_urls_seen + EXCLUDED.reputation_score * EXCLUDED.total_urls_seen) / NULLIF(domain_reputation.total_urls_seen + EXCLUDED.total_urls_seen, 0),
                        total_urls_seen = domain_reputation.total_urls_seen + EXCLUDED.total_urls_seen,
                        malicious_urls_count = domain_reputation.malicious_urls_count + EXCLUDED.malicious_urls_count,
                        benign_urls_count = domain_reputation.benign_urls_count + EXCLUDED.benign_urls_count,
                        updated_at = NOW()
                """, update_data)
            conn.commit()
            self._invalidate_cache()

            if profiler:
                log_database_operation(profiler, "batch_update", "domain_reputation", len(update_data))
                profiler.log_process_event("domain_batch_update_end", f"Completed batch update of {len(domain_aggregates)} domains")

        except Exception as e:
            conn.rollback()
            print(f"Database update failed: {e}")
            if profiler:
                profiler.log_process_event("domain_batch_update_error", f"Batch update failed: {str(e)}")
            raise
        finally:
            return_connection(conn)

    def _categorize_domain(self, reputation_score: float, total_urls: int, malicious_count: int, benign_count: int) -> str:
        if reputation_score >= 0.7:
            return "trusted"
        elif reputation_score >= 0.3:
            return "good"
        elif reputation_score >= -0.1:
            return "neutral"
        elif reputation_score >= -0.5:
            return "suspicious"
        else:
            return "malicious"

    def get_domain_summary_stats(self) -> Dict:
        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) as total_domains,
                        AVG(reputation_score) as avg_reputation,
                        SUM(total_urls_seen) as total_urls_processed,
                        SUM(malicious_urls_count) as total_malicious,
                        SUM(benign_urls_count) as total_benign,
                        COUNT(CASE WHEN reputation_score >= 0.7 THEN 1 END) as trusted_domains,
                        COUNT(CASE WHEN reputation_score >= 0.3 AND reputation_score < 0.7 THEN 1 END) as good_domains,
                        COUNT(CASE WHEN reputation_score >= -0.1 AND reputation_score < 0.3 THEN 1 END) as neutral_domains,
                        COUNT(CASE WHEN reputation_score >= -0.5 AND reputation_score < -0.1 THEN 1 END) as suspicious_domains,
                        COUNT(CASE WHEN reputation_score < -0.5 THEN 1 END) as malicious_domains
                    FROM domain_reputation
                """)

                result = cur.fetchone()
                if result:
                    return {
                        'total_domains': result[0],
                        'avg_reputation': float(result[1]) if result[1] else 0.0,
                        'total_urls_processed': result[2],
                        'total_malicious': result[3],
                        'total_benign': result[4],
                        'trusted_domains': result[5],
                        'good_domains': result[6],
                        'neutral_domains': result[7],
                        'suspicious_domains': result[8],
                        'malicious_domains': result[9]
                    }
                raise ValueError("No data returned from domain_reputation table")
        finally:
            return_connection(conn)

def get_domain_rank(domain: str) -> Optional[DomainRank]:
    ranker = DomainRanker()
    return ranker.get_domain_stats(domain)

def get_top_domains(limit: int = 100, min_urls: int = 10) -> List[DomainRank]:
    ranker = DomainRanker()
    return ranker.get_top_domains(limit, min_urls)

def get_worst_domains(limit: int = 100, min_urls: int = 10) -> List[DomainRank]:
    ranker = DomainRanker()
    return ranker.get_worst_domains(limit, min_urls)

def get_domain_summary() -> Dict:
    ranker = DomainRanker()
    return ranker.get_domain_summary_stats()

def print_domain_rankings(domains: List[DomainRank], title: str = "Domain Rankings"):
    print(f"\n{title}")
    print("=" * 80)
    print(f"{'Rank':<6} {'Domain':<30} {'Score':<8} {'URLs':<6} {'Malicious':<10} {'Category':<12}")
    print("-" * 80)

    for domain in domains:
        print(f"{domain.rank:<6} {domain.domain:<30} {domain.reputation_score:<8.3f} "
              f"{domain.total_urls_seen:<6} {domain.malicious_urls_count:<10} {domain.category:<12}")
