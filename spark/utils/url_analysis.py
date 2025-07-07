import hashlib
import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from collections import Counter
from datetime import datetime, timezone

@dataclass
class URLAnalysisResult:
    url: str
    timestamp: str
    domain: str
    priority_score: float
    reasons: Dict
    domain_frequency: int
    domain_frequency_pct: float
    is_active: bool
    received_at: str

class OptimizedURLPrioritizer:
    def __init__(self):
        self.compiled_patterns = self._compile_patterns()
        self._init_domain_sets()
        self._init_keyword_lookups()
    def _compile_patterns(self):
        return {
            'quality_patterns': re.compile(r'/(?:article|post|blog|news|research|documentation|guide|tutorial|paper)/'),
            'year_pattern': re.compile(r'\b(20[0-2][0-9])\b'),
            'age_restriction': re.compile(r'\b(?:18\+|21\+|adults?[\-_]?only|nsfw)\b'),
            'adult_site_pattern': re.compile(r'(?:tube|cam|live|chat)[\d]*\.(?:com|org|net)'),
            'suspicious_url_pattern': re.compile(r'(?:\.php\?.*=.*&.*=.*&.*=|[?&]id=\d{8,}|[?&]ref=\w{20,}|[?&]utm_[^&]{30,}|[?&]token=\w{30,})'),
            'long_numbers': re.compile(r'[0-9]{12,}'),
            'low_value_extensions': re.compile(r'\.(?:jpg|jpeg|png|gif|bmp|webp|svg|mp3|mp4|avi|mov|wmv|flv|zip|rar|7z|tar|gz|exe|msi|dmg|deb|rpm)$', re.IGNORECASE)
        }

    def _init_domain_sets(self):
        self.adult_domains = frozenset({
            'xnxx.com', 'pornhub.com', 'xvideos.com', 'redtube.com', 'youporn.com',
            'tube8.com', 'spankbang.com', 'xhamster.com', 'brazzers.com', 'realitykings.com',
            'sex.com', 'porn.com', 'xxx.com', 'adult.com', 'cam4.com', 'chaturbate.com',
            'onlyfans.com', 'pornstar.com', 'nuvid.com', 'motherless.com', 'slutload.com'
        })

        self.suspicious_tlds = frozenset({
            '.tk', '.ml', '.ga', '.cf', '.buzz', '.click', '.download', '.loan',
            '.win', '.bid', '.trade', '.date', '.racing', '.review', '.cricket',
            '.xxx', '.top', '.party', '.live', '.work', '.stream', '.faith',
            '.accountant', '.science', '.men', '.gdn', '.kim'
        })

        self.quality_domains = frozenset({
            'wikipedia.org', 'github.com', 'stackoverflow.com', 'medium.com',
            'reddit.com', 'youtube.com', 'vimeo.com', 'archive.org', 'arxiv.org',
            'nature.com', 'science.org', 'ieee.org', 'acm.org', 'springer.com',
            'cambridge.org', 'mit.edu', 'stanford.edu', 'harvard.edu', 'ox.ac.uk'
        })

        self.news_domains = frozenset({
            'bbc.com', 'cnn.com', 'reuters.com', 'ap.org', 'npr.org', 'pbs.org',
            'theguardian.com', 'nytimes.com', 'washingtonpost.com', 'wsj.com',
            'bloomberg.com', 'economist.com', 'time.com', 'newsweek.com'
        })

    def _init_keyword_lookups(self):
        adult_keywords = [
            'porn', 'sex', 'xxx', 'adult', 'nude', 'naked', 'boobs', 'ass', 'pussy',
            'cock', 'dick', 'fuck', 'milf', 'teen', 'amateur', 'webcam', 'cam',
            'escort', 'hookup', 'dating', 'singles', 'erotic', 'sexy', 'hot',
            'fetish', 'bdsm', 'orgasm', 'masturbat', 'cumshot', 'blowjob'
        ]

        spam_keywords = [
            'casino', 'poker', 'betting', 'viagra', 'cialis', 'pharmacy', 'pills',
            'weight-loss', 'make-money', 'work-from-home', 'get-rich', 'free-money',
            'click-here', 'limited-time', 'act-now', 'special-offer', 'lottery',
            'winner', 'congratulations', 'urgent', 'claim', 'prize', 'bonus'
        ]

        self.adult_keywords_regex = re.compile('|'.join(re.escape(kw) for kw in adult_keywords))
        self.spam_keywords_regex = re.compile('|'.join(re.escape(kw) for kw in spam_keywords))

    def extract_domain(self, url: str) -> Optional[str]:
        try:
            if '://' in url:
                domain = url.split('://', 1)[1]
            else:
                domain = url
            domain = domain.split('/', 1)[0].split('?', 1)[0].lower()
            return domain[4:] if domain.startswith('www.') else domain
        except:
            return None

    def get_url_fingerprint(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()[:16]

    def get_tld(self, domain: str) -> Optional[str]:
        if not domain:
            return None
        last_dot = domain.rfind('.')
        return domain[last_dot:] if last_dot >= 0 else None

    def calculate_priority_score_fast(self, url: str, timestamp: str, domain: str = None) -> Tuple[float, Dict]:
        if not domain:
            domain = self.extract_domain(url)
        if not domain:
            return 100.0, {"error": "invalid_url"}

        total_score = 0.0
        all_reasons = {}
        url_lower = url.lower()

        # Quality scoring
        quality_score, quality_reasons = 0, []
        if domain in self.quality_domains:
            quality_score -= 5
            quality_reasons.append("high_quality_domain")
        if domain in self.news_domains:
            quality_score -= 3
            quality_reasons.append("news_domain")
        if self.compiled_patterns['quality_patterns'].search(url_lower):
            quality_score -= 2
            quality_reasons.append("quality_path_pattern")
        if self.compiled_patterns['year_pattern'].search(url):
            quality_score -= 1
            quality_reasons.append("dated_content")

        total_score += quality_score
        if quality_reasons:
            all_reasons['quality'] = quality_reasons

        # Adult content scoring
        adult_score, adult_reasons = 0, []
        if domain in self.adult_domains:
            adult_score += 15
            adult_reasons.append("confirmed_adult_domain")

        adult_matches = len(self.adult_keywords_regex.findall(url_lower))
        if adult_matches > 0:
            adult_score += min(adult_matches * 3, 20)
            adult_reasons.append(f"adult_keywords({adult_matches})")

        if self.compiled_patterns['age_restriction'].search(url_lower):
            adult_score += 8
            adult_reasons.append("explicit_age_restriction")
        if self.compiled_patterns['adult_site_pattern'].search(url_lower):
            adult_score += 5
            adult_reasons.append("adult_site_pattern")

        total_score += adult_score * 1.5
        if adult_reasons:
            all_reasons['adult'] = adult_reasons

        # Spam scoring
        spam_score, spam_reasons = 0, []
        tld = self.get_tld(domain)
        if tld in self.suspicious_tlds:
            spam_score += 5
            spam_reasons.append(f"suspicious_tld({tld})")

        spam_matches = len(self.spam_keywords_regex.findall(url_lower))
        if spam_matches > 0:
            spam_score += min(spam_matches * 4, 15)
            spam_reasons.append(f"spam_keywords({spam_matches})")

        url_len = len(url)
        if url_len > 300:
            spam_score += 4
            spam_reasons.append("extremely_long_url")
        elif url_len > 200:
            spam_score += 2
            spam_reasons.append("very_long_url")

        if '?' in url:
            params = url.count('&') + 1
            if params > 20:
                spam_score += 4
                spam_reasons.append(f"excessive_params({params})")
            elif params > 10:
                spam_score += 2
                spam_reasons.append(f"many_params({params})")

        if self.compiled_patterns['long_numbers'].search(url):
            spam_score += 3
            spam_reasons.append("very_long_numbers")
        if url.count('-') > 8 or url.count('_') > 8:
            spam_score += 2
            spam_reasons.append("excessive_separators")

        if self.compiled_patterns['suspicious_url_pattern'].search(url):
            spam_score += 2
            spam_reasons.append("suspicious_url_patterns")

        total_score += spam_score
        if spam_reasons:
            all_reasons['spam'] = spam_reasons

        # Structure scoring
        structure_score, structure_reasons = 0, []
        if url.startswith('https://'):
            structure_score -= 2
            structure_reasons.append("https_secure")
        if self.compiled_patterns['low_value_extensions'].search(url):
            structure_score += 3
            structure_reasons.append("media_file")

        total_score += structure_score
        if structure_reasons:
            all_reasons['structure'] = structure_reasons

        return max(0, total_score), all_reasons

def process_batch_worker_optimized(rows_list):
    prioritizer = OptimizedURLPrioritizer()
    results = []

    # Convert to list if it's an iterator
    if hasattr(rows_list, '__iter__') and not isinstance(rows_list, (list, tuple)):
        rows_list = list(rows_list)

    # Count domain frequencies in this batch
    domain_counter = Counter()
    for row in rows_list:
        # Handle different row types - fix the access pattern
        if hasattr(row, 'url') and row.url:
            url = row.url
        elif isinstance(row, dict):
            url = row.get('url', '')
        elif isinstance(row, (list, tuple)) and len(row) > 1:
            url = row[1]  # URL is second column in TSV
        else:
            continue
            
        if url:
            domain = prioritizer.extract_domain(url)
            if domain:
                domain_counter[domain] += 1

    url_fingerprints = set()

    for row in rows_list:
        try:
            # Handle different row types - CORRECTED ACCESS PATTERN
            if hasattr(row, 'url'):
                url = row.url
                timestamp = getattr(row, 'timestamp', '')
            elif isinstance(row, dict):
                url = row.get('url', '')
                timestamp = row.get('timestamp', '')
            elif isinstance(row, (list, tuple)) and len(row) > 1:
                # TSV format: timestamp, url
                timestamp = row[0]  # First column is timestamp
                url = row[1]       # Second column is URL
            else:
                continue

            if not url:
                continue

            domain = prioritizer.extract_domain(url)
            if not domain:
                continue

            url_fingerprint = prioritizer.get_url_fingerprint(url)
            is_duplicate = url_fingerprint in url_fingerprints
            url_fingerprints.add(url_fingerprint)

            timestamp_str = str(timestamp) if timestamp is not None else ""
            score, reasons = prioritizer.calculate_priority_score_fast(url, timestamp_str, domain)

            if is_duplicate:
                score += 5
                if 'duplicate' not in reasons:
                    reasons['duplicate'] = []
                reasons['duplicate'].append("duplicate_content")

            total_urls_in_batch = len(rows_list)
            domain_freq_in_batch = domain_counter[domain]
            domain_frequency_pct = (domain_freq_in_batch / total_urls_in_batch * 100) if total_urls_in_batch > 0 else 0.0

            current_utc_time = datetime.now(timezone.utc).isoformat()

            result = URLAnalysisResult(
                url=url,
                timestamp=timestamp_str,
                domain=domain,
                priority_score=score,
                reasons=reasons,
                domain_frequency=domain_freq_in_batch,
                domain_frequency_pct=domain_frequency_pct,
                is_active=True,
                received_at=current_utc_time
            )

            # Apply frequency penalties
            freq_score = 0
            freq_reasons = []
            freq_pct = result.domain_frequency_pct

            freq_thresholds = [(15, 12, "extremely_high"), (8, 8, "very_high"),
                             (4, 5, "high"), (2, 3, "medium"), (1, 1, "elevated")]
            for threshold, score_add, level in freq_thresholds:
                if freq_pct > threshold:
                    freq_score += score_add
                    freq_reasons.append(f"{level}_frequency({freq_pct:.1f}%)")
                    break

            count_thresholds = [(50000, 5, "massive"), (10000, 3, "very_high")]
            for threshold, score_add, level in count_thresholds:
                if result.domain_frequency > threshold:
                    freq_score += score_add
                    freq_reasons.append(f"{level}_absolute_count({result.domain_frequency})")
                    break

            result.priority_score += freq_score
            if freq_reasons:
                result.reasons['frequency_penalty'] = freq_reasons

            results.append(result)

        except Exception as e:
            print(f"Error processing row: {e}")
            continue

    return results