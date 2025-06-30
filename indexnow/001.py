#001
import pandas as pd
import re
import urllib.parse
import requests
import time
from collections import Counter, defaultdict
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedURLPrioritizer:
    def __init__(self):
        self.adult_domains = {
            'xnxx.com', 'pornhub.com', 'xvideos.com', 'redtube.com', 'youporn.com',
            'tube8.com', 'spankbang.com', 'xhamster.com', 'brazzers.com', 'realitykings.com',
            'sex.com', 'porn.com', 'xxx.com', 'adult.com', 'cam4.com', 'chaturbate.com'
        }


        self.suspicious_tlds = {
            '.tk', '.ml', '.ga', '.cf', '.buzz', '.click', '.download', '.loan',
            '.win', '.bid', '.trade', '.date', '.racing', '.review', '.cricket',
            '.xxx',
        }


        self.adult_keywords = {
            'porn', 'sex', 'xxx', 'adult', 'nude', 'naked', 'boobs', 'ass', 'pussy',
            'cock', 'dick', 'fuck', 'milf', 'teen', 'amateur', 'webcam', 'cam',
            'escort', 'hookup', 'dating', 'singles', 'erotic', 'sexy', 'hot'
        }


        self.spam_keywords = {
            'casino', 'poker', 'betting', 'viagra', 'cialis', 'pharmacy', 'pills',
            'weight-loss', 'make-money', 'work-from-home', 'get-rich', 'free-money',
            'click-here', 'limited-time', 'act-now', 'special-offer'
        }

        self.last_request_time = defaultdict(float)
        self.min_request_interval = 1.0

        self.domain_stats = defaultdict(lambda: {
            'url_count': 0,
            'total_quality_issues': 0,
            'adult_violations': 0,
            'spam_violations': 0,
            'avg_url_length': 0,
            'long_url_count': 0,
            'suspicious_pattern_count': 0,
            'urls': []
        })

    def extract_domain(self, url):
        try:
            parsed = urllib.parse.urlparse(url)
            domain = parsed.netloc.lower()

            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return None

    def get_tld(self, domain):
        if not domain:
            return None
        parts = domain.split('.')
        if len(parts) >= 2:
            return '.' + parts[-1]
        return None

    def check_adult_content(self, url, domain):
        score = 0
        reasons = []


        if domain in self.adult_domains:
            score += 10
            reasons.append("known_adult_domain")


        url_lower = url.lower()
        adult_matches = sum(1 for keyword in self.adult_keywords if keyword in url_lower)
        if adult_matches > 0:
            score += adult_matches * 2
            reasons.append(f"adult_keywords({adult_matches})")


        if re.search(r'\b(18\+|21\+|adults?[\-_]?only)\b', url_lower):
            score += 5
            reasons.append("age_restriction")

        return score, reasons

    def check_spam_indicators(self, url, domain):
        score = 0
        reasons = []

        tld = self.get_tld(domain)
        if tld in self.suspicious_tlds:
            score += 3
            reasons.append(f"suspicious_tld({tld})")

        url_lower = url.lower()
        spam_matches = sum(1 for keyword in self.spam_keywords if keyword in url_lower)
        if spam_matches > 0:
            score += spam_matches * 2
            reasons.append(f"spam_keywords({spam_matches})")

        if len(url) > 200:
            score += 2
            reasons.append("very_long_url")

        parsed = urllib.parse.urlparse(url)
        if parsed.query:
            params = len(parsed.query.split('&'))
            if params > 10:
                score += 2
                reasons.append(f"many_params({params})")

        if re.search(r'[0-9]{8,}', url):
            score += 1
            reasons.append("long_numbers")

        if url.count('-') > 5 or url.count('_') > 5:
            score += 1
            reasons.append("excessive_separators")

        return score, reasons

    def check_url_quality(self, url, domain):
        score = 0
        reasons = []


        if url.startswith('https://'):
            score -= 1
            reasons.append("https")


        low_priority_extensions = ['.jpg', '.png', '.gif', '.pdf', '.zip', '.mp3', '.mp4']
        for ext in low_priority_extensions:
            if url.lower().endswith(ext):
                score += 1
                reasons.append(f"file_extension({ext})")
                break


        if domain and '.' in domain:
            parts = domain.split('.')
            if len(parts) > 3:
                score += 1
                reasons.append("deep_subdomain")

        return score, reasons

    def update_domain_stats(self, url, domain, individual_score, reasons):
        """Update domain-level statistics"""
        if not domain:
            return

        stats = self.domain_stats[domain]
        stats['url_count'] += 1
        stats['urls'].append(url)


        if individual_score > 0:
            stats['total_quality_issues'] += individual_score


        if 'adult' in reasons:
            stats['adult_violations'] += 1
        if 'spam' in reasons:
            stats['spam_violations'] += 1


        url_length = len(url)
        stats['avg_url_length'] = ((stats['avg_url_length'] * (stats['url_count'] - 1)) + url_length) / stats['url_count']

        if url_length > 200:
            stats['long_url_count'] += 1


        if any(pattern in str(reasons) for pattern in ['long_numbers', 'excessive_separators', 'many_params']):
            stats['suspicious_pattern_count'] += 1

    def calculate_domain_reputation_score(self, domain):
        """Calculate domain reputation score based on aggregate behavior"""
        if domain not in self.domain_stats:
            return 0, []

        stats = self.domain_stats[domain]
        score = 0
        reasons = []


        url_count = stats['url_count']
        if url_count == 0:
            return 0, []


        quality_issue_ratio = stats['total_quality_issues'] / url_count
        if quality_issue_ratio > 2:
            score += 3
            reasons.append(f"high_quality_issue_ratio({quality_issue_ratio:.2f})")


        adult_ratio = stats['adult_violations'] / url_count
        if adult_ratio > 0.1:
            score += 5
            reasons.append(f"high_adult_ratio({adult_ratio:.2f})")


        spam_ratio = stats['spam_violations'] / url_count
        if spam_ratio > 0.2:
            score += 4
            reasons.append(f"high_spam_ratio({spam_ratio:.2f})")


        if stats['avg_url_length'] > 150:
            score += 2
            reasons.append(f"long_avg_url_length({stats['avg_url_length']:.0f})")


        long_url_ratio = stats['long_url_count'] / url_count
        if long_url_ratio > 0.3:
            score += 2
            reasons.append(f"high_long_url_ratio({long_url_ratio:.2f})")


        suspicious_ratio = stats['suspicious_pattern_count'] / url_count
        if suspicious_ratio > 0.4:
            score += 3
            reasons.append(f"high_suspicious_ratio({suspicious_ratio:.2f})")

        return score, reasons

    def calculate_frequency_penalty(self, domain, total_urls, domain_counts):
        """Calculate penalty based on domain frequency"""
        if not domain or domain not in domain_counts:
            return 0, []

        domain_count = domain_counts[domain]
        frequency_percentage = (domain_count / total_urls) * 100

        score = 0
        reasons = []


        if frequency_percentage > 10:
            score += 8
            reasons.append(f"very_high_frequency({frequency_percentage:.1f}%)")
        elif frequency_percentage > 5:
            score += 5
            reasons.append(f"high_frequency({frequency_percentage:.1f}%)")
        elif frequency_percentage > 2:
            score += 2
            reasons.append(f"medium_frequency({frequency_percentage:.1f}%)")
        elif frequency_percentage > 1:
            score += 1
            reasons.append(f"elevated_frequency({frequency_percentage:.1f}%)")


        if domain_count > 10000:
            score += 3
            reasons.append(f"very_high_absolute_count({domain_count})")
        elif domain_count > 1000:
            score += 2
            reasons.append(f"high_absolute_count({domain_count})")

        return score, reasons

    def get_basic_http_info(self, url, domain):
        """Get basic HTTP headers with rate limiting"""

        current_time = time.time()
        if current_time - self.last_request_time[domain] < self.min_request_interval:
            return None, []

        try:
            self.last_request_time[domain] = current_time
            response = requests.head(url, timeout=5, allow_redirects=True)

            info = {
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'server': response.headers.get('server', ''),
                'last_modified': response.headers.get('last-modified', ''),
                'content_length': response.headers.get('content-length', '')
            }

            score = 0
            reasons = []


            if response.status_code == 200:
                score -= 1
                reasons.append("status_200")
            elif response.status_code >= 400:
                score += 5
                reasons.append(f"error_status({response.status_code})")


            content_type = info['content_type'].lower()
            if 'text/html' in content_type:
                score -= 1
                reasons.append("html_content")
            elif any(bad_type in content_type for bad_type in ['application/octet-stream', 'application/x-executable']):
                score += 3
                reasons.append("suspicious_content_type")

            return info, reasons

        except Exception as e:
            logger.debug(f"HTTP check failed for {url}: {e}")
            return None, []

    def calculate_priority_score(self, url, timestamp, domain_counts, total_urls, include_http=False):
        """Calculate overall priority score for a URL with domain reputation and frequency penalties"""
        domain = self.extract_domain(url)
        if not domain:
            return 100, {"error": "invalid_url"}, {}

        total_score = 0
        all_reasons = {}
        http_info = {}



        adult_score, adult_reasons = self.check_adult_content(url, domain)
        if adult_score > 0:
            total_score += adult_score * 2
            all_reasons['adult'] = adult_reasons


        spam_score, spam_reasons = self.check_spam_indicators(url, domain)
        total_score += spam_score
        if spam_reasons:
            all_reasons['spam'] = spam_reasons


        quality_score, quality_reasons = self.check_url_quality(url, domain)
        total_score += quality_score
        if quality_reasons:
            all_reasons['quality'] = quality_reasons


        individual_score = adult_score + spam_score + quality_score
        self.update_domain_stats(url, domain, individual_score, all_reasons)


        domain_rep_score, domain_rep_reasons = self.calculate_domain_reputation_score(domain)
        total_score += domain_rep_score
        if domain_rep_reasons:
            all_reasons['domain_reputation'] = domain_rep_reasons


        freq_score, freq_reasons = self.calculate_frequency_penalty(domain, total_urls, domain_counts)
        total_score += freq_score
        if freq_reasons:
            all_reasons['frequency_penalty'] = freq_reasons


        if include_http and len(self.last_request_time) < 100:
            http_info_data, http_reasons = self.get_basic_http_info(url, domain)
            if http_info_data:
                http_info = http_info_data
                if http_reasons:
                    all_reasons['http'] = http_reasons

        return total_score, all_reasons, http_info

    def analyze_file(self, filepath, sample_size=None, include_http=False):
        """Analyze IndexNow TSV file with two-pass approach for domain reputation"""
        logger.info(f"Loading file: {filepath}")


        df = pd.read_csv(filepath, sep='\t', header=None, names=['timestamp', 'url'])
        logger.info(f"Loaded {len(df)} URLs")


        if sample_size and len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)
            logger.info(f"Sampled {len(df)} URLs for analysis")


        logger.info("First pass: Building domain statistics...")
        domain_counts = Counter()
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Counting domains"):
            domain = self.extract_domain(row['url'])
            if domain:
                domain_counts[domain] += 1

        total_urls = len(df)
        logger.info(f"Found {len(domain_counts)} unique domains")


        logger.info("Second pass: Calculating priority scores...")
        results = []

        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Analyzing URLs"):
            url = row['url']
            timestamp = row['timestamp']

            score, reasons, http_info = self.calculate_priority_score(
                url, timestamp, domain_counts, total_urls, include_http
            )

            domain = self.extract_domain(url)

            results.append({
                'url': url,
                'timestamp': timestamp,
                'domain': domain,
                'priority_score': score,
                'reasons': str(reasons) if reasons else '',
                'http_info': str(http_info) if http_info else '',
                'domain_frequency': domain_counts.get(domain, 0) if domain else 0,
                'domain_frequency_pct': (domain_counts.get(domain, 0) / total_urls * 100) if domain else 0
            })


        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('priority_score')

        return results_df, domain_counts

    def generate_enhanced_summary(self, results_df, domain_counts):
        """Generate enhanced analysis summary with domain reputation insights"""
        total_urls = len(results_df)


        score_stats = results_df['priority_score'].describe()


        top_domains = domain_counts.most_common(20)


        high_quality = len(results_df[results_df['priority_score'] < 5])
        medium_quality = len(results_df[(results_df['priority_score'] >= 5) & (results_df['priority_score'] < 15)])
        low_quality = len(results_df[results_df['priority_score'] >= 15])


        domain_rep_issues = 0
        frequency_penalties = 0
        for _, row in results_df.iterrows():
            reasons_str = row['reasons']
            if 'domain_reputation' in reasons_str:
                domain_rep_issues += 1
            if 'frequency_penalty' in reasons_str:
                frequency_penalties += 1


        high_freq_domains = [(domain, count) for domain, count in domain_counts.items()
                           if (count / total_urls) > 0.01]

        summary = {
            'total_urls': total_urls,
            'unique_domains': len(domain_counts),
            'score_distribution': {
                'mean': score_stats['mean'],
                'median': score_stats['50%'],
                'std': score_stats['std'],
                'min': score_stats['min'],
                'max': score_stats['max']
            },
            'quality_distribution': {
                'high_quality': high_quality,
                'medium_quality': medium_quality,
                'low_quality': low_quality
            },
            'domain_analysis': {
                'urls_with_domain_reputation_issues': domain_rep_issues,
                'urls_with_frequency_penalties': frequency_penalties,
                'high_frequency_domains_count': len(high_freq_domains),
                'high_frequency_domains': high_freq_domains[:10]
            },
            'top_domains': top_domains
        }

        return summary

def main():
    INPUT_FILE = "data/indexnow/indexnow-log-bing-20250618-005959.tsv"
    OUTPUT_FILE = "urls.csv"
    SAMPLE_SIZE = 10000 # 10,000
    INCLUDE_HTTP = False

    prioritizer = EnhancedURLPrioritizer()

    try:
        results_df, domain_counts = prioritizer.analyze_file(
            INPUT_FILE,
            sample_size=SAMPLE_SIZE,
            include_http=INCLUDE_HTTP
        )

        summary = prioritizer.generate_enhanced_summary(results_df, domain_counts)

        results_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Results saved to {OUTPUT_FILE}")

        print("\n" + "="*60)
        print("ENHANCED ANALYSIS SUMMARY")
        print("="*60)
        print(f"Total URLs analyzed: {summary['total_urls']:,}")
        print(f"Unique domains: {summary['unique_domains']:,}")

        print("\nPriority Score Distribution:")
        print(f"  Mean: {summary['score_distribution']['mean']:.2f}")
        print(f"  Median: {summary['score_distribution']['median']:.2f}")
        print(f"  Min: {summary['score_distribution']['min']:.2f}")
        print(f"  Max: {summary['score_distribution']['max']:.2f}")

        print("\nQuality Categories:")
        print(f"  High Quality (score < 5): {summary['quality_distribution']['high_quality']:,} ({summary['quality_distribution']['high_quality']/summary['total_urls']*100:.1f}%)")
        print(f"  Medium Quality (5-15): {summary['quality_distribution']['medium_quality']:,} ({summary['quality_distribution']['medium_quality']/summary['total_urls']*100:.1f}%)")
        print(f"  Low Quality (score >= 15): {summary['quality_distribution']['low_quality']:,} ({summary['quality_distribution']['low_quality']/summary['total_urls']*100:.1f}%)")

        print("\nDomain Reputation Analysis:")
        print(f"  URLs with domain reputation issues: {summary['domain_analysis']['urls_with_domain_reputation_issues']:,}")
        print(f"  URLs with frequency penalties: {summary['domain_analysis']['urls_with_frequency_penalties']:,}")
        print(f"  High-frequency domains (>1% of URLs): {summary['domain_analysis']['high_frequency_domains_count']:,}")

        print("\nTop High-Frequency Domains:")
        for domain, count in summary['domain_analysis']['high_frequency_domains']:
            pct = (count / summary['total_urls']) * 100
            print(f"  {domain}: {count:,} URLs ({pct:.2f}%)")

        print("\nTop 10 Domains by Total Frequency:")
        for domain, count in summary['top_domains'][:10]:
            pct = (count / summary['total_urls']) * 100
            print(f"  {domain}: {count:,} URLs ({pct:.2f}%)")

        print("\nTop 10 Highest Priority URLs (lowest scores):")
        for idx, row in results_df.head(10).iterrows():
            print(f"  Score {row['priority_score']}: {row['url']}")

        print("\nTop 10 Lowest Priority URLs (highest scores):")
        for idx, row in results_df.tail(10).iterrows():
            print(f"  Score {row['priority_score']}: {row['url']}")
            if row['reasons']:
                print(f"    Reasons: {row['reasons']}")

    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    main()
