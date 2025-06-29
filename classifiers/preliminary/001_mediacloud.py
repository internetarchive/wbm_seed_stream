# For June 17th meeting.

import requests
import json
import re
import urllib.parse
from typing import Dict, List, Tuple
from dataclasses import dataclass
from collections import Counter, defaultdict
import tldextract

@dataclass
class ClassificationResult:
    id: int
    url: str
    domain: str
    canonical_domain: str
    title: str
    spam_score: float
    quality_score: float
    priority_score: float
    flags: List[str]
    metadata: Dict

class URLClassifier:
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url
        self.spam_domains = self._load_spam_domains()
        self.suspicious_patterns = self._compile_suspicious_patterns()
        self.high_quality_domains = self._load_high_quality_domains()
        self.domain_stats = defaultdict(int)
        
    def _load_spam_domains(self) -> set:
        spam_domains = {
            'clickbait.com', 'spam-site.net', 'malware-host.org',
        }
        return spam_domains
    
    def _load_high_quality_domains(self) -> set:
        return {
            'reuters.com', 'ap.org', 'bbc.com', 'nytimes.com', 'washingtonpost.com',
            'theguardian.com', 'npr.org', 'cnn.com', 'bloomberg.com', 'wsj.com',
            'nature.com', 'science.org', 'arxiv.org', 
            'stackoverflow.com', 'medium.com', 'substack.com'
        }
    
    def _compile_suspicious_patterns(self) -> List[re.Pattern]:
        patterns = [
            re.compile(r'\b(casino|poker|lottery|winner)\b', re.I),
            re.compile(r'\b(click here|buy now|limited time|act now)\b', re.I),
            re.compile(r'\b(crypto|bitcoin|investment|trading|profit)\b', re.I),
            re.compile(r'(\$\d+|\d+% off|free money|get rich)', re.I),
        ]
        return patterns
    
    def extract_domain_info(self, url: str) -> Tuple[str, str]:
        try:
            extracted = tldextract.extract(url)
            domain = f"{extracted.domain}.{extracted.suffix}"
            canonical_domain = extracted.domain
            return domain, canonical_domain
        except:
            parsed = urllib.parse.urlparse(url)
            domain = parsed.netloc.lower()
            canonical_domain = domain.split('.')[-2] if '.' in domain else domain
            return domain, canonical_domain
    
    def calculate_spam_score(self, url: str, title: str, domain: str) -> Tuple[float, List[str]]:
        score = 0.0
        flags = []
        if domain in self.spam_domains:
            score += 0.8
            flags.append("known_spam_domain")
        for pattern in self.suspicious_patterns:
            if pattern.search(title):
                score += 0.2
                flags.append("suspicious_title_pattern")
        if len(url) > 200:
            score += 0.1
            flags.append("very_long_url")
        parsed = urllib.parse.urlparse(url)
        if parsed.query and len(parsed.query.split('&')) > 10:
            score += 0.15
            flags.append("excessive_parameters")
        suspicious_tlds = {'.tk', '.ml', '.ga', '.cf', '.click', '.download', '.win'}
        if any(url.endswith(tld) for tld in suspicious_tlds):
            score += 0.3
            flags.append("suspicious_tld")
        if title and sum(1 for c in title if c.isupper()) / len(title) > 0.5:
            score += 0.2
            flags.append("excessive_caps")
        return min(score, 1.0), flags
    
    def calculate_quality_score(self, url: str, title: str, domain: str, canonical_domain: str) -> Tuple[float, List[str]]:
        score = 0.0
        flags = []
        if domain in self.high_quality_domains:
            score += 0.7
            flags.append("high_quality_domain")
        if title:
            if 20 <= len(title) <= 200:
                score += 0.2
                flags.append("good_title_length")
            if title.istitle() or (title[0].isupper() and not title.isupper()):
                score += 0.1
                flags.append("proper_capitalization")
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme == 'https':
            score += 0.1
            flags.append("https")
        if not re.search(r'[0-9]{8,}', parsed.path):
            score += 0.1
            flags.append("clean_url_structure")
        content_indicators = ['article', 'news', 'blog', 'post', 'story', 'report']
        if any(indicator in parsed.path.lower() for indicator in content_indicators):
            score += 0.15
            flags.append("content_indicator_in_path")
        established_domains = {'com', 'org', 'edu', 'gov', 'net'}
        if any(domain.endswith(f'.{tld}') for tld in established_domains):
            score += 0.05
            flags.append("established_tld")
        return min(score, 1.0), flags
    
    def calculate_priority_score(self, spam_score: float, quality_score: float, domain: str, canonical_domain: str) -> Tuple[float, List[str]]:
        flags = []
        base_priority = (1.0 - spam_score) * 0.4 + quality_score * 0.6
        domain_count = self.domain_stats[canonical_domain]
        frequency_penalty = min(domain_count * 0.1, 0.5)
        if frequency_penalty > 0.2:
            flags.append("high_domain_frequency")
        priority = max(0.0, base_priority - frequency_penalty)
        if quality_score > 0.7 and domain_count < 3:
            priority += 0.2
            flags.append("rare_quality_domain")
        return min(priority, 1.0), flags
    
    def classify_single_item(self, item: Dict) -> ClassificationResult:
        url = item['url']
        title = item.get('meta', {}).get('title', '')
        domain, canonical_domain = self.extract_domain_info(url)
        self.domain_stats[canonical_domain] += 1
        spam_score, spam_flags = self.calculate_spam_score(url, title, domain)
        quality_score, quality_flags = self.calculate_quality_score(url, title, domain, canonical_domain)
        priority_score, priority_flags = self.calculate_priority_score(spam_score, quality_score, domain, canonical_domain)
        all_flags = spam_flags + quality_flags + priority_flags
        return ClassificationResult(
            id=item['id'],
            url=url,
            domain=domain,
            canonical_domain=canonical_domain,
            title=title,
            spam_score=spam_score,
            quality_score=quality_score,
            priority_score=priority_score,
            flags=all_flags,
            metadata={
                'source': item.get('source'),
                'received_at': item.get('received_at'),
                'domain_count': self.domain_stats[canonical_domain],
                'original_meta': item.get('meta', {})
            }
        )
    
    def fetch_data(self, use_array_endpoint: bool = True) -> List[Dict]:
        if use_array_endpoint:
            response = requests.get(f"{self.api_base_url}/api/ingest")
            return response.json()
        else:
            items = []
            current_id = 1
            
            while True:
                try:
                    response = requests.get(f"{self.api_base_url}/api/ingest/{current_id}")
                    if response.status_code == 200:
                        items.append(response.json())
                    else:
                        break
                except:
                    break
                
                current_id += 1
                
            return items
    
    def classify_all(self, use_array_endpoint: bool = True) -> List[ClassificationResult]:
        print("Fetching data from API...")
        data = self.fetch_data(use_array_endpoint)
        print(f"Classifying {len(data)} URLs...")
        results = []
        for item in data:
            result = self.classify_single_item(item)
            results.append(result)
        return results
    
    def generate_report(self, results: List[ClassificationResult]) -> Dict:
        total = len(results)
        if total == 0:
            return {}
        spam_count = sum(1 for r in results if r.spam_score > 0.5)
        high_quality = sum(1 for r in results if r.quality_score > 0.7)
        high_priority = sum(1 for r in results if r.priority_score > 0.7)
        domain_counts = Counter(r.canonical_domain for r in results)
        top_domains = domain_counts.most_common(10)
        all_flags = [flag for r in results for flag in r.flags]
        flag_counts = Counter(all_flags)
        report = {
            'total_urls': total,
            'spam_likely': spam_count,
            'spam_percentage': (spam_count / total) * 100,
            'high_quality': high_quality,
            'high_quality_percentage': (high_quality / total) * 100,
            'high_priority': high_priority,
            'high_priority_percentage': (high_priority / total) * 100,
            'unique_domains': len(domain_counts),
            'top_domains': top_domains,
            'common_flags': flag_counts.most_common(10),
            'avg_spam_score': sum(r.spam_score for r in results) / total,
            'avg_quality_score': sum(r.quality_score for r in results) / total,
            'avg_priority_score': sum(r.priority_score for r in results) / total,
        }
        return report
    
    def save_results(self, results: List[ClassificationResult], filename: str = 'classification_results.json'):
        output = []
        for result in results:
            output.append({
                'id': result.id,
                'url': result.url,
                'domain': result.domain,
                'canonical_domain': result.canonical_domain,
                'title': result.title,
                'spam_score': result.spam_score,
                'quality_score': result.quality_score,
                'priority_score': result.priority_score,
                'flags': result.flags,
                'metadata': result.metadata
            })
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Results saved to {filename}")

def main():
    classifier = URLClassifier()
    results = classifier.classify_all(use_array_endpoint=False)
    report = classifier.generate_report(results)
    print("\n=== CLASSIFICATION REPORT ===")
    print(f"Total URLs analyzed: {report['total_urls']}")
    print(f"Likely spam: {report['spam_likely']} ({report['spam_percentage']:.1f}%)")
    print(f"High quality: {report['high_quality']} ({report['high_quality_percentage']:.1f}%)")
    print(f"High priority for archival: {report['high_priority']} ({report['high_priority_percentage']:.1f}%)")
    print(f"Unique domains: {report['unique_domains']}")
    print("\nAverage scores:")
    print(f"  Spam score: {report['avg_spam_score']:.3f}")
    print(f"  Quality score: {report['avg_quality_score']:.3f}")
    print(f"  Priority score: {report['avg_priority_score']:.3f}")
    print("\nTop domains:")
    for domain, count in report['top_domains']:
        print(f"  {domain}: {count} URLs")
    print("\nCommon classification flags:")
    for flag, count in report['common_flags']:
        print(f"  {flag}: {count}")
    classifier.save_results(results)
    with open('classification_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print("\nDetailed results saved to classification_results.json")
    print("Report saved to classification_report.json")

if __name__ == "__main__":
    main()
    
"""
Notes:

- uBlockOrigin
- convert domain names to SURT - this is how the CDX API gives the url in its arguments / params
- gives a tree like structure, left signifies priority
- top level domain comes towards the right-most side.

- you can use this to build a trie-data structure? based on individual segments of the url (instead of characteristics)
- this will be suitable if it fits in memory, helps with 
- key-value data store --> can use redis for in-memory.

- https://github.com/internetarchive/surt
- I heard that arquivo.pt is using Yake, 
a new keyword extraction algorithm, to categorize
its archived web pages. Maybe it can also help
with your web page classifier? https://github.com/LIAAD/yake/

- project format:
- things that are really really spammy ignore, everything else categorize and crawl by creating a .parquet file for archiving jobs.
- this can be provided as a stream for people. 
- on wednesdays, typically expect 30% more data. weekdays is typically more data -- this is for both index now and media cloud.
- AGPL v3 license.
- Put the media cloud URL in the .env

- spits out different parts of the URL, categorizations and classification.
- start with a tech report for publication. 
- if we're querying for headers, we might as well be archiving the page.
- certificate transparency logs for canonical domains.

- archiving malicious pages, and NSFW content as well. safely exclude .xxx tld for example because it's basically only NSFW.
- minimal efforts in archiving malware, but they do get 

- Here is the exclusion list used by many Zeno crawls - https://archive.org/download/global-crawling-exclusions/exclusions.txt
- Last week, I built a TF-IDF based spam classifier to identify spam articles on a blog platform (xlog.app). The biggest problem
  I encountered was that keyword-based detection was difficult to distinguish LLM SPAM from real articles.

- facebook doesn't cooperate (so not archiving 429, 503)
- no respect for robots.txt on .gov and some other domain?
- robots.txt has sort of been used as a wildcard blocker 

https://blog.archive.org/2016/12/17/robots-txt-gov-mil-websites/
https://blog.archive.org/2017/04/17/robots-txt-meant-for-search-engines-dont-work-well-for-web-archives/
Both relevant to how we think about crawling
- mime sniffing.
"""