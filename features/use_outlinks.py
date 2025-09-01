import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import re
from typing import Dict, List, Set, cast
from collections import Counter
import psycopg2
from utils.connect_to_db import get_connection, return_connection

def get_page_links(url: str, timeout: int = 10) -> Dict:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)

        if response.status_code != 200:
            return {'accessible': False, 'links': [], 'error': f'Status code: {response.status_code}'}

        soup = BeautifulSoup(response.text, 'html.parser')
        links = []

        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            if href:
                absolute_url = urljoin(url, href)
                links.append({
                    'url': absolute_url,
                    'text': link.get_text().strip(),
                    'title': link.get('title', ''),
                    'rel': link.get('rel', [])
                })

        return {
            'accessible': True,
            'links': links,
            'total_links': len(links)
        }

    except Exception as e:
        return {
            'accessible': False,
            'links': [],
            'total_links': 0,
            'error': str(e)
        }

def get_domain_reputations(domains: Set[str]) -> Dict[str, float]:
    if not domains:
        return {}

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            query = "SELECT domain, reputation_score FROM domain_reputation WHERE domain IN %s"
            cur.execute(query, (tuple(domains),))
            results = cur.fetchall()
            return cast(Dict[str, float], dict(results))
    except (Exception, psycopg2.Error) as error:
        print(f"Error while fetching domain reputations: {error}")
        return {}
    finally:
        if conn:
            return_connection(conn)

def classify_link_quality(link: Dict, source_domain: str, domain_reputations: Dict[str, float]) -> str:
    url = link['url']
    text = link['text'].lower()
    title = link['title'].lower()

    try:
        parsed = urlparse(url)
        link_domain = parsed.netloc.lower()
    except:
        return 'unknown'

    if link_domain in domain_reputations:
        reputation = domain_reputations[link_domain]
        if reputation >= 0.3:
            return 'quality'
        if reputation < -0.5:
            return 'spam'
        if reputation < -0.1:
            return 'suspicious'

    news_domains = {
        'nytimes.com', 'washingtonpost.com', 'wsj.com', 'theguardian.com',
        'cnn.com', 'bbc.co.uk', 'reuters.com', 'bloomberg.com', 'economist.com'
    }

    spam_domains = {
        'bit.ly', 'tinyurl.com', 'goo.gl', 't.co', 'ow.ly', 'short.link'
    }

    spam_keywords = [
        'click here', 'free money', 'make money', 'work from home', 'guaranteed',
        'miracle', 'amazing', 'incredible', 'unbelievable', 'shocking', 'viral',
        'you won', 'claim now', 'limited time', 'act fast', 'download now'
    ]

    if any(domain in link_domain for domain in spam_domains):
        return 'spam'

    if any(keyword in text for keyword in spam_keywords):
        return 'spam'

    if any(keyword in title for keyword in spam_keywords):
        return 'spam'

    if any(domain in link_domain for domain in news_domains):
        return 'news'

    if link_domain == source_domain:
        return 'internal'

    if re.search(r'\.(edu|gov|org)$', link_domain):
        return 'quality'

    if re.search(r'\.(tk|ml|ga|cf|gq)$', link_domain):
        return 'suspicious'

    if len(text) < 3 or text in ['click', 'here', 'link', 'more', 'read']:
        return 'low_quality'

    if re.search(r'[^\w\s\-\.]', link_domain):
        return 'suspicious'

    return 'neutral'

def analyze_link_patterns(links: List[Dict], source_domain: str, domain_reputations: Dict[str, float]) -> Dict:
    if not links:
        return {
            'total_outlinks': 0,
            'internal_links': 0,
            'external_links': 0,
            'quality_links': 0,
            'spam_links': 0,
            'suspicious_links': 0,
            'unique_domains': 0
        }

    classifications = []
    domains = set()
    internal_count = 0
    external_count = 0

    for link in links:
        try:
            link_domain = urlparse(link['url']).netloc.lower()
            domains.add(link_domain)

            if link_domain == source_domain:
                internal_count += 1
            else:
                external_count += 1

            classification = classify_link_quality(link, source_domain, domain_reputations)
            classifications.append(classification)

        except:
            classifications.append('unknown')

    classification_counts = Counter(classifications)

    return {
        'total_outlinks': len(links),
        'internal_links': internal_count,
        'external_links': external_count,
        'quality_links': classification_counts.get('quality', 0) + classification_counts.get('news', 0),
        'spam_links': classification_counts.get('spam', 0),
        'suspicious_links': classification_counts.get('suspicious', 0) + classification_counts.get('low_quality', 0),
        'neutral_links': classification_counts.get('neutral', 0),
        'unique_domains': len(domains),
        'classification_breakdown': dict(classification_counts)
    }

def calculate_outlink_scores(link_stats: Dict) -> Dict:
    total_links = link_stats['total_outlinks']

    if total_links == 0:
        return {
            'quality_ratio': 0.0,
            'spam_ratio': 0.0,
            'diversity_score': 0.0,
            'trust_score': 0.0
        }

    quality_ratio = link_stats['quality_links'] / total_links
    spam_ratio = link_stats['spam_links'] / total_links
    suspicious_ratio = link_stats['suspicious_links'] / total_links

    diversity_score = min(link_stats['unique_domains'] / max(total_links, 1), 1.0)

    external_ratio = link_stats['external_links'] / total_links if total_links > 0 else 0

    trust_score = (quality_ratio * 0.4 +
                   diversity_score * 0.2 +
                   external_ratio * 0.2 -
                   spam_ratio * 0.5 -
                   suspicious_ratio * 0.3)

    trust_score = max(0.0, min(1.0, trust_score))

    return {
        'quality_ratio': quality_ratio,
        'spam_ratio': spam_ratio + suspicious_ratio,
        'diversity_score': diversity_score,
        'trust_score': trust_score,
        'external_ratio': external_ratio
    }

def analyze_outlinks(url: str) -> Dict:
    start_time = time.time()

    try:
        source_domain = urlparse(url).netloc.lower()
    except:
        source_domain = ''

    page_data = get_page_links(url)

    if not page_data['accessible']:
        return {
            'total_outlinks': 0,
            'quality_ratio': 0.0,
            'spam_ratio': 0.0,
            'diversity_score': 0.0,
            'trust_score': 0.0,
            'accessible': False,
            'processing_time': time.time() - start_time,
            'error': page_data.get('error', 'Page not accessible')
        }

    links = page_data['links']

    external_domains = set()
    for link in links:
        try:
            link_domain = urlparse(link['url']).netloc.lower()
            if link_domain and link_domain != source_domain:
                external_domains.add(link_domain)
        except:
            continue

    domain_reputations = get_domain_reputations(external_domains)
    link_stats = analyze_link_patterns(links, source_domain, domain_reputations)
    scores = calculate_outlink_scores(link_stats)

    result = {
        'total_outlinks': link_stats['total_outlinks'],
        'internal_links': link_stats['internal_links'],
        'external_links': link_stats['external_links'],
        'unique_domains': link_stats['unique_domains'],
        'quality_links': link_stats['quality_links'],
        'spam_links': link_stats['spam_links'],
        'suspicious_links': link_stats['suspicious_links'],
        'quality_ratio': scores['quality_ratio'],
        'spam_ratio': scores['spam_ratio'],
        'diversity_score': scores['diversity_score'],
        'trust_score': scores['trust_score'],
        'external_ratio': scores['external_ratio'],
        'accessible': True,
        'processing_time': time.time() - start_time
    }

    return result
