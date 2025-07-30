import requests
import time
from typing import Dict
from urllib.parse import urlparse
import ssl
import socket

def analyze_requests(url: str, timeout: int = 10) -> Dict:
    start_time = time.time()
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(
            url, 
            timeout=timeout, 
            allow_redirects=True,
            verify=True,
            stream=False
        )
        
        response_time = time.time() - start_time
        
        redirect_count = len(response.history)
        final_url = response.url
        
        content_type = response.headers.get('content-type', '').lower()
        content_length = len(response.content)
        
        server = response.headers.get('server', '')
        
        security_headers = {
            'strict-transport-security': response.headers.get('strict-transport-security'),
            'x-frame-options': response.headers.get('x-frame-options'),
            'x-content-type-options': response.headers.get('x-content-type-options'),
            'x-xss-protection': response.headers.get('x-xss-protection'),
            'content-security-policy': response.headers.get('content-security-policy')
        }
        
        security_score = calculate_security_score(security_headers, url)
        
        return {
            'accessible': True,
            'status_code': response.status_code,
            'response_time': response_time,
            'redirect_count': redirect_count,
            'final_url': final_url,
            'content_type': content_type,
            'content_length': content_length,
            'server': server,
            'security_headers': security_headers,
            'security_score': security_score,
            'processing_time': time.time() - start_time
        }
        
    except requests.exceptions.SSLError as e:
        return {
            'accessible': False,
            'status_code': 0,
            'response_time': time.time() - start_time,
            'redirect_count': 0,
            'final_url': url,
            'content_type': '',
            'content_length': 0,
            'server': '',
            'security_score': 0.0,
            'error_type': 'ssl_error',
            'error': str(e),
            'processing_time': time.time() - start_time
        }
    
    except requests.exceptions.ConnectionError as e:
        return {
            'accessible': False,
            'status_code': 0,
            'response_time': time.time() - start_time,
            'redirect_count': 0,
            'final_url': url,
            'content_type': '',
            'content_length': 0,
            'server': '',
            'security_score': 0.0,
            'error_type': 'connection_error',
            'error': str(e),
            'processing_time': time.time() - start_time
        }
    
    except requests.exceptions.Timeout as e:
        return {
            'accessible': False,
            'status_code': 0,
            'response_time': timeout,
            'redirect_count': 0,
            'final_url': url,
            'content_type': '',
            'content_length': 0,
            'server': '',
            'security_score': 0.0,
            'error_type': 'timeout',
            'error': str(e),
            'processing_time': time.time() - start_time
        }
    
    except Exception as e:
        return {
            'accessible': False,
            'status_code': 0,
            'response_time': time.time() - start_time,
            'redirect_count': 0,
            'final_url': url,
            'content_type': '',
            'content_length': 0,
            'server': '',
            'security_score': 0.0,
            'error_type': 'unknown',
            'error': str(e),
            'processing_time': time.time() - start_time
        }

def calculate_security_score(security_headers: Dict, url: str) -> float:
    score = 0.0
    
    if url.startswith('https://'):
        score += 0.3
    
    if security_headers.get('strict-transport-security'):
        score += 0.2
    
    if security_headers.get('x-frame-options'):
        score += 0.1
    
    if security_headers.get('x-content-type-options'):
        score += 0.1
    
    if security_headers.get('x-xss-protection'):
        score += 0.1
    
    if security_headers.get('content-security-policy'):
        score += 0.2
    
    return min(score, 1.0)

def check_domain_reputation(url: str) -> Dict:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        suspicious_tlds = ['.tk', '.ml', '.ga', '.cf', '.gq', '.pw', '.top']
        is_suspicious_tld = any(domain.endswith(tld) for tld in suspicious_tlds)
        
        has_subdomain = len(domain.split('.')) > 2
        
        quality_domains = [
            'wikipedia.org', 'github.com', 'stackoverflow.com', 'mozilla.org',
            'w3.org', 'ieee.org', 'nature.com', 'science.org', 'bbc.com'
        ]
        is_quality_domain = any(quality_domain in domain for quality_domain in quality_domains)
        
        reputation_score = 0.5
        
        if is_quality_domain:
            reputation_score = 1.0
        elif is_suspicious_tld:
            reputation_score = 0.1
        elif has_subdomain and not any(legit in domain for legit in ['www.', 'blog.', 'news.', 'support.']):
            reputation_score = 0.3
        
        return {
            'domain': domain,
            'reputation_score': reputation_score,
            'is_suspicious_tld': is_suspicious_tld,
            'has_subdomain': has_subdomain,
            'is_quality_domain': is_quality_domain
        }
        
    except:
        return {
            'domain': '',
            'reputation_score': 0.0,
            'is_suspicious_tld': True,
            'has_subdomain': False,
            'is_quality_domain': False
        }