import requests
import re
from bs4 import BeautifulSoup
import time
from typing import Dict

def get_page_content(url: str, timeout: int = 10) -> Dict:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        
        return {
            'content': response.text,
            'status_code': response.status_code,
            'content_type': response.headers.get('content-type', ''),
            'content_length': len(response.text),
            'accessible': True
        }
        
    except Exception as e:
        return {
            'content': '',
            'status_code': 0,
            'content_type': '',
            'content_length': 0,
            'accessible': False,
            'error': str(e)
        }

def analyze_content_quality(soup: BeautifulSoup, content: str) -> float:
    quality_score = 0.0
    
    if soup.title and soup.title.string:
        title_length = len(soup.title.string.strip())
        if 10 <= title_length <= 60:
            quality_score += 0.2
        elif title_length > 60:
            quality_score += 0.1
    
    meta_desc = soup.find('meta', attrs={'name': 'description'})
    if meta_desc and meta_desc.get('content'):
        desc_length = len(meta_desc.get('content', '').strip())
        if 50 <= desc_length <= 160:
            quality_score += 0.2
        elif desc_length > 160:
            quality_score += 0.1
    
    headings = soup.find_all(['h1', 'h2', 'h3'])
    if len(headings) >= 1:
        quality_score += 0.1
    if len(headings) >= 3:
        quality_score += 0.1
    
    paragraphs = soup.find_all('p')
    total_text = ' '.join([p.get_text() for p in paragraphs])
    word_count = len(total_text.split())
    
    if word_count >= 300:
        quality_score += 0.2
    elif word_count >= 100:
        quality_score += 0.1
    
    images = soup.find_all('img')
    images_with_alt = [img for img in images if img.get('alt')]
    if len(images) > 0:
        alt_ratio = len(images_with_alt) / len(images)
        quality_score += alt_ratio * 0.1
    
    links = soup.find_all('a', href=True)
    if len(links) >= 5:
        quality_score += 0.1
    
    return min(quality_score, 1.0)

def detect_trust_signals(soup: BeautifulSoup, url: str) -> float:
    trust_score = 0.0
    
    if soup.find('meta', attrs={'name': 'robots'}):
        trust_score += 0.1
    
    if soup.find('link', attrs={'rel': 'canonical'}):
        trust_score += 0.1
    
    structured_data = soup.find_all('script', type='application/ld+json')
    if structured_data:
        trust_score += 0.2
    
    if soup.find('meta', attrs={'property': re.compile(r'og:')}):
        trust_score += 0.1
    
    if soup.find('meta', attrs={'name': 'twitter:card'}):
        trust_score += 0.1
    
    security_headers = ['https://' in url.lower()]
    if any(security_headers):
        trust_score += 0.2
    
    contact_indicators = soup.find_all(text=re.compile(r'contact|about|privacy|terms', re.I))
    if len(contact_indicators) >= 2:
        trust_score += 0.1
    
    copyright_text = soup.find_all(text=re.compile(r'copyright|©|\(c\)', re.I))
    if copyright_text:
        trust_score += 0.1
    
    return min(trust_score, 1.0)

def detect_spam_indicators(soup: BeautifulSoup, content: str, url: str) -> float:
    spam_score = 0.0
    
    spam_keywords = [
        'click here', 'free money', 'make money fast', 'guaranteed income',
        'work from home', 'lose weight fast', 'miracle cure', 'limited time',
        'act now', 'congratulations you won', 'claim your prize', 'no obligation'
    ]
    
    content_lower = content.lower()
    keyword_matches = sum(1 for keyword in spam_keywords if keyword in content_lower)
    spam_score += min(keyword_matches * 0.1, 0.5)
    
    excessive_caps = len(re.findall(r'[A-Z]{5,}', content))
    spam_score += min(excessive_caps * 0.02, 0.3)
    
    exclamation_count = content.count('!')
    if exclamation_count > 10:
        spam_score += min(exclamation_count * 0.01, 0.2)
    
    popup_scripts = soup.find_all('script', text=re.compile(r'alert|popup|window\.open', re.I))
    if popup_scripts:
        spam_score += 0.2
    
    redirect_meta = soup.find('meta', attrs={'http-equiv': 'refresh'})
    if redirect_meta:
        spam_score += 0.3
    
    hidden_text = soup.find_all(attrs={'style': re.compile(r'display:\s*none|visibility:\s*hidden', re.I)})
    if len(hidden_text) > 5:
        spam_score += 0.2
    
    forms = soup.find_all('form')
    suspicious_forms = 0
    for form in forms:
        inputs = form.find_all('input')
        password_inputs = [inp for inp in inputs if inp.get('type') == 'password']
        if len(password_inputs) > 1:
            suspicious_forms += 1
    
    if suspicious_forms > 0:
        spam_score += suspicious_forms * 0.2
    
    return min(spam_score, 1.0)

def analyze_content(url: str) -> Dict:
    start_time = time.time()
    
    page_data = get_page_content(url)
    
    if not page_data['accessible'] or page_data['status_code'] != 200:
        return {
            'has_content': False,
            'quality_score': 0.0,
            'trust_signals': 0.0,
            'spam_indicators': 1.0,
            'processing_time': time.time() - start_time,
            'error': page_data.get('error', 'Page not accessible')
        }
    
    content = page_data['content']
    
    if not content or page_data['content_length'] < 100:
        return {
            'has_content': False,
            'quality_score': 0.0,
            'trust_signals': 0.0,
            'spam_indicators': 0.5,
            'processing_time': time.time() - start_time
        }
    
    try:
        soup = BeautifulSoup(content, 'html.parser')
        
        quality_score = analyze_content_quality(soup, content)
        trust_signals = detect_trust_signals(soup, url)
        spam_indicators = detect_spam_indicators(soup, content, url)
        
        text_content = soup.get_text()
        word_count = len(text_content.split())
        
        return {
            'has_content': True,
            'quality_score': quality_score,
            'trust_signals': trust_signals,
            'spam_indicators': spam_indicators,
            'word_count': word_count,
            'content_length': page_data['content_length'],
            'title': soup.title.string.strip() if soup.title and soup.title.string else '',
            'has_images': len(soup.find_all('img')) > 0,
            'has_forms': len(soup.find_all('form')) > 0,
            'processing_time': time.time() - start_time
        }
        
    except Exception as e:
        return {
            'has_content': False,
            'quality_score': 0.0,
            'trust_signals': 0.0,
            'spam_indicators': 1.0,
            'processing_time': time.time() - start_time,
            'error': str(e)
        }