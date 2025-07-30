import requests
import time
from datetime import datetime, timedelta
from typing import Dict
from urllib.parse import urlparse
import json

def query_cdx_api(url: str, timeout: int = 10) -> Dict:
    try:
        cdx_url = f"http://web.archive.org/cdx/search/cdx"
        params = {
            'url': url,
            'output': 'json',
            'limit': 100,
            'fl': 'timestamp,statuscode,original'
        }
        
        response = requests.get(cdx_url, params=params, timeout=timeout)
        
        if response.status_code != 200:
            return {'accessible': False, 'snapshots': [], 'error': f'Status code: {response.status_code}'}
        
        data = response.json()
        
        if not data or len(data) < 2:
            return {'accessible': True, 'snapshots': []}
        
        snapshots = []
        for row in data[1:]:
            if len(row) >= 3:
                snapshots.append({
                    'timestamp': row[0],
                    'status_code': row[1],
                    'url': row[2]
                })
        
        return {
            'accessible': True,
            'snapshots': snapshots
        }
        
    except Exception as e:
        return {
            'accessible': False,
            'snapshots': [],
            'error': str(e)
        }

def parse_wayback_timestamp(timestamp: str) -> datetime:
    try:
        return datetime.strptime(timestamp, '%Y%m%d%H%M%S')
    except:
        return datetime.min

def calculate_archive_metrics(snapshots: list) -> Dict:
    if not snapshots:
        return {
            'last_archived': False,
            'first_archived': False,
            'archive_count': 0,
            'days_since_last_archive': -1,
            'days_since_first_archive': -1,
            'successful_snapshots': 0,
            'failed_snapshots': 0,
            'archive_frequency': 0.0
        }
    
    timestamps = []
    successful_count = 0
    failed_count = 0
    
    for snapshot in snapshots:
        try:
            timestamp = parse_wayback_timestamp(snapshot['timestamp'])
            if timestamp != datetime.min:
                timestamps.append(timestamp)
            
            status = str(snapshot.get('status_code', ''))
            if status.startswith('2'):
                successful_count += 1
            else:
                failed_count += 1
        except:
            failed_count += 1
    
    if not timestamps:
        return {
            'last_archived': False,
            'first_archived': False,
            'archive_count': len(snapshots),
            'days_since_last_archive': -1,
            'days_since_first_archive': -1,
            'successful_snapshots': successful_count,
            'failed_snapshots': failed_count,
            'archive_frequency': 0.0
        }
    
    timestamps.sort()
    first_archive = timestamps[0]
    last_archive = timestamps[-1]
    now = datetime.now()
    
    days_since_last = (now - last_archive).days
    days_since_first = (now - first_archive).days
    
    if days_since_first > 0:
        archive_frequency = len(timestamps) / days_since_first
    else:
        archive_frequency = 0.0
    
    return {
        'last_archived': True,
        'first_archived': True,
        'archive_count': len(snapshots),
        'days_since_last_archive': days_since_last,
        'days_since_first_archive': days_since_first,
        'successful_snapshots': successful_count,
        'failed_snapshots': failed_count,
        'archive_frequency': archive_frequency,
        'first_archive_date': first_archive.isoformat(),
        'last_archive_date': last_archive.isoformat()
    }

def calculate_archive_trust_score(metrics: Dict) -> float:
    if not metrics['last_archived']:
        return 0.0
    
    trust_score = 0.0
    
    archive_count = metrics['archive_count']
    if archive_count >= 50:
        trust_score += 0.3
    elif archive_count >= 20:
        trust_score += 0.2
    elif archive_count >= 5:
        trust_score += 0.1
    
    days_since_last = metrics['days_since_last_archive']
    if days_since_last <= 30:
        trust_score += 0.2
    elif days_since_last <= 90:
        trust_score += 0.15
    elif days_since_last <= 365:
        trust_score += 0.1
    elif days_since_last <= 1095:
        trust_score += 0.05
    
    days_since_first = metrics['days_since_first_archive']
    if days_since_first >= 1095:
        trust_score += 0.2
    elif days_since_first >= 365:
        trust_score += 0.15
    elif days_since_first >= 180:
        trust_score += 0.1
    
    success_rate = metrics['successful_snapshots'] / max(metrics['archive_count'], 1)
    trust_score += success_rate * 0.2
    
    frequency = metrics['archive_frequency']
    if frequency > 0.1:
        trust_score += 0.1
    elif frequency > 0.01:
        trust_score += 0.05
    
    return min(trust_score, 1.0)

def analyze_cdx(url: str) -> Dict:
    start_time = time.time()
    
    cdx_data = query_cdx_api(url)
    
    if not cdx_data['accessible']:
        return {
            'last_archived': False,
            'archive_count': 0,
            'days_since_last_archive': -1,
            'trust_score': 0.0,
            'accessible': False,
            'processing_time': time.time() - start_time,
            'error': cdx_data.get('error', 'CDX API not accessible')
        }
    
    snapshots = cdx_data['snapshots']
    metrics = calculate_archive_metrics(snapshots)
    trust_score = calculate_archive_trust_score(metrics)
    
    result = metrics.copy()
    result.update({
        'trust_score': trust_score,
        'accessible': True,
        'processing_time': time.time() - start_time
    })
    
    return result