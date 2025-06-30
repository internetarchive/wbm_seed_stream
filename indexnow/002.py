 import hashlib
 import json
 import logging
 import multiprocessing as mp
 import os
 import re
 import time
 from concurrent.futures import ProcessPoolExecutor, as_completed
 from dataclasses import dataclass
 from typing import Dict, Optional, Tuple, List
 from collections import defaultdict, Counter
 
 import pandas as pd
 import psutil
 import redis
 from dotenv import load_dotenv
 
 load_dotenv()
 
 MAX_WORKERS = min(mp.cpu_count() * 2, 16)
 BATCH_SIZE = 500000
 CHUNK_SIZE = 1000000
 QUEUE_MULTIPLIER = 2
 MAX_MEMORY_USAGE_PCT = 80
 PANDAS_MEMORY_MAP = True
 PANDAS_LOW_MEMORY = False
 
 REDIS_CONFIG = {
     'host': 'localhost', 'port': 6379, 'db': 0, 'max_connections': 50,
     'socket_keepalive': True, 'socket_keepalive_options': {}
 }
 
 CACHE_TTL = {'domain': 7200, 'url': 172800}
 
 FILE_CONFIG = {
     'input_dir': "data/indexnow",
     'output_file': "analyzed_urls_from_tsv.jsonl",
     'process_limit': 100000,
     'skip_bad_lines': True
 }
 
 LOG_CONFIG = {
     'level': logging.INFO,
     'progress_interval': 50000,
     'memory_usage': True,
     'memory_check_interval': 100000
 }
 
 MONITORING = {
     'monitor_memory': True,
     'auto_adjust_batch_size': True
 }
 
 logging.basicConfig(
     level=LOG_CONFIG['level'], format='%(asctime)s - %(levelname)s - %(message)s')
 logger = logging.getLogger(__name__)
 
 
 def get_memory_info():
     memory = psutil.virtual_memory()
     return {
         'total_gb': memory.total / (1024**3),
         'available_gb': memory.available / (1024**3),
         'used_gb': memory.used / (1024**3),
         'used_percent': memory.percent
     }
 
 
 def log_memory_usage():
     if LOG_CONFIG['memory_usage']:
         mem = get_memory_info()
         logger.info(f"Memory: {mem['used_gb']:.1f}GB ({mem['used_percent']:.1f}%) | "
                     f"{mem['available_gb']:.1f}GB available | {mem['total_gb']:.1f}GB total")
 
 
 def check_memory_limit():
     if MONITORING['monitor_memory']:
         mem = get_memory_info()
         if mem['used_percent'] > MAX_MEMORY_USAGE_PCT:
             logger.warning(
                 f"Memory usage ({mem['used_percent']:.1f}%) exceeds limit ({MAX_MEMORY_USAGE_PCT}%)")
             return False
     return True
 
 
 def get_optimal_batch_size():
     if MONITORING['auto_adjust_batch_size']:
         mem = get_memory_info()
         available_memory_bytes = mem['available_gb'] * (1024**3) * 0.75
         urls_per_gb = 1024**3 / 200
         optimal_batch = int((available_memory_bytes / (1024**3))
                             * urls_per_gb / MAX_WORKERS)
         optimal_batch = max(100000, min(optimal_batch, 2000000))
         logger.info(
             f"Auto-calculated batch size: {optimal_batch:,} URLs ({mem['available_gb']:.1f}GB available)")
         return optimal_batch
     return BATCH_SIZE
 
 
 @dataclass
 class URLAnalysisResult:
     url: str
     timestamp: str
     domain: str
     priority_score: float
     reasons: Dict
     domain_frequency: int
     domain_frequency_pct: float
 
 
 class OptimizedURLPrioritizer:
     def __init__(self):
         self.redis_pool = redis.ConnectionPool(
             decode_responses=True, **REDIS_CONFIG)
         self.redis_client = redis.Redis(connection_pool=self.redis_pool)
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
 
 
 def process_batch_worker_optimized(batch_data):
     urls, timestamps = batch_data
     prioritizer = OptimizedURLPrioritizer()
     results = []
 
     domain_counter = Counter()
     url_fingerprints = set()
 
     for url, timestamp in zip(urls, timestamps):
         domain = prioritizer.extract_domain(url)
         if not domain:
             continue
 
         domain_counter[domain] += 1
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
 
         results.append(URLAnalysisResult(
             url=url, timestamp=timestamp_str, domain=domain, priority_score=score,
             reasons=reasons, domain_frequency=domain_counter[domain], domain_frequency_pct=0.0
         ))
 
     total_urls = len(results)
     for result in results:
         result.domain_frequency_pct = (result.domain_frequency / total_urls * 100) if total_urls > 0 else 0.0
 
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
 
     return results
 
 
 class TSVFileStreamer:
     def __init__(self, file_path: str):
         self.file_path = file_path
 
     def stream_urls_chunked(self, batch_size: int = None, limit: Optional[int] = None):
         if batch_size is None:
             batch_size = get_optimal_batch_size()
 
         logger.info(f"Streaming from {self.file_path} with batch size: {batch_size:,}")
         log_memory_usage()
 
         processed_count = 0
         try:
             chunk_iter = pd.read_csv(
                 self.file_path, sep='\t', chunksize=CHUNK_SIZE, header=None,
                 names=['timestamp', 'url'], dtype={'timestamp': str, 'url': str},
                 on_bad_lines='skip' if FILE_CONFIG['skip_bad_lines'] else 'error',
                 low_memory=PANDAS_LOW_MEMORY, memory_map=PANDAS_MEMORY_MAP, engine='c'
             )
 
             current_batch_urls, current_batch_timestamps = [], []
 
             for chunk in chunk_iter:
                 if limit and processed_count >= limit:
                     break
 
                 chunk = chunk.dropna()
                 if len(chunk) == 0:
                     continue
 
                 current_batch_urls.extend(chunk['url'].tolist())
                 current_batch_timestamps.extend(chunk['timestamp'].tolist())
 
                 while len(current_batch_urls) >= batch_size:
                     batch_size_actual = min(batch_size, limit - processed_count if limit else batch_size)
                     batch_urls = current_batch_urls[:batch_size_actual]
                     batch_timestamps = current_batch_timestamps[:batch_size_actual]
                     current_batch_urls = current_batch_urls[batch_size_actual:]
                     current_batch_timestamps = current_batch_timestamps[batch_size_actual:]
 
                     processed_count += len(batch_urls)
                     yield batch_urls, batch_timestamps
 
                     if processed_count % LOG_CONFIG['progress_interval'] == 0:
                         logger.info(f"Streamed {processed_count:,} URLs")
                     if processed_count % LOG_CONFIG['memory_check_interval'] == 0:
                         log_memory_usage()
                         if not check_memory_limit():
                             logger.warning("Memory limit exceeded")
 
                 if limit and processed_count >= limit:
                     break
 
             if current_batch_urls and (not limit or processed_count < limit):
                 if limit and limit - processed_count < len(current_batch_urls):
                     current_batch_urls = current_batch_urls[:limit - processed_count]
                     current_batch_timestamps = current_batch_timestamps[:limit - processed_count]
                 yield current_batch_urls, current_batch_timestamps
 
         except Exception as e:
             logger.error(f"Error reading TSV file: {e}")
             raise
 
 
 def write_results_batch(results_batch: List[URLAnalysisResult], output_file: str):
     with open(output_file, 'a') as f:
         for result in results_batch:
             f.write(json.dumps({
                 'url': result.url, 'timestamp': result.timestamp, 'domain': result.domain,
                 'priority_score': result.priority_score, 'reasons': result.reasons,
                 'domain_frequency': result.domain_frequency, 'domain_frequency_pct': result.domain_frequency_pct
             }) + '\n')
 
 
 def analyze_urls_from_tsv(file_path: str,
                           batch_size: int = None,
                           max_workers: int = None,
                           output_file: str = FILE_CONFIG['output_file'],
                           limit: Optional[int] = FILE_CONFIG['process_limit']):
     batch_size = batch_size or get_optimal_batch_size()
     max_workers = max_workers or MAX_WORKERS
 
     logger.info(f"=== URL ANALYSIS ===")
     logger.info(f"File: {file_path} | Workers: {max_workers} | Batch: {batch_size:,}")
     logger.info(f"Limit: {limit:,} URLs" if limit else "Processing ALL URLs")
     log_memory_usage()
 
     if not os.path.exists(file_path):
         raise FileNotFoundError(f"TSV file not found: {file_path}")
 
     if os.path.exists(output_file):
         os.remove(output_file)
 
     streamer = TSVFileStreamer(file_path)
     total_processed = 0
     start_time = time.time()
     last_log_time = start_time
 
     with ProcessPoolExecutor(max_workers=max_workers) as executor:
         futures = []
 
         try:
             for batch_urls, batch_timestamps in streamer.stream_urls_chunked(batch_size, limit):
                 futures.append(executor.submit(process_batch_worker_optimized, (batch_urls, batch_timestamps)))
 
                 while len(futures) >= max_workers * QUEUE_MULTIPLIER:
                     completed_futures = []
                     for future in as_completed(futures[:max_workers]):
                         batch_results = future.result()
                         write_results_batch(batch_results, output_file)
                         total_processed += len(batch_results)
                         completed_futures.append(future)
 
                         current_time = time.time()
                         elapsed = current_time - start_time
                         rate = total_processed / elapsed if elapsed > 0 else 0
 
                         if current_time - last_log_time >= 30:
                             logger.info(f"Progress: {total_processed:,} URLs ({rate:.0f} URLs/sec) | Queue: {len(futures)}")
                             log_memory_usage()
                             last_log_time = current_time
 
                     for future in completed_futures:
                         futures.remove(future)
 
             logger.info("Processing remaining batches...")
             for future in as_completed(futures):
                 batch_results = future.result()
                 write_results_batch(batch_results, output_file)
                 total_processed += len(batch_results)
 
         except Exception as e:
             logger.error(f"Error during processing: {e}")
             raise
 
     elapsed = time.time() - start_time
     final_rate = total_processed / elapsed if elapsed > 0 else 0
 
     logger.info("=== ANALYSIS COMPLETE ===")
     logger.info(f"Processed: {total_processed:,} URLs | Time: {elapsed:.2f}s ({elapsed/60:.1f}m)")
     logger.info(f"Rate: {final_rate:.0f} URLs/sec | Output: {output_file}")
     log_memory_usage()
 
     return total_processed, final_rate
 
 
 if __name__ == "__main__":
     mem_info = get_memory_info()
     logger.info("=== SYSTEM INFO ===")
     logger.info(f"CPU cores: {mp.cpu_count()} | RAM: {mem_info['total_gb']:.1f}GB total, {mem_info['available_gb']:.1f}GB available")
     logger.info(f"Max workers: {MAX_WORKERS} | Batch size: {BATCH_SIZE:,}")
 
     files = [f for f in os.listdir(FILE_CONFIG['input_dir']) if f.startswith("indexnow-log-bing-")]
     if not files:
         raise FileNotFoundError(f"No indexnow files found in {FILE_CONFIG['input_dir']}")
 
     first_file = os.path.join(FILE_CONFIG['input_dir'], sorted(files)[0])
     logger.info(f"Processing file: {first_file}")
     analyze_urls_from_tsv(file_path=first_file)