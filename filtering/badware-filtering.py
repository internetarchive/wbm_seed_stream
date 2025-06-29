import re
import os
import requests
from urllib.parse import urlparse
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

@dataclass
class FilterRuleData:
    pattern: str
    rule_type: str
    source_file: Optional[str] = None
    line_number: Optional[int] = None
    modifiers: Optional[str] = None
    description: Optional[str] = None
    is_active: bool = True
    confidence_score: Optional[int] = None

class FilterDatabaseClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
    
    def create_filter_rule(self, rule: FilterRuleData) -> Optional[Dict[str, Any]]:
        try:
            response = self.session.post(
                f"{self.base_url}/api/filter-rules",
                json={
                    "pattern": rule.pattern,
                    "rule_type": rule.rule_type,
                    "source_file": rule.source_file,
                    "line_number": rule.line_number,
                    "modifiers": rule.modifiers,
                    "description": rule.description,
                    "is_active": rule.is_active,
                    "confidence_score": rule.confidence_score
                }
            )
            if response.status_code == 201:
                return response.json()
            elif response.status_code == 409:
                print(f"Rule already exists: {rule.pattern} ({rule.rule_type})")
                return None
            else:
                print(f"Error creating rule: {response.status_code} - {response.text}")
                return None
        except requests.RequestException as e:
            print(f"Network error creating rule: {e}")
            return None
    
    def batch_create_filter_rules(self, rules: List[FilterRuleData], batch_size: int = 100) -> Dict[str, int]:
        stats = {"created": 0, "errors": 0, "duplicates": 0}
        for i in range(0, len(rules), batch_size):
            batch = rules[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(rules) + batch_size - 1)//batch_size} ({len(batch)} rules)")
            for rule in batch:
                result = self.create_filter_rule(rule)
                if result:
                    stats["created"] += 1
                elif result is None:
                    stats["duplicates"] += 1
                else:
                    stats["errors"] += 1
            import time
            time.sleep(0.1)
        return stats

def parse_ublock_filter_file(file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    
    url_patterns = {
        'domain_blocks': [],
        'path_patterns': [],
        'regex_patterns': [],
        'specific_urls': [],
        'wildcard_patterns': [],
        'comments': [],
        'cosmetic_filters': []
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found!")
        return url_patterns
    except Exception as e:
        print(f"Error reading file: {e}")
        return url_patterns
    source_filename = os.path.basename(file_path)
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        if line.startswith('!'):
            url_patterns['comments'].append({
                'line': line_num,
                'content': line
            })
            continue
        if '##' in line or '#@#' in line:
            url_patterns['cosmetic_filters'].append({
                'line': line_num,
                'pattern': line
            })
            continue
        domain_block_match = re.match(r'\|\|([^$^|*]+)(\^?)(\$.*)?', line)
        if domain_block_match:
            domain = domain_block_match.group(1)
            modifiers = domain_block_match.group(3) or ''
            url_patterns['domain_blocks'].append({
                'line': line_num,
                'domain': domain,
                'modifiers': modifiers,
                'full_pattern': line,
                'source_file': source_filename
            })
            continue
        regex_match = re.match(r'/(.+)/\$?(.*)$', line)
        if regex_match and not line.startswith('||'):
            pattern = regex_match.group(1)
            modifiers = regex_match.group(2) or ''
            url_patterns['regex_patterns'].append({
                'line': line_num,
                'pattern': pattern,
                'modifiers': modifiers,
                'full_pattern': line,
                'source_file': source_filename
            })
            continue
        if line.startswith(('http://', 'https://')):
            parsed = urlparse(line.split('$')[0])
            url_patterns['specific_urls'].append({
                'line': line_num,
                'url': line.split('$')[0],
                'domain': parsed.netloc,
                'path': parsed.path,
                'modifiers': '$' + line.split('$', 1)[1] if '$' in line else '',
                'full_pattern': line,
                'source_file': source_filename
            })
            continue
        if any(char in line for char in ['*', '?', '/', '^']) and not line.startswith('!'):
            if not any(marker in line for marker in ['##', '#@#', '!', 'style(']):
                url_patterns['wildcard_patterns'].append({
                    'line': line_num,
                    'pattern': line.split('$')[0],
                    'modifiers': '$' + line.split('$', 1)[1] if '$' in line else '',
                    'full_pattern': line,
                    'source_file': source_filename
                })
    return url_patterns

def convert_patterns_to_filter_rules(patterns: Dict[str, List[Dict[str, Any]]], confidence_score: int = 8) -> List[FilterRuleData]:
    filter_rules = []
    for item in patterns['domain_blocks']:
        rule = FilterRuleData(
            pattern=item['domain'],
            rule_type="domain_block",
            source_file=item.get('source_file'),
            line_number=item['line'],
            modifiers=item.get('modifiers') if item.get('modifiers') else None,
            description=f"Domain block from {item.get('source_file', 'unknown')}",
            confidence_score=confidence_score
        )
        filter_rules.append(rule)
    for item in patterns['regex_patterns']:
        rule = FilterRuleData(
            pattern=item['pattern'],
            rule_type="regex",
            source_file=item.get('source_file'),
            line_number=item['line'],
            modifiers=item.get('modifiers') if item.get('modifiers') else None,
            description=f"Regex pattern from {item.get('source_file', 'unknown')}",
            confidence_score=confidence_score
        )
        filter_rules.append(rule)
    for item in patterns['specific_urls']:
        rule = FilterRuleData(
            pattern=item['url'],
            rule_type="exact_match",
            source_file=item.get('source_file'),
            line_number=item['line'],
            modifiers=item.get('modifiers') if item.get('modifiers') else None,
            description=f"Exact URL match from {item.get('source_file', 'unknown')}",
            confidence_score=confidence_score
        )
        filter_rules.append(rule)
    for item in patterns['wildcard_patterns']:
        rule = FilterRuleData(
            pattern=item['pattern'],
            rule_type="wildcard",
            source_file=item.get('source_file'),
            line_number=item['line'],
            modifiers=item.get('modifiers') if item.get('modifiers') else None,
            description=f"Wildcard pattern from {item.get('source_file', 'unknown')}",
            confidence_score=confidence_score
        )
        filter_rules.append(rule)
    return filter_rules

def print_summary(patterns: Dict[str, List[Dict[str, Any]]]):
    print("=" * 80)
    print("uBlock Origin Filter Analysis & Database Import")
    print("=" * 80)
    total_patterns = sum(len(patterns[key]) for key in patterns if key not in ['comments', 'cosmetic_filters'])
    print("\nPatterns to import:")
    print(f"  Domain blocks: {len(patterns['domain_blocks'])}")
    print(f"  Regex patterns: {len(patterns['regex_patterns'])}")
    print(f"  Specific URLs: {len(patterns['specific_urls'])}")
    print(f"  Wildcard patterns: {len(patterns['wildcard_patterns'])}")
    print(f"  Total importable: {total_patterns}")
    print("\nSkipped:")
    print(f"  Comments: {len(patterns['comments'])}")
    print(f"  Cosmetic filters: {len(patterns['cosmetic_filters'])}")
    print("-" * 40)

def main():
    file_path = 'filters/badware.txt'
    api_base_url = "http://localhost:8000"
    confidence_score = 8
    print(f"Parsing uBlock Origin filter file: {file_path}")
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found!")
        print("Please make sure the file exists in the 'filters' directory.")
        return
    patterns = parse_ublock_filter_file(file_path)
    print_summary(patterns)
    filter_rules = convert_patterns_to_filter_rules(patterns, confidence_score)
    if not filter_rules:
        print("No filter rules to import!")
        return
    response = input(f"\nImport {len(filter_rules)} filter rules to database? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Import cancelled.")
        return
    db_client = FilterDatabaseClient(api_base_url)
    try:
        test_response = requests.get(f"{api_base_url}/api/stats")
        if test_response.status_code != 200:
            print(f"Error: Cannot connect to API at {api_base_url}")
            print(f"Make sure your FastAPI server is running on {api_base_url}")
            return
        print("Connected to API successfully!")
        stats_data = test_response.json()
        print(f"Current database stats: {stats_data.get('total_filter_rules', 0)} filter rules")
    except requests.RequestException as e:
        print(f"Error connecting to API: {e}")
        return
    print(f"\nImporting {len(filter_rules)} filter rules...")
    import_stats = db_client.batch_create_filter_rules(filter_rules)
    print("\n" + "=" * 80)
    print("Import Results:")
    print(f"  Created: {import_stats['created']}")
    print(f"  Duplicates skipped: {import_stats['duplicates']}")
    print(f"  Errors: {import_stats['errors']}")
    print(f"  Total processed: {sum(import_stats.values())}")
    print("=" * 80)
    if import_stats['created'] > 0:
        print(f"✅ Successfully imported {import_stats['created']} filter rules!")
    if import_stats['errors'] > 0:
        print(f"⚠️  {import_stats['errors']} rules failed to import")
    try:
        final_stats = requests.get(f"{api_base_url}/api/stats").json()
        print(f"\nFinal database stats: {final_stats.get('total_filter_rules', 0)} total filter rules")
    except:
        pass

if __name__ == "__main__":
    main()

"""
Notes:

- Consider NSFW Classification via simple key word extraction
"""