#!/usr/bin/env python3

import re
import os
from urllib.parse import urlparse

def parse_ublock_filter_file(file_path):
    
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
                'full_pattern': line
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
                'full_pattern': line
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
                'full_pattern': line
            })
            continue
            
        if any(char in line for char in ['*', '?', '/', '^']) and not line.startswith('!'):
            if not any(marker in line for marker in ['##', '#@#', '!', 'style(']):
                url_patterns['wildcard_patterns'].append({
                    'line': line_num,
                    'pattern': line.split('$')[0],
                    'modifiers': '$' + line.split('$', 1)[1] if '$' in line else '',
                    'full_pattern': line
                })
    
    return url_patterns

def print_header_and_summary(patterns):
    print("=" * 80)
    print("uBlock Origin Filter Analysis")
    print("=" * 80)
    
    total_patterns = sum(len(patterns[key]) for key in patterns if key != 'comments')
    print(f"\nTotal patterns found: {total_patterns}")
    print(f"Comments: {len(patterns['comments'])}")
    print("-" * 40)

def print_pattern_blocks(patterns):
    domain_blocks = patterns['domain_blocks']
    regex_patterns = patterns['regex_patterns']
    
    if domain_blocks:
        print(f"\nDOMAIN BLOCKS ({len(domain_blocks)}):")
        print("-" * 40)
        
        for item in domain_blocks[:20]:
            print(f"Line {item['line']:4d}: {item['domain']}")
            if item['modifiers']:
                print(f"           Modifiers: {item['modifiers']}")
        
        if len(domain_blocks) > 20:
            print(f"... and {len(domain_blocks) - 20} more")
    
    if regex_patterns:
        print(f"\nREGEX PATTERNS ({len(regex_patterns)}):")
        print("-" * 40)
        
        for item in regex_patterns[:10]:
            print(f"Line {item['line']:4d}: /{item['pattern']}/")
            if item['modifiers']:
                print(f"           Modifiers: {item['modifiers']}")
        
        if len(regex_patterns) > 10:
            print(f"... and {len(regex_patterns) - 10} more")

def print_url_patterns(patterns):
    specific_urls = patterns['specific_urls']
    wildcard_patterns = patterns['wildcard_patterns']
    
    if specific_urls:
        print(f"\nSPECIFIC URLs ({len(specific_urls)}):")
        print("-" * 40)
        
        for item in specific_urls[:10]:
            print(f"Line {item['line']:4d}: {item['url']}")
            print(f"           Domain: {item['domain']}")
            print(f"           Path: {item['path']}")
        
        if len(specific_urls) > 10:
            print(f"... and {len(specific_urls) - 10} more")
    
    if wildcard_patterns:
        print(f"\nWILDCARD/PATH PATTERNS ({len(wildcard_patterns)}):")
        print("-" * 40)
        
        for item in wildcard_patterns[:15]:
            print(f"Line {item['line']:4d}: {item['pattern']}")
        
        if len(wildcard_patterns) > 15:
            print(f"... and {len(wildcard_patterns) - 15} more")

def print_cosmetic_filters(cosmetic_filters):
    if not cosmetic_filters:
        return
    
    print(f"\nCOSMETIC FILTERS ({len(cosmetic_filters)}):")
    print("-" * 40)
    
    for item in cosmetic_filters[:10]:
        pattern = item['pattern']
        truncated = pattern[:80] + '...' if len(pattern) > 80 else pattern
        print(f"Line {item['line']:4d}: {truncated}")
    
    if len(cosmetic_filters) > 10:
        print(f"... and {len(cosmetic_filters) - 10} more")

def print_extracted_patterns(patterns):
    print_header_and_summary(patterns)
    print_pattern_blocks(patterns)
    print_url_patterns(patterns)
    print_cosmetic_filters(patterns['cosmetic_filters'])

def extract_unique_domains(patterns):
    domains = set()
    for item in patterns['domain_blocks']:
        domains.add(item['domain'])
    for item in patterns['specific_urls']:
        domains.add(item['domain'])
    for item in patterns['wildcard_patterns']:
        pattern = item['pattern']
        if '/' in pattern and not pattern.startswith('/'):
            potential_domain = pattern.split('/')[0]
            if '.' in potential_domain and not any(char in potential_domain for char in ['*', '^', '$']):
                domains.add(potential_domain)
    return sorted(domains)

def main():
    file_path = 'filters/badware.txt'
    print(f"Parsing uBlock Origin filter file: {file_path}")
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found!")
        print("Please make sure the file exists in the 'filters' directory.")
        return
    patterns = parse_ublock_filter_file(file_path)
    print_extracted_patterns(patterns)
    unique_domains = extract_unique_domains(patterns)
    if unique_domains:
        print(f"\nUNIQUE DOMAINS FOUND ({len(unique_domains)}):")
        print("-" * 40)
        for i, domain in enumerate(unique_domains[:50], 1):
            print(f"{i:3d}. {domain}")
        if len(unique_domains) > 50:
            print(f"... and {len(unique_domains) - 50} more domains")
    print("\n" + "=" * 80)
    print("Analysis complete!")

if __name__ == "__main__":
    main()