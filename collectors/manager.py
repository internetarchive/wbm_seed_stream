import os
import sys
import time
import signal
import logging
import threading
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import json

class CollectorManager:
    def __init__(self):
        self.collectors_dir = Path(".") 
        
        self.processes: Dict[str, subprocess.Popen] = {}
        self.threads: Dict[str, threading.Thread] = {}
        self.running = True
        
        self.session_timestamp = datetime.now().strftime('%B %d - %H:%M')
        self.session_dir = Path("../logs/collectors") / self.session_timestamp
        
        self.stats = {
            'started_at': datetime.now().isoformat(),
            'collectors': {},
            'restarts': {}
        }
        
        self.setup_logging()
        self.collectors = {
            'gdelt_collector.py': {
                'name': 'GDELT Collector',
                'description': 'Collects URLs from GDELT news feeds',
                'restart_delay': 30,
                'max_restarts': 5
            },
            'wikipedia_collector.py': {
                'name': 'Wikipedia Collector', 
                'description': 'Monitors Wikipedia recent changes',
                'restart_delay': 20,
                'max_restarts': 3
            },
            'web_sources_collector.py': {
                'name': 'Web Sources Crawler',
                'description': 'Crawls news sites and forums for links',
                'restart_delay': 60,
                'max_restarts': 3
            },
            'reddit_collector.py': {
                'name': 'Reddit Collector',
                'description': 'Monitors Reddit for links and discussions', 
                'restart_delay': 30,
                'max_restarts': 3
            },
            'rss_collector.py': {
                'name': 'RSS Feed Collector',
                'description': 'Processes RSS feeds for new content',
                'restart_delay': 45,
                'max_restarts': 3
            },
            'mediacloud_collector.py': {
                'name': 'MediaCloud Collector',
                'description': 'Collects articles from MediaCloud daily RSS feeds',
                'restart_delay': 60,
                'max_restarts': 3
            },
        }
        
        self.logger.info(f"Initialized CollectorManager with {len(self.collectors)} collectors")
        self.logger.info(f"Looking for collectors in: {self.collectors_dir.absolute()}")

    def setup_logging(self):
        os.makedirs(self.session_dir, exist_ok=True)
        
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s'
        )
        
        self.logger = logging.getLogger('CollectorManager')
        self.logger.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        
        log_filename = self.session_dir / "collector_manager.log"
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        unified_log = self.session_dir / "unified_collectors.log"
        unified_handler = logging.FileHandler(unified_log)
        unified_handler.setLevel(logging.INFO)
        unified_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(unified_handler)

    def check_collector_exists(self, collector_file: str) -> bool:
        collector_path = self.collectors_dir / collector_file
        return collector_path.exists() and collector_path.is_file()

    def start_collector(self, collector_file: str) -> bool:
        if not self.check_collector_exists(collector_file):
            self.logger.error(f"Collector file not found: {collector_file}")
            return False
            
        collector_path = self.collectors_dir / collector_file
        config = self.collectors[collector_file]
        
        try:
            process = subprocess.Popen(
                [sys.executable, str(collector_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            self.processes[collector_file] = process
            self.stats['collectors'][collector_file] = {
                'name': config['name'],
                'pid': process.pid,
                'started_at': datetime.now().isoformat(),
                'restarts': 0,
                'status': 'running'
            }
            
            thread = threading.Thread(
                target=self.monitor_collector_output,
                args=(collector_file, process),
                daemon=True
            )
            thread.start()
            self.threads[collector_file] = thread
            
            self.logger.info(f"✅ Started {config['name']} (PID: {process.pid})")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start {config['name']}: {e}")
            return False

    def _parse_structured_log(self, line: str) -> "tuple[str, str]":
        if ' - ' in line and any(level in line for level in ['INFO:', 'ERROR:', 'WARNING:', 'DEBUG:']):
            parts = line.split(' - ', 1)
            if len(parts) == 2:
                log_part = parts[1]
                return self._get_emoji_for_log_level(log_part), self._extract_message_from_log(log_part)
        
        if any(marker in line for marker in [' - INFO - ', ' - ERROR - ', ' - WARNING - ', ' - DEBUG - ']):
            return self._parse_hyphen_separated_log(line)
        
        if any(line.startswith(prefix) for prefix in ['ERROR:', 'WARNING:', 'INFO:']):
            return self._parse_prefixed_log(line)
        
        return self._get_emoji_by_content(line), line

    def _get_emoji_for_log_level(self, log_part: str) -> str:
        if log_part.startswith('ERROR:'):
            return "❌"
        elif log_part.startswith('WARNING:'):
            return "⚠️ "
        elif log_part.startswith('INFO:'):
            return "📰"
        elif log_part.startswith('DEBUG:'):
            return "🔍"
        return "📄"

    def _extract_message_from_log(self, log_part: str) -> str:
        if ':' in log_part:
            return log_part.split(':', 2)[-1]
        return log_part

    def _parse_hyphen_separated_log(self, line: str) -> "tuple[str, str]":
        if ' - ERROR - ' in line:
            return "❌", line.split(' - ERROR - ', 1)[-1]
        elif ' - WARNING - ' in line:
            return "⚠️ ", line.split(' - WARNING - ', 1)[-1]
        elif ' - INFO - ' in line:
            return "📰", line.split(' - INFO - ', 1)[-1]
        elif ' - DEBUG - ' in line:
            return "🔍", line.split(' - DEBUG - ', 1)[-1]
        return "📄", line

    def _parse_prefixed_log(self, line: str) -> "tuple[str, str]":
        if line.startswith('ERROR:'):
            return "❌", line.split(':', 1)[-1].strip()
        elif line.startswith('WARNING:'):
            return "⚠️ ", line.split(':', 1)[-1].strip()
        elif line.startswith('INFO:'):
            return "📰", line.split(':', 1)[-1].strip()
        return "📄", line

    def _get_emoji_by_content(self, line: str) -> str:
        line_lower = line.lower()
        if any(phrase in line_lower for phrase in ['error', 'failed', 'exception', 'traceback']):
            return "❌"
        elif any(phrase in line_lower for phrase in ['warning', 'warn']):
            return "⚠️ "
        elif any(phrase in line_lower for phrase in ['archived', 'ingested', 'submitted', 'success']):
            return "✅"
        elif any(phrase in line_lower for phrase in ['found', 'discovered', 'collecting']):
            return "🔍"
        return "📄"

    def monitor_collector_output(self, collector_file: str, process: subprocess.Popen):
        config = self.collectors[collector_file]
        collector_name = config['name']
        
        try:
            for line in iter(process.stdout.readline, ''): # pyright: ignore[reportOptionalMemberAccess]
                if not line.strip():
                    continue
                    
                clean_line = line.strip()
                emoji, message = self._parse_structured_log(clean_line)
                
                print(f"{emoji} [{collector_name}] {message}")
                
                collector_logger = logging.getLogger(f"Collector.{collector_name}")
                collector_logger.info(clean_line)
                        
        except Exception as e:
            print(f"❌ Error monitoring output for {collector_name}: {e}")

    def stop_collector(self, collector_file: str) -> bool:
        if collector_file not in self.processes:
            return True
            
        process = self.processes[collector_file]
        config = self.collectors[collector_file]
        
        try:
            process.terminate()
            
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            
            del self.processes[collector_file]
            if collector_file in self.stats['collectors']:
                self.stats['collectors'][collector_file]['status'] = 'stopped'
                
            self.logger.info(f"🛑 Stopped {config['name']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping {config['name']}: {e}")
            return False

    def restart_collector(self, collector_file: str) -> bool:
        config = self.collectors[collector_file]
        
        restart_count = self.stats['restarts'].get(collector_file, 0) + 1
        self.stats['restarts'][collector_file] = restart_count
        
        if restart_count > config['max_restarts']:
            self.logger.error(f"⚠️ {config['name']} exceeded max restarts ({config['max_restarts']}). Disabling.")
            return False
        
        self.logger.warning(f"🔄 Restarting {config['name']} (attempt {restart_count}/{config['max_restarts']})")
        
        self.stop_collector(collector_file)
        time.sleep(config['restart_delay'])
        
        return self.start_collector(collector_file)

    def check_collector_health(self, collector_file: str) -> bool:
        if collector_file not in self.processes:
            return False
            
        process = self.processes[collector_file]
        
        if process.poll() is not None:
            return False
            
        return True

    def monitor_collectors(self):
        while self.running:
            try:
                for collector_file in list(self.processes.keys()):
                    if not self.check_collector_health(collector_file):
                        config = self.collectors[collector_file]
                        self.logger.warning(f"⚠️ {config['name']} appears unhealthy, restarting...")
                        self.restart_collector(collector_file)
                
                time.sleep(30)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(60)

    def print_status(self):
        print("\n" + "="*80)
        print(f"📊 COLLECTOR STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        for collector_file, config in self.collectors.items():
            status = "❌ Not Running"
            pid = "N/A"
            
            if collector_file in self.processes:
                process = self.processes[collector_file]
                if process.poll() is None:
                    status = "✅ Running"
                    pid = str(process.pid)
                else:
                    status = "💀 Dead"
            
            restart_count = self.stats['restarts'].get(collector_file, 0)
            print(f"{config['name']:<25} | {status:<12} | PID: {pid:<8} | Restarts: {restart_count}")
        
        print("="*80)

    def save_stats(self):
        try:
            stats_file = self.session_dir / f"collector_stats_{datetime.now().strftime('%Y%m%d')}.json"
            with open(stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to save stats: {e}")

    def signal_handler(self, signum, frame):
        print("\n🛑 Received shutdown signal, stopping all collectors...")
        self.shutdown()

    def shutdown(self):
        self.running = False
        
        for collector_file in list(self.processes.keys()):
            self.stop_collector(collector_file)
        
        self.save_stats()
        
        print("✅ All collectors stopped. Goodbye!")
        sys.exit(0)

    def run(self, collectors_to_run: Optional[List[str]] = None):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        if collectors_to_run:
            active_collectors = {k: v for k, v in self.collectors.items() if k in collectors_to_run}
        else:
            active_collectors = self.collectors
        
        print("🚀 Starting Collector Manager")
        print(f"🎯 Active collectors: {list(active_collectors.keys())}")
        print("="*80)
        
        failed_starts = []
        for collector_file in active_collectors:
            if not self.start_collector(collector_file):
                failed_starts.append(collector_file)
        
        if failed_starts:
            print(f"⚠️ Failed to start: {', '.join(failed_starts)}")
        
        print("\n🔍 Collector Output (Live Feed):")
        print("-" * 80)
        
        monitor_thread = threading.Thread(target=self.monitor_collectors, daemon=True)
        monitor_thread.start()
        
        try:
            while self.running:
                time.sleep(300)
                self.print_status()
                self.save_stats()
                
        except KeyboardInterrupt:
            self.shutdown()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified Collector Manager')
    parser.add_argument('--collectors', nargs='+', 
                       help='Specific collectors to run (default: all)')
    
    args = parser.parse_args()
    manager = CollectorManager()
    manager.run(args.collectors)

if __name__ == "__main__":
    main()