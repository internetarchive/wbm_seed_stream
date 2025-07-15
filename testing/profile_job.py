import os
import sys
import time
import psutil
import threading
from datetime import datetime
from collections import deque
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class ProcessProfiler:
    def __init__(self, output_dir=None, sample_interval=1.0):
        self.output_dir = output_dir or "data/storage/profiling"
        self.sample_interval = sample_interval
        self.monitoring = False
        self.start_time = None
        self.end_time = None

        self.metrics_data = {
            'timestamp': deque(),
            'timestamps': deque(),
            'cpu_percent': deque(),
            'memory_mb': deque(),
            'memory_percent': deque(),
            'disk_io_read': deque(),
            'disk_io_write': deque(),
            'network_sent': deque(),
            'network_recv': deque(),
            'process_count': deque(),
            'spark_processes': deque(),
        }

        self.process_events = []
        self.current_processes = {}
        self.spark_pids = set()

        self.monitor_thread = None
        self.lock = threading.Lock()

        os.makedirs(self.output_dir, exist_ok=True)

    def _get_system_metrics(self):
        try:
            cpu_percent = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory()

            disk_io = psutil.disk_io_counters()
            disk_read = disk_io.read_bytes if disk_io else 0
            disk_write = disk_io.write_bytes if disk_io else 0

            network_io = psutil.net_io_counters()
            network_sent = network_io.bytes_sent if network_io else 0
            network_recv = network_io.bytes_recv if network_io else 0

            process_count = len(psutil.pids())

            spark_processes = self._count_spark_processes()

            return {
                'timestamp': datetime.now(),
                'cpu_percent': cpu_percent,
                'memory_mb': memory.used / (1024 * 1024),
                'memory_percent': memory.percent,
                'disk_io_read': disk_read,
                'disk_io_write': disk_write,
                'network_sent': network_sent,
                'network_recv': network_recv,
                'process_count': process_count,
                'spark_processes': spark_processes,
            }
        except Exception as e:
            print(f"Error collecting metrics: {e}")
            return None

    def _count_spark_processes(self):
        spark_count = 0
        current_spark_pids = set()

        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    proc_info = proc.info
                    cmdline = ' '.join(proc_info['cmdline'] or [])

                    if any(
                        keyword in cmdline.lower()
                        for keyword in [
                            'spark-submit',
                            'sparkdriver',
                            'sparkexecutor',
                            'pyspark',
                            'spark.driver',
                            'spark.executor',
                            'process_urls.py',
                            'get_good_data.py',
                        ]
                    ):
                        spark_count += 1
                        current_spark_pids.add(proc_info['pid'])

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

        except Exception as e:
            print(f"Error counting Spark processes: {e}")

        new_pids = current_spark_pids - self.spark_pids
        ended_pids = self.spark_pids - current_spark_pids

        if new_pids:
            for pid in new_pids:
                self.log_process_event("spark_process_start", f"New Spark process: {pid}")

        if ended_pids:
            for pid in ended_pids:
                self.log_process_event("spark_process_end", f"Spark process ended: {pid}")

        self.spark_pids = current_spark_pids
        return spark_count

    def _monitoring_loop(self):
        print(f"Starting performance monitoring (interval: {self.sample_interval}s)")

        psutil.cpu_percent(interval=None)
        sample_count = 0

        while self.monitoring:
            try:
                metrics = self._get_system_metrics()
                if metrics:
                    with self.lock:
                        for key, value in metrics.items():
                            if key in self.metrics_data:
                                self.metrics_data[key].append(value)

                                if len(self.metrics_data[key]) > 10000:
                                    self.metrics_data[key].popleft()

                        if 'timestamp' in metrics:
                            self.metrics_data['timestamps'].append(metrics['timestamp'])
                            if len(self.metrics_data['timestamps']) > 10000:
                                self.metrics_data['timestamps'].popleft()

                        sample_count += 1
                        if sample_count % 10 == 0:
                            print(
                                f"Collected {sample_count} samples, timestamps: {len(self.metrics_data['timestamps'])}"
                            )

                time.sleep(self.sample_interval)
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
                time.sleep(self.sample_interval)

    def start_monitoring(self):
        if self.monitoring:
            print("Monitoring already started")
            return

        self.monitoring = True
        self.start_time = datetime.now()
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()

        self.log_process_event("monitoring_start", "Performance monitoring started")
        print(f"Performance monitoring started at {self.start_time}")

    def stop_monitoring(self):
        if not self.monitoring:
            print("Monitoring not running")
            return

        self.monitoring = False
        self.end_time = datetime.now()

        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)

        self.log_process_event("monitoring_end", "Performance monitoring stopped")
        print(f"Performance monitoring stopped at {self.end_time}")

        self.generate_reports()

    def log_process_event(self, event_type, description, process_name=None):
        event = {
            'timestamp': datetime.now(),
            'event_type': event_type,
            'description': description,
            'process_name': process_name or 'system',
        }

        with self.lock:
            self.process_events.append(event)

        print(f"[{event['timestamp'].strftime('%H:%M:%S')}] {event_type}: {description}")

    def generate_reports(self):
        with self.lock:
            timestamp_count = len(self.metrics_data['timestamps'])
            print(f"DEBUG: Generating reports with {timestamp_count} timestamp samples")
            if not self.metrics_data['timestamps'] or timestamp_count == 0:
                print("No metrics data to generate reports")
                return None, None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        graph_path = self.generate_performance_graph(timestamp)

        report_path = self.generate_text_report(timestamp)

        print(f"Performance reports generated:")
        if graph_path:
            print(f"  Graph: {graph_path}")
        if report_path:
            print(f"  Report: {report_path}")

        return graph_path, report_path

    def generate_performance_graph(self, timestamp):
        graph_path = os.path.join(self.output_dir, f"performance_graph_{timestamp}.png")

        try:
            with self.lock:
                df_data = {
                    'timestamp': list(self.metrics_data['timestamps']),
                    'cpu_percent': list(self.metrics_data['cpu_percent']),
                    'memory_mb': list(self.metrics_data['memory_mb']),
                    'memory_percent': list(self.metrics_data['memory_percent']),
                    'spark_processes': list(self.metrics_data['spark_processes']),
                }

            if not df_data['timestamp'] or len(df_data['timestamp']) == 0:
                print("No timestamp data available for graph")
                return None

            min_length = min(len(arr) for arr in df_data.values())
            for key in df_data:
                df_data[key] = df_data[key][:min_length]

            df = pd.DataFrame(df_data)
            plt.switch_backend('Agg')

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'System Performance Profile - {timestamp}', fontsize=16, fontweight='bold')

            ax1.plot(df['timestamp'], df['cpu_percent'], color='red', linewidth=2, label='CPU %')
            ax1.set_ylabel('CPU Usage (%)', fontweight='bold')
            ax1.set_title('CPU Usage Over Time')
            ax1.grid(True, alpha=0.3)
            ax1.legend()

            ax2.plot(df['timestamp'], df['memory_mb'], color='blue', linewidth=2, label='Memory (MB)')
            ax2_twin = ax2.twinx()
            ax2_twin.plot(
                df['timestamp'],
                df['memory_percent'],
                color='lightblue',
                linewidth=1,
                label='Memory %',
                alpha=0.7,
            )
            ax2.set_ylabel('Memory Usage (MB)', color='blue', fontweight='bold')
            ax2_twin.set_ylabel('Memory Usage (%)', color='lightblue', fontweight='bold')
            ax2.set_title('Memory Usage Over Time')
            ax2.grid(True, alpha=0.3)
            ax2.legend(loc='upper left')
            ax2_twin.legend(loc='upper right')

            ax3.plot(
                df['timestamp'],
                df['spark_processes'],
                color='green',
                linewidth=2,
                marker='o',
                markersize=3,
                label='Spark Processes',
            )
            ax3.set_ylabel('Process Count', fontweight='bold')
            ax3.set_title('Spark Process Count Over Time')
            ax3.grid(True, alpha=0.3)
            ax3.legend()

            ax4_cpu = ax4
            ax4_mem = ax4.twinx()
            ax4_spark = ax4.twinx()

            ax4_spark.spines['right'].set_position(('outward', 60))

            line1 = ax4_cpu.plot(df['timestamp'], df['cpu_percent'], color='red', linewidth=2, label='CPU %')
            line2 = ax4_mem.plot(df['timestamp'], df['memory_percent'], color='blue', linewidth=2, label='Memory %')
            line3 = ax4_spark.plot(
                df['timestamp'], df['spark_processes'], color='green', linewidth=2, label='Spark Processes'
            )

            ax4_cpu.set_ylabel('CPU (%)', color='red', fontweight='bold')
            ax4_mem.set_ylabel('Memory (%)', color='blue', fontweight='bold')
            ax4_spark.set_ylabel('Spark Processes', color='green', fontweight='bold')
            ax4.set_title('System Overview')
            ax4.grid(True, alpha=0.3)

            self._add_process_markers_to_plots([ax1, ax2, ax3, ax4])

            for ax in [ax1, ax2, ax3, ax4]:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
                ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()
            plt.savefig(graph_path, dpi=300, bbox_inches='tight')
            plt.close()

            return graph_path

        except Exception as e:
            print(f"Error generating performance graph: {e}")
            import traceback

            traceback.print_exc()
            return None

    def _add_process_markers_to_plots(self, axes):
        try:
            with self.lock:
                events = self.process_events.copy()

            event_colors = {
                'file_detected': 'orange',
                'spark_job_start': 'green',
                'spark_job_end': 'red',
                'good_data_start': 'purple',
                'good_data_end': 'purple',
                'tsv_conversion_start': 'brown',
                'tsv_conversion_end': 'brown',
                'monitoring_start': 'black',
                'monitoring_end': 'black',
            }

            for event in events:
                color = event_colors.get(event['event_type'], 'gray')

                for ax in axes:
                    ax.axvline(x=event['timestamp'], color=color, linestyle='--', alpha=0.7, linewidth=1)

                    if ax == axes[0]:
                        ax.annotate(
                            event['event_type'].replace('_', ' ').title(),
                            xy=(event['timestamp'], ax.get_ylim()[1] * 0.9),
                            xytext=(5, 5),
                            textcoords='offset points',
                            fontsize=8,
                            rotation=90,
                            alpha=0.8,
                            bbox=dict(boxstyle='round,pad=0.2', facecolor=color, alpha=0.3),
                        )

        except Exception as e:
            print(f"Error adding process markers: {e}")

    def generate_text_report(self, timestamp):
        report_path = os.path.join(self.output_dir, f"performance_report_{timestamp}.txt")

        try:
            with open(report_path, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("SYSTEM PERFORMANCE ANALYSIS REPORT\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Monitoring Period: {self.start_time} to {self.end_time}\n")

                if self.start_time and self.end_time:
                    duration = self.end_time - self.start_time
                    f.write(f"Total Duration: {duration}\n")

                f.write("\n")

                self._write_system_summary(f)

                self._write_process_timeline(f)

                self._write_performance_stats(f)

                self._write_resource_analysis(f)

            return report_path

        except Exception as e:
            print(f"Error generating text report: {e}")
            return None

    def _write_system_summary(self, f):
        f.write("SYSTEM SUMMARY\n")
        f.write("-" * 40 + "\n")

        try:
            f.write(f"CPU Cores: {psutil.cpu_count()}\n")
            f.write(f"Total Memory: {psutil.virtual_memory().total / (1024**3):.2f} GB\n")
            f.write(f"Python Version: {sys.version.split()[0]}\n")
            f.write(f"Platform: {sys.platform}\n")

            f.write(f"Current CPU Usage: {psutil.cpu_percent()}%\n")
            f.write(f"Current Memory Usage: {psutil.virtual_memory().percent}%\n")
            f.write(f"Current Process Count: {len(psutil.pids())}\n")

        except Exception as e:
            f.write(f"Error collecting system info: {e}\n")

        f.write("\n")

    def _write_process_timeline(self, f):
        f.write("PROCESS EVENTS TIMELINE\n")
        f.write("-" * 40 + "\n")

        with self.lock:
            events = sorted(self.process_events, key=lambda x: x['timestamp'])

        if not events:
            f.write("No process events recorded.\n\n")
            return

        for event in events:
            timestamp_str = event['timestamp'].strftime('%H:%M:%S.%f')[:-3]
            f.write(f"[{timestamp_str}] {event['event_type'].upper()}: {event['description']}\n")

        f.write("\n")

    def _write_performance_stats(self, f):
        f.write("PERFORMANCE STATISTICS\n")
        f.write("-" * 40 + "\n")

        try:
            with self.lock:
                if not self.metrics_data['cpu_percent'] or len(self.metrics_data['cpu_percent']) == 0:
                    f.write("No performance data available.\n\n")
                    return

                cpu_data = list(self.metrics_data['cpu_percent'])
                memory_data = list(self.metrics_data['memory_mb'])
                memory_pct_data = list(self.metrics_data['memory_percent'])
                spark_data = list(self.metrics_data['spark_processes'])

            f.write("CPU Usage:\n")
            f.write(f"  Average: {sum(cpu_data)/len(cpu_data):.2f}%\n")
            f.write(f"  Maximum: {max(cpu_data):.2f}%\n")
            f.write(f"  Minimum: {min(cpu_data):.2f}%\n")
            f.write(f"  Samples: {len(cpu_data)}\n")
            f.write("\n")

            f.write("Memory Usage:\n")
            f.write(
                f"  Average: {sum(memory_data)/len(memory_data):.2f} MB ({sum(memory_pct_data)/len(memory_pct_data):.2f}%)\n"
            )
            f.write(
                f"  Maximum: {max(memory_data):.2f} MB ({max(memory_pct_data):.2f}%)\n"
            )
            f.write(
                f"  Minimum: {min(memory_data):.2f} MB ({min(memory_pct_data):.2f}%)\n"
            )
            f.write("\n")

            f.write("Spark Processes:\n")
            f.write(f"  Average: {sum(spark_data)/len(spark_data):.2f}\n")
            f.write(f"  Maximum: {max(spark_data)}\n")
            f.write(f"  Total Process Events: {len(self.process_events)}\n")
            f.write("\n")

        except Exception as e:
            f.write(f"Error calculating statistics: {e}\n")

    def _write_resource_analysis(self, f):
        f.write("RESOURCE USAGE ANALYSIS\n")
        f.write("-" * 40 + "\n")

        try:
            with self.lock:
                if not self.metrics_data['cpu_percent'] or len(self.metrics_data['cpu_percent']) == 0:
                    f.write("No data available for analysis.\n\n")
                    return

                cpu_data = list(self.metrics_data['cpu_percent'])
                memory_pct_data = list(self.metrics_data['memory_percent'])

            high_cpu_count = sum(1 for x in cpu_data if x > 80)
            high_memory_count = sum(1 for x in memory_pct_data if x > 80)

            f.write(
                f"High CPU Usage (>80%): {high_cpu_count}/{len(cpu_data)} samples ({high_cpu_count/len(cpu_data)*100:.1f}%)\n"
            )
            f.write(
                f"High Memory Usage (>80%): {high_memory_count}/{len(memory_pct_data)} samples ({high_memory_count/len(memory_pct_data)*100:.1f}%)\n"
            )
            f.write("\n")

            spark_events = [e for e in self.process_events if 'spark' in e['event_type']]
            f.write(f"Spark-related Events: {len(spark_events)}\n")

            phase_durations = self._calculate_phase_durations()
            if phase_durations:
                f.write("\nProcessing Phase Durations:\n")
                for phase, duration in phase_durations.items():
                    f.write(f"  {phase}: {duration}\n")

            f.write("\n")

        except Exception as e:
            f.write(f"Error in resource analysis: {e}\n")

    def _calculate_phase_durations(self):
        phases = {}

        try:
            with self.lock:
                events = sorted(self.process_events, key=lambda x: x['timestamp'])

            start_times = {}

            for event in events:
                event_type = event['event_type']

                if event_type.endswith('_start'):
                    phase_name = event_type[:-6]
                    start_times[phase_name] = event['timestamp']

                elif event_type.endswith('_end'):
                    phase_name = event_type[:-4]
                    if phase_name in start_times:
                        duration = event['timestamp'] - start_times[phase_name]
                        phases[phase_name] = duration
                        del start_times[phase_name]

        except Exception as e:
            print(f"Error calculating phase durations: {e}")

        return phases


class ProfiledExecution:
    def __init__(self, output_dir=None, sample_interval=1.0):
        self.profiler = ProcessProfiler(output_dir, sample_interval)

    def __enter__(self):
        self.profiler.start_monitoring()
        return self.profiler

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.profiler.stop_monitoring()


def create_profiler_for_job(job_output_dir):
    profiling_dir = os.path.join(job_output_dir, "profiling")
    return ProcessProfiler(output_dir=profiling_dir, sample_interval=0.5)


def profile_spark_job(input_file_path, job_output_dir):
    profiler = create_profiler_for_job(job_output_dir)

    try:
        profiler.start_monitoring()
        profiler.log_process_event("file_detected", f"TSV file detected: {os.path.basename(input_file_path)}")

        return profiler

    except Exception as e:
        print(f"Error starting profiler: {e}")
        profiler.stop_monitoring()
        return None