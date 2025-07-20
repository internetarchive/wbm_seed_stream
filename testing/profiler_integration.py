import os
import sys
import functools
import time
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from testing.profile_job import ProcessProfiler


class SparkJobProfiler:
    def __init__(self, job_name, output_dir=None):
        self.job_name = job_name
        self.output_dir = output_dir
        self.profiler = None
        self.start_time = None

    def __enter__(self):
        if self.output_dir:
            profiling_dir = os.path.join(self.output_dir, "profiling")
        else:
            profiling_dir = "data/storage/profiling"

        self.profiler = ProcessProfiler(output_dir=profiling_dir, sample_interval=0.5)
        self.profiler.start_monitoring()
        self.start_time = time.time()

        self.profiler.log_process_event("job_start", f"Started profiling job: {self.job_name}")

        return self.profiler

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.profiler:
            end_time = time.time()
            duration = end_time - self.start_time if self.start_time else 0

            if exc_type:
                self.profiler.log_process_event(
                    "job_error", f"Job {self.job_name} failed after {duration:.2f}s: {exc_val}"
                )
            else:
                self.profiler.log_process_event(
                    "job_end", f"Completed job: {self.job_name} in {duration:.2f}s"
                )

            self.profiler.stop_monitoring()


def profile_function(job_name, output_dir=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with SparkJobProfiler(job_name, output_dir) as profiler:
                profiler.log_process_event("function_start", f"Starting function: {func.__name__}")

                try:
                    result = func(*args, **kwargs)
                    profiler.log_process_event("function_end", f"Completed function: {func.__name__}")
                    return result
                except Exception as e:
                    profiler.log_process_event("function_error", f"Function {func.__name__} failed: {str(e)}")
                    raise

        return wrapper

    return decorator


def profile_spark_stage(profiler, stage_name):
    class StageProfiler:
        def __init__(self, profiler, stage_name):
            self.profiler = profiler
            self.stage_name = stage_name
            self.start_time = None

        def __enter__(self):
            self.start_time = time.time()
            if self.profiler:
                self.profiler.log_process_event("stage_start", f"Starting Spark stage: {self.stage_name}")
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            end_time = time.time()
            duration = end_time - self.start_time if self.start_time else 0

            if self.profiler:
                if exc_type:
                    self.profiler.log_process_event(
                        "stage_error", f"Stage {self.stage_name} failed after {duration:.2f}s: {exc_val}"
                    )
                else:
                    self.profiler.log_process_event(
                        "stage_end", f"Completed stage: {self.stage_name} in {duration:.2f}s"
                    )

    return StageProfiler(profiler, stage_name)


_current_profiler = None

def get_active_profiler():
    return _current_profiler


def set_active_profiler(profiler):
    global _current_profiler
    _current_profiler = profiler


def profile_domain_ranking(profiler, operation_name):
    return profile_spark_stage(profiler, f"domain_ranking_{operation_name}")


def profile_url_scoring(profiler, batch_size=None):
    stage_name = f"url_scoring"
    if batch_size:
        stage_name += f"_batch_{batch_size}"
    return profile_spark_stage(profiler, stage_name)


def profile_good_data_generation(profiler, wiki_days, mediacloud_days):
    stage_name = f"good_data_gen_wiki_{wiki_days}_mc_{mediacloud_days}"
    return profile_spark_stage(profiler, stage_name)


def profile_parquet_conversion(profiler, conversion_type):
    return profile_spark_stage(profiler, f"parquet_conversion_{conversion_type}")


def profile_summary_creation(profiler):
    return profile_spark_stage(profiler, "summary_creation")


def log_domain_stats_update(profiler, domain_count, operation_type="batch_update"):
    if profiler:
        profiler.log_process_event("domain_stats_update", f"Updated {domain_count} domains ({operation_type})")


def log_spark_dataframe_operation(profiler, operation, row_count=None, partition_count=None):
    if profiler:
        details = f"Spark DataFrame {operation}"
        if row_count is not None:
            details += f" - {row_count:,} rows"
        if partition_count is not None:
            details += f" - {partition_count} partitions"

        profiler.log_process_event("spark_dataframe_op", details)


def log_file_operation(profiler, operation, file_path, file_size=None):
    if profiler:
        details = f"File {operation}: {os.path.basename(file_path)}"
        if file_size is not None:
            details += f" ({file_size:,} bytes)"

        profiler.log_process_event("file_operation", details)


def log_database_operation(profiler, operation, table_name, record_count=None):
    if profiler:
        details = f"Database {operation} on {table_name}"
        if record_count is not None:
            details += f" ({record_count:,} records)"

        profiler.log_process_event("database_operation", details)


def log_memory_checkpoint(profiler, checkpoint_name):
    if profiler:
        import psutil

        memory_info = psutil.virtual_memory()
        profiler.log_process_event(
            "memory_checkpoint",
            f"{checkpoint_name}: {memory_info.percent:.1f}% used ({memory_info.used/(1024**3):.2f}GB)",
        )


def log_spark_job_metrics(profiler, metrics_dict):
    if profiler:
        metrics_str = ", ".join([f"{k}={v}" for k in metrics_dict for v in metrics_dict[k]])
        profiler.log_process_event("spark_metrics", f"Job metrics: {metrics_str}")
