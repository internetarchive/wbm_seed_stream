import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '''..''')))

import shutil
import subprocess
import time
import zipfile
import logging
from datetime import datetime

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from utils.parquet_to_tsv import convert_parquet_to_tsv
from spark.config.spark_config import (
    SparkConfig,
    configure_spark_logging,
    get_spark_memory_config,
    get_java_opens_options,
    setup_spark_environment
)
from testing.profile_job import create_profiler_for_job
from testing.profiler_integration import set_active_profiler

logger = logging.getLogger(__name__)

class FileWatcher:
    def __init__(self, watch_directory=None, output_base_directory=None):
        self.watch_directory = watch_directory or SparkConfig.WATCH_DIRECTORY
        self.output_base_directory = output_base_directory or SparkConfig.OUTPUT_BASE_DIRECTORY
        self.spark_zip_path = SparkConfig.SPARK_ZIP_PATH

        configure_spark_logging()

        should_recreate = self._should_recreate_zip()
        if should_recreate:
            logger.info(f"Creating new spark zip at {self.spark_zip_path}")
            self._create_spark_zip()

    def _should_recreate_zip(self):
        if not os.path.exists(self.spark_zip_path):
            logger.info(f"Spark zip does not exist at {self.spark_zip_path}")
            return True

        zip_mtime = os.path.getmtime(self.spark_zip_path)

        for module in SparkConfig.PROJECT_MODULES:
            path = module

            if os.path.exists(path) and os.path.isdir(path):
                for root, _, files in os.walk(path):
                    for file in files:
                        if file.endswith('.py') and os.path.getmtime(os.path.join(root, file)) > zip_mtime:
                            logger.info(f"Module {module} has been modified, recreating zip")
                            return True
            elif os.path.exists(path) and path.endswith('.py') and os.path.getmtime(path) > zip_mtime:
                    logger.info(f"Module {module} has been modified, recreating zip")
                    return True

        return False

    def _create_spark_zip(self):
        with zipfile.ZipFile(self.spark_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for module in SparkConfig.PROJECT_MODULES:
                if os.path.exists(module) and os.path.isdir(module):
                        for root, _, files in os.walk(module):
                            for file in files:
                                if file.endswith('.py'):
                                    file_path = os.path.join(root, file)
                                    arcname = os.path.relpath(file_path, '.')
                                    zipf.write(file_path, arcname)
                elif module.endswith('.py'):
                    zipf.write(module, module)

    def _trigger_spark_job(self, original_input_file_path):
        logger.info(f"Processing file: {original_input_file_path}")
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        job_output_base_dir = os.path.join(self.output_base_directory, timestamp_str)
        os.makedirs(job_output_base_dir, exist_ok=True)

        profiler = create_profiler_for_job(job_output_base_dir)
        profiler.start_monitoring()
        set_active_profiler(profiler)

        input_filename = os.path.basename(original_input_file_path)
        profiler.log_process_event("file_detected", f"TSV file detected: {input_filename}")

        job_input_dir = os.path.join(job_output_base_dir, "input")
        job_output_root_dir = os.path.join(job_output_base_dir, "output")
        job_output_parquet_dir = os.path.join(job_output_root_dir, "parquet")

        os.makedirs(job_input_dir, exist_ok=True)
        os.makedirs(job_output_parquet_dir, exist_ok=True)

        job_output_tsv_dir = None

        if SparkConfig.WRITE_TSV:
            job_output_tsv_dir = os.path.join(job_output_root_dir, "tsv")
            os.makedirs(job_output_tsv_dir, exist_ok=True)

        current_input_file_path = os.path.join(job_input_dir, input_filename)
        shutil.move(original_input_file_path, current_input_file_path)

        profiler.log_process_event("file_setup", f"File moved to job directory: {input_filename}")

        spark_output_path = job_output_parquet_dir

        if not os.path.exists(SparkConfig.SPARK_JOB_PATH):
            return

        if not os.path.exists(current_input_file_path):
            profiler.log_process_event("error", "Input file missing after move")
            profiler.stop_monitoring()
            return

        logger.info(f"Setting up Spark environment for {input_filename}")
        profiler.log_process_event("spark_setup_start", "Setting up Spark environment")
        setup_spark_environment()
        profiler.log_process_event("spark_setup_end", "Spark environment setup completed")

        java_options = get_java_opens_options()
        spark_submit_command = get_spark_memory_config()

        if os.path.exists(SparkConfig.POSTGRESQL_JDBC_DRIVER_PATH):
            spark_submit_command.extend(["--jars", SparkConfig.POSTGRESQL_JDBC_DRIVER_PATH])

        driver_java_opts = " ".join(java_options)
        executor_java_opts = " ".join(java_options)

        spark_submit_command.extend([
            "--conf", f"spark.driver.extraJavaOptions={driver_java_opts}",
            "--conf", f"spark.executor.extraJavaOptions={executor_java_opts}",
            "--py-files", self.spark_zip_path,
            SparkConfig.SPARK_JOB_PATH,
            current_input_file_path,
            spark_output_path
        ])

        try:
            env = os.environ.copy()
            env.update(SparkConfig.ENVIRONMENT_VARIABLES)

            logger.info(f"Launching Spark job - process_urls.py now handling {input_filename}")
            print(f"Spark job submitted for {input_filename}")
            profiler.log_process_event("spark_job_start", f"Spark job started for {input_filename}")

            subprocess.run(
                spark_submit_command,
                check=True,
                env=env,
                text=True,
                timeout=SparkConfig.SUBPROCESS_TIMEOUT,
                stderr=subprocess.PIPE
            )

            logger.info(f"Spark job completed - process_urls.py finished processing {input_filename}")
            profiler.log_process_event("spark_job_end", f"Spark job completed for {input_filename}")

            if SparkConfig.WRITE_TSV and job_output_tsv_dir:
                logger.info(f"Starting parquet to TSV conversion for {input_filename}")
                profiler.log_process_event("tsv_conversion_start", "Starting parquet to TSV conversion")
                tsv_output_file_path = os.path.join(job_output_tsv_dir, "data.tsv")
                convert_parquet_to_tsv(spark_output_path, tsv_output_file_path)
                logger.info(f"TSV conversion completed for {input_filename}")
                profiler.log_process_event("tsv_conversion_end", "TSV conversion completed")

        except subprocess.CalledProcessError as e:
            logger.error(f"Spark job failed: {e.stderr}")
            profiler.log_process_event("spark_job_error", f"Spark job failed: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            profiler.log_process_event("error", f"Unexpected error: {str(e)}")
        finally:
            profiler.log_process_event("job_completed", f"Job processing completed for {input_filename}")
            profiler.stop_monitoring()

    def start(self):
        logger.info(f"Starting file watcher on {self.watch_directory}")
        for filename in os.listdir(self.watch_directory):
            file_path = os.path.join(self.watch_directory, filename)
            if os.path.isfile(file_path) and filename.endswith('.tsv'):
                logger.info(f"Found existing TSV file: {filename}")
                self._trigger_spark_job(file_path)

        logger.info("File watcher initialized - monitoring for new TSV files")
        event_handler = FileEventHandler(self._trigger_spark_job)
        observer = Observer()
        observer.schedule(event_handler, self.watch_directory, recursive=False)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping file watcher")
            observer.stop()
        observer.join()

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, trigger_spark_job_callback):
        self.trigger_spark_job_callback = trigger_spark_job_callback

    def on_created(self, event):
        if not event.is_directory and str(event.src_path).endswith('.tsv'):
            print(f"TSV file detected: {os.path.basename(event.src_path)}")
            time.sleep(2)
            self.trigger_spark_job_callback(event.src_path)

if __name__ == "__main__":
    os.makedirs(SparkConfig.WATCH_DIRECTORY, exist_ok=True)
    os.makedirs(SparkConfig.OUTPUT_BASE_DIRECTORY, exist_ok=True)

    watcher = FileWatcher()
    print(f"Monitoring input directory: {watcher.watch_directory}")
    print(f"Output will be saved to: {watcher.output_base_directory}")
    print("Waiting for new TSV files...")
    watcher.start()
