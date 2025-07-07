import os
import shutil
import subprocess
import sys
import time
import zipfile
import logging

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class FileWatcher:
    def __init__(self, watch_directory, staging_directory, processed_directory):
        self.watch_directory = watch_directory
        self.staging_directory = staging_directory
        self.processed_directory = processed_directory
        self.spark_zip_path = "spark_modules.zip"
        
        # Configure logging to suppress Spark INFO logs
        self._configure_logging()

        if self._should_recreate_zip():
            self._create_spark_zip()

    def _configure_logging(self):
        """Configure logging to suppress Spark INFO logs while keeping our custom logs"""
        # Set up root logger to WARNING level to suppress most Spark logs
        logging.getLogger().setLevel(logging.WARNING)
        
        # Suppress specific Spark loggers
        spark_loggers = [
            'org.apache.spark',
            'org.sparkproject',
            'org.apache.hadoop',
            'org.eclipse.jetty',
            'org.apache.parquet',
            'parquet',
            'py4j',
            'akka',
            'hive',
            'org.apache.spark.sql.catalyst.expressions.CodeGenerator',
            'org.apache.spark.sql.execution.datasources.InMemoryFileIndex',
            'org.apache.spark.sql.catalyst.optimizer.Optimizer',
            'org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec',
            'org.apache.spark.sql.execution.streaming.state.StateStore',
            'org.apache.spark.storage.BlockManager',
            'org.apache.spark.scheduler.TaskSetManager',
            'org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend',
            'org.apache.spark.SparkContext',
            'org.apache.spark.sql.SparkSession',
            'org.apache.spark.sql.internal.SharedState'
        ]
        
        for logger_name in spark_loggers:
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    def _should_recreate_zip(self):
        if not os.path.exists(self.spark_zip_path):
            return True

        if not os.path.exists("spark") or not os.path.isdir("spark"):
            return False

        zip_mtime = os.path.getmtime(self.spark_zip_path)
        for root, dirs, files in os.walk("spark"):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    if os.path.getmtime(file_path) > zip_mtime:
                        print(f"Spark module {file_path} has been modified, recreating zip...")
                        return True
        return False

    def _create_spark_zip(self):
        with zipfile.ZipFile(self.spark_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:

            if os.path.exists("spark") and os.path.isdir("spark"):
                for root, dirs, files in os.walk("spark"):
                    for file in files:
                        if file.endswith('.py'):
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, '.')
                            zipf.write(file_path, arcname)


            project_modules = ["config", "models.py", "schemas.py"]
            for module in project_modules:
                if os.path.exists(module):
                    if os.path.isdir(module):
                        for root, dirs, files in os.walk(module):
                            for file in files:
                                if file.endswith('.py'):
                                    file_path = os.path.join(root, file)
                                    arcname = os.path.relpath(file_path, '.')
                                    zipf.write(file_path, arcname)
                    elif module.endswith('.py'):
                        zipf.write(module, module)

        print(f"Created {self.spark_zip_path} for Spark job distribution")

    def _move_file_to_staging(self, src_path):
        filename = os.path.basename(src_path)
        dest_path = os.path.join(self.staging_directory, filename)
        shutil.move(src_path, dest_path)
        print(f"Moved {src_path} to {dest_path}")
        return dest_path

    def _archive_processed_file(self, src_path):
        filename = os.path.basename(src_path)
        dest_path = os.path.join(self.processed_directory, filename)
        shutil.move(src_path, dest_path)
        print(f"Archived {src_path} to {dest_path}")

    def _setup_spark_environment(self):

        python_executable = sys.executable


        os.environ["PYSPARK_PYTHON"] = python_executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable


        os.environ["MKL_SERVICE_FORCE_INTEL"] = "1"


        os.environ["HADOOP_HOME"] = "/tmp"
        os.environ["HADOOP_CONF_DIR"] = "/tmp"


        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    def _get_java_opens_options(self):
        java_opens = [
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
        ]
        return java_opens

    def _trigger_spark_job(self, input_file_path):
        print("=" * 80)
        print(f"DEBUG: Starting Spark job trigger for: {input_file_path}")
        print(f"DEBUG: Current working directory: {os.getcwd()}")
        print("=" * 80)
        
        # Check if spark job file exists
        spark_job_path = "spark/jobs/url_processor.py"
        if not os.path.exists(spark_job_path):
            print(f"ERROR: Spark job file not found at {spark_job_path}")
            print(f"Available files in spark/jobs/:")
            if os.path.exists("spark/jobs"):
                for f in os.listdir("spark/jobs"):
                    print(f"  - {f}")
            return
        else:
            print(f"DEBUG: Found spark job file at {spark_job_path}")
        
        # Check if input file exists
        if not os.path.exists(input_file_path):
            print(f"ERROR: Input file not found at {input_file_path}")
            return
        else:
            print(f"DEBUG: Input file exists: {input_file_path}")
        
        output_file_name = os.path.splitext(os.path.basename(input_file_path))[0] + ".parquet"
        output_directory = os.path.join(self.processed_directory, "spark_output")
        os.makedirs(output_directory, exist_ok=True)
        output_file_path = os.path.join(output_directory, output_file_name)
        print(f"DEBUG: Output will be written to: {output_file_path}")

        self._setup_spark_environment()

        java_options = self._get_java_opens_options()
        jdbc_jar_path = "/Users/akshithchowdary/jars/postgresql-42.7.7.jar"
        if not os.path.exists(jdbc_jar_path):
            print(f"WARNING: JDBC jar not found at {jdbc_jar_path}")
        else:
            print(f"DEBUG: JDBC jar found at {jdbc_jar_path}")

        spark_submit_command = [
            "spark-submit",
            "--master", "local[*]",
            "--jars", jdbc_jar_path,
            "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--conf", "spark.ui.showConsoleProgress=false",
            "--conf", "spark.sql.execution.arrow.pyspark.enabled=true",
            "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
            "--conf", "spark.sql.adaptive.localShuffleReader.enabled=true",
            # Add logging configuration to suppress INFO logs
            "--conf", "spark.log.level=WARN",
            # Added for performance tuning
            "--driver-memory", "32g",
            "--executor-memory", "16g",
            "--executor-cores", "8",
            "--num-executors", "2"
        ]

        driver_java_opts = " ".join(java_options)
        executor_java_opts = " ".join(java_options)

        spark_submit_command.extend([
            "--conf", f"spark.driver.extraJavaOptions={driver_java_opts}",
            "--conf", f"spark.executor.extraJavaOptions={executor_java_opts}",
            "--py-files", self.spark_zip_path,
            "spark/jobs/url_processor.py",
            input_file_path,
            output_file_path
        ])

        print("DEBUG: Triggering Spark job with command:")
        print(" ".join(spark_submit_command))
        print("=" * 80)

        try:
            env = os.environ.copy()
            env["SPARK_CONF_DIR"] = ""
            env["PYTHONHASHSEED"] = "0"
            
            # Add logging configuration to environment
            env["SPARK_LOG_LEVEL"] = "WARN"

            print("DEBUG: About to execute spark-submit...")
            # Remove capture_output=True to see real-time logs
            result = subprocess.run(
                spark_submit_command,
                check=True,
                env=env,
                text=True,
                timeout=3600
            )
            print("=" * 80)
            print(f"DEBUG: Spark job process completed with return code: {result.returncode}")
            print(f"DEBUG: Spark job completed successfully for {input_file_path}")
            print("=" * 80)

            self._archive_processed_file(input_file_path)

        except subprocess.TimeoutExpired:
            print("=" * 80)
            print(f"ERROR: Spark job timed out for {input_file_path}")
            print("=" * 80)

        except subprocess.CalledProcessError as e:
            print("=" * 80)
            print(f"ERROR: Spark job failed for {input_file_path}")
            print(f"Return code: {e.returncode}")
            if hasattr(e, 'stdout') and e.stdout:
                print("STDOUT:", e.stdout)
            if hasattr(e, 'stderr') and e.stderr:
                print("STDERR:", e.stderr)
            print("=" * 80)

        except FileNotFoundError:
            print("=" * 80)
            print("ERROR: 'spark-submit' command not found. Ensure Apache Spark is installed and in your PATH.")
            print("=" * 80)

        except Exception as e:
            print("=" * 80)
            print(f"ERROR: Unexpected exception during spark-submit: {e}")
            import traceback
            traceback.print_exc()
            print("=" * 80)

    def start(self):

        print(f"Checking for existing files in {self.watch_directory}")
        for filename in os.listdir(self.watch_directory):
            file_path = os.path.join(self.watch_directory, filename)
            if os.path.isfile(file_path) and filename.endswith('.tsv'):
                print(f"Processing existing file: {file_path}")
                staged_file_path = self._move_file_to_staging(file_path)
                self._trigger_spark_job(staged_file_path)

        event_handler = FileEventHandler(self._move_file_to_staging, self._trigger_spark_job)
        observer = Observer()
        observer.schedule(event_handler, self.watch_directory, recursive=False)
        observer.start()
        print(f"Watching directory: {self.watch_directory}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, move_to_staging_callback, trigger_spark_job_callback):
        self.move_to_staging_callback = move_to_staging_callback
        self.trigger_spark_job_callback = trigger_spark_job_callback

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.tsv'):
            print(f"New TSV file detected: {event.src_path}")

            time.sleep(2)
            staged_file_path = self.move_to_staging_callback(event.src_path)
            self.trigger_spark_job_callback(staged_file_path)

if __name__ == "__main__":
    WATCH_DIRECTORY = "indexnow/data"
    STAGING_DIRECTORY = "data/staging"
    PROCESSED_DIRECTORY = "data/processed"

    os.makedirs(WATCH_DIRECTORY, exist_ok=True)
    os.makedirs(STAGING_DIRECTORY, exist_ok=True)
    os.makedirs(PROCESSED_DIRECTORY, exist_ok=True)

    watcher = FileWatcher(WATCH_DIRECTORY, STAGING_DIRECTORY, PROCESSED_DIRECTORY)
    watcher.start()