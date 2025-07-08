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

from utils.parquet_to_csv import convert_parquet_to_csv
from utils.crawl_summary import generate_crawl_summary

class FileWatcher:
    def __init__(self, watch_directory, output_base_directory):
        self.watch_directory = watch_directory
        self.output_base_directory = output_base_directory
        self.spark_zip_path = "spark_modules.zip"

        self._configure_logging()

        should_recreate = self._should_recreate_zip()
        if should_recreate:
            self._create_spark_zip()

    def _configure_logging(self):
        logging.getLogger().setLevel(logging.WARNING)

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

        zip_mtime = os.path.getmtime(self.spark_zip_path)

        if os.path.exists("spark") and os.path.isdir("spark"):
            for root, dirs, files in os.walk("spark"):
                for file in files:
                    if file.endswith('.py'):
                        file_path = os.path.join(root, file)
                        if os.path.getmtime(file_path) > zip_mtime:
                            print(f"Spark module {file_path} has been modified, recreating zip...")
                            return True

        project_modules = ["config", "models.py", "schemas.py", "services", "utils"]
        for module in project_modules:
            if os.path.exists(module):
                if os.path.isdir(module):
                    for root, dirs, files in os.walk(module):
                        for file in files:
                            if file.endswith('.py'):
                                file_path = os.path.join(root, file)
                                if os.path.getmtime(file_path) > zip_mtime:
                                    print(f"Project module {file_path} has been modified, recreating zip...")
                                    return True
                elif module.endswith('.py'):
                    if os.path.getmtime(module) > zip_mtime:
                        print(f"Project file {module} has been modified, recreating zip...")
                        return True

        return False

    def _get_spark_memory_config(self):
        return [
            "spark-submit",
            "--master", "local[*]",
            "--driver-memory", "32g",
            "--executor-memory", "16g",
            "--executor-cores", "8",
            "--num-executors", "2",
            "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--conf", "spark.ui.showConsoleProgress=false",
            "--conf", "spark.sql.execution.arrow.pyspark.enabled=true",
            "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
            "--conf", "spark.sql.adaptive.localShuffleReader.enabled=true",
            "--conf", "spark.log.level=WARN",
            "--conf", "spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB",
            "--conf", "spark.sql.adaptive.coalescePartitions.minPartitionSize=64MB",
            "--conf", "spark.sql.adaptive.coalescePartitions.parallelismFirst=false",
            "--conf", "spark.sql.columnar.cache.enabled=false",
            "--conf", "spark.sql.inMemoryColumnarStorage.compressed=true",
            "--conf", "spark.sql.inMemoryColumnarStorage.batchSize=10000",
            "--conf", "spark.sql.adaptive.logLevel=WARN",
            "--conf", "spark.ui.retainedJobs=50",
            "--conf", "spark.ui.retainedStages=100",
            "--conf", "spark.sql.parquet.compression.codec=snappy",
            "--conf", "spark.sql.parquet.block.size=268435456",
            "--conf", "spark.sql.parquet.page.size=1048576",
            "--conf", "spark.sql.parquet.dictionary.enabled=true",
            "--conf", "spark.sql.parquet.writer.block.size=268435456",
            "--conf", "spark.sql.parquet.enableVectorizedReader=true",
            "--conf", "spark.sql.parquet.enableVectorizedWriter=true",
            "--conf", "spark.sql.parquet.recordLevelFilter.enabled=true",
            "--conf", "spark.sql.parquet.aggregatePushdown=true",
            "--conf", "spark.sql.parquet.respectSummaryFiles=false",
            "--conf", "spark.sql.files.maxPartitionBytes=268435456",
            "--conf", "spark.sql.files.openCostInBytes=4194304",
            "--conf", "spark.sql.files.minPartitionNum=1",
            "--conf", "spark.sql.shuffle.partitions=400",
            "--conf", "spark.sql.execution.arrow.maxRecordsPerBatch=20000",
            "--conf", "spark.sql.execution.arrow.pyspark.enabled=true",
            "--conf", "spark.sql.execution.arrow.pyspark.fallback.enabled=true",
            "--conf", "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=0",
            "--conf", "spark.sql.adaptive.localShuffleReader.enabled=true",
            "--conf", "spark.sql.adaptive.skewJoin.skewedPartitionFactor=5",
            "--conf", "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB",
            "--conf", "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.2",
            "--conf", "spark.sql.adaptive.coalescePartitions.parallelismFirst=false",
            "--conf", "spark.sql.adaptive.coalescePartitions.initialPartitionNum=400",
            "--conf", "spark.sql.adaptive.coalescePartitions.minPartitionSize=64MB",
            "--conf", "spark.serializer.objectStreamReset=100",
            "--conf", "spark.rdd.compress=true",
            "--conf", "spark.shuffle.compress=true",
            "--conf", "spark.shuffle.spill.compress=true",
            "--conf", "spark.io.compression.codec=snappy",
            "--conf", "spark.network.timeout=600s",
            "--conf", "spark.sql.broadcastTimeout=600",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        ]

    def _generate_job_summary(self, job_output_base_dir):
        try:
            parquet_path = os.path.join(job_output_base_dir, "output", "parquet")
            if os.path.exists(parquet_path):
                print(f"DEBUG: Generating summary for {parquet_path}")
                summary_path = generate_crawl_summary(parquet_path, job_output_base_dir)
                print(f"DEBUG: Summary generated: {summary_path}")
            else:
                print(f"DEBUG: No parquet data found at {parquet_path}")
        except Exception as e:
            print(f"ERROR: Failed to generate summary: {e}")
            import traceback
            traceback.print_exc()

    def _create_spark_zip(self):
        with zipfile.ZipFile(self.spark_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:

            if os.path.exists("spark") and os.path.isdir("spark"):
                for root, dirs, files in os.walk("spark"):
                    for file in files:
                        if file.endswith('.py'):
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, '.')
                            zipf.write(file_path, arcname)

            project_modules = ["config", "models.py", "schemas.py", "services", "utils"]
            for module in project_modules:
                if os.path.exists(module):
                    if os.path.isdir(module):
                        for root, _, files in os.walk(module):
                            for file in files:
                                if file.endswith('.py'):
                                    file_path = os.path.join(root, file)
                                    arcname = os.path.relpath(file_path, '.')
                                    zipf.write(file_path, arcname)
                    elif module.endswith('.py'):
                        zipf.write(module, module)
                else:
                    print(f"WARNING: Module path does not exist: {module}. Skipping.")

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

    def _trigger_spark_job(self, original_input_file_path):
        print("=" * 80)
        print(f"DEBUG: Starting Spark job trigger for: {original_input_file_path}")
        print(f"DEBUG: Current working directory: {os.getcwd()}")
        print("=" * 80)

        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        job_output_base_dir = os.path.join(self.output_base_directory, timestamp_str)
        os.makedirs(job_output_base_dir, exist_ok=True)

        job_input_dir = os.path.join(job_output_base_dir, "input")
        job_output_root_dir = os.path.join(job_output_base_dir, "output")
        job_output_parquet_dir = os.path.join(job_output_root_dir, "parquet")
        job_output_csv_dir = os.path.join(job_output_root_dir, "csv")

        os.makedirs(job_input_dir, exist_ok=True)
        os.makedirs(job_output_parquet_dir, exist_ok=True)
        os.makedirs(job_output_csv_dir, exist_ok=True)

        input_filename = os.path.basename(original_input_file_path)
        current_input_file_path = os.path.join(job_input_dir, input_filename)
        shutil.move(original_input_file_path, current_input_file_path)
        print(f"DEBUG: Moved input file {original_input_file_path} to {current_input_file_path}")

        spark_output_path = job_output_parquet_dir
        print(f"DEBUG: Spark output will be written to: {spark_output_path}")

        spark_job_path = "spark/jobs/url_processor.py"
        if not os.path.exists(spark_job_path):
            print(f"ERROR: Spark job file not found at {spark_job_path}")
            if os.path.exists("spark/jobs"):
                for f in os.listdir("spark/jobs"):
                    print(f"  - {f}")
            return

        if not os.path.exists(current_input_file_path):
            print(f"ERROR: Input file not found at {current_input_file_path}")
            return

        self._setup_spark_environment()

        java_options = self._get_java_opens_options()
        jdbc_jar_path = "/Users/akshithchowdary/jars/postgresql-42.7.7.jar"
        if not os.path.exists(jdbc_jar_path):
            print(f"WARNING: JDBC jar not found at {jdbc_jar_path}")

        spark_submit_command = self._get_spark_memory_config()

        if os.path.exists(jdbc_jar_path):
            spark_submit_command.extend(["--jars", jdbc_jar_path])

        driver_java_opts = " ".join(java_options)
        executor_java_opts = " ".join(java_options)

        spark_submit_command.extend([
            "--conf", f"spark.driver.extraJavaOptions={driver_java_opts}",
            "--conf", f"spark.executor.extraJavaOptions={executor_java_opts}",
            "--py-files", self.spark_zip_path,
            "spark/jobs/url_processor.py",
            current_input_file_path,
            spark_output_path
        ])

        print("DEBUG: Triggering Spark job with command:")
        print(" ".join(spark_submit_command))
        print("=" * 80)

        try:
            env = os.environ.copy()
            env["SPARK_CONF_DIR"] = ""
            env["PYTHONHASHSEED"] = "0"
            env["SPARK_LOG_LEVEL"] = "WARN"

            print("DEBUG: About to execute spark-submit...")
            result = subprocess.run(
                spark_submit_command,
                check=True,
                env=env,
                text=True,
                timeout=7200
            )
            print("=" * 80)
            print(f"DEBUG: Spark job process completed with return code: {result.returncode}")
            print(f"DEBUG: Spark job completed successfully for {current_input_file_path}")
            print("=" * 80)

            csv_output_file_path = os.path.join(job_output_csv_dir, "data.csv")
            print(f"DEBUG: Converting Parquet to CSV. Input: {spark_output_path}, Output: {csv_output_file_path}")
            convert_parquet_to_csv(spark_output_path, csv_output_file_path)
            print(f"DEBUG: Parquet to CSV conversion completed for {spark_output_path}")

        except subprocess.TimeoutExpired:
            print("=" * 80)
            print(f"ERROR: Spark job timed out for {current_input_file_path}")
            print("=" * 80)

        except subprocess.CalledProcessError as e:
            print("=" * 80)
            print(f"ERROR: Spark job failed for {current_input_file_path}")
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

        self._generate_job_summary(job_output_base_dir)

    def start(self):
        print(f"Checking for existing files in {self.watch_directory}")
        for filename in os.listdir(self.watch_directory):
            file_path = os.path.join(self.watch_directory, filename)
            if os.path.isfile(file_path) and filename.endswith('.tsv'):
                print(f"Processing existing file: {file_path}")
                self._trigger_spark_job(file_path)

        event_handler = FileEventHandler(self._trigger_spark_job)
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
    def __init__(self, trigger_spark_job_callback):
        self.trigger_spark_job_callback = trigger_spark_job_callback

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.tsv'):
            print(f"New TSV file detected: {event.src_path}")
            time.sleep(2)
            self.trigger_spark_job_callback(event.src_path)

if __name__ == "__main__":
    WATCH_DIRECTORY = "data/input"
    OUTPUT_BASE_DIRECTORY = "data/output"

    os.makedirs(WATCH_DIRECTORY, exist_ok=True)
    os.makedirs(OUTPUT_BASE_DIRECTORY, exist_ok=True)

    watcher = FileWatcher(WATCH_DIRECTORY, OUTPUT_BASE_DIRECTORY)
    watcher.start()