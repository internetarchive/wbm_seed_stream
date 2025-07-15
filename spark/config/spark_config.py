from pyspark.sql import SparkSession
import os
import sys
import logging

class SparkConfig:
    
    # booleans
    CREATE_TSV = False
    USE_GOOD_DATA = True
    # WRITE_TO_DB = False
    # CREATE_SUMMARY = True
    
    # numbers
    SUBPROCESS_TIMEOUT = 18000
    WIKI_DAYS = 1
    MEDIACLOUD_DAYS = 1
    
    # user-set file paths
    SPARK_ZIP_PATH = "spark_modules.zip"
    SPARK_JOB_PATH = "spark_logic/process_urls.py"
    WATCH_DIRECTORY = "data/input"
    OUTPUT_BASE_DIRECTORY = "data/output"
    GOOD_DATA_FOLDER = "data/storage/good_data"
    GOOD_DATA_LABEL = "good_data"
    
    # computer-specific file paths
    POSTGRESQL_JDBC_DRIVER_PATH = "/Users/akshithchowdary/jars/postgresql-42.7.7.jar"

    SPARK_MEMORY_CONFIG = {
        "master": "local[12]",
        "driver_memory": "12g",
        "executor_memory": "48g",
        "executor_cores": "12",
        "num_executors": "1"
    }

    SPARK_OPTIMIZATION_CONFIG = {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrationRequired": "false",
        "spark.kryo.unsafe": "true",
        "spark.kryoserializer.buffer": "128k",
        "spark.kryoserializer.buffer.max": "128m",
        "spark.kryo.referenceTracking": "false",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.execution.pyspark.udf.faulthandler.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.log.level": "WARN",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "128MB",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "100",
        "spark.sql.columnar.cache.enabled": "true",
        "spark.sql.inMemoryColumnarStorage.compressed": "true",
        "spark.sql.inMemoryColumnarStorage.batchSize": "10000",
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        "spark.sql.parquet.compression.codec": "lz4",
        "spark.sql.parquet.block.size": "134217728",
        "spark.sql.parquet.page.size": "1048576",
        "spark.sql.parquet.dictionary.enabled": "true",
        "spark.sql.parquet.writer.block.size": "134217728",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.parquet.recordLevelFilter.enabled": "true",
        "spark.sql.parquet.aggregatePushdown": "true",
        "spark.sql.files.maxPartitionBytes": "134217728",
        "spark.sql.files.openCostInBytes": "8388608",
        "spark.sql.shuffle.partitions": "100",
        "spark.shuffle.compress": "true",
        "spark.shuffle.spill.compress": "true",
        "spark.io.compression.codec": "lz4",
        "spark.rdd.compress": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "200000",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "false",
        "spark.network.timeout": "800s",
        "spark.sql.broadcastTimeout": "600",
        "spark.executor.heartbeatInterval": "60s",
        "spark.rpc.askTimeout": "600s",
        "spark.rpc.lookupTimeout": "600s",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        "spark.sql.codegen.wholeStage": "true",
        "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        "spark.local.dir": "/tmp/spark-local",
        "spark.executor.maxResultSize": "4g",
        "spark.driver.maxResultSize": "4g",
        "spark.sql.execution.arrow.pyspark.selfDestruct.enabled": "true",
        "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
        "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.parquet.writeLegacyFormat": "false",
        "spark.task.maxFailures": "3",
        "spark.stage.maxConsecutiveAttempts": "8",
        "spark.executor.cores": "12",
        "spark.task.cpus": "1",
        "spark.default.parallelism": "120",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1024MB",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "256MB"
    }

    JAVA_OPENS_OPTIONS = [
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

    ENVIRONMENT_VARIABLES = {
        "MKL_SERVICE_FORCE_INTEL": "1",
        "HADOOP_HOME": "/tmp",
        "HADOOP_CONF_DIR": "/tmp",
        "SPARK_LOCAL_IP": "127.0.0.1",
        "SPARK_CONF_DIR": "",
        "PYTHONHASHSEED": "0",
        "SPARK_LOG_LEVEL": "WARN",
        "SPARK_KRYO_UNSAFE": "true"
    }

    PROJECT_MODULES = ["config", "spark_logic", "spark", "utils", "models.py"]
    SPARK_LOGGERS = [
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

def get_spark_session(app_name="sentinel", master="local[*]", job_description=None):
    final_app_name = job_description if job_description else app_name
    
    builder = SparkSession.builder \
        .appName(final_app_name) \
        .master(master) \
        .config("spark.executor.memory", SparkConfig.SPARK_MEMORY_CONFIG["executor_memory"]) \
        .config("spark.driver.memory", SparkConfig.SPARK_MEMORY_CONFIG["driver_memory"]) \
        .config("spark.executor.cores", SparkConfig.SPARK_MEMORY_CONFIG["executor_cores"]) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrationRequired", "false") \
        .config("spark.kryo.unsafe", "true")

    if job_description:
        builder = builder.config("spark.app.description", job_description)

    for key, value in SparkConfig.SPARK_OPTIMIZATION_CONFIG.items():
        builder = builder.config(key, value)

    if os.path.exists(SparkConfig.POSTGRESQL_JDBC_DRIVER_PATH):
        builder = builder.config("spark.jars", SparkConfig.POSTGRESQL_JDBC_DRIVER_PATH)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def configure_spark_logging():
    logging.getLogger().setLevel(logging.WARNING)

    for logger_name in SparkConfig.SPARK_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

def get_spark_memory_config():
    command = [
        "spark-submit",
        "--master", SparkConfig.SPARK_MEMORY_CONFIG["master"],
        "--driver-memory", SparkConfig.SPARK_MEMORY_CONFIG["driver_memory"],
        "--executor-memory", SparkConfig.SPARK_MEMORY_CONFIG["executor_memory"],
        "--executor-cores", SparkConfig.SPARK_MEMORY_CONFIG["executor_cores"],
        "--num-executors", SparkConfig.SPARK_MEMORY_CONFIG["num_executors"]
    ]

    for key, value in SparkConfig.SPARK_OPTIMIZATION_CONFIG.items():
        command.extend(["--conf", f"{key}={value}"])

    return command

def get_java_opens_options():
    return SparkConfig.JAVA_OPENS_OPTIONS

def setup_spark_environment():
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    for key, value in SparkConfig.ENVIRONMENT_VARIABLES.items():
        os.environ[key] = value