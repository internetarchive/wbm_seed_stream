from pyspark.sql import SparkSession
import os
import sys
import logging

class SparkConfig:
    USE_GOOD_DATA = True # works

    READ_REPUTATION = False # works
    WRITE_REPUTATION = False # works

    WRITE_DB = True # works -- enabled
    WRITE_PARQUET = False # works
    WRITE_TSV = False # not working yet

    WRITE_SUMMARY = True # works

    SUBPROCESS_TIMEOUT = 18000
    WIKI_DAYS = 0
    MEDIACLOUD_DAYS = 1

    SPARK_ZIP_PATH = "spark_modules.zip"
    SPARK_JOB_PATH = "spark_logic/process_urls.py"
    WATCH_DIRECTORY = "data/input"
    OUTPUT_BASE_DIRECTORY = "data/output"
    GOOD_DATA_FOLDER = "data/storage/good_data"

    POSTGRESQL_JDBC_DRIVER_PATH = "/Users/akshithchowdary/jars/postgresql-42.7.7.jar"

    SPARK_MEMORY_CONFIG = {
        "master": "local[12]",
        "driver_memory": "56g",
        "executor_memory": "56g",
        "executor_cores": "12",
        "num_executors": "1"
    }

    SPARK_OPTIMIZATION_CONFIG = {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrationRequired": "false",
        "spark.kryo.unsafe": "true",
        "spark.kryoserializer.buffer.max": "2047m",
        "spark.sql.adaptive.enabled": "true",
        "spark.python.worker.faulthandler.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        "spark.sql.shuffle.partitions": "96",
        "spark.default.parallelism": "96",
        "spark.driver.maxResultSize": "32g",
        "spark.executor.maxResultSize": "32g",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "16g",
        "spark.network.timeout": "800s",
        "spark.executor.heartbeatInterval": "30s",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.python.worker.reuse": "true",
        "spark.python.worker.memory": "24g",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "100000",
        "spark.sql.inMemoryColumnarStorage.batchSize": "100000",
        "spark.memory.storageFraction": "0.15",
        "spark.memory.executionFraction": "0.85",

        "spark.task.maxFailures": "3",
        "spark.stage.maxConsecutiveAttempts": "3",
        "spark.excludeOnFailure.enabled": "false",
        "spark.speculation": "false",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled": "true",
        "spark.rpc.askTimeout": "800s",
        "spark.rpc.lookupTimeout": "800s",
        "spark.sql.broadcastTimeout": "800s",
        "spark.driver.maxDirectResultSize": "24g",
        "spark.sql.execution.arrow.pyspark.selfDestruct.enabled": "true",
        "spark.sql.streaming.metricsEnabled": "false",
        "spark.metrics.conf.*.sink.slf4j.class": "org.apache.spark.metrics.sink.Slf4jSink",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin": "0.2",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        "spark.sql.sources.parallelPartitionDiscovery.threshold": "96",
        "spark.sql.sources.parallelPartitionDiscovery.parallelism": "12",
        "spark.sql.files.maxPartitionBytes": "134217728",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum": "12",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "96",
        "spark.sql.execution.sortBeforeRepartition": "true",
        "spark.locality.wait": "0s",
        "spark.locality.wait.node": "0s",
        "spark.locality.wait.process": "0s",
        "spark.locality.wait.rack": "0s",
        "spark.scheduler.mode": "FIFO",
        "spark.task.cpus": "1",
        "spark.executor.instances": "1",
        "spark.dynamicAllocation.enabled": "false",
        "spark.shuffle.service.enabled": "false",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "256MB",
        "spark.sql.adaptive.join.enabled": "true",
        "spark.sql.cbo.enabled": "true",
        "spark.sql.cbo.joinReorder.enabled": "true",
        "spark.sql.statistics.histogram.enabled": "true",
        "spark.serializer.objectStreamReset": "2000",
        "spark.kryo.referenceTracking": "false",
        "spark.kryoserializer.buffer": "2048k",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.session.locale": "en_US",
        "spark.rdd.compress": "true",
        "spark.io.compression.codec": "lz4",
        "spark.shuffle.compress": "true",
        "spark.shuffle.spill.compress": "true",
        "spark.broadcast.compress": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.binaryAsString": "false",
        "spark.sql.parquet.int96AsTimestamp": "true",
        "spark.sql.parquet.cacheMetadata": "true",
        "spark.sql.parquet.columnarReaderBatchSize": "16384",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.orc.filterPushdown": "true",
        "spark.sql.orc.splits.include.file.footer": "true",
        "spark.sql.orc.cache.stripe.details.size": "10000",
        "spark.sql.orc.enableVectorizedReader": "true",
        "spark.sql.hive.metastorePartitionPruning": "true",
        "spark.sql.hive.filesourcePartitionFileCacheSize": "262144000",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true"
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
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.net=ALL-UNNAMED"
    ]

    ENVIRONMENT_VARIABLES = {
        "MKL_SERVICE_FORCE_INTEL": "1",
        "HADOOP_HOME": "/tmp",
        "HADOOP_CONF_DIR": "/tmp",
        "SPARK_LOCAL_IP": "127.0.0.1",
        "SPARK_CONF_DIR": "",
        "PYTHONHASHSEED": "0",
        "SPARK_LOG_LEVEL": "WARN",
        "SPARK_KRYO_UNSAFE": "true",
        "TZ": "UTC",
        "LC_ALL": "en_US.UTF-8",
        "OMP_NUM_THREADS": "12",
        "MKL_NUM_THREADS": "12",
        "OPENBLAS_NUM_THREADS": "12"
    }

    PROJECT_MODULES = ["config", "spark_logic", "spark", "utils", "models.py", "writers", "testing"]
    SPARK_LOGGERS = [
        'org.apache.spark',
        'org.sparkproject',
        'org.apache.hadoop',
        'org.eclipse.jetty',
        'py4j',
    ]

def get_spark_session(app_name="sentinel", master="local[*]", job_description=None):
    final_app_name = job_description if job_description else app_name

    builder = SparkSession.builder \
        .appName(final_app_name) \
        .master(master) \
        .config("spark.executor.memory", SparkConfig.SPARK_MEMORY_CONFIG["executor_memory"]) \
        .config("spark.driver.memory", SparkConfig.SPARK_MEMORY_CONFIG["driver_memory"]) \
        .config("spark.executor.cores", SparkConfig.SPARK_MEMORY_CONFIG["executor_cores"])

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

    python_path = os.environ.get("PYTHONPATH", "")
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if current_dir not in python_path:
        python_path = f"{current_dir}:{python_path}" if python_path else current_dir
        os.environ["PYTHONPATH"] = python_path

    for key, value in SparkConfig.ENVIRONMENT_VARIABLES.items():
        os.environ[key] = value