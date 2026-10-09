"""Bounded local Spark configuration, leaving the Week 1 session unchanged."""
import os
import sys

from config.settings import ROOT
from exploration.spark_demo import configure_java


def create_etl_spark(*, master="local[4]", driver_memory="6g", shuffle_partitions=192):
    configure_java()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession
    temporary = ROOT / ".runtime/spark-week3"
    temporary.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName("DevPulse-Week3-ETL").master(master)
             .config("spark.driver.memory", driver_memory)
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.local.dir", str(temporary))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
             .config("spark.sql.inMemoryColumnarStorage.batchSize", "1000")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
