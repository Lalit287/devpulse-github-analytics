import os
import sys

from pyspark.sql import SparkSession
from config.settings import ROOT
from exploration.spark_demo import configure_java


def create_analytics_spark(master="local[4]", driver_memory="4g", partitions=64):
    configure_java()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    temporary = ROOT / ".runtime/spark-week4"
    temporary.mkdir(parents=True, exist_ok=True)
    session = (SparkSession.builder.appName("DevPulse-Week4-Analytics").master(master)
               .config("spark.driver.memory", driver_memory)
               .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.local.dir", str(temporary)).config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", str(partitions))
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.inMemoryColumnarStorage.batchSize", "1000")
               .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
               .config("spark.hadoop.fs.defaultFS", "file:///")
               .config("spark.ui.enabled", "false").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    return session
