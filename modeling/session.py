"""Local Spark settings for the bounded historical model experiment."""
import os
import sys
from pyspark.sql import SparkSession
from config.settings import ROOT
from exploration.spark_demo import configure_java


def create_spark():
    configure_java();os.environ.setdefault('SPARK_LOCAL_IP','127.0.0.1');os.environ['PYSPARK_PYTHON']=sys.executable
    temporary=ROOT/'.runtime/spark-week6';temporary.mkdir(parents=True,exist_ok=True)
    spark=(SparkSession.builder.appName('DevPulse-Week6-Chronological-Prediction').master('local[4]')
           .config('spark.driver.memory','6g').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
           .config('spark.local.dir',str(temporary)).config('spark.sql.session.timeZone','UTC')
           .config('spark.sql.shuffle.partitions','64').config('spark.sql.adaptive.enabled','true')
           .config('spark.hadoop.fs.defaultFS','file:///').config('spark.ui.enabled','false')
           .config('spark.ui.showConsoleProgress','false')
           .config('spark.sql.parquet.outputTimestampType','TIMESTAMP_MICROS').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');return spark
