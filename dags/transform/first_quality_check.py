import configparser
from datetime import datetime
import os
import datetime
import pandas as pd
from pyspark import SparkConf
from pyspark.context import SparkContext
import pyspark.sql.functions as f
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,split,expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,isnan,isnull, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ShortType,BooleanType, TimestampType

spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()

findspark.init()
sc=spark.sparkContext
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", "awsAccessKeyId")
hadoop_conf.set("fs.s3n.awsSecretAccessKey","awsSecretAccessKey")

spark.sparkContext.setLogLevel('WARN')
Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")

def check(path, table):
    df = spark.read.parquet(path)
    if len(df.columns) > 0 and df.count() > 0:
        mylogger.warn("{} SUCCESS".format(table))
    else:
        mylogger.warn("{} FAIL".format(table))


check("s3n://psp-capstone/lake/checkin", "checkin/")
check("s3n://psp-capstone/lake/user", "user/")
check("s3n://psp-capstone/lake/tip", "tip/")
