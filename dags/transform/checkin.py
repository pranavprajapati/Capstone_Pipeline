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
from pyspark.sql.functions import udf, col,split, expr
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

checkin = spark.read.format('csv').load('s3n://psp-capstone/psp-capstone/raw/yelp_academic_dataset_checkin.csv', header=True)

checkin = checkin.withColumn("date_only",expr("to_date(date, 'yyyy-MM-dd')"))
checkin = checkin.select('date','date_only','business_id', date_format('date_only', 'u').alias('day_number'), date_format('date_only', 'E').alias('day'))

checkin.createOrReplaceTempView("countd")
checkin = spark.sql("""select business_id,date,date_only,day_number,
                    day,
                    size(split(date, ",")) as checkin_count
              from countd""")

checkin.write.mode("overwrite").parquet("s3n://psp-capstone/lake/checkin/")
