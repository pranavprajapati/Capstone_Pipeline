import configparser
from datetime import datetime
import os
import boto3
import datetime
import pandas as pd
from pyspark import SparkConf
from pyspark.context import SparkContext
import pyspark.sql.functions as f
import findspark

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
hadoop_conf.set("fs.s3n.awsAccessKeyId", "AKIA2RG2B4ULSRDQY6LW")
hadoop_conf.set("fs.s3n.awsSecretAccessKey","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")
business = spark.read.format('csv').load('s3://psp-capstone/raw/yelp_academic_dataset_business.csv', header=True)

bsplit = split(business['categories'], ',')
business = business.withColumn('category_1', bsplit.getItem(0))
business = business.withColumn('category_2', bsplit.getItem(1))
business = business.withColumn('category_3', bsplit.getItem(2))

business = business.withColumnRenamed('business_id', 'b_id')
business = business.withColumnRenamed('stars', 'star')

business.write.mode("overwrite").parquet("s3://psp-capstone/lake/business/")

# EDA to try out. Open a jupter session and try the following

# Highest stars
rating = business.select('stars')
group_rating = rating.groupby('stars').count()
rating_top = group_rating.sort('count',ascending=False)
#rating_top.show(truncate=False)

# Count open and closed businesses
is_count = business.groupBy('is_open').count().orderBy('count').show()

category = business.select('categories')
individual_category = category.select(explode(split('categories', ',')).alias('category'))
grouped_category = individual_category.groupby('category').count()
top_category = grouped_category.sort('count',ascending=False)
# top_category.show(10,truncate=False)

# business.select([count(when(isnan(c), c)).alias(c) for c in business.columns]).show()
