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

review = spark.read.format('json').load('s3n://psp-capstone/psp-capstone/raw/yelp_academic_dataset_review.json')
business = spark.read.format('csv').load('s3n://psp-capstone/psp-capstone/raw/yelp_academic_dataset_business.csv', header=True)

business = business.drop('attributes')
bsplit = split(business['categories'], ',')
business = business.withColumn('category_1', bsplit.getItem(0))
business = business.withColumn('category_2', bsplit.getItem(1))
business = business.withColumn('category_3', bsplit.getItem(2))
business = business.withColumnRenamed('business_id', 'b_id')
business = business.withColumnRenamed('stars', 'star')

review = review.join(business, review.business_id == business.b_id)
columns_to_drop = ['b_id','address', 'categories','city','hours','is_open','latitude','longitude','postal_code','review_count','star','state','category_1','category_2','category_3']
review = review.drop(*columns_to_drop)

# Some EDA

review_star_three = review.filter('stars >3')
grouped_review = review_star_three.groupby('name').count()
review_sort = grouped_review.sort('count',ascending=False)
#review_sort.show(10)

review.write.mode("overwrite").parquet("s3n://psp-capstone/lake/review/")
