import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,split, explode
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,isnan,isnull, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ShortType,BooleanType, TimestampType

user = spark.read.format('csv').load('s3://psp-capstone/raw/yelp_academic_dataset_user.json')


user.write.mode("overwrite").parquet("s3://psp-capstone/lake/user/")
