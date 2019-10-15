import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,split, explode
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,isnan,isnull, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ShortType,BooleanType, TimestampType

business = spark.read.format('csv').load('s3://psp-capstone/raw/yelp_academic_dataset_business.csv', header=True)

bsplit = split(business['categories'], ',')
business = business.withColumn('category_1', bsplit.getItem(0))
business = business.withColumn('category_2', bsplit.getItem(1))
business = business.withColumn('category_3', bsplit.getItem(2))

business_rest = business.filter(((f.col('category_1') == 'Restaurants') | \
         (f.col('category_2')== 'Restaurants') | (f.col('category_3')== 'Restaurants')))

business_rest = business_rest.withColumnRenamed('business_id', 'b_id')
business_rest = business_rest.withColumnRenamed('stars', 'star')

business_rest.write.mode("overwrite").parquet("s3://psp-capstone/lake/business_rest/")
