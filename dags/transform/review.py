import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,split, explode
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,isnan,isnull, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ShortType,BooleanType, TimestampType

review = spark.read.format('csv').load('s3://psp-capstone/raw/yelp_academic_dataset_review.json')

review = review.join(business, review.business_id == business.b_id)
columns_to_drop = ['b_id','address', 'categories','city','hours','is_open','latitude','longitude','postal_code','review_count','star','state','category_1','category_2','category_3']
review = review.drop(*columns_to_drop)

# Some EDA

review_star_three = review.filter('stars >3')
grouped_review = review_star_three.groupby('name').count()
review_sort = grouped_review.sort('count',ascending=False)
#review_sort.show(10)

review.write.mode("overwrite").parquet("s3://psp-capstone/lake/review/")
