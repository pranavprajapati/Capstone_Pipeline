import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,split, explode, date_format
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,isnan,isnull, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ShortType,BooleanType, TimestampType

checkin = spark.read.format('csv').load('s3://psp-capstone/raw/yelp_academic_dataset_checkin.csv', header=True)

checkin = checkin.withColumn("date_only",expr("to_date(date, 'yyyy-MM-dd')"))
checkin = checkin.select('date','date_only','business_id', date_format('date_only', 'u').alias('day_number'), date_format('dates', 'E').alias('day'))

checkin.createOrReplaceTempView("countd")
checkin = spark.sql("""select business_id,dates,date_only,day_number,
                    date,
                    size(split(date, ",")) as checkin_count
              from countd""")

checkin.write.mode("overwrite").parquet("s3://psp-capstone/lake/checkin.csv/")
