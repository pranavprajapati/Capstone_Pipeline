from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
spark.sparkContext.setLogLevel('WARN')

Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")


def check(path, table):
    df = spark.read.parquet(path)
    if len(df.columns) > 0 and df.count() > 0:
        mylogger.warn("{} SUCCESS".format(table))
    else:
        mylogger.warn("{} FAIL".format(table))



check("s3a://psp-capstone/lake/", "business")
check("s3a://psp-capstone/lake/", "review")
check("s3a://psp-capstone/lake/", "checkin")
check("s3a://psp-capstone/lake/", "user")
check("s3a://psp-capstone/lake/", "tip")
