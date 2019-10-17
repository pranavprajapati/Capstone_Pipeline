
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
#from airflow.operators.custom_plugin import S3DataExistsOperator
#from airflow.operators import S3DataExistsOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 5),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

s3data = 's3://psp-capstone/raw/'
s3bucket = 'psp-capstone'
lookup_prefix = 'psp-capstone/raw/'

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('dag_cluster',schedule_interval='@once', default_args=default_args)



start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
# Check all files exist on S3
done = DummyOperator(task_id='Done',  dag=dag)

"""
check_business_s3  = S3DataExistsOperator(
    task_id='check_business_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_business.csv'
)

check_checkin_s3  = S3DataExistsOperator(
    task_id='check_checkin_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_checkin.csv'
)

check_review_s3  = S3DataExistsOperator(
    task_id='check_review_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_review.json'
)

check_tip_s3  = S3DataExistsOperator(
    task_id='check_tip_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_tip.csv'
)

check_user_s3  = S3DataExistsOperator(
    task_id='check_user_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_user.json'
)
"""
start_operator >> done
#start_operator >> check_business_s3
#start_operator >> check_checkin_s3
#start_operator >> check_review_s3
#start_operator >> check_tip_s3
#start_operator >> check_user_s3
